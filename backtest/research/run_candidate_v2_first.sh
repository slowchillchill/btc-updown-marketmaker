#!/usr/bin/env bash
# #315 待签发首包；仅在用户批准准确范围和预算后调用。
set -uo pipefail
if (( $# != 1 )); then
  printf 'usage: %s JOB_ROOT\n' "$0" >&2
  exit 2
fi
JOB_ROOT=$(realpath -- "$1") || exit $?
cd -- "$JOB_ROOT" || exit $?
# 作业材料先准备并冻结；本入口只创建本轮独立输出，不复用旧结果。
mkdir outputs || exit $?
unit=issue315-candidate-v2-first-20261017
systemd-run --user --wait --expand-environment=no \
  --unit="$unit" --property=Type=exec \
  --property=RuntimeMaxSec=1800 --property=KillMode=control-group \
  --property=KillSignal=SIGKILL --property=CPUAccounting=yes \
  --property=MemoryAccounting=yes --working-directory="$JOB_ROOT" \
  /usr/bin/bash -c '
set -uo pipefail
mkdir -p outputs/2026-09-24/center || exit $?
env -u RAYON_NUM_THREADS BACKTEST_TRANSPORT_DIAGNOSTICS=0 \
  /usr/bin/time -v -o cli-resource-usage.txt \
  ./bin/backtest-cli --mode day --config configs/research-days.json \
  --progress-interval-seconds 60 --research-summary \
  --wall-budget-seconds 1800 --summary-reserve-seconds 300 \
  > research-output.json 2> progress.log
cli_status=$?
/usr/bin/time -v -o summary-resource-usage.txt \
  python3 ./bin/summarize_research.py \
  configs/research-days.json research-output.json summary.json \
  > summary-output.log 2> summary-error.log
summary_status=$?
/usr/bin/du -sb outputs research-output.json summary.json \
  > output-byte-counts.txt 2> output-byte-errors.log
if (( cli_status != 0 )); then exit "$cli_status"; fi
exit "$summary_status"
' > service-result.log 2>&1
service_status=$?
# 保留unit直至读出硬停时仍可用的systemd计量；此处只读取固定unit，未追加计算。
systemctl --user show "$unit.service" \
  -p Result -p ExecMainCode -p ExecMainStatus -p ActiveState -p SubState \
  -p ExecMainStartTimestamp -p ExecMainExitTimestamp -p CPUUsageNSec \
  -p MemoryPeak -p MemorySwapPeak > service-properties.txt
journalctl --user --unit="$unit.service" --no-pager > service-journal.txt
printf '%s\n' "$service_status" > service-exit-code.txt
exit "$service_status"
