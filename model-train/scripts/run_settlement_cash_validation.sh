#!/usr/bin/env bash
# AutoDL有限原NOE费用切片；非正式训练、非连续GPU全根/恢复。
set -euo pipefail
artifact_dir=${1:?传入数据盘新证据目录}
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
case "$repo_root:$artifact_dir" in /root/autodl-tmp/*:/root/autodl-tmp/*) ;; *) exit 2 ;; esac
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact_dir"
git rev-parse HEAD > "$artifact_dir/source-commit.txt"
git status --porcelain > "$artifact_dir/status.txt"
export B_GPU_PYTHON="$repo_root/model-train/.venv/bin/python"
export B_GPU_ORDER_ARTIFACT_DIR="$artifact_dir"
export B_GPU_SETTLEMENT_CASH_VALIDATOR="$repo_root/ledger/gpu/validate_settlement_cash.py"
status=0
for scene in actual_chain_requested_settlement_s0_to_s4_and_stable_ledger \
    resolution_before_late_success_fixes_fee_bucket_and_updates_pi_without_frames \
    original_external_cash_identity_correction_and_no_frame_nav; do
    command=(cargo test -p shared-run --test economic_settlement --locked --offline "$scene" -- --exact --nocapture)
    printf '%q ' "${command[@]}" >> "$artifact_dir/commands.txt"
    printf '\n' >> "$artifact_dir/commands.txt"
    echo "stage=$scene commit=$(git rev-parse HEAD)"
    exit_code=0
    "${command[@]}" > "$artifact_dir/$scene.log" 2>&1 || exit_code=$?
    printf '%s\n' "$exit_code" > "$artifact_dir/$scene.exit"
    tail -n 6 "$artifact_dir/$scene.log"
    if ((exit_code)); then status=$exit_code; fi
done
printf '%s\n' "$status" > "$artifact_dir/validation.exit"
exit "$status"
