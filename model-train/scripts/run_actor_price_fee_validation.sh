#!/usr/bin/env bash
# 原FOK候选市场估费→同前缀价格头；全部CPU/GPU仅AutoDL。
set -euo pipefail
artifact=${1:?传入AutoDL新证据目录}
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
for location in "$artifact" "$repo_root"; do
    case "$location" in /root/autodl-tmp/*) ;; *) exit 2 ;; esac
done
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact";mkdir "$artifact/sources"
git rev-parse HEAD > "$artifact/source-commit.txt"
git status --porcelain > "$artifact/status.txt"
export PYTHONPATH="$repo_root/model-train"
export B_GPU_ACTOR_CANCEL_REFERENCE="$artifact/sources"
export B_GPU_COLUMNS_BINARY="$CARGO_TARGET_DIR/debug/dpt-public-compact"
env | sort > "$artifact/environment.txt"
finish() { code=$?;printf '%s\n' "$code" > "$artifact/validation.exit";nvidia-smi > "$artifact/resources-end.txt" 2>&1 || true;exit "$code"; }
trap finish EXIT
nvidia-smi > "$artifact/resources-start.txt"
run() {
    local stage=$1 code=0;shift
    printf '%q ' "$@" > "$artifact/$stage.command";printf '\n' >> "$artifact/$stage.command"
    echo "stage=$stage commit=$(git rev-parse HEAD) started=$(date -u +%FT%TZ)"
    "$@" > "$artifact/$stage.log" 2>&1 || code=$?
    printf '%s\n' "$code" > "$artifact/$stage.exit";tail -n 4 "$artifact/$stage.log";return "$code"
}
run syntax "$repo_root/model-train/.venv/bin/python" -m py_compile \
    model-train/native-inputs/gpu/actor_price_fee.py model-train/native-inputs/gpu/actor_support.py \
    model-train/scripts/gpu_actor_price_fee_validation.py
run build-native cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --no-run --locked --offline -j24
run build-public cargo build -p data-pipeline-training --bin dpt-public-compact --locked --offline -j24
run buy-original cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline -j24 \
    action_tests::buy_tests::export_gpu_exact_buy_reference_v1 -- --exact --ignored --nocapture & buy_job=$!
run accepted-original cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline -j24 \
    action_tests::buy_tests::accepted_only_zero_gap_keeps_original_actor_support -- --exact --nocapture & accepted_job=$!
code=0;wait "$buy_job" || code=$?;wait "$accepted_job" || code=$?;if (( code!=0 )); then exit "$code";fi
run extract "$repo_root/model-train/.venv/bin/python" - "$artifact" <<'PY'
import json,sys
from pathlib import Path
root=Path(sys.argv[1])
line,=[line.removeprefix('BTC_GPU_EXACT_REFERENCE_V1=') for line in (root/'buy-original.log').read_text().splitlines()
       if line.startswith('BTC_GPU_EXACT_REFERENCE_V1=')]
(root/'sources/buy.json').write_text(json.dumps(json.loads(line),indent=2)+'\n')
PY
run price-fee "$repo_root/model-train/.venv/bin/python" model-train/scripts/gpu_actor_price_fee_validation.py \
    --sources "$artifact/sources" --output "$artifact/report.json"
