#!/usr/bin/env bash
# 同一GTC原工程账户的单笔Base Query/显式关联；不启动正式训练。
set -euo pipefail
artifact=${1:?传入AutoDL新证据目录}
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
for location in "$artifact" "$repo_root"; do
    case "$location" in /root/autodl-tmp/*) ;; *) exit 2 ;; esac
done
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact"
git rev-parse HEAD > "$artifact/source-commit.txt"
git status --porcelain > "$artifact/status.txt"
export B_GPU_PYTHON="$repo_root/model-train/.venv/bin/python"
export B_GPU_ORDER_VALIDATOR="$repo_root/sim-system/gpu/validate_base_orders.py"
export B_GPU_ORDER_ARTIFACT_DIR="$artifact"
export B_GPU_SIM_ORDER_QUERY=1
export PYTHONPATH="$repo_root/model-train:$repo_root/sim-system/gpu:$repo_root/data-pipeline/gpu"
command=(cargo test -p sim-system --features shared-execution --lib --locked --offline -j24
    gpu_legal_gtc_request_taker_maker_cancel_matches_original_execution -- --ignored --nocapture)
printf '%q ' "${command[@]}" > "$artifact/commands.txt"
printf '\n' >> "$artifact/commands.txt"
echo "stage=single_base_order_query commit=$(git rev-parse HEAD) log=$artifact/tests.log started=$(date -u +%FT%TZ)"
code=0
"${command[@]}" > "$artifact/tests.log" 2>&1 || code=$?
printf '%s\n' "$code" > "$artifact/validation.exit"
tail -n 4 "$artifact/tests.log"
exit "$code"
