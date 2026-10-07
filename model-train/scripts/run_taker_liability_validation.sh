#!/usr/bin/env bash
# 仅AutoDL；有限公共V2非零负债，不是自然SIM producer或正式训练。
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
export B_GPU_TAKER_LIABILITY_VALIDATOR="$repo_root/ledger/gpu/validate_taker_liability.py"
command=(cargo test -p ledger --test memory_v2 --locked --offline
    gpu_public_v2_signed_taker_liability_matches_original_ledger -- --ignored --exact --nocapture)
printf '%q ' "${command[@]}" > "$artifact_dir/commands.txt"
printf '\n' >> "$artifact_dir/commands.txt"
echo "stage=public_v2_taker_liability commit=$(git rev-parse HEAD)"
status=0
"${command[@]}" > "$artifact_dir/validation.log" 2>&1 || status=$?
printf '%s\n' "$status" > "$artifact_dir/validation.exit"
tail -n 6 "$artifact_dir/validation.log"
exit "$status"
