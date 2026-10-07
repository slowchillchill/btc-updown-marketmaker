#!/usr/bin/env bash
# 原Ledger stage3有限Query消费者；不启动SIM热宿主或正式训练。
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
export B_GPU_ORDER_QUERY_VALIDATOR="$repo_root/ledger/gpu/validate_order_query.py"
export B_GPU_ORDER_QUERY_ARTIFACT_ROOT="$artifact"
export PYTHONPATH="$repo_root/model-train:$repo_root/data-pipeline/gpu"
status=0
for scene in registered_query_found_converts_while_not_found_and_out_of_retention_retain \
    same_reservation_is_consumed_by_only_one_definitive_result; do
    printf '%s\n' "cargo test -p ledger --test stage3_economic_lifecycle --locked --offline -j24 $scene -- --exact --nocapture" >> "$artifact/commands.txt"
    echo "stage=$scene commit=$(git rev-parse HEAD) started=$(date -u +%FT%TZ)"
    code=0
    cargo test -p ledger --test stage3_economic_lifecycle --locked --offline -j24 "$scene" -- --exact --nocapture \
        > "$artifact/$scene.log" 2>&1 || code=$?
    printf '%s\n' "$code" > "$artifact/$scene.exit"
    tail -n 4 "$artifact/$scene.log"
    if test "$code" != 0; then status=$code; fi
done
printf '%s\n' "$status" > "$artifact/validation.exit"
exit "$status"
