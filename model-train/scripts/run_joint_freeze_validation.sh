#!/usr/bin/env bash
# 原同账户1500/1600联合冻结及两步全部层GRU；不是完整根或正式训练。
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
export B_GPU_COLUMNS_BINARY="$CARGO_TARGET_DIR/debug/dpt-public-compact"
export B_GPU_RULE_CONTEXT_BINARY="$CARGO_TARGET_DIR/debug/sim-gpu-rule-context"
export B_GPU_JOINT_FREEZE=1
run() {
    local stage=$1 code=0
    shift
    printf '%q ' "$@" >> "$artifact/commands.txt"
    printf '\n' >> "$artifact/commands.txt"
    echo "stage=$stage commit=$(git rev-parse HEAD) started=$(date -u +%FT%TZ)"
    "$@" > "$artifact/$stage.log" 2>&1 || code=$?
    printf '%s\n' "$code" > "$artifact/$stage.exit"
    tail -n 4 "$artifact/$stage.log"
    return "$code"
}
run build-public cargo build -p data-pipeline-training --bin dpt-public-compact --locked --offline -j24
run build-rules cargo build -p sim-system --bin sim-gpu-rule-context --locked --offline -j24
code=0
run joint-freeze bash model-train/scripts/run_sim_order_query_validation.sh "$artifact/scenario" || code=$?
printf '%s\n' "$code" > "$artifact/validation.exit"
exit "$code"
