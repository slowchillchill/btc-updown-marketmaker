#!/usr/bin/env bash
# 普通Query的实际V3公开引用与Registry组件；SIM发送/完整人口另行接线。
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
export B_GPU_QUERY_REGISTRY_VALIDATOR="$repo_root/shared-run/gpu/validate_query_registry.py"
export B_GPU_QUERY_REGISTRY_ARTIFACT="$artifact/query-registry"
export PYTHONPATH="$repo_root/model-train:$repo_root/data-pipeline/gpu"
scene=real_terminal_query_then_late_open_precede_lifecycle
printf '%s\n' "cargo test -p shared-run --test continuous --locked --offline -j24 $scene -- --exact --nocapture" > "$artifact/commands.txt"
echo "stage=$scene commit=$(git rev-parse HEAD) started=$(date -u +%FT%TZ)"
code=0
cargo test -p shared-run --test continuous --locked --offline -j24 "$scene" -- --exact --nocapture \
    > "$artifact/$scene.log" 2>&1 || code=$?
printf '%s\n' "$code" > "$artifact/$scene.exit"
tail -n 4 "$artifact/$scene.log"
printf '%s\n' "$code" > "$artifact/validation.exit"
exit "$code"
