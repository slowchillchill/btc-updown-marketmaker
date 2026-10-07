#!/usr/bin/env bash
# 只生成#181新增学习依赖的精确锁；不安装，不执行模型或训练。
set -euo pipefail
artifact_dir=${1:?传入数据盘的新证据目录}
progress_seconds=${2:-10}
[[ "$progress_seconds" =~ ^[0-9]+$ ]] && ((progress_seconds > 0)) || { echo '进度间隔必须为正整数秒' >&2; exit 2; }
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
case "$repo_root:$artifact_dir" in /root/autodl-tmp/*:/root/autodl-tmp/*) ;; *) echo '依赖求解必须在AutoDL数据盘' >&2; exit 2 ;; esac
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact_dir"
git rev-parse HEAD > "$artifact_dir/source-commit.txt"
git status --porcelain > "$artifact_dir/status.txt"
run_stage() {
    local stage=$1
    shift
    printf '%q ' "$@" >> "$artifact_dir/commands.txt"
    printf '\n' >> "$artifact_dir/commands.txt"
    local started=$SECONDS
    "$@" > "$artifact_dir/$stage.log" 2>&1 &
    local job=$!
    while kill -0 "$job" 2>/dev/null; do
        echo "stage=$stage elapsed_seconds=$((SECONDS-started)) total_work=unknown log=$artifact_dir/$stage.log"
        sleep "$progress_seconds"
    done
    wait "$job"
    tail -5 "$artifact_dir/$stage.log"
}
# 普通uv lock保留原已有精确版本；仅加入新传递依赖，不使用--upgrade。
run_stage cpu-lock uv lock --project model-train --no-python-downloads --default-index https://pypi.org/simple
run_stage cu128-lock "$repo_root/model-train/.venv/bin/python" model-train/scripts/autodl_environment.py lock \
    --uv "$(command -v uv)" --cache "$artifact_dir/resolver"
git diff --stat > "$artifact_dir/lock-diff.txt"
echo "stage=generated source_commit=$(git rev-parse HEAD) locks_are_generated_changes=true"
