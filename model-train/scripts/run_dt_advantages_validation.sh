#!/usr/bin/env bash
# 参数化实际dt数学的有限工程验证，不执行训练更新。
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
export PYTHONPATH="$repo_root/model-train"
env | sort > "$artifact/environment.txt"
command=("$repo_root/model-train/.venv/bin/python"
    model-train/scripts/gpu_dt_advantages_validation.py --output "$artifact/report.json")
printf '%q ' "${command[@]}" > "$artifact/commands.txt"
printf '\n' >> "$artifact/commands.txt"
echo "stage=actual_dt_GAE commit=$(git rev-parse HEAD) started=$(date -u +%FT%TZ)"
code=0
"${command[@]}" > "$artifact/GPU.stdout" 2> "$artifact/GPU.stderr" || code=$?
printf '%s\n' "$code" > "$artifact/validation.exit"
cat "$artifact/GPU.stdout"
exit "$code"
