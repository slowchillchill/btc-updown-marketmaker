#!/usr/bin/env bash
# 参数化奖励组件，复用已通过的原Native场景持久来源；不启动奖励采集或训练。
set -euo pipefail
artifact=${1:?传入AutoDL新证据目录}
original_sources=${2:?传入原NAV两场景的准确持久证据根目录}
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
for location in "$artifact" "$repo_root" "$original_sources"; do
    case "$location" in /root/autodl-tmp/*) ;; *) exit 2 ;; esac
done
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact"
git rev-parse HEAD > "$artifact/source-commit.txt"
git status --porcelain > "$artifact/status.txt"
cp "$original_sources/source-commit.txt" "$artifact/original-source-commit.txt"
export PYTHONPATH="$repo_root/model-train"
env | sort > "$artifact/environment.txt"
command=("$repo_root/model-train/.venv/bin/python" model-train/scripts/gpu_interval_reward_validation.py
    --source-root "$original_sources" --output "$artifact/report.json")
printf '%q ' "${command[@]}" > "$artifact/commands.txt"
printf '\n' >> "$artifact/commands.txt"
echo "stage=exact_interval_reward commit=$(git rev-parse HEAD) started=$(date -u +%FT%TZ)"
code=0
"${command[@]}" > "$artifact/GPU.stdout" 2> "$artifact/GPU.stderr" || code=$?
printf '%s\n' "$code" > "$artifact/validation.exit"
tail -n 5 "$artifact/GPU.stdout"
exit "$code"
