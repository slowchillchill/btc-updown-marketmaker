#!/usr/bin/env bash
# #181现公共设备列入口；复用已验收首分钟compact，不重新prepare源数据。
set -euo pipefail
artifact_dir=${1:?传入数据盘新证据目录}
material=${2:?传入已完成公共compact材料}
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
case "$repo_root:$artifact_dir:$material" in
    /root/autodl-tmp/*:/root/autodl-tmp/*:/root/autodl-tmp/*) ;;
    *) echo 'B执行/材料/证据必须位于AutoDL数据盘' >&2; exit 2 ;;
esac
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact_dir"
git rev-parse HEAD > "$artifact_dir/source-commit.txt"
git status --porcelain > "$artifact_dir/status.txt"
export PYTHONPATH="$repo_root/model-train/src:$repo_root/data-pipeline/gpu"
command=(cargo build -p data-pipeline-training --bin dpt-public-compact --locked --offline)
printf '%q ' "${command[@]}" >> "$artifact_dir/commands.txt"
printf '\n' >> "$artifact_dir/commands.txt"
"${command[@]}" > "$artifact_dir/build.log" 2>&1
command=("$repo_root/model-train/.venv/bin/python" model-train/scripts/gpu_public_state_validation.py
    --binary "$CARGO_TARGET_DIR/debug/dpt-public-compact" --material "$material"
    --device-page "$artifact_dir/public-page" --output "$artifact_dir/report.json")
printf '%q ' "${command[@]}" >> "$artifact_dir/commands.txt"
printf '\n' >> "$artifact_dir/commands.txt"
"${command[@]}" > "$artifact_dir/tests.log" 2>&1
tail -n 1 "$artifact_dir/tests.log"
