#!/usr/bin/env bash
# #181有限Native冻结验证；原core合法场景与已完成首分钟材料，不是正式训练。
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
export B_GPU_PYTHON="$repo_root/model-train/.venv/bin/python"
export B_GPU_COLUMNS_BINARY="$CARGO_TARGET_DIR/debug/dpt-public-compact"
export B_GPU_STATE_VALIDATOR="$repo_root/model-train/scripts/gpu_public_state_validation.py"
export B_GPU_RULE_CONTEXT_BINARY="$CARGO_TARGET_DIR/debug/sim-gpu-rule-context"
export PYTHONPATH="$repo_root/model-train:$repo_root/data-pipeline/gpu"
run() {
    local log_name=$1
    shift
    printf '%q ' "$@" >> "$artifact_dir/commands.txt"
    printf '\n' >> "$artifact_dir/commands.txt"
    echo "stage=$log_name commit=$(git rev-parse HEAD)"
    local status=0
    "$@" > "$artifact_dir/$log_name.log" 2>&1 || status=$?
    printf '%s\n' "$status" > "$artifact_dir/$log_name.exit"
    tail -n 4 "$artifact_dir/$log_name.log"
    return "$status"
}
run build-public cargo build -p data-pipeline-training --bin dpt-public-compact --locked --offline -j 24
run build-rules cargo build -p sim-system --bin sim-gpu-rule-context --locked --offline -j 24
# 单个既有测试二进制依次执行两个原构造场景，避免两个CUDA验证并发争用。
run native-freeze cargo test -p data-pipeline-training --lib --locked --offline -j 24 gpu_public_ -- --ignored --nocapture --test-threads=1
# 原首分钟只走已有compact读面；无需为容量接口重建已合格设备页。
run public-minute "$B_GPU_PYTHON" "$B_GPU_STATE_VALIDATOR" --binary "$B_GPU_COLUMNS_BINARY" --material "$material" --output "$artifact_dir/public-minute.json"
