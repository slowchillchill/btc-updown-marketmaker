#!/usr/bin/env bash
# 共享BookEncoder影响范围：仅AutoDL、既有锁环境、有限CPU/ONNX与实际GPU验证。
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
export PYTHONPATH="$repo_root/model-train:$repo_root/model-train/tests"
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
run GPU "$repo_root/model-train/.venv/bin/python" model-train/scripts/gpu_book_validation.py --output "$artifact_dir/GPU.json"
run shared-CPU-ONNX "$repo_root/model-train/.venv/bin/python" -m unittest \
    test_book.BookTests.test_compact_values_and_gradients_match_original_dense_coordinates \
    test_book.BookTests.test_independent_padding_keeps_outputs_gradients_and_recurrent_state \
    test_book.BookTests.test_negative_max_first_tie_padding_empty_and_cancellation \
    test_book.BookTests.test_compact_standard_onnx_dynamic_and_empty_groups \
    test_book.BookTests.test_standard_onnx_loop_matches_dynamic_signed_reductions
