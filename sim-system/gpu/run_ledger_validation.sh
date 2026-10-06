#!/usr/bin/env bash
# B路线已授权的有限纵向工程验证；只在AutoDL数据盘运行。
set -euo pipefail
case_name=${1:?传入 trade-read、maker-read 或 failed-fill}
artifact_dir=${2:?传入数据盘上的新证据目录}
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
case "$repo_root" in /root/autodl-tmp/*) ;; *) echo 'B计算必须在AutoDL数据盘' >&2; exit 2 ;; esac
case "$artifact_dir" in /root/autodl-tmp/*) ;; *) echo '证据目录必须在数据盘' >&2; exit 2 ;; esac
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact_dir"
git rev-parse HEAD > "$artifact_dir/source-commit.txt"
git status --porcelain > "$artifact_dir/status.txt"
export B_GPU_PYTHON="$repo_root/model-train/.venv/bin/python"
export B_GPU_MULTIASK_VALIDATOR="$repo_root/sim-system/gpu/validate_multiask.py"
export B_GPU_OUTER_MAKER_VALIDATOR="$repo_root/sim-system/gpu/validate_outer_maker_day.py"
export B_GPU_POSITION_VALIDATOR="$repo_root/sim-system/gpu/validate_position_cost.py"
export B_GPU_ORDER_ARTIFACT_DIR="$artifact_dir"
export PYTHONPATH="$repo_root/model-train/src:$repo_root/sim-system/gpu"
run_test() {
    local filter=$1 log=$2
    local -a command=(cargo test -p sim-system --features shared-execution --lib --locked --offline
        "$filter" -- --ignored --nocapture)
    printf '%q ' "${command[@]}" >> "$artifact_dir/commands.txt"
    printf '\n' >> "$artifact_dir/commands.txt"
    echo "stage=$filter commit=$(git rev-parse HEAD) log=$artifact_dir/$log"
    "${command[@]}" > "$artifact_dir/$log" 2>&1
    tail -4 "$artifact_dir/$log"
}
case "$case_name" in
    trade-read)
        run_test gpu_legal_multiask tests.log
        run_test gpu_actual_maker_mature_day_preserves_noe_batch_and_continuous_ledger maker-day.log
        ;;
    maker-read)
        run_test gpu_actual_maker_mature_day_preserves_noe_batch_and_continuous_ledger maker-day.log
        ;;
    failed-fill)
        run_test gpu_real_failed_fill_reverses_original_applied_cost_and_fee_once tests.log
        ;;
    *) echo "未知有限入口: $case_name" >&2; exit 2 ;;
esac
