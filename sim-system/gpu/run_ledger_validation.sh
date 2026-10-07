#!/usr/bin/env bash
# B路线已授权的有限纵向工程验证；只在AutoDL数据盘运行。
set -euo pipefail
case_name=${1:?传入 trade-read、trade-records、maker-read、registry-reference、base-cancel 或 failed-fill}
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
export B_GPU_ORDER_VALIDATOR="$repo_root/sim-system/gpu/validate_base_execution.py"
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
    hidden-asset)
        export B_GPU_SORT_WORKER="$repo_root/data-pipeline/gpu/sort_worker.py"
        export B_GPU_HIDDEN_ASSET_VALIDATOR="$repo_root/sim-system/gpu/validate_hidden.py"
        export B_GPU_HIDDEN_CLI="$CARGO_TARGET_DIR/debug/dpt-compact-hidden"
        command=(cargo build -p data-pipeline-training --bin dpt-compact-hidden --locked --offline)
        printf '%q ' "${command[@]}" >> "$artifact_dir/commands.txt"
        printf '\n' >> "$artifact_dir/commands.txt"
        "${command[@]}" > "$artifact_dir/build.log" 2>&1
        run_test gpu_compact_hidden_drives_exact_asset_state_and_queues tests.log
        ;;
    base-hidden)
        export B_GPU_ORDER_VALIDATOR="$repo_root/sim-system/gpu/validate_base_orders.py"
        export B_GPU_COMPACT_BASE_SOURCE=1
        export B_GPU_SORT_WORKER="$repo_root/data-pipeline/gpu/sort_worker.py"
        export B_GPU_HIDDEN_CLI="$CARGO_TARGET_DIR/debug/dpt-compact-hidden"
        command=(cargo build -p data-pipeline-training --bin dpt-compact-hidden --locked --offline)
        printf '%q ' "${command[@]}" >> "$artifact_dir/commands.txt"
        printf '\n' >> "$artifact_dir/commands.txt"
        "${command[@]}" > "$artifact_dir/build.log" 2>&1
        run_test gpu_legal_gtc_request_taker_maker_cancel_matches_original_execution tests.log
        ;;
    base-directory)
        export B_GPU_ORDER_VALIDATOR="$repo_root/sim-system/gpu/validate_base_orders.py"
        run_test gpu_legal_gtc_request_taker_maker_cancel_matches_original_execution tests.log
        ;;
    mutation-requests)
        export B_GPU_MUTATION_VALIDATOR="$repo_root/sim-system/gpu/validate_mutation_requests.py"
        run_test gpu_mutation_registration_preserves_original_pending_and_first_receipt tests.log
        ;;
    latency)
        export B_GPU_LATENCY_VALIDATOR="$repo_root/sim-system/gpu/validate_latency.py"
        run_test gpu_latency_matches_original_sampler_and_full_u64_feedback_identity tests.log
        ;;
    native-success-pool)
        export B_GPU_SUCCESS_POOL_VALIDATOR="$repo_root/model-train/scripts/gpu_success_pool_validation.py"
        command=(cargo test -p shared-run --test economic_settlement --locked --offline
            real_partial_makers_fix_average_cost_and_nonterminating_fraction -- --nocapture)
        printf '%q ' "${command[@]}" > "$artifact_dir/commands.txt"
        printf '\n' >> "$artifact_dir/commands.txt"
        echo "stage=original_two_leg_success_population_to_Native commit=$(git rev-parse HEAD)"
        "${command[@]}" > "$artifact_dir/success-pool.log" 2>&1
        tail -4 "$artifact_dir/success-pool.log"
        ;;
    native-account-reference)
        export NATIVE_ACCOUNT_SOURCE_OUTPUT="$artifact_dir/original-native-account.json"
        command=(cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline
            real_public_chain_freezes_cash_encodes_original_legs_and_cold_root -- --nocapture)
        printf '%q ' "${command[@]}" > "$artifact_dir/commands.txt"
        printf '\n' >> "$artifact_dir/commands.txt"
        echo "stage=original_NativeInputRoot_consume_and_encode commit=$(git rev-parse HEAD)"
        "${command[@]}" > "$artifact_dir/native-reference.log" 2>&1
        tail -4 "$artifact_dir/native-reference.log"
        command=("$B_GPU_PYTHON" "$repo_root/model-train/native-inputs/gpu/validate_original_account.py"
            "$artifact_dir/original-native-account.json")
        printf '%q ' "${command[@]}" >> "$artifact_dir/commands.txt"
        printf '\n' >> "$artifact_dir/commands.txt"
        "${command[@]}" > "$artifact_dir/native-device.log" 2>&1
        tail -1 "$artifact_dir/native-device.log"
        ;;
    trade-records)
        run_test gpu_legal_multiask tests.log
        ;;
    trade-read)
        run_test gpu_legal_multiask tests.log
        run_test gpu_actual_maker_mature_day_preserves_noe_batch_and_continuous_ledger maker-day.log
        ;;
    registry-reference)
        export B_GPU_REGISTRY_REFERENCE_DIR="$artifact_dir"
        command=(cargo test -p shared-run --test continuous --locked --offline
            v4_real_partial_cancel_and_zero_target_completion_preserve_economics -- --nocapture)
        printf '%q ' "${command[@]}" > "$artifact_dir/commands.txt"
        printf '\n' >> "$artifact_dir/commands.txt"
        echo "stage=original_SharedExecutionV2_ActionBeforeReadV2 commit=$(git rev-parse HEAD)"
        "${command[@]}" > "$artifact_dir/registry-reference.log" 2>&1
        tail -4 "$artifact_dir/registry-reference.log"
        command=("$B_GPU_PYTHON" "$repo_root/shared-run/gpu/validate_registry.py"
            "$artifact_dir/original-registry-single.json" "$artifact_dir/original-registry-paired.json"
            "$artifact_dir/original-registry-market.json")
        printf '%q ' "${command[@]}" >> "$artifact_dir/commands.txt"
        printf '\n' >> "$artifact_dir/commands.txt"
        "${command[@]}" > "$artifact_dir/registry-device.log" 2>&1
        tail -1 "$artifact_dir/registry-device.log"
        ;;
    maker-read)
        run_test gpu_actual_maker_mature_day_preserves_noe_batch_and_continuous_ledger maker-day.log
        ;;
    base-cancel)
        run_test gpu_legal_gtc_request_taker_maker_cancel_matches_original_execution tests.log
        ;;
    failed-fill)
        run_test gpu_real_failed_fill_reverses_original_applied_cost_and_fee_once tests.log
        ;;
    *) echo "未知有限入口: $case_name" >&2; exit 2 ;;
esac
