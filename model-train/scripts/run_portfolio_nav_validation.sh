#!/usr/bin/env bash
# 原两个合法Native NAV工程场景；不执行正式根、旧全窗诊断或CPU热宿主。
set -euo pipefail
artifact_dir=${1:?传入AutoDL新证据目录}
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
for location in "$repo_root" "$artifact_dir"; do
    case "$location" in /root/autodl-tmp/*) ;; *) exit 2 ;; esac
done
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact_dir"
git rev-parse HEAD > "$artifact_dir/source-commit.txt"
git status --porcelain > "$artifact_dir/status.txt"
export B_GPU_PYTHON="$repo_root/model-train/.venv/bin/python"
export B_GPU_COLUMNS_BINARY="$CARGO_TARGET_DIR/debug/dpt-public-compact"
export B_GPU_NAV_VALIDATOR="$repo_root/shared-run/gpu/validate_portfolio_nav.py"
export B_GPU_NAV_ARTIFACT_ROOT="$artifact_dir"
export PYTHONPATH="$repo_root/model-train:$repo_root/data-pipeline/gpu"
run() {
    local stage=$1 status=0
    shift
    printf '%q ' "$@" >> "$artifact_dir/commands.txt"
    printf '\n' >> "$artifact_dir/commands.txt"
    echo "stage=$stage commit=$(git rev-parse HEAD) started=$(date -u +%FT%TZ)"
    "$@" > "$artifact_dir/$stage.log" 2>&1 || status=$?
    printf '%s\n' "$status" > "$artifact_dir/$stage.exit"
    tail -n 4 "$artifact_dir/$stage.log"
    return "$status"
}
run build-public cargo build -p data-pipeline-training --bin dpt-public-compact --locked --offline -j24
status=0
for scene in same_cutoff_orphan_marks_freeze_all_markets_and_encode_true_zero_unknown \
    missing_quote_without_that_market_exposure_does_not_poison_nav; do
    run "$scene" cargo test --manifest-path model-train/native-inputs/Cargo.toml --locked --offline -j24 \
        "$scene" -- --nocapture || status=$?
done
export B_GPU_NAV_FEE_VALIDATION=1
export B_GPU_ORDER_ARTIFACT_DIR="$artifact_dir"
export B_GPU_SETTLEMENT_CASH_VALIDATOR="$repo_root/ledger/gpu/validate_settlement_cash.py"
run nav-Maker-Directional-fees cargo test -p shared-run --test economic_settlement --locked --offline -j24 \
    resolution_before_late_success_fixes_fee_bucket_and_updates_pi_without_frames -- --exact --nocapture || status=$?
printf '%s\n' "$status" > "$artifact_dir/validation.exit"
exit "$status"
