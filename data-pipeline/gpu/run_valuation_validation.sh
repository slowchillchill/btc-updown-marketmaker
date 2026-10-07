#!/usr/bin/env bash
# #181公共估值版本有限验证；不启动正式根、旧全窗诊断或raw全量准备。
set -euo pipefail
artifact_dir=${1:?传入AutoDL新证据目录}
material=${2:?传入已验收首分钟compact}
legacy_page=${3:?传入同compact现有v1或v2公共页}
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
for location in "$repo_root" "$artifact_dir" "$material" "$legacy_page"; do
    case "$location" in /root/autodl-tmp/*) ;; *) echo 'B计算/材料/证据须在AutoDL数据盘' >&2;exit 2 ;; esac
done
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact_dir"
git rev-parse HEAD > "$artifact_dir/source-commit.txt"
git status --porcelain > "$artifact_dir/status.txt"
export B_GPU_PYTHON="$repo_root/model-train/.venv/bin/python"
export B_GPU_COLUMNS_BINARY="$CARGO_TARGET_DIR/debug/dpt-public-compact"
export B_GPU_STATE_VALIDATOR="$repo_root/model-train/scripts/gpu_public_state_validation.py"
export B_GPU_VALUATION_ARTIFACT_ROOT="$artifact_dir/valuation-scenarios"
export B_GPU_RULE_CONTEXT_BINARY="$CARGO_TARGET_DIR/debug/sim-gpu-rule-context"
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
run build-rules cargo build -p sim-system --bin sim-gpu-rule-context --locked --offline -j24
for test_name in valuation_bid_selects_public_versions_without_rewriting_direct_bba \
    unrelated_high_delete_cannot_promote_old_l2_over_new_direct_bid \
    lower_quantity_update_cannot_refresh_direct_bid_over_new_high_bid; do
    run "$test_name" cargo test -p data-pipeline-core --lib --locked --offline -j24 "$test_name" -- --nocapture
done
run valuation-gpu cargo test -p data-pipeline-training --lib --locked --offline -j24 \
    gpu_public_valuation_versions_match_original_core -- --ignored --nocapture
run public-gpu cargo test -p data-pipeline-training --lib --locked --offline -j24 \
    gpu_public_zero_selection_matches_original_core -- --ignored --nocapture
run public-minute "$B_GPU_PYTHON" "$B_GPU_STATE_VALIDATOR" --binary "$B_GPU_COLUMNS_BINARY" \
    --material "$material" --device-page "$artifact_dir/public-v3-page" --legacy-device-page "$legacy_page" \
    --output "$artifact_dir/public-minute.json"
