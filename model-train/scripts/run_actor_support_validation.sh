#!/usr/bin/env bash
# 原NativeActor逐前缀与真实联合冻结上下文；不是完整Actor采样/提交或训练。
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
mkdir "$artifact/sources"
git rev-parse HEAD > "$artifact/source-commit.txt"
git status --porcelain > "$artifact/status.txt"
export PYTHONPATH="$repo_root/model-train"
export B_GPU_ACTOR_CANCEL_REFERENCE="$artifact/sources"
export B_GPU_COLUMNS_BINARY="$CARGO_TARGET_DIR/debug/dpt-public-compact"
env | sort > "$artifact/environment.txt"
run() {
    local stage=$1 code=0
    shift
    printf '%q ' "$@" > "$artifact/$stage.command"
    printf '\n' >> "$artifact/$stage.command"
    echo "stage=$stage commit=$(git rev-parse HEAD) started=$(date -u +%FT%TZ)"
    "$@" > "$artifact/$stage.log" 2>&1 || code=$?
    printf '%s\n' "$code" > "$artifact/$stage.exit"
    tail -n 4 "$artifact/$stage.log"
    return "$code"
}
run build-native cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --no-run --locked --offline -j24
run build-public cargo build -p data-pipeline-training --bin dpt-public-compact --locked --offline -j24
run buy-original cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline -j24 \
    action_tests::buy_tests::export_gpu_exact_buy_reference_v1 -- --exact --ignored --nocapture & buy_job=$!
run cancel-original cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline -j24 \
    action_tests::complete_pair_terminal_leg_and_257_global_selection -- --exact --nocapture & cancel_job=$!
run encoding-original cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline -j24 \
    action_tests::buy_tests::cash_root_is_not_replaced_by_encoding_limit -- --exact --nocapture & encoding_job=$!
run repair-original cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline -j24 \
    action_tests::buy_tests::actual_single_repair_cap_counts_visible_success_and_nonterminal_commitment -- --exact --nocapture & repair_job=$!
run accepted-original cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline -j24 \
    action_tests::buy_tests::accepted_only_zero_gap_keeps_original_actor_support -- --exact --nocapture & accepted_job=$!
code=0
for job in "$buy_job" "$cancel_job" "$encoding_job" "$repair_job" "$accepted_job"; do wait "$job" || code=$?; done
if (( code!=0 )); then printf '%s\n' "$code" > "$artifact/validation.exit"; exit "$code"; fi
run extract "$repo_root/model-train/.venv/bin/python" - "$artifact" <<'PY'
import json,sys
from pathlib import Path
root=Path(sys.argv[1])
line,=[line.removeprefix('BTC_GPU_EXACT_REFERENCE_V1=') for line in (root/'buy-original.log').read_text().splitlines()
       if line.startswith('BTC_GPU_EXACT_REFERENCE_V1=')]
(root/'sources/buy.json').write_text(json.dumps(json.loads(line),indent=2)+'\n')
PY
code=0
run support "$repo_root/model-train/.venv/bin/python" model-train/scripts/gpu_actor_support_validation.py \
    --sources "$artifact/sources" --output "$artifact/report.json" || code=$?
export B_GPU_ACTOR_CONTEXT_VALIDATION=1
run joint-context bash model-train/scripts/run_joint_freeze_validation.sh "$artifact/joint-context" || code=$?
printf '%s\n' "$code" > "$artifact/validation.exit"
exit "$code"
