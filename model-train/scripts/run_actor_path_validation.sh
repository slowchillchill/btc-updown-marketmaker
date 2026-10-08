#!/usr/bin/env bash
# 完整设备动作路径/原概率AdamW/实际1500与1600冻结；不是PPO或正式训练。
set -euo pipefail
artifact=${1:?传入AutoDL新证据目录}
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
for location in "$artifact" "$repo_root"; do
    case "$location" in /root/autodl-tmp/*) ;; *) exit 2 ;; esac
done
source /root/autodl-tmp/environments/issue-181-cu128.sh
cd "$repo_root"
test -z "$(git status --porcelain)"
mkdir "$artifact";mkdir "$artifact/sources"
git rev-parse HEAD > "$artifact/source-commit.txt"
git status --porcelain > "$artifact/status.txt"
export PYTHONPATH="$repo_root/model-train"
export B_GPU_ACTOR_CANCEL_REFERENCE="$artifact/sources"
export B_GPU_ACTOR_SOURCE_COMMIT=$(git rev-parse HEAD)
export B_GPU_ACTOR_PATH_REPORT="$artifact/probability-report.json"
export B_GPU_COLUMNS_BINARY="$CARGO_TARGET_DIR/debug/dpt-public-compact"
export B_GPU_RULE_CONTEXT_BINARY="$CARGO_TARGET_DIR/debug/sim-gpu-rule-context"
env | sort > "$artifact/environment.txt"
finish() { code=$?;printf '%s\n' "$code" > "$artifact/validation.exit";nvidia-smi > "$artifact/resources-end.txt" 2>&1 || true;exit "$code"; }
trap finish EXIT
nvidia-smi > "$artifact/resources-start.txt"
cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max > "$artifact/quotas.txt"
df -hT "$artifact" > "$artifact/data-disk.txt"
run() {
    local stage=$1 code=0;shift
    printf '%q ' "$@" > "$artifact/$stage.command";printf '\n' >> "$artifact/$stage.command"
    echo "stage=$stage commit=$(git rev-parse HEAD) started=$(date -u +%FT%TZ)"
    "$@" > "$artifact/$stage.log" 2>&1 || code=$?
    printf '%s\n' "$code" > "$artifact/$stage.exit";tail -n 4 "$artifact/$stage.log";return "$code"
}
run syntax "$repo_root/model-train/.venv/bin/python" -m py_compile \
    model-train/model_train/action_inputs.py model-train/model_train/action_random.py \
    model-train/model_train/device_actor.py model-train/model_train/action_replay.py \
    model-train/model_train/device_actor_validation.py shared-run/gpu/public_ledger.py \
    shared-run/gpu/cancel_projection.py sim-system/gpu/validate_joint_freeze.py \
    sim-system/gpu/validate_base_noe_ledger.py sim-system/gpu/validate_sim_order_query.py
run build-native cargo test --manifest-path model-train/native-inputs/Cargo.toml --features python --lib --no-run --locked --offline -j24
run build-public cargo build -p data-pipeline-training --bin dpt-public-compact --locked --offline -j24
run build-rules cargo build -p sim-system --bin sim-gpu-rule-context --locked --offline -j24
run cancel-source cargo test --manifest-path model-train/native-inputs/Cargo.toml --features python --lib --locked --offline -j24 \
    action_tests::complete_pair_terminal_leg_and_257_global_selection -- --exact --nocapture & cancel_job=$!
run repair-source cargo test --manifest-path model-train/native-inputs/Cargo.toml --features python --lib --locked --offline -j24 \
    action_tests::buy_tests::actual_single_repair_cap_counts_visible_success_and_nonterminal_commitment -- --exact --nocapture & repair_job=$!
code=0;wait "$cancel_job" || code=$?;wait "$repair_job" || code=$?;if (( code!=0 )); then exit "$code";fi
run full-probability cargo test --manifest-path model-train/native-inputs/Cargo.toml --features python --lib --locked --offline -j24 \
    action_tests::buy_tests::device_full_actor_path_matches_original_probability_and_optimizer -- --exact --ignored --nocapture
run branches "$repo_root/model-train/.venv/bin/python" - "$artifact" <<'PY'
import sys
from pathlib import Path
from model_train.device_actor_validation import check_branches
root=Path(sys.argv[1]);check_branches(root/'sources',root/'branches-report.json')
PY
export B_GPU_ACTOR_CONTEXT_VALIDATION=1
export B_GPU_ACTOR_PATH_VALIDATION=1
export B_GPU_JOINT_FREEZE=1
run actual-joint bash model-train/scripts/run_joint_freeze_validation.sh "$artifact/joint"
