#!/usr/bin/env bash
# 原一个账户259/257撤单对象→公共来源/原意图关联→Native184组件。
set -euo pipefail
artifact=${1:?传入AutoDL新的证据目录}
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
export B_GPU_CANCEL_INPUT_REFERENCE="$artifact/sources"
env | sort > "$artifact/environment.txt"
finish() {
    code=$?
    printf '%s\n' "$code" > "$artifact/validation.exit"
    nvidia-smi > "$artifact/resources-end.txt" 2>&1 || true
    exit "$code"
}
trap finish EXIT
nvidia-smi > "$artifact/resources-start.txt"
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
run syntax "$repo_root/model-train/.venv/bin/python" -m py_compile \
    shared-run/gpu/intent_terms.py shared-run/gpu/cancel_projection.py \
    model-train/native-inputs/gpu/input_orders.py model-train/native-inputs/gpu/input_cancel.py \
    model-train/scripts/gpu_cancel_inputs_validation.py
run build-native cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --no-run --locked --offline -j24
run original cargo test --manifest-path model-train/native-inputs/Cargo.toml --lib --locked --offline -j24 \
    action_tests::complete_pair_terminal_leg_and_257_global_selection -- --exact --nocapture
run cancel-inputs "$repo_root/model-train/.venv/bin/python" model-train/scripts/gpu_cancel_inputs_validation.py \
    --sources "$artifact/sources" --output "$artifact/report.json"
