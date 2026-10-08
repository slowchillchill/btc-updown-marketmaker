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
mkdir "$artifact";mkdir "$artifact/sources"
git rev-parse HEAD > "$artifact/source-commit.txt"
git status --porcelain=v1 --untracked-files=all > "$artifact/status.txt"
git diff --binary --no-ext-diff --cached > "$artifact/index.patch"
git diff --binary --no-ext-diff > "$artifact/worktree.patch"
# 新增文件不在git diff中；保留其实际内容和仓库相对名称。
git ls-files --others --exclude-standard -z > "$artifact/untracked-files.list"
"$repo_root/model-train/.venv/bin/python" - "$artifact" <<'PYFILES'
from pathlib import Path
import sys
root=Path(sys.argv[1]);target=root/'untracked';target.mkdir()
for name in (root/'untracked-files.list').read_bytes().split(b'\0'):
    if not name:continue
    path=Path(name.decode());destination=target/path;destination.parent.mkdir(parents=True,exist_ok=True)
    if path.is_symlink():destination.symlink_to(path.readlink())
    else:destination.write_bytes(path.read_bytes())
PYFILES
export PYTHONPATH="$repo_root/model-train"
export B_GPU_ACTOR_CANCEL_REFERENCE="$artifact/sources"
export B_GPU_ACTOR_SOURCE_COMMIT=$(git rev-parse HEAD)
export B_GPU_ACTOR_PATH_REPORT="$artifact/probability-report.json"
export B_GPU_COLUMNS_BINARY="$CARGO_TARGET_DIR/debug/dpt-public-compact"
export B_GPU_RULE_CONTEXT_BINARY="$CARGO_TARGET_DIR/debug/sim-gpu-rule-context"
# 仅保留复现所需项目环境项，禁止导出全量进程环境。
"$repo_root/model-train/.venv/bin/python" - > "$artifact/environment.json" <<'PYENV'
import json,os,platform,torch
keys=('UV_CACHE_DIR','XDG_CACHE_HOME','PIP_CACHE_DIR','TMPDIR','TORCH_HOME',
    'TORCH_EXTENSIONS_DIR','TRITON_CACHE_DIR','CUDA_CACHE_PATH','PYTHONPYCACHEPREFIX',
    'CARGO_HOME','CARGO_TARGET_DIR','RUSTUP_HOME','VIRTUAL_ENV','PYO3_PYTHON',
    'PYTHONPATH','PYTHONNOUSERSITE','CARGO_NET_OFFLINE','B_GPU_COLUMNS_BINARY',
    'B_GPU_RULE_CONTEXT_BINARY','OMP_NUM_THREADS','MKL_NUM_THREADS')
print(json.dumps(dict(environment={key:os.environ[key] for key in keys if key in os.environ},
    python=platform.python_version(),torch=torch.__version__,cuda=torch.version.cuda),indent=2))
PYENV
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
# 直接复用原joint脚本的实际场景命令；候选已在本入口完整保存。
mkdir "$artifact/joint"
export B_GPU_PYTHON="$repo_root/model-train/.venv/bin/python"
export B_GPU_ORDER_VALIDATOR="$repo_root/sim-system/gpu/validate_base_orders.py"
export B_GPU_ORDER_ARTIFACT_DIR="$artifact/joint"
export B_GPU_SIM_ORDER_QUERY=1
export PYTHONPATH="$repo_root/model-train:$repo_root/sim-system/gpu:$repo_root/data-pipeline/gpu"
run actual-joint cargo test -p sim-system --features shared-execution --lib --locked --offline -j24 \
    gpu_legal_gtc_request_taker_maker_cancel_matches_original_execution -- --ignored --nocapture
