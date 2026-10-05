#!/usr/bin/env python3
"""既有 MAIN-1x256 组件的有界 GPU 接线验证，不是 100ms/PPO/经济后端验收。

使用项目 Python 3.13.15 / torch 2.11.0+cu128 环境运行，例如：
  python scripts/gpu_component_smoke.py --devices 0 1 \
      --output-dir /root/autodl-tmp/validation/component-smoke-001
输出目录必须尚不存在；每张指定卡独立执行一次更新，按顺序运行。
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
import traceback


MODEL_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = MODEL_ROOT.parent
SCOPE = "既有纯图组件与合成技术夹具的 GPU 接线；不是业务训练、100ms 模型、PPO 或公共 GPU 后端验收"
BATCH, PAIRS, BOOK_DEPTH = 2, 1, 7


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def source_record() -> dict:
    def git(*args):
        return subprocess.check_output(
            ["git", "-C", str(REPOSITORY), *args], text=True
        ).strip()

    files = [Path(__file__).resolve()] + [
        MODEL_ROOT / "model_train" / name
        for name in ("graphs.py", "components.py", "book.py", "graph_fixtures.py")
    ]
    return {
        "commit": git("rev-parse", "HEAD"),
        "model_train_worktree_status": git(
            "status", "--porcelain", "--untracked-files=normal", "--", "model-train"
        ),
        "files_sha256": {str(path.relative_to(REPOSITORY)): sha256(path) for path in files},
    }


def assert_saved_equal(torch, expected, actual, label="optimizer"):
    """核对保存的优化器状态，不执行第二次更新。"""
    if isinstance(expected, torch.Tensor):
        torch.testing.assert_close(expected.detach().cpu(), actual.detach().cpu(), rtol=0, atol=0)
    elif isinstance(expected, dict):
        if expected.keys() != actual.keys():
            raise AssertionError(f"{label} 字段不一致")
        for key in expected:
            assert_saved_equal(torch, expected[key], actual[key], f"{label}.{key}")
    elif isinstance(expected, (list, tuple)):
        if len(expected) != len(actual):
            raise AssertionError(f"{label} 长度不一致")
        for index, (left, right) in enumerate(zip(expected, actual)):
            assert_saved_equal(torch, left, right, f"{label}[{index}]")
    elif expected != actual:
        raise AssertionError(f"{label} 值不一致")


def run_device(torch, config, index, output_dir, progress):
    from model_train.graph_fixtures import book_fixture, fixture
    from model_train.graphs import ACTION_OUTPUTS, STATE_OUTPUTS, TwoGraphs, encode_all

    device = torch.device("cuda", index)
    torch.cuda.set_device(device)
    torch.cuda.synchronize(device)
    torch.cuda.reset_peak_memory_stats(device)
    started = time.perf_counter()
    directory = output_dir / f"cuda-{index}"
    directory.mkdir()
    properties = torch.cuda.get_device_properties(device)
    progress(f"cuda:{index}：建立真实组件与固定技术夹具")

    model = TwoGraphs(config).to(device=device, dtype=torch.float32).train()
    inputs = tuple(t.to(device) for t in fixture(batch=BATCH, pair_count=PAIRS))
    books = tuple(t.to(device) for t in book_fixture(batch=BATCH, depth=BOOK_DEPTH))
    hidden = torch.zeros(config.layers, BATCH, config.hidden, device=device)
    # 仅为已有分数头提供有限的零费用特征，不推导平台费用或动作支持集。
    fees = torch.zeros(BATCH, 999, device=device)
    optimizer = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=0.01, foreach=False)

    def forward(network):
        fast, slow, rule, prefix, objects, mask = inputs
        z, h = network.state(fast, slow, *books, hidden)
        reduced, exists = encode_all(
            network.action, z, rule, objects, mask, [objects.shape[1]], empty=False
        )
        return (z, h, *network.action(z, rule, prefix, objects, mask, reduced, exists, fees))

    before = {name: parameter.detach().clone() for name, parameter in model.named_parameters()}
    torch.cuda.synchronize(device)
    update_started = time.perf_counter()
    progress(f"cuda:{index}：前向、反向和一次 AdamW 更新")
    outputs = forward(model)
    if not all(bool(torch.isfinite(value).all()) for value in outputs):
        raise AssertionError("组件前向产生非有限值")
    # 诊断损失只驱动组件梯度，既不是 PPO 损失，也不是交易奖励。
    loss = torch.stack([value.square().mean() for value in outputs]).sum()
    if not bool(torch.isfinite(loss)):
        raise AssertionError("诊断损失非有限")
    loss.backward()
    gradients = {}
    for name, parameter in model.named_parameters():
        if parameter.grad is None or not bool(torch.isfinite(parameter.grad).all()):
            raise AssertionError(f"参数梯度缺失或非有限：{name}")
        gradients[name] = float(parameter.grad.detach().double().norm())
    if not any(value > 0 for value in gradients.values()):
        raise AssertionError("全部梯度为零")
    optimizer.step()
    torch.cuda.synchronize(device)
    update_seconds = time.perf_counter() - update_started
    changes = {}
    for name, parameter in model.named_parameters():
        if not bool(torch.isfinite(parameter).all()):
            raise AssertionError(f"更新后参数非有限：{name}")
        delta = float((parameter.detach() - before[name]).abs().max())
        if delta > 0:
            changes[name] = delta
    for prefix in ("state.", "action."):
        if not any(name.startswith(prefix) and gradients[name] > 0 for name in changes):
            raise AssertionError(f"{prefix} 没有具有非零梯度且实际改变的参数")
    for state in optimizer.state.values():
        if any(isinstance(value, torch.Tensor) and not bool(torch.isfinite(value).all())
               for value in state.values()):
            raise AssertionError("AdamW 状态非有限")

    progress(f"cuda:{index}：保存／加载权重和优化器，核对同输入输出")
    torch.cuda.synchronize(device)
    restore_started = time.perf_counter()
    model.eval()
    with torch.no_grad():
        expected = tuple(value.detach().clone() for value in forward(model))
    checkpoint_path = directory / "component-checkpoint.pt"
    torch.save({
        "scope": SCOPE,
        "configuration": asdict(config),
        "model": model.state_dict(),
        "optimizer": optimizer.state_dict(),
        "optimizer_updates": 1,
    }, checkpoint_path)
    checkpoint = torch.load(checkpoint_path, map_location=device, weights_only=True)
    restored = TwoGraphs(config).to(device=device, dtype=torch.float32).eval()
    restored.load_state_dict(checkpoint["model"], strict=True)
    restored_optimizer = torch.optim.AdamW(
        restored.parameters(), lr=1e-3, weight_decay=0.01, foreach=False
    )
    restored_optimizer.load_state_dict(checkpoint["optimizer"])
    assert_saved_equal(torch, model.state_dict(), restored.state_dict(), "model")
    assert_saved_equal(torch, optimizer.state_dict(), restored_optimizer.state_dict())
    with torch.no_grad():
        actual = forward(restored)
    errors = {}
    for name, left, right in zip((*STATE_OUTPUTS, *ACTION_OUTPUTS), expected, actual):
        # 沿既有纯图数值验证容限；参数及优化器状态已另外逐值精确核对。
        torch.testing.assert_close(left, right, atol=1e-5, rtol=1e-4)
        errors[name] = float((left - right).abs().max())
    torch.cuda.synchronize(device)
    restore_seconds = time.perf_counter() - restore_started
    elapsed = time.perf_counter() - started
    result = {
        "device": {"logical_index": index, "name": properties.name,
                   "capability": list(torch.cuda.get_device_capability(device)),
                   "total_memory_bytes": properties.total_memory},
        "configuration": asdict(config),
        "sample": {"source": "graph_fixtures.fixture / book_fixture；固定合成技术切片",
                   "batch": BATCH, "pair_count": PAIRS, "removed_a": False, "step": 0,
                   "book_depth_per_side": BOOK_DEPTH, "hidden": "全零初始值",
                   "price_fee_features": "[2,999] 全零；不验证费用语义",
                   "input_shapes": [list(value.shape) for value in (*inputs, *books, hidden, fees)]},
        "parameter_dtype": "float32",
        "parameter_count": sum(parameter.numel() for parameter in model.parameters()),
        "diagnostic_loss": {"definition": "各组件输出平方均值之和；非业务／PPO 损失",
                            "value": float(loss.detach())},
        "optimizer": {"name": "AdamW", "lr": 1e-3, "weight_decay": 0.01,
                      "foreach": False, "updates": 1, "state_entries": len(optimizer.state)},
        "gradient_norms": gradients,
        "changed_parameters_max_abs_delta": changes,
        "save_load": {"model_state_exact": True, "optimizer_state_exact": True,
                      "output_atol": 1e-5, "output_rtol": 1e-4, "output_max_abs_error": errors},
        "timing_seconds": {
            "forward_backward_update_including_finite_checks": update_seconds,
            "save_load_and_verification": restore_seconds,
            "end_to_end_including_setup_checks_and_checkpoint_io": elapsed,
        },
        "memory_bytes": {"peak_allocated": torch.cuda.max_memory_allocated(device),
                         "peak_reserved": torch.cuda.max_memory_reserved(device)},
        "measurement_scope": "无预热的单次组件检查，无吞吐／容量结论；阶段端点同步 CUDA，逐卡总计含模型/夹具建立与检查点I/O，不含解释器导入、CUDA上下文初始化和产物哈希；显存仅为 PyTorch 分配器统计，包含核对副本",
        "checkpoint": {"path": str(checkpoint_path), "bytes": checkpoint_path.stat().st_size,
                       "sha256": sha256(checkpoint_path)},
    }
    progress(f"cuda:{index}：通过，端到端 {elapsed:.3f} 秒，产物 {checkpoint_path}")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--devices", type=int, nargs="+", required=True, help="一或两张可见卡的逻辑编号，逐卡执行")
    parser.add_argument("--output-dir", type=Path, required=True, help="/root/autodl-tmp 下尚不存在的输出目录")
    parser.add_argument("--progress", choices=("stage", "quiet"), default="stage", help="中文阶段进度，默认 stage")
    args = parser.parse_args()
    if len(args.devices) not in (1, 2) or len(set(args.devices)) != len(args.devices) or min(args.devices) < 0:
        parser.error("--devices 必须给出一或两个不同的非负逻辑编号")
    output = args.output_dir.resolve()
    root = Path("/root/autodl-tmp").resolve()
    if not args.output_dir.is_absolute() or output == root or not output.is_relative_to(root):
        parser.error("输出目录必须显式位于 /root/autodl-tmp 下")
    output.mkdir(parents=True, exist_ok=False)
    report = {"schema": "gpu-component-smoke-v1", "scope": SCOPE, "status": "running",
              "started_at_utc": datetime.now(timezone.utc).isoformat(),
              "output_directory": str(output), "devices_requested": args.devices, "results": []}
    report_path = output / "report.json"
    run_started = time.perf_counter()

    def save_report():
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")

    def progress(message):
        if args.progress == "stage":
            print(message, flush=True)

    try:
        report["source"] = source_record()
        report["python"] = {"version": platform.python_version(), "executable": sys.executable}
        if platform.python_version() != "3.13.15":
            raise RuntimeError("请显式使用项目 Python 3.13.15；本脚本不会安装或切换解释器")
        sys.dont_write_bytecode = True
        os.environ["CUDA_CACHE_PATH"] = str(output / "cuda-cache")
        sys.path.insert(0, str(MODEL_ROOT))
        import torch
        from model_train.graphs import CONFIGURATIONS

        report["torch"] = {"version": str(torch.__version__), "cuda_runtime": torch.version.cuda,
                           "cudnn": torch.backends.cudnn.version(),
                           "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES")}
        try:
            drivers = subprocess.check_output(
                ["nvidia-smi", "--query-gpu=driver_version", "--format=csv,noheader"],
                text=True, timeout=10,
            ).splitlines()
            report["torch"]["nvidia_driver_versions"] = sorted(set(drivers))
        except (OSError, subprocess.SubprocessError) as error:
            report["torch"]["nvidia_driver_query_error"] = str(error)
        if str(torch.__version__) != "2.11.0+cu128":
            raise RuntimeError("本次验证要求项目 GPU 锁 torch 2.11.0+cu128，不使用镜像版本代替")
        if not torch.cuda.is_available() or max(args.devices) >= torch.cuda.device_count():
            raise RuntimeError("指定 CUDA 设备不可用")
        torch.set_float32_matmul_precision("highest")
        torch.backends.cuda.matmul.allow_tf32 = False
        torch.backends.cudnn.allow_tf32 = False
        torch.backends.cudnn.benchmark = False
        report["precision"] = {"autocast": False, "tf32_matmul": False, "tf32_cudnn": False}
        config = next(item for item in CONFIGURATIONS if item.name == "MAIN-1x256")
        save_report()
        for index in args.devices:
            report["active_device"] = index
            save_report()
            report["results"].append(run_device(torch, config, index, output, progress))
            save_report()
        report.pop("active_device", None)
        report["status"] = "passed"
        report["run_wall_seconds_including_imports_and_artifact_hashes"] = time.perf_counter() - run_started
        save_report()
        print(f"既有组件 GPU 验证通过；仅限上述技术切片。报告：{report_path}", flush=True)
        return 0
    except Exception as error:
        report["status"] = "failed"
        report["error"] = {"type": type(error).__name__, "message": str(error)}
        report["run_wall_seconds_including_imports_and_artifact_hashes"] = time.perf_counter() - run_started
        save_report()
        print(f"GPU 验证停止：{error}；报告：{report_path}", file=sys.stderr, flush=True)
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
