#!/usr/bin/env python3
"""#181 有限有理归约与 f32 最近偶数投影：构建、逐卡执行及公共 CPU 参考对照。"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess

from gpu_exact_sample import CGBN_COMMIT, REPO, dump, run, sha

SOURCE = REPO / "model-train/native-inputs/gpu-diagnostics/exact_rational_sample.cu"
HEADER = REPO / "contracts/gpu-diagnostics/exact_rational.cuh"
SCOPE = "有限既有案例与两项技术构造的有符号归约/四则/比较/round_f32；不含GPU log/sqrt或生产动态宽域"


def input_material(reference: dict) -> tuple[str, dict]:
    """只提取权威输入；不在 Python 计算 gcd、四则结果或 f32 位模式。"""
    if reference["schema"] != "gpu_exact_numeric_reference_v1":
        raise ValueError("CPU参考schema不匹配")
    rational = reference["rational_reduction"]
    normalization = rational["normalization"]
    comparisons = rational["comparisons"]
    steps = rational["rms"]["steps"]
    rounded = reference["round_f32"]
    if (len(normalization), len(comparisons), len(steps), len(rounded)) != (5, 3, 2, 6):
        raise ValueError("CPU参考不是本次固定的5项归约/3项比较/2步RMS/6项舍入")
    if (steps[0]["current"] != steps[1]["previous"]
        or steps[0]["sum"] != steps[1]["previous_sum"]
        or steps[1]["sum"] != rational["rms"]["sum"]):
        raise ValueError("CPU参考RMS步骤没有保持同一条数据依赖")
    if any(row["operation"] != "round_f32" for row in rounded):
        raise ValueError("本次GPU投影只包含round_f32")
    probes = rational["technical_probes"]
    pairs = [(f"normalization[{i}]", row["input"]) for i, row in enumerate(normalization)]
    pairs.append(("cross_limb_normalization", probes["cross_limb_normalization"]["input"]))
    for i, row in enumerate(comparisons):
        pairs.extend((f"comparisons[{i}].{side}", row[side]) for side in ("left", "right"))
    pairs.extend((f"cancellation.{side}", rational["cancellation"][side]) for side in ("left", "right"))
    pairs.extend((f"signed_division.{side}", probes["signed_division"][side]) for side in ("left", "right"))
    pairs.extend((f"rms.midpoint[{i}]", value) for i, value in enumerate(
        (steps[0]["previous"], steps[0]["current"], steps[1]["current"])))
    pairs.append(("rms.divisor", rational["rms"]["divisor"]))
    pairs.extend((f"round_f32[{i}]", row["input"]) for i, row in enumerate(rounded))
    lines, layout = [], []
    for label, value in pairs:
        for key in ("numerator", "denominator"):
            text = value[key]
            if not isinstance(text, str) or not re.fullmatch(r"[+-]?[0-9]+", text):
                raise ValueError(f"{label}.{key}不是十进制整数字符串")
            if abs(int(text)).bit_length() > 256:
                raise ValueError(f"{label}.{key}超出本次256位诊断装载容量")
        if int(value["denominator"]) == 0:
            raise ValueError(f"{label}分母为零")
        lines.append(f'{value["numerator"]} {value["denominator"]}')
        layout.append({"label": label, "input": value,
                       "numerator_magnitude_bits": abs(int(value["numerator"])).bit_length(),
                       "denominator_magnitude_bits": abs(int(value["denominator"])).bit_length()})
    if len(lines) != 26:
        raise ValueError("固定CUDA输入协议必须恰好26对整数")
    return "\n".join(lines) + "\n", {
        "schema": "gpu_exact_rational_input_layout_v1", "pairs": layout,
        "scope": "仅记录输入及表示位数；期望值全部取自公共CPU结果",
    }


def compare(actual: dict, reference: dict) -> dict:
    failures = []

    def equal(label, got, expected):
        # 保留JSON标量类型，不让True与1等价掩盖结果错误。
        if json.dumps(got, sort_keys=True) != json.dumps(expected, sort_keys=True):
            failures.append({"field": label, "actual": got, "expected": expected})

    rational = reference["rational_reduction"]
    probes = rational["technical_probes"]
    steps = rational["rms"]["steps"]
    equal("schema", actual.get("schema"), "gpu_exact_rational_sample_v1")
    for key, expected in {"bits": 256, "wide_bits": 512, "threads_per_integer": 8,
                          "block_threads": 32, "h2d_calls": 1, "d2h_calls": 2}.items():
        equal(key, actual.get(key), expected)
    equal("normalization", actual.get("normalization"),
          [row["output"] for row in rational["normalization"]]
          + [probes["cross_limb_normalization"]["output"]])
    ordering = {"Less": -1, "Equal": 0, "Greater": 1}
    equal("comparisons", actual.get("comparisons"),
          [ordering[row["ordering"]] for row in rational["comparisons"]])
    equal("cancellation", actual.get("cancellation"), rational["cancellation"]["sum"])
    equal("signed_division", actual.get("signed_division"), probes["signed_division"]["quotient"])
    equal("rms", actual.get("rms"), {
        "differences": [row["difference"] for row in steps],
        "squares": [row["square"] for row in steps],
        "sums": [row["sum"] for row in steps], "mean": rational["rms"]["mean"],
    })
    equal("round_f32_bits", actual.get("round_f32_bits"),
          [row["bits_u32"] for row in reference["round_f32"]])
    return {"passed": not failures, "failures": failures,
            "compared": ["6个规范化分数", "3个比较结果", "正负相消", "有符号除法",
                         "RMS两步差/平方/累计与最终除法", "6个round_f32位模式"],
            "deferred": "CPU参考中log/sqrt另行保留，本次GPU不执行或验收"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("cpu-reference", "cgbn-root", "cgbn-record", "gmp-prefix", "nvcc", "output-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--devices", nargs="+", type=int, default=[0])
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--progress-seconds", type=float, default=10)
    args = parser.parse_args()
    if args.warmups < 1 or args.iterations < 1 or args.progress_seconds <= 0 or any(i < 0 for i in args.devices):
        parser.error("次数、进度间隔须为正；设备序号须非负")
    if len(args.devices) > 2 or len(set(args.devices)) != len(args.devices):
        parser.error("一次只逐卡验证一或两张不同的卡")
    output = args.output_dir.resolve()
    if not output.is_relative_to(Path("/root/autodl-tmp")) or output == Path("/root/autodl-tmp"):
        parser.error("输出必须是 /root/autodl-tmp 下的新目录")
    for name in ("cpu_reference", "cgbn_root", "cgbn_record", "gmp_prefix", "nvcc"):
        setattr(args, name, getattr(args, name).resolve())
    output.mkdir(parents=True, exist_ok=False)
    report = {"schema": "gpu_exact_rational_sample_report_v1", "scope": SCOPE, "status": "started"}
    try:
        reference = json.loads(args.cpu_reference.read_text())
        material = json.loads(args.cgbn_record.read_text())
        if material["commit"] != CGBN_COMMIT or material.get("pr35_applied") is not False:
            raise ValueError("CGBN材料记录不是本次批准的原始提交")
        text, layout = input_material(reference)
        (output / "input.txt").write_text(text)
        dump(output / "input-layout.json", layout)
        dump(output / "cpu-reference.json", reference)
        files = (SOURCE, HEADER, Path(__file__).resolve(),
                 REPO / "model-train/scripts/gpu_exact_sample.py",
                 REPO / "model-train/native-inputs/src/exact.rs", REPO / "contracts/src/economics.rs")
        report["source"] = {
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
            "files_sha256": {str(path.relative_to(REPO)): sha(path) for path in files},
            "cpu_reference_sha256": sha(args.cpu_reference),
            "cgbn_commit_from_material": material["commit"], "cgbn_record_sha256": sha(args.cgbn_record),
            "cgbn_headers_sha256": {str(path.relative_to(args.cgbn_root)): sha(path)
                                     for path in sorted((args.cgbn_root / "include").rglob("*")) if path.is_file()},
            "gmp_header_sha256": sha(args.gmp_prefix / "usr/include/x86_64-linux-gnu/gmp.h"),
            "gmp_library_sha256": sha(args.gmp_prefix / "usr/lib/x86_64-linux-gnu/libgmp.so"),
        }
        env = os.environ.copy()
        for variable, directory in (("TMPDIR", "tmp"), ("CUDA_CACHE_PATH", "cuda-cache")):
            (output / directory).mkdir()
            env[variable] = str(output / directory)
        library = args.gmp_prefix / "usr/lib/x86_64-linux-gnu"
        env["LD_LIBRARY_PATH"] = str(library) + (":" + env["LD_LIBRARY_PATH"] if env.get("LD_LIBRARY_PATH") else "")
        report["nvcc_version"] = subprocess.check_output([str(args.nvcc), "--version"], text=True, env=env)
        binary = output / "gpu-exact-rational-sample"
        command = [str(args.nvcc), "-std=c++17", "-O2", "-lineinfo", "-arch=sm_120",
                   "-I", str(args.cgbn_root / "include"), "-I", str(args.gmp_prefix / "usr/include/x86_64-linux-gnu"),
                   "-I", str(args.gmp_prefix / "usr/include"), str(SOURCE), "-L", str(library),
                   "-Xlinker", "-rpath," + str(library), "-lgmp", "-o", str(binary)]
        report["compile_command"] = command
        report["compile_wall_seconds"] = run(command, output / "compile.log", env, args.progress_seconds, "有理数CUDA编译")
        report["binary_sha256"] = sha(binary)
        report["devices"] = []
        for device in args.devices:
            log = output / f"device-{device}.log"
            wall = run([str(binary), str(output / "input.txt"), str(device), str(args.warmups), str(args.iterations)],
                       log, env, args.progress_seconds, f"GPU {device}有理数有限诊断")
            rows = [line for line in log.read_text().splitlines()
                    if line.startswith('{"schema":"gpu_exact_rational_sample_v1"')]
            if len(rows) != 1:
                raise ValueError("GPU日志没有唯一完整结果JSON")
            actual = json.loads(rows[0])
            comparison = compare(actual, reference)
            if (actual.get("device"), actual.get("warmups"), actual.get("iterations")) != (device, args.warmups, args.iterations):
                comparison["passed"] = False
                comparison["failures"].append({"field": "device_or_iterations", "actual": actual,
                                                "expected": [device, args.warmups, args.iterations]})
            entry = {"output": actual, "comparison": comparison, "process_wall_seconds": wall}
            report["devices"].append(entry)
            dump(output / f"device-{device}.json", entry)
            if not comparison["passed"]:
                raise RuntimeError(f"GPU {device}逐值比较失败")
        report["measurement_scope"] = {
            "iteration": "一个kernel内的固定归约/四则/比较/舍入序列；一个TPI8组顺序执行，无RL环境步数",
            "timing": "编译墙钟、预热CUDA事件、稳态CUDA事件和进程墙钟分别记录；稳态不是单运算延迟",
            "memory": "owned_device_allocation_bytes是显式分配之和，未含CUDA上下文/模块/事件，不是整卡峰值",
            "transfer": "全部输入一次H2D；结束后输出和错误报告各一次D2H；中间依赖不回CPU",
            "arithmetic": "256位幅值与512位完整乘积只用于有限诊断；不证明生产动态宽域，不新增生产拒绝规则",
        }
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        dump(output / "report.json", report)
        print(f"报告：{output / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
