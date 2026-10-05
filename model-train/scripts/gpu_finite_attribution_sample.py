#!/usr/bin/env python3
"""#181 有限定点分配诊断：构建、逐卡执行与公共 CPU 参考逐值比较。"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import subprocess

from gpu_exact_sample import CGBN_COMMIT, REPO, dump, run, sha

SOURCE = REPO / "model-train/native-inputs/gpu-diagnostics/finite_attribution_sample.cu"
HEADER = REPO / "contracts/gpu-diagnostics/finite_attribution.cuh"
SCOPE = "12位RNE分配/精确补差的有限案例和3654轮连续更新；非完整账户、任意数值域或训练性能验收"
CHECKPOINTS = [1, 2, 10, 100, 1000, 3654]


def decimal_input(value: dict) -> tuple[int, int, int]:
    """只求源有理数的精确十进制表示尺度；不计算分配或舍入期望值。"""
    for key in ("numerator", "denominator"):
        if not isinstance(value[key], str) or not re.fullmatch(r"-?[0-9]+", value[key]):
            raise ValueError("CPU参考的分子分母须为十进制整数字符串")
    numerator, denominator = int(value["numerator"]), int(value["denominator"])
    if denominator <= 0:
        raise ValueError("CPU参考须使用规范化正分母")
    rest, twos, fives = denominator, 0, 0
    while rest % 2 == 0:
        rest //= 2
        twos += 1
    while rest % 5 == 0:
        rest //= 5
        fives += 1
    if rest != 1:
        raise ValueError("本次有限源材料不是终止十进制；不得近似装载")
    return numerator, denominator, max(twos, fives)


def at_scale(value: dict, scale: int) -> int:
    numerator, denominator, _ = decimal_input(value)
    coefficient, remainder = divmod(numerator * 10 ** scale, denominator)
    if remainder:
        raise ValueError("指定材料尺度不能精确保留源值")
    if abs(coefficient).bit_length() > 256:
        raise ValueError("材料超出本次256位诊断容量；这不是生产域限制")
    return coefficient


def amount(value: dict, scale: int) -> dict:
    return {"coefficient": str(at_scale(value, scale)), "scale": scale}


def input_material(reference: dict) -> tuple[str, dict]:
    if (reference["schema"] != "finite_attribution_reference_v1" or reference["scale"] != 12
        or reference["rule"] != "finite_attribution_scale12_rne_v1"):
        raise ValueError("CPU参考schema或尺度不符")
    rounds, splits = reference["rounding"], reference["splits"]
    capacities, growth = reference["capacity_allocations_candidate"], reference["growth"]
    if len(rounds) != 6 or len(splits) != 6 or [len(row["capacities"]) for row in capacities] != [3, 2, 3, 3]:
        raise ValueError("参考案例数量与本次固定CUDA布局不符")
    if growth["iterations"] != 3654 or [row["iteration"] for row in growth["checkpoints"]] != CHECKPOINTS:
        raise ValueError("增长序列必须包含3654轮和已选6个检查点")
    if growth["denominator_measurement"] != "cost_after_each_split;includes_initial":
        raise ValueError("成本分母统计口径与本轮GPU不符")
    lines, layout = [], {"schema": "gpu_finite_attribution_input_layout_v1", "rounding": [], "splits": [],
                         "capacity_allocations_candidate": []}

    def nonnegative(value: dict, scale: int) -> int:
        result = at_scale(value, scale)
        if result < 0:
            raise ValueError("分配材料须符合公共helper的非负前提")
        return result

    for row in rounds:
        scale = max(12, decimal_input(row["input"])[2])
        coefficient = at_scale(row["input"], scale)
        lines.append(f"{abs(coefficient)} {10 ** (scale - 12)} {int(coefficient < 0)} {scale}")
        layout["rounding"].append({"id": row["id"], "scale": scale, "input": amount(row["input"], scale)})
    for row in splits:
        scale = max(12, decimal_input(row["total"])[2])
        weight_scale = max(decimal_input(row[key])[2] for key in ("part", "whole"))
        total = nonnegative(row["total"], scale)
        part, whole = (nonnegative(row[key], weight_scale) for key in ("part", "whole"))
        if whole == 0 or part > whole:
            raise ValueError("分配权重不符合公共helper前提")
        lines.append(f"{total} {part} {whole} {10 ** (scale - 12)} {scale}")
        layout["splits"].append({"id": row["id"], "scale": scale, "weight_scale": weight_scale,
                                 "total_coefficient": str(total), "part_coefficient": str(part),
                                 "whole_coefficient": str(whole)})
    for row in capacities:
        scale = max(12, *(decimal_input(value)[2] for value in [row["total"], *row["capacities"]]))
        total = nonnegative(row["total"], scale)
        values = [nonnegative(value, scale) for value in row["capacities"]]
        if total > sum(values):
            raise ValueError("容量材料不符合公共helper前提")
        lines.append(f"{total} {10 ** (scale - 12)} {scale} {len(values)} " + " ".join(map(str, values)))
        layout["capacity_allocations_candidate"].append({"id": row["id"], "scale": scale,
                                                         "total_coefficient": str(total),
                                                         "capacities": list(map(str, values))})
    scale = max(12, *(decimal_input(growth[key])[2] for key in ("initial_cost", "added_cost_per_iteration")))
    weight_scale = max(decimal_input(growth[key])[2] for key in ("part", "whole"))
    initial, added = (nonnegative(growth[key], scale) for key in ("initial_cost", "added_cost_per_iteration"))
    part, whole = (nonnegative(growth[key], weight_scale) for key in ("part", "whole"))
    if (initial, added, part, whole, scale, weight_scale) != (1500000000000, 4000000000, 1, 501, 12, 0):
        raise ValueError("增长材料不是本轮已指定的1.5/+0.004/1:501序列")
    lines.append(f"{initial} {added} {part} {whole} {10 ** (scale - 12)} {10 ** scale} {scale} 3654 "
                 + " ".join(map(str, CHECKPOINTS)))
    layout["growth"] = {"scale": scale, "initial_coefficient": str(initial), "added_coefficient": str(added),
                        "part_coefficient": str(part), "whole_coefficient": str(whole),
                        "iterations": 3654, "checkpoints": CHECKPOINTS}
    layout["scope"] = "仅进行精确表示转换和输入前提核对；不计算RNE、分配结果或递推期望值"
    return "\n".join(lines) + "\n", layout


def compare(actual: dict, reference: dict, layout: dict) -> dict:
    failures = []

    def equal(label, got, expected):
        if json.dumps(got, sort_keys=True) != json.dumps(expected, sort_keys=True):
            failures.append({"field": label, "actual": got, "expected": expected})

    equal("schema", actual.get("schema"), "gpu_finite_attribution_sample_v1")
    for key, expected in {"bits": 256, "wide_bits": 512, "threads_per_integer": 8, "block_threads": 32,
                          "h2d_calls": 1, "d2h_calls": 2, "kernels_per_iteration": 1,
                          "growth_steps_per_iteration": 3654, "arithmetic_fault": 0}.items():
        equal(key, actual.get(key), expected)
    equal("rounding", actual.get("rounding"), [amount(row["rounded"], source["scale"])
          for row, source in zip(reference["rounding"], layout["rounding"], strict=True)])
    equal("splits", actual.get("splits"), [{key: amount(row[key], source["scale"])
          for key in ("allocated", "remaining")}
          for row, source in zip(reference["splits"], layout["splits"], strict=True)])
    equal("capacity_allocations_candidate", actual.get("capacity_allocations_candidate"),
          [[amount(value, source["scale"]) for value in row["allocated"]]
           for row, source in zip(reference["capacity_allocations_candidate"],
                                  layout["capacity_allocations_candidate"], strict=True)])
    growth, scale = reference["growth"], layout["growth"]["scale"]
    expected_growth = {key: amount(growth[key], scale) for key in
                       ("initial_cost", "final_cost", "cumulative_allocated", "original_total_cost")}
    expected_growth.update({"iterations": growth["iterations"],
                           "max_persisted_denominator_bits": growth["max_persisted_denominator_bits"],
                           "checkpoints": [{"iteration": row["iteration"],
                              "cost": amount(row["cost"], scale),
                              "cumulative_allocated": amount(row["cumulative_allocated"], scale)}
                             for row in growth["checkpoints"]]})
    equal("growth", actual.get("growth"), expected_growth)
    return {"passed": not failures, "failures": failures,
            "compared": ["6项正负最近偶数舍入", "6项分配与精确余量", "4项有容量上限的顺序分配候选",
                         "3654轮初值/终值/累计/总额/6检查点/持久成本分母最大位数"],
            "scope": "只与公共CPU helper的实际结果比较；无Python业务oracle"}


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
    report = {"schema": "gpu_finite_attribution_sample_report_v1", "scope": SCOPE, "status": "started"}
    try:
        reference = json.loads(args.cpu_reference.read_text())
        material = json.loads(args.cgbn_record.read_text())
        if material["commit"] != CGBN_COMMIT or material.get("pr35_applied") is not False:
            raise ValueError("CGBN材料记录不是本次批准的原始提交")
        text, layout = input_material(reference)
        (output / "input.txt").write_text(text)
        dump(output / "input-layout.json", layout)
        dump(output / "cpu-reference.json", reference)
        files = (SOURCE, HEADER, Path(__file__).resolve(), REPO / "model-train/scripts/gpu_exact_sample.py",
                 REPO / "contracts/src/finite_attribution.rs", REPO / "contracts/src/economics.rs",
                 REPO / "contracts/examples/finite_attribution_reference.rs")
        report["source"] = {
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
            "files_sha256": {str(path.relative_to(REPO)): sha(path) for path in files},
            "cpu_reference_sha256": sha(args.cpu_reference), "cgbn_commit_from_material": material["commit"],
            "cgbn_record_sha256": sha(args.cgbn_record),
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
        binary = output / "gpu-finite-attribution-sample"
        command = [str(args.nvcc), "-std=c++17", "-O2", "-lineinfo", "-arch=sm_120",
                   "-I", str(args.cgbn_root / "include"), "-I", str(args.gmp_prefix / "usr/include/x86_64-linux-gnu"),
                   "-I", str(args.gmp_prefix / "usr/include"), str(SOURCE), "-L", str(library),
                   "-Xlinker", "-rpath," + str(library), "-lgmp", "-o", str(binary)]
        report["compile_command"] = command
        report["compile_wall_seconds"] = run(command, output / "compile.log", env, args.progress_seconds, "有限定点CUDA编译")
        report["binary_sha256"] = sha(binary)
        report["devices"] = []
        for device in args.devices:
            log = output / f"device-{device}.log"
            wall = run([str(binary), str(output / "input.txt"), str(device), str(args.warmups), str(args.iterations)],
                       log, env, args.progress_seconds, f"GPU {device}有限定点诊断")
            rows = [line for line in log.read_text().splitlines()
                    if line.startswith('{"schema":"gpu_finite_attribution_sample_v1"')]
            if len(rows) != 1:
                raise ValueError("GPU日志没有唯一完整结果JSON")
            actual = json.loads(rows[0])
            comparison = compare(actual, reference, layout)
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
            "iteration": "一个kernel执行全部有限案例和3654次连续成本更新；不是完整100ms帧或训练步",
            "timing": "编译墙钟、预热/稳态CUDA事件、分配传输区间和进程墙钟分别记录；含诊断守恒及每轮分母GCD统计",
            "memory": "显式设备分配不含CUDA上下文/模块/事件，不是整卡峰值显存",
            "transfer": "输入一次H2D；全程设备内依赖；结束时输出与错误报告各一次D2H",
            "capacity": "256位整数/512位完整乘积仅证明当前有限材料；未定义生产域上限或生产拒绝规则",
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
