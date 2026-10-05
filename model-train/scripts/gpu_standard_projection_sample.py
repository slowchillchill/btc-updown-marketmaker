#!/usr/bin/env python3
"""#181 标准CUDA末端特征有限诊断：分开记录FP64候选与FP32对照，不判定盈利等价。"""
from __future__ import annotations

import argparse
from decimal import Decimal, localcontext
from fractions import Fraction
import json
import math
import os
from pathlib import Path
import struct
import subprocess

from gpu_exact_sample import REPO, dump, run, sha

SOURCE = REPO / "model-train/native-inputs/gpu-diagnostics/standard_projection_sample.cu"
HEADER = REPO / "contracts/gpu-diagnostics/standard_projection.cuh"
METHODS = ("fp64_then_fp32", "fp32_control")
SCOPE = "19项既有CPU位模式与3项有限技术探针；非生产后端、全域误差保证、训练或收益验收"


def bits32(value: float) -> int:
    return struct.unpack("!I", struct.pack("!f", value))[0]


def bits64(value: float) -> int:
    return struct.unpack("!Q", struct.pack("!d", value))[0]


def from_bits(value: int, width: int) -> float:
    if type(value) is not int or not 0 <= value < 1 << width:
        raise ValueError(f"无效binary{width}位模式")
    return struct.unpack("!f" if width == 32 else "!d", value.to_bytes(width // 8, "big"))[0]


def ordered_bits(value: int, width: int) -> int:
    sign = 1 << (width - 1)
    return ((1 << width) - 1) ^ value if value & sign else value | sign


def describe(value_bits: int, width: int) -> dict:
    value = from_bits(value_bits, width)
    classification = "nan" if math.isnan(value) else "infinity" if math.isinf(value) else "zero" if value == 0 else "finite"
    return {"bits_u" + str(width): value_bits, "bits_hex": f"0x{value_bits:0{width // 4}x}",
            "value": value if math.isfinite(value) else None, "classification": classification,
            "negative": bool(value_bits & (1 << (width - 1)))}


def difference(actual_bits: int, reference_bits: int, width: int) -> dict:
    actual, reference = from_bits(actual_bits, width), from_bits(reference_bits, width)
    finite = math.isfinite(actual) and math.isfinite(reference)
    return {"actual": describe(actual_bits, width), "reference": describe(reference_bits, width),
            "same_bits": actual_bits == reference_bits,
            "absolute_difference": abs(actual - reference) if finite else None,
            "relative_difference": abs((actual - reference) / reference) if finite and reference != 0 else None,
            "ulp_distance": abs(ordered_bits(actual_bits, width) - ordered_bits(reference_bits, width)) if finite else None}


def technical_reference(operation: str, numerator: int, denominator: int) -> tuple[int, dict]:
    """新增有限探针使用标准Decimal，双精度等级交叉核对；不实现另一套log/sqrt算法。"""
    evaluations = []
    for precision in (128, 192):
        with localcontext() as context:
            context.prec = precision
            value = Decimal(numerator) / Decimal(denominator)
            if operation == "sqrt_log_f32":
                result = (Decimal(1) + value.sqrt()).ln()
            else:
                result = (Decimal(1) + abs(value)).ln()
                if value < 0:
                    result = -result
            evaluations.append({"decimal_precision": precision, "decimal_result": str(result),
                                "bits_u32": bits32(float(result))})
    if evaluations[0]["bits_u32"] != evaluations[1]["bits_u32"]:
        raise ValueError("新增有限探针的两档Decimal参考未一致；保留缺口，不生成期望位模式")
    return evaluations[0]["bits_u32"], {
        "producer": "Python标准Decimal除法/sqrt/ln；128与192位十进制计算后经binary64转binary32",
        "guarantee": "仅是这3项有限探针的高精度交叉核对；非公共精确CPU合同或全域正确舍入证明",
        "evaluations": evaluations,
    }


def limb_words(value: int) -> list[int]:
    value = abs(value)
    words = []
    while value:
        words.append(value & 0xffffffff)
        value >>= 32
    return words or [0]


def input_material(reference: dict) -> tuple[str, dict]:
    if reference["schema"] in ("projection_capture_v1", "projection_sensitivity_run_v1"):
        capture = reference.get("capture", reference)
        rows = [{"id": f"captured-{index}", "operation": row["operation"], "input": row["input"],
                 "reference_bits_u32": row["bits_u32"], "reference_source": {
                     "kind": "captured_public_exact_cpu", "contexts": row["contexts"], "calls": row["calls"]}}
                for index, row in enumerate(capture["rows"])]
        if not rows:
            raise ValueError("真实组件捕获为空")
        return encode_material(rows, "真实公共夹具/Actor捕获的有限末端输入；非训练数据或全域误差证明")
    if reference["schema"] != "gpu_exact_numeric_reference_v1":
        raise ValueError("CPU参考schema不符")
    selected = reference["deferred_gpu_scope"]
    if len(selected["signed_log_f32"]) != 15 or len(selected["sqrt_log_f32"]) != 4:
        raise ValueError("参考不是原有15项signed-log和4项sqrt-log")
    rows = []
    for operation in ("signed_log_f32", "sqrt_log_f32"):
        for index, original in enumerate(selected[operation]):
            if original["operation"] != operation:
                raise ValueError("参考operation不匹配")
            rows.append({"id": f"original-{operation}-{index}", "operation": operation,
                         "input": original["input"], "reference_bits_u32": original["bits_u32"],
                         "reference_source": {"kind": "existing_public_exact_cpu",
                             "json_path": f"deferred_gpu_scope.{operation}[{index}]"}})
    for name, operation, numerator, denominator in (
        ("large-integers-ordinary-ratio", "signed_log_f32", (7 << 1100) + 1, (10 << 1100) + 3),
        ("fp32-square-underflow", "sqrt_log_f32", 1, 10 ** 56),
        ("fp32-square-overflow", "sqrt_log_f32", 10 ** 40, 1),
    ):
        expected_bits, provenance = technical_reference(operation, numerator, denominator)
        rows.append({"id": name, "operation": operation,
                     "input": {"numerator": str(numerator), "denominator": str(denominator)},
                     "reference_bits_u32": expected_bits,
                     "reference_source": {"kind": "finite_decimal_technical_probe", **provenance}})
    return encode_material(rows, SCOPE)


def encode_material(rows: list[dict], scope: str) -> tuple[str, dict]:
    lines = [str(len(rows))]
    total_words = 0
    for index, row in enumerate(rows):
        if row["operation"] not in ("signed_log_f32", "sqrt_log_f32"):
            raise ValueError("未支持的末端投影运算")
        numerator, denominator = (int(row["input"][key]) for key in ("numerator", "denominator"))
        if denominator <= 0 or (row["operation"] == "sqrt_log_f32" and numerator < 0):
            raise ValueError("参考材料不符合选定函数定义域")
        nwords, dwords = limb_words(numerator), limb_words(denominator)
        if max(len(nwords), len(dwords)) > 40:
            raise ValueError("超出本轮有限诊断布局；不是生产金额或位宽限制")
        lines.append(" ".join(map(str, [int(row["operation"] == "sqrt_log_f32"), int(numerator < 0),
                                       len(nwords), len(dwords), *nwords, *dwords])))
        total_words += len(nwords) + len(dwords)
        row.update({"index": index, "numerator_bits": abs(numerator).bit_length(),
                    "denominator_bits": denominator.bit_length(),
                    "ratio_reference_f64_bits": bits64(float(Fraction(numerator, denominator))),
                    "reference": describe(row["reference_bits_u32"], 32)})
    return "\n".join(lines) + "\n", {
        "schema": "gpu_standard_projection_input_layout_v1", "rows": rows, "total_limb_words": total_words,
        "encoding": "完整整数的uint32小端limb；主机只编码，不把浮点比值交给GPU",
        "ratio_reference": "CPU Fraction整体转binary64，只供GPU转换误差比较，不写入GPU输入",
        "gpu_conversion": "设备端取各整数最多53个有效高位，截去低位，相除后ldexp指数缩放；不承诺正确舍入",
        "scope": scope,
    }


def compare(actual: dict, layout: dict) -> dict:
    failures, comparisons, summaries = [], [], {}
    if (actual.get("schema") != "gpu_standard_projection_sample_v1" or actual.get("sample_count") != len(layout["rows"])
            or actual.get("h2d_calls") != 2 or actual.get("d2h_calls") != 2):
        raise ValueError("GPU输出schema、有限布局或传输次数不符")
    for method in METHODS:
        if len(actual[method]["rows"]) != len(layout["rows"]):
            raise ValueError("GPU输出样本数不匹配")
    for index, expected in enumerate(layout["rows"]):
        row = {"id": expected["id"], "operation": expected["operation"], "input": expected["input"],
               "reference_source": expected["reference_source"]}
        for method in METHODS:
            got = actual[method]["rows"][index]
            if got["index"] != index:
                raise ValueError("GPU样本顺序不匹配")
            output = difference(got["result_bits"], expected["reference_bits_u32"], 32)
            output["ratio_conversion"] = difference(got["ratio_f64_bits"], expected["ratio_reference_f64_bits"], 64)
            width = 32 if method == "fp32_control" else 64
            output["stages"] = {"transform": describe(got["transform_bits"], width)}
            if method == "fp32_control":
                output["stages"]["input"] = describe(got["input_f32_bits"], 32)
            if expected["operation"] == "sqrt_log_f32":
                output["stages"]["sqrt"] = describe(got["sqrt_bits"], width)
            output["zero_from_nonzero_reference"] = (output["actual"]["classification"] == "zero"
                                                     and output["reference"]["classification"] != "zero")
            row[method] = output
            if method == "fp64_then_fp32":
                if output["actual"]["classification"] in ("nan", "infinity"):
                    failures.append({"id": expected["id"], "reason": "有限候选产生非有限结果"})
                elif output["actual"]["negative"] != output["reference"]["negative"]:
                    failures.append({"id": expected["id"], "reason": "有限候选结果符号错误"})
                elif output["zero_from_nonzero_reference"]:
                    failures.append({"id": expected["id"], "reason": "有限候选丢失本次非零参考"})
        comparisons.append(row)
    for method in METHODS:
        values = [row[method] for row in comparisons]
        summaries[method] = {
            "samples": len(values), "identical_bits": sum(value["same_bits"] for value in values),
            "nonfinite_results": sum(value["actual"]["classification"] in ("nan", "infinity") for value in values),
            "zero_from_nonzero_reference": sum(value["zero_from_nonzero_reference"] for value in values),
            "max_absolute_difference_finite_results": max(value["absolute_difference"] for value in values
                                                         if value["absolute_difference"] is not None),
            "max_relative_difference_nonzero_finite_reference": max(value["relative_difference"] for value in values
                                                                    if value["relative_difference"] is not None),
            "max_ulp_distance_finite_results": max(value["ulp_distance"] for value in values if value["ulp_distance"] is not None),
            "ratio_conversion_identical_f64_bits": sum(value["ratio_conversion"]["same_bits"] for value in values),
            "max_ratio_conversion_ulp_distance": max(value["ratio_conversion"]["ulp_distance"] for value in values
                                                       if value["ratio_conversion"]["ulp_distance"] is not None),
        }
    return {"measurement_completed": True, "candidate_execution_failures": failures,
            "numerical_acceptance": "未设统一误差阈值；误差仅为有限样本测量，不判定生产或收益等价",
            "fp32_control_scope": "FP32上溢、下溢等损失如实输出，不按FP64候选失败处理",
            "summary": summaries, "rows": comparisons}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("cpu-reference", "nvcc", "output-dir"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--devices", nargs="+", type=int, default=[0])
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--progress-seconds", type=float, default=10)
    args = parser.parse_args()
    if args.warmups < 1 or args.iterations < 1 or not math.isfinite(args.progress_seconds) or args.progress_seconds <= 0:
        parser.error("次数与进度间隔须为正且有限")
    if not 1 <= len(args.devices) <= 2 or len(set(args.devices)) != len(args.devices) or any(i < 0 for i in args.devices):
        parser.error("逐卡验证一或两张不同的卡，设备序号须非负")
    output = args.output_dir.resolve()
    if not output.is_relative_to(Path("/root/autodl-tmp")) or output == Path("/root/autodl-tmp"):
        parser.error("输出必须是 /root/autodl-tmp 下的新目录")
    args.cpu_reference, args.nvcc = args.cpu_reference.resolve(), args.nvcc.resolve()
    output.mkdir(parents=True, exist_ok=False)
    report = {"schema": "gpu_standard_projection_sample_report_v1", "scope": SCOPE, "status": "started"}
    try:
        reference = json.loads(args.cpu_reference.read_text())
        material, layout = input_material(reference)
        report["scope"] = layout["scope"]
        (output / "input.txt").write_text(material)
        dump(output / "input-layout.json", layout)
        dump(output / "cpu-reference.json", reference)
        files = (SOURCE, HEADER, Path(__file__).resolve(), REPO / "model-train/scripts/gpu_exact_sample.py")
        relative_files = [str(path.relative_to(REPO)) for path in files]
        report["source"] = {
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
            "files_sha256": {str(path.relative_to(REPO)): sha(path) for path in files},
            "executed_files_git_status": subprocess.check_output(
                ["git", "status", "--porcelain", "--", *relative_files], cwd=REPO, text=True),
            "cpu_reference_sha256": sha(args.cpu_reference), "input_material_sha256": sha(output / "input.txt"),
        }
        env = os.environ.copy()
        for variable, directory in (("TMPDIR", "tmp"), ("CUDA_CACHE_PATH", "cuda-cache")):
            (output / directory).mkdir()
            env[variable] = str(output / directory)
        report["nvcc_version"] = subprocess.check_output([str(args.nvcc), "--version"], text=True, env=env)
        binary = output / "gpu-standard-projection-sample"
        command = [str(args.nvcc), "-std=c++17", "-O2", "-lineinfo", "-arch=sm_120",
                   "--ftz=false", "--prec-div=true", "--prec-sqrt=true", str(SOURCE), "-o", str(binary)]
        report["compile_command"] = command
        report["compile_wall_seconds"] = run(command, output / "compile.log", env, args.progress_seconds, "标准数学CUDA编译")
        report["binary_sha256"] = sha(binary)
        report["devices"] = []
        for device in args.devices:
            log = output / f"device-{device}.log"
            command = [str(binary), str(output / "input.txt"), str(device), str(args.warmups), str(args.iterations)]
            wall = run(command, log, env, args.progress_seconds, f"GPU {device}标准数学有限诊断")
            rows = [line for line in log.read_text().splitlines()
                    if line.startswith('{"schema":"gpu_standard_projection_sample_v1"')]
            if len(rows) != 1:
                raise ValueError("GPU日志没有唯一完整结果JSON")
            actual = json.loads(rows[0])
            if (actual.get("device"), actual.get("warmups"), actual.get("iterations")) != (device, args.warmups, args.iterations):
                raise ValueError("GPU设备或迭代次数与命令不匹配")
            comparison = compare(actual, layout)
            entry = {"output": actual, "comparison": comparison, "process_wall_seconds": wall, "command": command}
            report["devices"].append(entry)
            dump(output / f"device-{device}.json", entry)
            if comparison["candidate_execution_failures"]:
                raise RuntimeError(f"GPU {device}有限FP64候选出现非有限、符号或非零值丢失问题")
        report["measurement_scope"] = {
            "iteration": "每种方法单独测量一个kernel：有限输入各由一个线程计算；非100ms帧、训练步或持续饱和吞吐",
            "timing": "两种方法依次测量，包含启动间隔和比值转换；编译/预热/稳态/分配传输区间/进程墙钟分别记录",
            "memory": "4次显式设备分配；不含CUDA上下文/模块/事件，不是整卡峰值显存",
            "transfer": "元数据和完整整数limb各一次H2D；两种方法完成后各一次结果D2H；无主机预算浮点比值输入",
            "fp32_control": "使用相同设备端FP64比值转换后舍入为FP32，再调用真实CUDA sqrtf/log1pf",
            "reference": ("实际公共组件捕获值沿用同一次原精确CPU参考" if reference["schema"] != "gpu_exact_numeric_reference_v1"
                          else "原19项沿用公共精确CPU位模式；3项新增技术探针由标准Decimal高精度交叉核对"),
            "scope": layout["scope"],
        }
        report["status"] = "completed_measurement_only"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = f"{type(error).__name__}: {error}"
        raise
    finally:
        dump(output / "report.json", report)
        print(f"报告：{output / 'report.json'}", flush=True)


if __name__ == "__main__":
    main()
