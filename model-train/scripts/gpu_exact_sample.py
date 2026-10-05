#!/usr/bin/env python3
"""#181 有限 CGBN 精确样板：编译、逐卡运行及 CPU 参考比较，不是训练入口。"""
from __future__ import annotations

import argparse
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
CGBN_COMMIT = "114601b07ae8c3e4d46b250208ea8d9def7a12f0"
SOURCE = REPO / "model-train/native-inputs/gpu-diagnostics/exact_buy_sample.cu"
HEADER = REPO / "sim-system/gpu-diagnostics/exact_buy_cost.cuh"
SCOPE = "固定数量/类型后的价格支持与意图、四笔公共LIMIT费用、独立宽整数探针；非全域GPU后端或训练"


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def dump(path: Path, value) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")


def decimal_units(value, factor=1) -> int:
    exact = Fraction(str(value)) * factor
    if exact.denominator != 1:
        raise ValueError("参考输入不能精确表示为所需整数单位")
    return exact.numerator


def unit_text(value: int, denominator: int) -> str:
    with localcontext() as context:
        context.prec = 200
        return format((Decimal(value) / Decimal(denominator)).normalize(), "f")


def input_material(reference: dict, wide: dict) -> tuple[str, dict]:
    # 固定技术用例的输入边界；不是生产交易接纳规则。
    inp = reference["input"]
    if (reference["schema"] != "gpu_exact_buy_reference_v1"
        or inp["quantity_centishares"] != "500" or Fraction(inp["cash_pusd"]) != 2
        or inp["fee_exponent"] != 1 or Fraction(inp["fee_rate"]) != Fraction(7, 100)
        or inp["ticks_pusd_per_share"] != ["0.01", "0.01"]
        or inp["variants"] != ["GtcLimit", "FokMarket"] or inp["post_only"] is not False
        or Fraction(inp["market_buy_min_spend_pusd"]) != 1
        or Fraction(inp["limit_min_quantity_shares"]) != 5):
        raise ValueError("CPU参考不是本次已指定的普通费用/混合双腿用例")
    win = wide["input"]
    if (wide["schema"] != "gpu_wide_buy_reference_v1"
        or win["quantity_centishares"] != "7922816251426433759354395"
        or win["fee_rate"] != "79228162514264337593543950335"
        or win["fee_exponent"] != 1 or win["tick_pusd_per_share"] != "0.0001"
        or win["price_index"] != 9999 or win["post_only"] is not False):
        raise ValueError("宽整数费用参考不是既有超过u128的指定反例")
    costs = reference["limit_buy_cost_v1"]
    if [row["price_index"] for row in costs] != [19, 20, 25]:
        raise ValueError("三笔普通费用价格索引不匹配")
    rows = []
    rate = Fraction(inp["fee_rate"])
    for cost in costs:
        rows.append(f'{inp["quantity_centishares"]} {rate.numerator} {rate.denominator} 100 {cost["price_index"]}')
    wrate = Fraction(win["fee_rate"])
    rows.append(f'{win["quantity_centishares"]} {wrate.numerator} {wrate.denominator} 10000 {win["price_index"]}')
    rows.extend([str(decimal_units(inp["cash_pusd"], 1_000_000)),
                 str(decimal_units(inp["market_buy_min_spend_pusd"], 1_000_000))])
    # 纯算术探针。CPU Python整数只计算这些探针，不计算费用或价格支持。
    probes = {"add_a": (1 << 256) - 1, "add_b": 1,
              "multiply_a": (1 << 200) + 123, "multiply_b": (1 << 100) + 7,
              "divisor": (1 << 100) + 9, "round_n": (1 << 200) + 1, "round_d": 2}
    rows.extend(str(value) for value in probes.values())
    intent = reference["selected_intent"]
    rows.extend(order["token_id"] for order in intent["orders"])
    choices = reference["selected_path"][-2:]
    if choices != [18, 19]:
        raise ValueError("参考中的已选A/B价格路径不匹配")
    rows.append(f'{choices[0]} {choices[1]} {intent["condition_id"]}')
    product = probes["multiply_a"] * probes["multiply_b"]
    quotient, remainder = divmod(product, probes["divisor"])
    rq, rr = divmod(probes["round_n"], probes["round_d"])
    expected = {"sum": "0", "carry": 1,
                "product_low": str(product % (1 << 256)), "product_high": str(product >> 256),
                "quotient": str(quotient), "remainder": str(remainder),
                "rounded": str(rq + (rr * 2 >= probes["round_d"]))}
    return "\n".join(rows) + "\n", {
        "scope": "整数原语探针，不是费用规则或完整经济域的证明",
        "inputs": {key: str(value) for key, value in probes.items()}, "expected": expected,
        "multiply_result_bits": product.bit_length(), "wide_division_quotient_bits": quotient.bit_length(),
    }


def compare(actual: dict, reference: dict, wide: dict, probes: dict) -> dict:
    failures = []

    def equal(label, got, expected):
        # 序列化后比较，保留JSON标量类型，避免 Python True == 1 掩盖错误。
        if json.dumps(got, sort_keys=True) != json.dumps(expected, sort_keys=True):
            failures.append({"field": label, "actual": got, "expected": expected})

    equal("schema", actual["schema"], "gpu_exact_sample_v1")
    expected_costs = [{key: row[key] for key in ("principal_micro", "fee_1e5_units")}
                      for row in reference["limit_buy_cost_v1"]] + [wide["output"]]
    equal("public_limit_costs", actual["costs"], expected_costs)
    equal("full_999_price_mask", actual["mask"], reference["a_price_support"]["mask"])
    equal("integer_probes", actual["probes"], probes["expected"])
    selected = actual["selected"]
    equal("quantity_centishares", selected["quantity_centishares"], reference["input"]["quantity_centishares"])
    equal("selected_price_coordinates", [k - 1 for k in selected["price_indices"]], reference["selected_path"][-2:])
    equal("variants", selected["variants"], [0, 1])
    # 这里只把GPU输出的整数单位格式化为公共意图字段，不重新求费用或可负担性。
    intent = {"condition_id": selected["condition_id"], "orders": [
        {"token_id": selected["tokens"][0], "type": "GtcLimit",
         "limit_price_pusd_per_share": unit_text(selected["price_indices"][0], 100),
         "size_shares": unit_text(int(selected["quantity_centishares"]), 100),
         "post_only": selected["post_only"]},
        {"token_id": selected["tokens"][1], "type": "FokMarket",
         "spend_pusd": unit_text(int(selected["spend_b_micro"]), 1_000_000),
         "worst_price_pusd_per_share": unit_text(selected["price_indices"][1], 100)}]}
    equal("selected_intent", intent, reference["selected_intent"])
    return {"passed": not failures, "failures": failures, "selected_intent": intent,
            "compared": ["all four principal/fee integer pairs", "all 999 mask positions",
                         "selected quantity/types/prices/tokens/condition/amount", "wide arithmetic outputs"]}


def run(command: list[str], log: Path, env: dict, interval: float, phase: str) -> float:
    print(f"{phase}：开始；日志 {log}", flush=True)
    started = time.perf_counter()
    with log.open("w") as output:
        process = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT, env=env, cwd=REPO)
        while True:
            try:
                code = process.wait(timeout=interval)
                break
            except subprocess.TimeoutExpired:
                print(f"{phase}：已耗时 {time.perf_counter()-started:.1f} 秒；剩余时间未知", flush=True)
    if code:
        raise RuntimeError(f"{phase}退出码 {code}；保留日志 {log}")
    elapsed = time.perf_counter() - started
    print(f"{phase}：完成，{elapsed:.3f} 秒", flush=True)
    return elapsed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cpu-reference", required=True, type=Path)
    parser.add_argument("--wide-reference", required=True, type=Path)
    parser.add_argument("--cgbn-root", required=True, type=Path)
    parser.add_argument("--cgbn-record", required=True, type=Path)
    parser.add_argument("--gmp-prefix", required=True, type=Path)
    parser.add_argument("--nvcc", required=True, type=Path)
    parser.add_argument("--devices", nargs="+", type=int, default=[0])
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--progress-seconds", type=float, default=10)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    if args.warmups < 1 or args.iterations < 1 or args.progress_seconds <= 0 or any(i < 0 for i in args.devices):
        parser.error("次数、进度间隔须为正；设备序号须非负")
    if len(args.devices) > 2 or len(set(args.devices)) != len(args.devices):
        parser.error("一次只逐卡验证一或两张不同的卡")
    output = args.output_dir.resolve()
    if not output.is_relative_to(Path("/root/autodl-tmp")) or output == Path("/root/autodl-tmp"):
        parser.error("输出必须是 /root/autodl-tmp 下的新目录")
    for name in ("cgbn_root", "gmp_prefix", "nvcc", "cgbn_record", "cpu_reference", "wide_reference"):
        setattr(args, name, getattr(args, name).resolve())
    output.mkdir(parents=True, exist_ok=False)
    report = {"schema": "gpu_exact_sample_report_v1", "scope": SCOPE, "status": "started"}
    try:
        reference = json.loads(args.cpu_reference.read_text())
        wide = json.loads(args.wide_reference.read_text())
        material = json.loads(args.cgbn_record.read_text())
        if material["commit"] != CGBN_COMMIT or material.get("pr35_applied") is not False:
            raise ValueError("CGBN材料记录不是本次批准的原始提交")
        text, probes = input_material(reference, wide)
        (output / "input.txt").write_text(text)
        dump(output / "cpu-reference.json", reference)
        dump(output / "wide-reference.json", wide)
        dump(output / "integer-probes.json", probes)
        report["source"] = {
            "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPO, text=True).strip(),
            "files_sha256": {str(path.relative_to(REPO)): sha(path) for path in (SOURCE, HEADER, Path(__file__).resolve())},
            "cgbn_commit_from_material": material["commit"], "cgbn_record_sha256": sha(args.cgbn_record),
            "cgbn_headers_sha256": {str(path.relative_to(args.cgbn_root)): sha(path)
                                     for path in sorted((args.cgbn_root / "include").rglob("*")) if path.is_file()},
            "gmp_header_sha256": sha(args.gmp_prefix / "usr/include/x86_64-linux-gnu/gmp.h"),
            "gmp_library_sha256": sha(args.gmp_prefix / "usr/lib/x86_64-linux-gnu/libgmp.so"),
            "cpu_reference_sha256": sha(args.cpu_reference), "wide_reference_sha256": sha(args.wide_reference),
        }
        env = os.environ.copy()
        for variable, directory in (("TMPDIR", "tmp"), ("CUDA_CACHE_PATH", "cuda-cache")):
            (output / directory).mkdir()
            env[variable] = str(output / directory)
        library = args.gmp_prefix / "usr/lib/x86_64-linux-gnu"
        env["LD_LIBRARY_PATH"] = str(library) + (":" + env["LD_LIBRARY_PATH"] if env.get("LD_LIBRARY_PATH") else "")
        report["nvcc_version"] = subprocess.check_output([str(args.nvcc), "--version"], text=True, env=env)
        binary = output / "gpu-exact-sample"
        command = [str(args.nvcc), "-std=c++17", "-O2", "-lineinfo", "-arch=sm_120",
                   "-I", str(args.cgbn_root / "include"), "-I", str(args.gmp_prefix / "usr/include/x86_64-linux-gnu"),
                   "-I", str(args.gmp_prefix / "usr/include"), str(SOURCE), "-L", str(library),
                   "-Xlinker", "-rpath," + str(library), "-lgmp", "-o", str(binary)]
        report["compile_command"] = command
        report["compile_wall_seconds"] = run(command, output / "compile.log", env, args.progress_seconds, "CUDA编译")
        report["binary_sha256"] = sha(binary)
        report["devices"] = []
        for device in args.devices:
            log = output / f"device-{device}.log"
            wall = run([str(binary), str(output / "input.txt"), str(device), str(args.warmups), str(args.iterations)],
                       log, env, args.progress_seconds, f"GPU {device}有限诊断")
            rows = [line for line in log.read_text().splitlines() if line.startswith('{"schema":"gpu_exact_sample_v1"')]
            if len(rows) != 1:
                raise ValueError("GPU日志没有唯一完整结果JSON")
            actual = json.loads(rows[0])
            comparison = compare(actual, reference, wide, probes)
            entry = {"output": actual, "comparison": comparison, "process_wall_seconds": wall}
            report["devices"].append(entry)
            dump(output / f"device-{device}.json", entry)
            if not comparison["passed"]:
                raise RuntimeError(f"GPU {device}逐值比较失败")
        report["measurement_scope"] = {
            "iteration": "4笔费用（含极端反例）+999位mask+一个已指定双腿意图+宽整数原语；无RL环境步数",
            "timing": "编译墙钟单列；CUDA事件含每轮4次launch之间的排队间隔；未计成单个运算延迟",
            "memory": "owned_device_allocation_bytes为三个显式分配之和；不是整卡峰值，未含CUDA上下文/模块/事件",
            "transfer": "输入一次H2D；结束后输出和错误报告各一次D2H；GPU费用/支持/选择之间无CPU读取",
            "selection": "family、数量、GTC/FOK类型和两个待选坐标是给定用例输入；未实现网络或16叉数量选择链",
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
