#!/usr/bin/env python3
"""有限真实组件敏感性：导出实际CUDA替换表，或比较无缺项的独立根报告。"""
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

from gpu_exact_sample import dump, sha
from gpu_standard_projection_sample import METHODS, describe


def export_tables(directory: Path) -> dict:
    report_file, layout_file = directory / "report.json", directory / "input-layout.json"
    report, layout = json.loads(report_file.read_text()), json.loads(layout_file.read_text())
    if report["schema"] != "gpu_standard_projection_sample_report_v1" or report["status"] != "completed_measurement_only":
        raise ValueError("需要已完成的实际CUDA标准投影诊断报告")
    if any(row["reference_source"]["kind"] != "captured_public_exact_cpu" for row in layout["rows"]):
        raise ValueError("组件替换表只能来自真实公共组件捕获，不能用22个独立标量代替")
    outputs = []
    for device in report["devices"]:
        actual = device["output"]
        for policy in METHODS:
            observed = actual[policy]["rows"]
            if len(observed) != len(layout["rows"]):
                raise ValueError("实际CUDA结果与捕获材料数量不符")
            nonfinite = [index for index, row in enumerate(observed)
                         if describe(row["result_bits"], 32)["classification"] in ("nan", "infinity")]
            if nonfinite:
                outputs.append({"device": actual["device"], "policy": policy,
                                "status": "nonfinite_no_component_table", "nonfinite_indices": nonfinite})
                continue
            rows = []
            for index, (source, result) in enumerate(zip(layout["rows"], observed, strict=True)):
                if result["index"] != index:
                    raise ValueError("CUDA结果顺序不符")
                rows.append({"operation": source["operation"], "input": source["input"],
                             "bits_u32": result["result_bits"], "reference_bits_u32": source["reference_bits_u32"]})
            table = {"schema": "projection_replacements_v1", "policy": policy, "device": actual["device"],
                     "source_commit": report["source"]["commit"], "gpu_report_sha256": sha(report_file),
                     "input_layout_sha256": sha(layout_file), "rows": rows,
                     "scope": "实际CUDA有限输出；仅供cfg(test)独立根回填，不是生产全域政策"}
            destination = directory / f"device-{actual['device']}-{policy}-replacements.json"
            if destination.exists():
                raise ValueError(f"替换表已存在，保留原证据：{destination}")
            dump(destination, table)
            outputs.append({"device": actual["device"], "policy": policy, "status": "exported",
                            "path": str(destination), "sha256": sha(destination), "rows": len(rows)})
    return {"schema": "projection_replacement_export_v1", "outputs": outputs}


def numerical_difference(left, right) -> dict:
    """只描述同形状特征和模型输出的实际差值，不定义统一容差。"""
    count = different = 0
    maximum = 0.0

    def walk(a, b):
        nonlocal count, different, maximum
        if isinstance(a, list):
            if not isinstance(b, list) or len(a) != len(b):
                raise ValueError("数值数组形状不一致")
            for x, y in zip(a, b, strict=True):
                walk(x, y)
        else:
            if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
                raise ValueError("数值数组包含非数值项")
            if not math.isfinite(a) or not math.isfinite(b):
                raise ValueError("组件数值比较包含非有限值")
            count += 1
            different += a != b
            maximum = max(maximum, abs(a - b))
    walk(left, right)
    return {"elements": count, "different_elements": different, "maximum_absolute_difference": maximum}


def compare_runs(reference: dict, candidate: dict) -> dict:
    for report in (reference, candidate):
        if report["schema"] != "projection_sensitivity_run_v1" or report["status"] != "complete":
            raise ValueError("只能比较完整执行报告；发现轮或失败轮不能计入敏感性证据")
        if report["capture"]["missing_replacements"] != 0:
            raise ValueError("候选存在未取得CUDA结果的末端输入")
    left, right = reference["components"], candidate["components"]
    if len(left["groups"]) != len(right["groups"]):
        raise ValueError("公共组数量不一致")
    invariants = {"same_fixed_weights": left["weights_sha256"] == right["weights_sha256"],
                  "same_component_environment": left["environment"] == right["environment"],
                  "same_initialization_seed": left["initialization_seed"] == right["initialization_seed"],
                  "same_sampling_seed": left["sampling_seed"] == right["sampling_seed"],
                  "same_external_intent": reference["external_intent"] == candidate["external_intent"],
                  "weights_unchanged": left["weights_unchanged"] and right["weights_unchanged"]}
    rows = []
    for index, (a, b) in enumerate(zip(left["groups"], right["groups"], strict=True)):
        if len(a["features"]["rows"]) != len(b["features"]["rows"]):
            raise ValueError("公共组行数不一致")
        row = {"index": index, "features": [], "states": [], "common": None,
               "cancel_features": numerical_difference(a["features"]["cancel"], b["features"]["cancel"]),
               "sample_path_same": (a["sample"] is None and b["sample"] is None)
                    or (a["sample"] is not None and b["sample"] is not None and a["sample"]["path"] == b["sample"]["path"])}
        for x, y in zip(a["features"]["rows"], b["features"]["rows"], strict=True):
            invariants[f"group_{index}_row_{len(row['features'])}_role_and_identity"] = (
                x["role"] == y["role"] and x["hidden_state_key"] == y["hidden_state_key"])
            row["features"].append({name: numerical_difference(x[name], y[name])
                                    for name in ("slow", "fast", "rule", "book_levels")})
        for name in ("cutoff", "current", "old", "pre_frame_hidden_ops", "post_frame_hidden_ops"):
            invariants[f"group_{index}_{name}"] = a["features"][name] == b["features"][name]
        for x, y in zip(a["states"], b["states"], strict=True):
            row["states"].append({name: numerical_difference(x[name], y[name]) for name in ("z", "h")})
        if (a["common"] is None) != (b["common"] is None):
            raise ValueError("共同Actor路径缺失")
        if a["common"] is not None:
            x, y = a["common"], b["common"]
            same_path = x["path"] == y["path"]
            same_intent = x["intent"] == y["intent"]
            steps = []
            for first, second in zip(x["steps"], y["steps"], strict=True):
                support_same = first["support"] == second["support"]
                exact_state_same = first["validation_state"] == second["validation_state"]
                invariants[f"group_{index}_common_step_{len(steps)}_support"] = support_same
                invariants[f"group_{index}_common_step_{len(steps)}_exact_state"] = exact_state_same
                invariants[f"group_{index}_common_step_{len(steps)}_price_coordinates"] = (
                    first["price_indices"] == second["price_indices"])
                steps.append({"support_same": support_same, "exact_state_same": exact_state_same,
                              "greedy_index_same": first["greedy_index"] == second["greedy_index"],
                              **{name: numerical_difference(first[name], second[name]) for name in
                                 ("prefix_features", "price_fee_features", "logits", "probabilities")}})
            invariants[f"group_{index}_common_path"] = same_path
            invariants[f"group_{index}_common_intent"] = same_intent
            row["common"] = {"path_same": same_path, "intent_same": same_intent, "steps": steps,
                              "log_prob": numerical_difference(x["log_prob"], y["log_prob"])}
        for policy, group in (("reference", a), ("candidate", b)):
            invariants[f"group_{index}_{policy}_hidden_preserved"] = group["parameter_calls_preserved_hidden"]
            if group["sample"] is not None:
                sample = group["sample"]
                for field in ("log_prob_equal", "replay_preserved_rng"):
                    invariants[f"group_{index}_{policy}_{field}"] = sample[field]
                invariants[f"group_{index}_{policy}_gradient_equal"] = sample["gradient_check"]["all_equal"]
        rows.append(row)
    return {"schema": "projection_sensitivity_comparison_v1", "reference_policy": reference["policy"],
            "candidate_policy": candidate["policy"], "exact_component_invariants": invariants,
            "all_exact_component_invariants_hold": all(invariants.values()), "groups": rows,
            "scope": "有限真实公共组件固定模型敏感性；各自采样只形成意图，未自主改变账户；不设统一数值容差或收益结论"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_subparsers(dest="mode", required=True)
    tables = modes.add_parser("tables", help="从实际CUDA捕获诊断导出逐卡替换表")
    tables.add_argument("--gpu-output-dir", required=True, type=Path)
    compare = modes.add_parser("compare", help="比较基准和无缺项的候选组件报告")
    compare.add_argument("--reference", required=True, type=Path)
    compare.add_argument("--candidate", required=True, type=Path)
    compare.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.mode == "tables":
        result = export_tables(args.gpu_output_dir.resolve())
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        if args.output.exists():
            parser.error("比较输出已存在；保留原证据")
        result = compare_runs(json.loads(args.reference.read_text()), json.loads(args.candidate.read_text()))
        result["source_reports"] = {"reference_sha256": sha(args.reference), "candidate_sha256": sha(args.candidate)}
        dump(args.output, result)
        print(json.dumps({"output": str(args.output), "all_exact_component_invariants_hold":
                          result["all_exact_component_invariants_hold"]}, ensure_ascii=False))
        if not result["all_exact_component_invariants_hold"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
