#!/usr/bin/env python3
"""有限真实组件敏感性：导出实际CUDA替换表，或比较无缺项的独立根报告。"""
from __future__ import annotations

import argparse
import gzip
import hashlib
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


def read_group_stream(components: dict):
    """流式验证压缩文件摘要、逐组序号及完整尾记录；不会装入整窗。"""
    artifact = components["group_stream"]
    path = Path(artifact["path"])
    if artifact["compression"] != "gzip" or path.stat().st_size != artifact["bytes"]:
        raise ValueError("公共组件流格式或长度不符")
    compressed = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1 << 20), b""):
            compressed.update(block)
    if compressed.hexdigest() != artifact["sha256"]:
        raise ValueError("公共组件流压缩文件摘要不符")
    digest = hashlib.sha256()
    count = state_rows = decision_groups = checked_groups = 0
    ended = False
    try:
        with gzip.open(path, "rb") as source:
            first = source.readline()
            header = json.loads(first)
            if header["record_type"] != "header" or header["schema"] != "projection_component_stream_v1":
                raise ValueError("公共组件流头不符")
            if any(header[key] != components[key] for key in header if key != "record_type"):
                raise ValueError("公共组件流头与完成报告不符")
            digest.update(first)
            for line in source:
                record = json.loads(line)
                if record["record_type"] == "end":
                    if record["groups"] != count or record["sha256_before_end"] != digest.hexdigest():
                        raise ValueError("公共组件流尾计数或内容摘要不符")
                    if source.read(1):
                        raise ValueError("公共组件流尾后仍有数据")
                    ended = True
                    break
                if record["record_type"] != "group" or record["index"] != count:
                    raise ValueError("公共组件流组序号不连续")
                decisions = sum(row["role"] == "DecisionRequired" for row in record["features"]["rows"])
                expected_check = decisions > 0 and checked_groups < 32
                if decisions > 1 or record["gradient_checked"] != expected_check:
                    raise ValueError("局部梯度检查未严格覆盖前32个实际决定组")
                if len(record["states"]) != len(record["features"]["rows"]):
                    raise ValueError("公共组件流状态数与实际行数不符")
                state_rows += len(record["states"])
                decision_groups += decisions > 0
                checked_groups += record["gradient_checked"]
                digest.update(line)
                count += 1
                yield record
    except (EOFError, OSError, json.JSONDecodeError) as error:
        raise ValueError("公共组件流损坏或截断") from error
    if not ended or count != components["groups"]:
        raise ValueError("公共组件流缺少完整尾记录或完成组数不符")
    if (state_rows != components["state_rows"] or decision_groups != components["decision_groups"]
        or checked_groups != components["gradient_checked_groups"]
        or components["gradient_target_reached"] != (checked_groups == 32)):
        raise ValueError("公共组件流实际覆盖与完成报告不符")


def compare_stream_runs(reference: dict, candidate: dict) -> dict:
    for report in (reference, candidate):
        if report["schema"] != "projection_sensitivity_stream_run_v1" or report["status"] != "complete":
            raise ValueError("流式比较只接受完成报告，发现轮或失败轮不可计入证据")
        if (report["capture"]["missing_replacements"] != 0
            or not report["components"]["gradient_target_reached"]
            or report["components"]["gradient_checked_groups"] != 32):
            raise ValueError("替换表缺项或前32个决定组检查未完成")
    left, right = reference["components"], candidate["components"]
    if left["groups"] != right["groups"]:
        raise ValueError("公共组数量不一致")
    invariants = {
        "same_runtime_semantics": reference["runtime_semantics"] == candidate["runtime_semantics"],
        "same_runtime_semantics_sha256": reference["runtime_semantics_sha256"] == candidate["runtime_semantics_sha256"],
        "same_prepared_material": reference["prepared_material"]["sha256"] == candidate["prepared_material"]["sha256"],
        "same_source_scope": reference["source_scope"] == candidate["source_scope"],
        "same_common_paths": left["common_paths"] == right["common_paths"],
    }
    for name in ("operating_start_utc_ms", "operating_end_utc_ms", "native_root_started_at_utc_ms", "execution_history"):
        invariants[f"same_{name}"] = reference[name] == candidate[name]
    totals = {}
    failures = []
    failed_groups = changed_sample_paths = count = 0
    checked = []

    def merge(name, difference):
        target = totals.setdefault(name, {"elements": 0, "different_elements": 0,
                                          "maximum_absolute_difference": 0.0})
        for field in ("elements", "different_elements"):
            target[field] += difference[field]
        target["maximum_absolute_difference"] = max(target["maximum_absolute_difference"], difference["maximum_absolute_difference"])

    for a, b in zip(read_group_stream(left), read_group_stream(right), strict=True):
        index = count
        # 复用原有限比较器，内存中每次仅有一个真实组；输出汇总而不再积累整窗列表。
        def one(report, component, group):
            return {**report, "schema": "projection_sensitivity_run_v1",
                    "components": {**component, "groups": [group]}}
        compared = compare_runs(one(reference, left, a), one(candidate, right, b))
        row = compared["groups"][0]
        local = compared["exact_component_invariants"]
        local["same_gradient_check_group"] = a["gradient_checked"] == b["gradient_checked"]
        for policy, group in (("reference", a), ("candidate", b)):
            local[f"{policy}_no_retained_history_graphs"] = not group["retained_history_graphs"]
            local[f"{policy}_gradient_scope"] = all(state["h_prev_detached"] and
                state["state_graph_enabled"] == group["gradient_checked"] for state in group["states"])
        for x, y in zip(a["states"], b["states"], strict=True):
            merge("state_h_prev", numerical_difference(x["h_prev"], y["h_prev"]))
        for name, holds in local.items():
            invariants[name] = invariants.get(name, True) and holds
        failed = [name for name, holds in local.items() if not holds]
        failed_groups += bool(failed)
        if failed and len(failures) < 32:
            failures.append({"index": index, "invariants": failed})
        for features in row["features"]:
            for name, difference in features.items():
                merge(f"feature_{name}", difference)
        for state in row["states"]:
            for name, difference in state.items():
                merge(f"state_{name}", difference)
        merge("cancel_features", row["cancel_features"])
        changed_sample_paths += not row["sample_path_same"]
        if a["gradient_checked"] or b["gradient_checked"]:
            row["index"] = index
            checked.append(row)
        count += 1
    return {"schema": "projection_sensitivity_stream_comparison_v1", "reference_policy": reference["policy"],
            "candidate_policy": candidate["policy"], "groups": count, "numerical_differences": totals,
            "checked_groups": checked, "different_sample_paths": changed_sample_paths,
            "failed_group_count": failed_groups, "first_failed_groups": failures,
            "exact_component_invariants": invariants, "all_exact_component_invariants_hold": all(invariants.values()),
            "scope": "固定真实源范围连续状态；前32决定组局部梯度；不设统一误差阈值，不代表TBPTT/PPO或收益验收"}


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
        reference, candidate = json.loads(args.reference.read_text()), json.loads(args.candidate.read_text())
        compare = compare_stream_runs if reference["schema"] == "projection_sensitivity_stream_run_v1" else compare_runs
        result = compare(reference, candidate)
        result["source_reports"] = {"reference_sha256": sha(args.reference), "candidate_sha256": sha(args.candidate)}
        dump(args.output, result)
        print(json.dumps({"output": str(args.output), "all_exact_component_invariants_hold":
                          result["all_exact_component_invariants_hold"]}, ensure_ascii=False))
        if not result["all_exact_component_invariants_hold"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
