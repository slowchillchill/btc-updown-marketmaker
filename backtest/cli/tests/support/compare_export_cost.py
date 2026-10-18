#!/usr/bin/env python3
"""比较有界成本夹具的完整业务事实，并核对独立5股成交期望；需要 pyarrow。"""
import argparse
import importlib.util
import json
from pathlib import Path
import pyarrow.parquet as pq


def normalize(value, root, context):
    # 仅映射该候选自己的物理SIM路径与由它派生的NOE上下文引用；保留全部业务字段。
    text = json.dumps(value)
    context_dir=next((root / "noe").glob("*/context.json")).parent
    text=text.replace(str(context_dir),"<candidate-root>/noe/<context>")
    text = text.replace(str(root), "<candidate-root>").replace(context, "<normalized-context>")
    return json.loads(text)


def durable(root, kind):
    context_path = next((root / "noe").glob("*/context.json"))
    context = json.loads(context_path.read_text())["payload"]["context"]["context_reference"]
    values = []
    for path in sorted((root / kind).rglob("*.parquet")):
        for row in pq.read_table(path).to_pylist():
            row["payload"] = json.loads(row["payload"])
            values.append(normalize(row, root, context))
    return {"context": normalize(json.loads(context_path.read_text()), root, context), "rows": values}


def compare(root):
    baseline = json.loads((root / "off/business.json").read_text())
    reference = {}
    for candidate in sorted((root / "off").glob("candidate-*")):
        reference[candidate.name] = {kind: durable(candidate, kind) for kind in ("noe", "ledger")}
    for mode in ("full", "old", "new"):
        actual = json.loads((root / mode / "business.json").read_text())
        assert actual == baseline, f"{mode} 请求/诊断/回执/报告/SIM存在差异"
        for name, wanted in reference.items():
            for kind in ("noe", "ledger"):
                assert durable(root / mode / name, kind) == wanted[kind], (mode, name, kind)
    # 隐藏耗量100→95，价格0.62；finality经济生效在2250+10ms。
    for stream in (0, 1):
        expected = reference[f"candidate-{stream}-0"]["noe"]["rows"]
        transitions = [row["payload"].get("fact", {}).get("disposition", {}).get("AccountTransition", {})
                       for row in expected]
        fills = [t for t in transitions if t.get("balance_delta", {}).get("outcome_balance_deltas")]
        assert len(fills) == 1
        fill = fills[0]
        assert fill["economic_effective_at_utc_ms"] == 1787529602260
        assert fill["balance_delta"]["cash_pusd_delta"] == "-3.1"
        assert fill["balance_delta"]["outcome_balance_deltas"] == [{"token_id": "20", "balance_shares_delta": "5"}]
    spec = importlib.util.spec_from_file_location(
        "minimal_reader", Path(__file__).resolve().parents[3] / "scripts/read_minimal_research.py")
    reader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(reader)
    restored, definitions = 0, 0
    for candidate in sorted((root / "new").glob("candidate-*")):
        _, stream, number = candidate.name.split("-")
        public = next(path for path in (root / "new/public").glob("*.jsonl")
                      if json.loads(next(open(path)))["input_stream_id"] == f"cost-stream-{stream}")
        rows = list(reader.calculations(public, candidate / "candidate.jsonl"))
        expected = baseline["business"][f"cost-{stream}-{number}"]["evaluations"]
        assert len(rows) == len(expected)
        for row, wanted in zip(rows, expected):
            assert row["started_at_utc_ms"] == wanted["at"]
            assert row["diagnostics"] == wanted["output"]["Continue"]["diagnostics"]
        restored += len(rows)
        definitions += sum(row["diagnostics"]["definition"] is not None
                           for row in reader.records(candidate / "candidate.jsonl")
                           if row["record_type"] == "calculation")
    return {"exact_business_equal": True, "sim_frames_equal": True,
            "noe_and_ledger_records_equal": True,
            "normalized_metadata": "candidate physical path and its derived context reference only; normalized context compared in full", "independent_fill": "5 shares at 0.62; cash -3.1; economic time 2250+10ms",
            "restored_evaluations": restored, "stable_definitions": definitions,
            "durable_record_counts": {name: {kind: len(rows["rows"]) for kind, rows in value.items()}
                                      for name, value in reference.items()},
            "modes": [json.loads((root / mode / "cost.json").read_text()) for mode in ("off", "full", "old", "new")]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = compare(args.root)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in result.items() if key != "modes"}, ensure_ascii=False))
