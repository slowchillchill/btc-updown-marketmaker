#!/usr/bin/env python3
"""逐次恢复 minimal-research-v1。只保留当前公共状态与一个稳定诊断版本。"""
import argparse
import copy
import json

FORMAT = "minimal-research-v1"


def records(path):
    with open(path, encoding="utf-8") as source:
        for line in source:
            yield json.loads(line)


def calculations(public_path, candidate_path):
    public = iter(records(public_path))
    header = next(public)
    if header.get("format") != FORMAT:
        raise ValueError("公共格式不匹配；旧文件须用旧读取器")
    stream = header["input_stream_id"]
    state = {}
    group = None
    pending = next(public, None)
    stable, version = None, None
    for row in records(candidate_path):
        if row.get("record_type") != "calculation":
            continue
        frozen = row["input_cutoff"]["receipt"]["group"]
        if frozen["input_stream_id"] != stream:
            raise ValueError("候选引用与公共输入流不同")
        limit = frozen["group_position"]
        while pending is not None:
            kind = pending["record_type"]
            if kind == "group":
                if pending["group"]["group_position"] > limit:
                    break
                group = pending["group"]
            elif kind in ("blb_state", "pm_state"):
                state[kind, pending["condition_id"]] = pending
            # 原始请求逐档表可晚于生产 EOF；按其自身完整 group 单独关联。
            pending = next(public, None)
        if group != frozen:
            raise ValueError("完整冻结组身份不匹配")
        compact = row["diagnostics"]
        if compact.get("format") != "stable-diagnostics-v1":
            raise ValueError("本样例仅恢复人工策略稳定诊断格式")
        if compact["definition"] is not None:
            stable, version = compact["definition"], compact["version"]
        if stable is None or version != compact["version"]:
            raise ValueError("稳定诊断版本缺失")
        diagnostic = copy.deepcopy(stable)
        dynamic = {m["condition_id"]: m for m in compact["dynamic"]}
        for market in diagnostic["markets"]:
            values = dynamic[market["condition_id"]]
            market["urgency"] = values["urgency"]
            market["probability"] = values["probability"]
        linked = {}
        for condition, refs in (row["public_state"] or {}).items():
            if refs["input_stream_id"] != stream:
                raise ValueError("状态引用串流")
            values = {}
            for field, kind in (("blb_group", "blb_state"), ("pm_group", "pm_state")):
                record = state[kind, condition]
                if record["group_position"] != refs[field]:
                    raise ValueError("公共状态版本不匹配")
                values[kind] = record["state"]
            linked[condition] = values
        yield {"calculation_index": row["calculation_index"],
               "started_at_utc_ms": row["started_at_utc_ms"], "group": frozen,
               "diagnostics": diagnostic, "public_state": linked,
               "intents": row["intents"], "outcome": row["outcome"],
               "original_pair_evidence": row["original_pair_evidence"]}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("public")
    parser.add_argument("candidate")
    args = parser.parse_args()
    for calculation in calculations(args.public, args.candidate):
        print(json.dumps(calculation, ensure_ascii=False, separators=(",", ":")))
