"""只读合成原结果核对；不调用模型，不重做撮合或账本。"""
import argparse
from collections import Counter
from decimal import Decimal
import json
import math
from pathlib import Path


def validate(root, expected, require_static):
    summary = json.loads((root / "summary.json").read_text())
    accounts = []
    totals = Counter()
    for path in sorted(root.glob("case-*/account-*/decisions.jsonl")):
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        calculations = [r for r in rows if r["record_type"] == "calculation"]
        previous_hidden = 0.0
        previous_cutoff = None
        static_rejections = 0
        zero_risk_static = 0
        market_frame_count = 0
        for call, record in enumerate(calculations, 1):
            diagnostic = record["diagnostics"]
            assert diagnostic["call"] == call == diagnostic["forward"]["account_call"]
            assert abs(diagnostic["forward"]["hidden_before_norm"] - previous_hidden) < 1e-5
            previous_hidden = diagnostic["forward"]["hidden_after_norm"]
            market = record["market_frame_timing"]["market_cutoff"]["decision_time"]["logical_time_utc_ms"]
            account = record["input_cutoff"]["decision_time"]["logical_time_utc_ms"]
            at = record["started_at_utc_ms"]
            assert market <= account == at
            assert diagnostic["time"]["frame_age_ms"] == at - market
            assert diagnostic["time"]["queue_wait_ms"] == at - record["market_frame_timing"]["earliest_start_at_utc_ms"]
            if previous_cutoff is not None:
                assert market == previous_cutoff + 100
            previous_cutoff = market
            market_frame_count += 1
            trace = diagnostic["head_trace"]
            assert len(trace) == diagnostic["heads"]
            probability = 0.0
            for term in trace:
                allowed = term["allowed"]
                assert term["selected"] in allowed
                logits = term["logits"]
                maximum = max(logits[i] for i in allowed)
                independent = logits[term["selected"]] - maximum - math.log(sum(math.exp(logits[i] - maximum) for i in allowed))
                assert abs(independent - term["log_probability"]) < 1e-10
                probability += independent
            assert math.isfinite(probability)
            assert abs(probability - diagnostic["joint_log_probability"]) < 1e-9
            action = diagnostic["action"]
            totals[action] += 1
            intents = record["intents"]
            if action == "NoTrade":
                assert not intents
            else:
                assert len(intents) == 1
                intent = intents[0]["intent"]
                if action == "Pair":
                    legs = intent["SubmitPairedBuy"]["orders"]
                    assert len(legs) == 2
                    assert legs[0]["token_id"] != legs[1]["token_id"]
                    assert Decimal(legs[0]["order"]["GtcLimit"]["size_shares"]) == Decimal(legs[1]["order"]["GtcLimit"]["size_shares"])
                else:
                    assert action == "Repair"
                    legs = [intent["SubmitSingleBuy"]["order"]]
                for leg in legs:
                    assert list(leg["order"]) == ["GtcLimit"]
                    order = leg["order"]["GtcLimit"]
                    assert isinstance(order["post_only"], bool)
                    assert 0 < Decimal(order["limit_price_pusd_per_share"]) < 1
                    quantity = Decimal(order["size_shares"])
                    assert quantity > 0 and quantity * 100 == (quantity * 100).to_integral_value()
            for evidence, eligible in zip(diagnostic["repair_evidence"], diagnostic["repair_eligible"]):
                rejected = evidence.get("known_static_rejection_count", 0)
                static_rejections += rejected
                if rejected and evidence["confirmed_net_side0_minus_side1"]["numerator"] == "0" and all(q["numerator"] == "0" for q in evidence["known_remaining_target_shares"]):
                    assert eligible == [False, False]
                    zero_risk_static += 1
        accounts.append({"path": str(path.relative_to(root)), "calls": market_frame_count,
                         "last_hidden_norm": previous_hidden,
                         "known_static_rejections_seen": static_rejections,
                         "zero_risk_static_frames": zero_risk_static})
    actual = sum(a["calls"] for a in accounts)
    assert actual == expected == summary["model"]["forwards"]
    assert sum(summary["model"]["account_calls"].values()) == actual
    if require_static:
        assert totals["Repair"] == 0
        assert sum(a["zero_risk_static_frames"] for a in accounts) > 0, "必须实际看到必拒承诺尚未消失的帧"
        assert totals["Pair"] > 0, "低于MOS的新配对仍须实际可选"
    for day in summary["days"]:
        for candidate in day["candidates"]:
            report = candidate["report"]
            assert report["end_reason"] == "RangeEnd"
            assert report["recording_drained"]
            assert report["fifo"]["generated_market_frames"] == report["fifo"]["started_market_frames"] + report["fifo"]["queued_market_frames"]
    return {"forwards": actual, "actions": dict(totals), "accounts": accounts,
            "checked": ["FIFO每100ms逐帧", "市场与账户双截止", "每调用一次记忆推进", "原logits独立softmax与联合概率", "原GTC动作/等量/零意图", "原EOF报告"],
            "static_rejection_case_required": require_static,
            "limits": "该核对不证明盈利、学习、实际突发或完整日容量"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("--expected-forwards", type=int, required=True)
    parser.add_argument("--require-static-rejection-exclusion", action="store_true")
    args = parser.parse_args()
    print(json.dumps(validate(args.root, args.expected_forwards, args.require_static_rejection_exclusion), ensure_ascii=False, indent=2))
