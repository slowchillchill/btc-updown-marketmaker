"""D-0371有限合成验收。只生成原Parquet夹具，复用正式learner，不接真实输入。"""
import argparse
import copy
import json
import math
from pathlib import Path
import subprocess
import time


def write(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def run(command, log):
    before = time.perf_counter()
    with log.open("x") as output:
        result = subprocess.run(command, stdout=output, stderr=subprocess.STDOUT, check=False)
    return {"command": command, "exit_code": result.returncode,
            "wall_seconds": time.perf_counter() - before}


def load(path):
    return json.loads(path.read_text())


def counters(model):
    return [model.get(k, 0) for k in
            ("forwards", "recompute_forwards", "optimizer_steps", "successful_commits")]



def validate_precommit(path, configuration):
    evidence = load(path)
    audits = evidence["audits"]
    max_probability_error = 0.0
    max_old_difference = 0.0
    for row in audits:
        teacher = row["result"]
        independent = 0.0
        for head in teacher["terms"]:
            logits = [head["logits"][index] for index in head["allowed"]]
            maximum = max(logits)
            independent += (head["logits"][head["selected"]] - maximum
                            - math.log(sum(math.exp(v - maximum) for v in logits)))
        max_probability_error = max(max_probability_error, abs(independent - teacher["logp"]))
        max_old_difference = max(max_old_difference, abs(row["old_logp"] - teacher["logp"]))
    # 仅此合成验收的浮点误差；不是模型/交易门或训练确定性保证。
    if max_probability_error > 1e-10 or max_old_difference > 1e-4:
        raise RuntimeError("精确冻结域的独立联合概率/行为概率重算不符")
    epochs = evidence["epochs"]
    if len(epochs) != 2:
        raise RuntimeError("整组没有恰好两轮联合更新")
    parameters = configuration["parameters"]
    labels = load(Path(configuration["output_directory"]) / "frozen-labels.json")
    max_clip_error = 0.0
    max_value_error = 0.0
    for epoch in epochs:
        for sample in epoch["samples"]:
            target = labels[sample["account"]][sample["call"]-1]
            advantage = target["advantage"] / parameters["money_scale_pusd"]
            ratio = math.exp(sample["logp"]-target["old_joint_logp"])
            clipped = min(1+parameters["epsilon"], max(1-parameters["epsilon"], ratio))
            actor = -min(ratio*advantage, clipped*advantage)
            critic = 0.5 * parameters["value_weight"] * ((sample["value"]-target["critic_target"])/parameters["money_scale_pusd"])**2
            max_clip_error = max(max_clip_error, abs(actor-sample["actor_loss"]))
            max_value_error = max(max_value_error, abs(critic-sample["critic_loss"]))
    if max_clip_error > 1e-10 or max_value_error > 1e-6:
        raise RuntimeError("联合PPO clip/完整剩余Critic监督与独立公式不符")
    second_changed = any(abs(s["ratio"] - 1) > 1e-8 for s in epochs[1]["samples"])
    if not second_changed:
        raise RuntimeError("第二轮没有使用更新后的当前参数重算")
    for epoch in epochs:
        gradients = epoch["gradients"]
        if gradients["gru"] <= 0 or gradients["encoder"] <= 0 or gradients["critic"] <= 0:
            raise RuntimeError("联合梯度没有到达共享主干/价值头")
    references = evidence["gradient_references"]
    gradients_report = None
    if references:
        full, truncated = references["bptt"]["references"]
        critic = references["critic"]["references"][0]
        hidden_difference = max(abs(a-b) for a,b in zip(full["hidden"], truncated["hidden"]))
        gradient_difference = max(abs(a-b) for a,b in zip(full["gru_gradient"], truncated["gru_gradient"]))
        if hidden_difference > 1e-6 or gradient_difference <= 0 or critic["norms"]["gru"] <= 0 or critic["norms"]["encoder"] <= 0:
            raise RuntimeError("TBPTT连续数值/截梯度或共享Critic梯度反例不符")
        gradients_report = {"tbptt_hidden_max_difference": hidden_difference,
                            "tbptt_gru_gradient_max_difference": gradient_difference,
                            "critic_only_norms": critic["norms"]}
    return {"independent_joint_logp_max_error": max_probability_error,
            "old_teacher_joint_logp_max_difference": max_old_difference,
            "fixture_old_logp_tolerance": 1e-4,
            "second_epoch_ratio_changes": second_changed,
            "independent_ppo_clip_max_error": max_clip_error,
            "independent_critic_half_mse_max_error": max_value_error,
            "gradient_references": gradients_report,
            "epoch_gradient_norms": [e["gradients"] for e in epochs]}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("mode", choices=("functional", "cost1", "cost3", "prepare"))
    p.add_argument("configuration")
    p.add_argument("output")
    p.add_argument("--resume-after-g0", help="同功能档G0已commit，直接按原G1入口继续")
    p.add_argument("--resume-functional", help="仅接回同一功能档已完成G0采集，保持原预算累计")
    args = p.parse_args()
    cfg = load(Path(args.configuration))
    root = Path(args.output)
    root.mkdir()
    started = time.perf_counter()
    totals = [0, 0, 0, 0]
    original = Path(args.resume_functional) if args.resume_functional else None
    if original is not None:
        if args.mode != "functional":
            raise ValueError("仅功能档允许按显式原失败接回")
        previous = load(original / "g0" / "completed-collection.json")
        totals = counters(previous["summary"]["model"])
        if totals != [16, 0, 0, 0]:
            raise ValueError("接回前缀不匹配已记录的16次采集失败停点")
    attempts = []
    after_g0 = Path(args.resume_after_g0) if args.resume_after_g0 else None
    if after_g0 is not None:
        if original is None:
            raise ValueError("须同时明确原功能停点")
        totals = load(after_g0 / "summary.json")["model_counters"]
        if totals != [16, 72, 2, 1]:
            raise ValueError("已提交G0前缀不匹配实际验收计数")
    faults = 0
    bound = {"functional": [40, 160, 6, 2], "cost1": [16, 32, 2, 1],
             "cost3": [48, 96, 6, 3], "prepare": [0, 0, 0, 0]}[args.mode]

    def prepare(name):
        destination = root / name
        timing = run([cfg["fixture_program"], "prepare", cfg["fixture_config"], str(destination)], root / (name + "-prepare.log"))
        attempts.append({"phase": "prepare", **timing})
        if timing["exit_code"]:
            raise RuntimeError("合成材料准备失败，停止本档")
        return load(destination / "consumer-run-config.json")

    def job(consumer, group_id, group_output, checkpoint, name, fault=None, audit=False, gradients=False):
        nonlocal faults
        parameters = copy.deepcopy(cfg["parameters"])
        parameters["checkpoint_directory"] = str(checkpoint)
        report = root / (name + "-report.json")
        job_cfg = {"consumer": consumer, "parameters": parameters, "group_id": group_id,
                   "output_directory": str(group_output), "active_end_utc_ms": cfg["active_end_utc_ms"],
                   "audit_before_update": audit, "gradient_checks": gradients,
                   "fault": fault, "attempt_report": str(report)}
        path = root / (name + "-config.json")
        write(path, job_cfg)
        timing = run([cfg["learner_program"], "run", str(path)], root / (name + ".log"))
        attempt = {"name": name, "fault": fault, **timing}
        attempts.append(attempt)
        if fault in ("before_commit", "after_commit"):
            witness = load(report.with_suffix(".fault.json"))
            model = witness["metrics"]
            expected = "injected-before-commit-fault" if fault == "before_commit" else "injected-after-commit-ack-fault"
            if timing["exit_code"] == 0 or expected not in (root / (name + ".log")).read_text():
                raise RuntimeError("没有命中声明的学习故障点")
        else:
            if report.exists():
                outcome = load(report)
            else:
                completed = group_output / "completed-collection.json"
                if completed.exists():
                    actual = load(completed)["summary"]["model"]
                    attempt["completed_collection_model_counters"] = counters(actual)
                raise RuntimeError("缺少当前工作计量回执；不猜测新重算/更新计数")
            model = outcome["model"]
            if fault == "collection":
                if timing["exit_code"] == 0 or not outcome.get("failed_collection") or model["forwards"] != 4:
                    raise RuntimeError("没有命中第4次真实调用后的采集故障")
            elif timing["exit_code"]:
                raise RuntimeError("有限合成学习失败，停止本档")
        values = counters(model)
        for i, v in enumerate(values):
            totals[i] += v
        faults += int(fault is not None)
        attempt["model_counters"] = values
        if any(v > limit for v, limit in zip(totals, bound)):
            raise RuntimeError("实际模型计数越过本档授权上限，保留前缀")
        precommit = report.with_suffix(".precommit.json")
        if precommit.exists():
            write(root / (name + "-validation.json"), validate_precommit(precommit, job_cfg))
        write(root / "progress-{}.json".format(len(attempts)),
              {"totals": totals, "faults": faults, "attempts": attempts})
        print(json.dumps({"attempt": name, "counts": values, "total": totals}, ensure_ascii=False), flush=True)
        return load(report)

    try:
        if args.mode == "prepare":
            prepare("material")
        elif args.mode == "functional":
            if original is None:
                c0 = prepare("g0-material")
                g0_output = root / "g0"
            else:
                c0 = load(original / "g0-config.json")["consumer"]
                g0_output = original / "g0"
            checkpoint = (original or root) / "checkpoint"
            if after_g0 is None:
                job(c0, "synthetic-functional-g0", g0_output, checkpoint, "g0", audit=True, gradients=True)
                c1 = prepare("g1-material")
                output = root / "g1"
            else:
                c1 = load(after_g0 / "g1-collection-fault-config.json")["consumer"]
                output = after_g0 / "g1"
            job(c1, "synthetic-functional-g1", output, checkpoint, "g1-collection-fault", fault="collection")
            job(c1, "synthetic-functional-g1", output, checkpoint, "g1-before-commit", fault="before_commit", audit=True)
            job(c1, "synthetic-functional-g1", output, checkpoint, "g1-after-commit", fault="after_commit")
            acknowledged = job(c1, "synthetic-functional-g1", output, checkpoint, "g1-acknowledge")
            if not acknowledged.get("already_committed") or counters(acknowledged["model"]) != [0, 0, 0, 0]:
                raise RuntimeError("已成功提交的丢回执恢复重复消费")
        else:
            for index in range(1 if args.mode == "cost1" else 3):
                consumer = prepare("run-{}-material".format(index))
                job(consumer, "synthetic-cost-{}".format(index), root / ("run-{}".format(index)),
                    root / ("checkpoint-{}".format(index)), "run-{}".format(index))
        status = "complete"
    except Exception as error:
        status = "failed: " + str(error)
        raise
    finally:
        write(root / "summary.json", {"status": status, "model_counters": totals,
              "counter_order": ["collection", "recompute", "joint_step", "commit"],
              "faults": faults, "bound": bound, "attempts": attempts,
              "internal_wall_seconds": time.perf_counter() - started})


if __name__ == "__main__":
    main()
