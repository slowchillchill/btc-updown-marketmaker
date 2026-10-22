"""专项v1有限工程包；prepare零模型，run须另有明确Torch/模型预算。"""
import argparse
import copy
from fractions import Fraction
import hashlib
import json
import os
import signal
from pathlib import Path
import subprocess
import time


def load(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2)
        stream.write("\n")


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def group_processes(group):
    result=[]
    for entry in Path("/proc").iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            pid=int(entry.name)
            if pid!=os.getpid() and os.getpgid(pid)==group:
                result.append({"pid":pid,"name":(entry/"comm").read_text().strip()})
        except (FileNotFoundError,ProcessLookupError,PermissionError):
            continue
    return result


def command(root, name, args, observation_seconds=0, wall_limit_seconds=None):
    if wall_limit_seconds is not None and (wall_limit_seconds <= 0 or os.getpid() != os.getpgrp()):
        raise ValueError("有限阶段须由本包独立进程组及正数预算启动")
    before = time.perf_counter()
    with (root / (name + ".log")).open("x") as output:
        process = subprocess.Popen(args, stdout=output, stderr=subprocess.STDOUT)
        native_pid = process.pid
        started = {"stage": name, "command": args, "native_pid": native_pid, "package_pgid": os.getpgrp(),
                   "stage_wall_limit_seconds": wall_limit_seconds, "status": "started"}
        write(root / (name + ".started.json"), started)
        print(json.dumps(started, ensure_ascii=False), flush=True)
        try:
            native_code = process.wait(timeout=wall_limit_seconds)
        except subprocess.TimeoutExpired:
            unfinished = {**started, "status": "unfinished", "native_exit_code": None,
                          "wall_seconds": time.perf_counter() - before,
                          "processes_before_stop": group_processes(os.getpgrp()),
                          "stop": "本工程阶段到限，向准确本包PG发送SIGKILL；实际最终回收由外部监督核对"}
            write(root / (name + ".command.json"), unfinished)
            print(json.dumps(unfinished, ensure_ascii=False), flush=True)
            os.killpg(os.getpgrp(), signal.SIGKILL)
            raise RuntimeError("本包强停未发生")
    observations=[]
    remaining=[]
    if observation_seconds:
        observation_start=time.perf_counter()
        while True:
            remaining=group_processes(os.getpgrp())
            observations.append({"after_native_exit_seconds":time.perf_counter()-observation_start,"processes":remaining})
            if not remaining or time.perf_counter()-observation_start>=observation_seconds:
                break
            time.sleep(0.05)
    record = {"command": args, "native_pid":native_pid,"package_pgid":os.getpgrp(),"native_exit_code": native_code,
              "post_exit_observations":observations,"remaining_package_processes":remaining,
              "wall_seconds": time.perf_counter() - before}
    write(root / (name + ".command.json"), record)
    print(json.dumps({"stage": name, **record}, ensure_ascii=False), flush=True)
    if remaining and wall_limit_seconds is not None:
        # 只在本工程独立组收尾，不改生产worker退出政策，也不重试这一阶段。
        os.killpg(os.getpgrp(), signal.SIGKILL)
    if native_code or remaining:
        raise RuntimeError("本阶段退出或本包剩余进程需要所属工程核对；停止依赖步骤，不自动重试、不预判泄漏: " + name)


def policy(alpha):
    return {"version": "D-0375-special-v1", "alpha": alpha, "frame_age_threshold_ms": 100,
            "safe_notrade_credit_pusd": "0.01", "repair_credit_per_share_pusd": "0.01",
            "credit_bound_pusd": "0.10"}


def candidate_paths(candidate, directory):
    directory.mkdir()
    for key, name in (("sim_log", "sim.jsonl"), ("noe_root", "noe"),
                      ("ledger_root", "ledger"), ("metadata_path", "metadata.json"),
                      ("result_path", "result.json"), ("research_evidence_path", "decisions.jsonl")):
        candidate[key] = str(directory / name)


def prepare(config, root):
    root.mkdir()
    material = root / "material"
    command(root, "prepare-material", [config["fixture_binary"], "prepare", config["fixture_config"], str(material)])
    original = load(material / "consumer-run-config.json")
    start = original["days"][0]["preparation"]["scope"]["received_start_utc_ms"]
    initial = root / "initial-model.pt"
    write(root / "initialization.json", config["initialization"])
    for alpha in (0, 1):
        arm = root / ("arm-" + str(alpha))
        arm.mkdir()
        consumer = copy.deepcopy(original)
        consumer["worker"].update(python=config["python"], script=config["worker"], model=str(initial),
                                  device="cpu", torch_threads=config["torch_threads"], max_batch=2)
        for index, candidate in enumerate(consumer["days"][0]["candidates"]):
            candidate_paths(candidate, arm / ("input-account-" + str(index)))
            candidate["timing"]["dispatch_delay_ms"] = {"Fixed": 0}
            candidate["timing"]["calculation_start_delay_ms"] = {"Fixed": 10}
            candidate["timing"]["calculation_duration_ms"] = {"Sequence": [250, 20, 20, 20]} if index == 0 else {"Fixed": 20}
        parameters = copy.deepcopy(config["learning_parameters"])
        parameters.update(checkpoint_directory=str(arm / "checkpoint"), special=policy(alpha))
        job = {"consumer": consumer, "parameters": parameters, "group_id": "special-v1-engineering-alpha-" + str(alpha),
               "output_directory": str(arm / "group"), "active_end_utc_ms": start + 600,
               "active_end_by_day_utc_ms": None, "audit_before_update": False, "gradient_checks": False,
               "fault": None, "attempt_report": str(arm / "attempt.json")}
        write(arm / "learning-job.json", job)
    write(root / "prepared-package.json", {"config": config, "start_utc_ms": start,
          "active_end_utc_ms": start + 600, "evidence_end_utc_ms": original["days"][0]["preparation"]["scope"]["received_end_utc_ms"],
          "expected": {"initializations": 1, "model_services": 4, "exports": 2, "readonly_tensor_checks": 1,
                       "collect": 32, "recompute": 32, "joint_steps": 4, "commits": 2, "main_gru_samples": 64},
          "status": "只准备；未初始化、读取模型或执行Torch"})


def validate_learning_labels(arm, alpha, completed):
    """只核对本包已发生的有限样本，不新增模型调用或平行经济计算。"""
    labels = load(arm / "group" / "frozen-labels.json")
    units = load(arm / "group" / "unit-objectives.json")
    rational = lambda value: Fraction(int(value["numerator"]), int(value["denominator"]))
    details = []
    for index, candidate in enumerate(completed["consumer"]["days"][0]["candidates"]):
        key = candidate["strategy"]["effective_parameters"]["model_consumer"]["account_key"]
        rows = labels[key]
        credit = [Fraction(0), Fraction(0), Fraction(1, 100), Fraction(1, 100)] if index == 0 else [Fraction(0)] * 4
        assert len(rows) == 4 and units[key]["actor_samples"] == 4 and not units[key]["n0"]
        assert rational(units[key]["p_start"]) == rational(units[key]["j_end"]) == 0
        for i, row in enumerate(rows):
            following = credit[i + 1] if i + 1 < len(credit) else Fraction(0)
            assert rational(row["k_exact"]) == rational(row["j_end_exact"]) == 0
            assert rational(row["credit_pusd"]) == credit[i]
            assert rational(row["credit_delta_pusd"]) == following - credit[i]
            assert rational(row["reward_exact"]) == alpha * (following - credit[i])
            assert rational(row["critic_target_exact"]) == -alpha * credit[i]
        assert sum(rational(row["reward_exact"]) for row in rows) == 0
        report = completed["summary"]["days"][0]["candidates"][index]["report"]
        state = report["learning_result"]
        assert state["safe_notrade_accepted_call"] == (2 if index == 0 else None)
        assert not state["repair_requests"]
        details.append({"account": key, "samples": 4, "safe_grant_call": state["safe_notrade_accepted_call"],
                        "base_economics_zero": True, "credit_and_terminal_exact": True})
    write(arm / "special-label-validation.json", {"alpha": alpha, "accounts": details,
          "scope": "无成交小额信用接线；不证明Repair模型训练或经营收益"})


def run(config, root):
    if os.getpgrp()!=os.getpid():
        raise ValueError("实际模型工程包须由提供的独立进程组预算监督器启动")
    write(root / "execution-process.json",{"pid":os.getpid(),"pgid":os.getpgrp(),"post_exit_observation_seconds":config["post_exit_observation_seconds"]})
    package = load(root / "prepared-package.json")
    if package["config"] != config:
        raise ValueError("已准备的工程包配置不同")
    expected = package["expected"]
    limits = config["stage_wall_seconds"]
    if (root / "initial-model.pt").exists():
        raise ValueError("本工程包不自动重试或覆盖原初模")
    command(root, "initialize", [config["python"], config["worker"], "initialize", str(root / "initialization.json"), str(root / "initial-model.pt")], config["post_exit_observation_seconds"], limits["initialize"])
    initial_sha = sha(root / "initial-model.pt")
    total = [0, 0, 0, 0]
    export_configs = []
    for alpha in (0, 1):
        arm = root / ("arm-" + str(alpha))
        job = load(arm / "learning-job.json")
        command(root, "learner-" + str(alpha), [config["learner_binary"], "run", str(arm / "learning-job.json")], config["post_exit_observation_seconds"], limits["learner"])
        report = load(arm / "attempt.json")
        counters = report["model"]
        measured = [counters.get(k, 0) for k in ("forwards", "recompute_forwards", "optimizer_steps", "successful_commits")]
        if measured != [8, 16, 2, 1]:
            raise ValueError("实际学习工作量偏离已准备的8/16/2/1；保留报告，不重跑")
        total = [a + b for a, b in zip(total, measured)]
        if counters["identity"]["model_sha256"] != initial_sha:
            raise ValueError("两臂必须从同一初模建立独立fresh优化器")
        completed = load(arm / "group" / "completed-collection.json")
        validate_learning_labels(arm, alpha, completed)
        for candidate in completed["summary"]["days"][0]["candidates"]:
            if candidate["report"]["calculation_count"] != 4:
                raise ValueError("每账户实际调用数偏离4")
        checkpoint = arm / "checkpoint" / "checkpoint.pt"
        source_sha = sha(checkpoint)
        frozen = arm / "frozen-model.pt"
        export = {"checkpoint": str(checkpoint), "checkpoint_sha256": source_sha, "sequence": 1,
                  "source_implementation": job["consumer"]["days"][0]["candidates"][0]["implementation_identity"],
                  "model_version": "b-gru-actor-critic-gtc-v2", "encoding_version": "b-public-token-log1p-v1",
                  "target_version": "confirmed-finite-j-special-v1", "trajectory_version": "learning-trajectory-special-v1",
                  "shape": config["initialization"], "output_model": str(frozen)}
        write(arm / "export-config.json", export)
        command(root, "export-" + str(alpha), [config["python"], config["export_script"], str(arm / "export-config.json")], config["post_exit_observation_seconds"], limits["export"])
        if sha(checkpoint) != source_sha:
            raise ValueError("只读导出改变源checkpoint")
        export_configs.append(str(arm / "export-config.json"))
        consumer = copy.deepcopy(job["consumer"])
        consumer["worker"]["model"] = str(frozen)
        for i, candidate in enumerate(consumer["days"][0]["candidates"]):
            directory = arm / ("frozen-account-" + str(i))
            candidate_paths(candidate, directory)
            candidate["strategy"]["effective_parameters"]["model_consumer"]["learning"] = {
                "group_id": "frozen-consumer-alpha-" + str(alpha), "active_end_utc_ms": package["active_end_utc_ms"],
                "evidence_end_utc_ms": package["evidence_end_utc_ms"], "trajectory_path": str(directory / "trajectory.jsonl"),
                "collection_fault_after_call": None, "special": policy(alpha)}
        write(arm / "frozen-consumer.json", consumer)
        command(root, "consumer-" + str(alpha), [config["consumer_binary"], "run", str(arm / "frozen-consumer.json"), str(arm / "frozen-summary.json")], config["post_exit_observation_seconds"], limits["consumer"])
        model = load(arm / "frozen-summary.json")["model"]
        if model["forwards"] != 8 or model.get("optimizer_steps", 0) or model.get("successful_commits", 0):
            raise ValueError("冻结消费者工作量/无学习边界偏离")
        total[0] += model["forwards"]
        write(arm / "actual-work.json", {"learner": measured, "frozen_consumer_calls": model["forwards"],
                                        "source_checkpoint_sha256": source_sha, "frozen_model_sha256": sha(frozen)})
    check = {"exports": export_configs, "initial_model_sha256": initial_sha}
    write(root / "tensor-check-config.json", check)
    command(root, "readonly-tensor-check", [config["python"], config["tensor_check_script"], str(root / "tensor-check-config.json"), str(root / "tensor-check.json")], config["post_exit_observation_seconds"], limits["readonly_tensor_check"])
    if total != [expected["collect"], expected["recompute"], expected["joint_steps"], expected["commits"]]:
        raise ValueError("整包工作量偏离，保留实际结果")
    write(root / "summary.json", {"actual": total, "main_gru_samples": total[0] + total[1], "initial_model_sha256": initial_sha,
                                  "limits": "仅有限CPU工程接线；不证明经营、真实负载或旧退出疑问已解决"})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["prepare", "run"])
    parser.add_argument("config")
    parser.add_argument("root")
    args = parser.parse_args()
    config = load(args.config)
    root = Path(args.root)
    (prepare if args.mode == "prepare" else run)(config, root)


if __name__ == "__main__":
    main()
