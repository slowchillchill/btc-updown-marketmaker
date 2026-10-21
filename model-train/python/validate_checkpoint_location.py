"""检查点目录变更的有限CPU工程反例；显式入口，不随cargo test运行。"""
import argparse
import copy
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time


def parameters(root, directory):
    return {"learning_rate": .001, "epsilon": .2, "value_weight": 1.,
            "money_scale_pusd": 1., "tau_gae_s": 300., "tbptt_steps": 1,
            "ppo_epochs": 2, "checkpoint_directory": str(root / directory),
            "trajectory_page_rows": 4, "trajectory_cached_pages": 2,
            "training_sequence_batch": 1}


def group(root, config, ordinal):
    return {"id": "cpu-group-" + str(ordinal), "trajectory_version": "learning-trajectory-v2",
            "target_version": "confirmed-finite-j-v1",
            "binding": {"parameters": config, "group_id": "cpu-group-" + str(ordinal),
                        "consumer": {"account": "cpu-account-" + str(ordinal),
                                     "trajectory_path": str(root / ("trajectory-%d.jsonl" % ordinal)),
                                     "checkpoint_directory": "此处同名字段仍属于其他语义",
                                     "source": "纯合成token夹具，无公共供料/SIM",
                                     "S": ordinal * 1000, "E": ordinal * 1000 + 100,
                                     "H": ordinal * 1000 + 200},
                        "active_end_utc_ms": ordinal * 1000 + 100}}


def frame(ordinal):
    return {"account": "cpu-account-" + str(ordinal), "call": 1,
            "global": [[1, 0, .25 * ordinal, 0], [2, 3, 0., 3]],
            "markets": [[[3, 0, .5, 0]]], "options": [{"kind": "NoTrade"}],
            "selected_option": 0, "rpc": []}


def phase(root, name):
    import torch
    from model_worker import Service, initialize
    from learning_engine import learning_group_semantics
    torch.set_num_interop_threads(1)
    assert str(torch.__version__) == "2.8.0+cu128", "须使用项目锁定依赖"
    service = None
    report = {"phase": name, "device": "cpu", "torch": str(torch.__version__),
              "torch_threads": 4, "torch_interop_threads": 1}

    def equal(actual, expected, path="state"):
        if isinstance(expected, torch.Tensor):
            assert isinstance(actual, torch.Tensor) and actual.dtype == expected.dtype
            assert actual.shape == expected.shape and torch.equal(actual, expected), path
        elif isinstance(expected, dict):
            assert actual.keys() == expected.keys(), path
            for key in expected:
                equal(actual[key], expected[key], path + "/" + str(key))
        elif isinstance(expected, (list, tuple)):
            assert type(actual) is type(expected) and len(actual) == len(expected), path
            for index, (a, b) in enumerate(zip(actual, expected)):
                equal(a, b, path + "/" + str(index))
        else:
            assert actual == expected, path

    def request(config, unit):
        return {"config": config, "group": unit,
                "account_weights": {unit["binding"]["consumer"]["account"]: 1.}}

    def learn(engine, unit):
        ordinal = int(unit["id"].rsplit("-", 1)[1])
        for epoch in range(2):
            print("CPU工程验证", name, unit["id"], "轮", epoch + 1, flush=True)
            engine.start_epoch()
            engine.step({"frame": frame(ordinal), "label": {
                "advantage": 1., "old_joint_logp": 0., "critic_target": 2.}})
            engine.end_epoch()
        return engine.commit({})

    try:
        model = root / "initial-model.pt"
        if name == "A" and not model.exists():
            initialize(str(root / "initialization.json"), str(model))
        service = Service(model, "cpu", 4)
        engine = service.learning
        config = parameters(root, "A" if name == "A" else "B")
        unit = group(root, config, 0)
        if name == "A":
            assert engine.open(request(config, unit))["already_committed"] is False
            report["commit"] = learn(engine, unit)
            states = engine.optimizer.state_dict()["state"]
            assert states and all(float(s["step"]) == 2. for s in states.values())
            assert any(torch.count_nonzero(s["exp_avg"]).item() for s in states.values())
            assert any(torch.count_nonzero(s["exp_avg_sq"]).item() for s in states.values())
            report["nonempty_adam_state_count"] = len(states)
        elif name == "B":
            source = torch.load(root / "A/checkpoint.pt", map_location="cpu", weights_only=True)
            original_consumed = copy.deepcopy(source["consumed"])
            before = dict(service.metrics)
            result = engine.open(request(config, unit))
            assert result == {"already_committed": True, "sequence": 1}
            equal(engine.model.state_dict(), source["model"], "model")
            equal(engine.optimizer.state_dict(), source["optimizer"], "Adam")
            equal(torch.get_rng_state(), source["torch_rng"], "TorchCPU_RNG")
            equal(engine.consumed, source["consumed"], "consumed")
            assert engine.sequence == source["sequence"] == 1
            assert source["cuda_rng"] == [] and source["account_sampling_rng"] == {}
            assert source["scheduler"] is None
            assert source["config"]["checkpoint_directory"] == str(root / "A")
            assert engine.config["checkpoint_directory"] == str(root / "B")
            for key in ("forwards", "recompute_forwards", "optimizer_steps", "successful_commits"):
                assert before.get(key, 0) == service.metrics.get(key, 0), "已消费确认不运行前向/更新/提交"
            report["loaded_checkpoint"] = str(root / "B/checkpoint.pt")
            report["source_checkpoint"] = str(root / "A/checkpoint.pt")
            report["new_commit_directory"] = engine.config["checkpoint_directory"]
            report["restored_model_tensor_count"] = len(source["model"])
            report["restored_adam_param_groups"] = source["optimizer"]["param_groups"]
            report["restored_adam_state"] = [
                {"parameter": key, "step": float(value["step"]),
                 "exp_avg_nonzero": int(torch.count_nonzero(value["exp_avg"])),
                 "exp_avg_sq_nonzero": int(torch.count_nonzero(value["exp_avg_sq"]))}
                for key, value in source["optimizer"]["state"].items()]
            report["confirmation_metrics"] = dict(service.metrics)
            # 同名其他路径不被递归忽略；这里只核准确比较边界，不触发额外模型工作。
            changed = copy.deepcopy(unit)
            changed["binding"]["consumer"]["checkpoint_directory"] = "另一语义"
            assert learning_group_semantics(unit) != learning_group_semantics(changed)
            unit = group(root, config, 1)
            assert engine.open(request(config, unit))["already_committed"] is False
            report["commit"] = learn(engine, unit)
            assert engine.sequence == 2
            assert all(float(s["step"]) == 4. for s in engine.optimizer.state_dict()["state"].values())
            assert engine.consumed["cpu-group-0"] == original_consumed["cpu-group-0"]
            assert engine.consumed["cpu-group-1"] == unit
            assert any(not torch.equal(value, source["model"][key])
                       for key, value in engine.model.state_dict().items())
            try:
                engine.open(request(parameters(root, "A"), unit))
            except ValueError as error:
                assert str(error) == "同一学习器不能静默改变生效配置"
                report["same_process_directory_change_rejected"] = str(error)
            else:
                raise AssertionError("未获支持的同进程切目录被放行")
        else:
            if name == "numeric":
                config["epsilon"] = .21
                expected = "恢复配置或模型版本不同"
            else:
                unit["binding"]["consumer"]["account"] = "different-account"
                expected = "同一消费身份对应不同轨迹/目标"
            try:
                engine.open(request(config, unit))
            except ValueError as error:
                assert str(error) == expected
                report["expected_rejection"] = str(error)
            else:
                raise AssertionError("真正不同的数值或组身份被放行")
            assert not service.metrics.get("recompute_forwards", 0)
            assert not service.metrics.get("optimizer_steps", 0)
            assert not service.metrics.get("successful_commits", 0)
        report["result"] = "passed"
    except Exception as error:
        report["error"] = repr(error)
        raise
    finally:
        if service is not None:
            report["metrics"] = service.snapshot()
        (root / (name + "-result.json")).write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--phase", choices=["A", "B", "numeric", "semantic"])
    args = parser.parse_args()
    if args.phase:
        return phase(args.output, args.phase)
    root = args.output
    root.mkdir()
    config = {"width": 8, "layers": 1, "field_buckets": 32, "category_buckets": 32,
              "initialization_seed": 1810521, "initialization": "random_v1"}
    (root / "initialization.json").write_text(json.dumps(config, indent=2) + "\n")
    (root / "effective-parameters.json").write_text(json.dumps(parameters(root, "A"), indent=2) + "\n")
    for ordinal in [0, 1]:
        (root / ("trajectory-%d.jsonl" % ordinal)).write_text(json.dumps(frame(ordinal)) + "\n")
    started = time.perf_counter()
    env = os.environ | {"CUDA_VISIBLE_DEVICES": "", "OMP_NUM_THREADS": "4", "MKL_NUM_THREADS": "4",
                        "PYTHONDONTWRITEBYTECODE": "1", "PYTHONNOUSERSITE": "1"}
    original = None
    for name in ["A", "B", "numeric", "semantic"]:
        if name == "B":
            (root / "B").mkdir()
            shutil.copyfile(root / "A/checkpoint.pt", root / "B/checkpoint.pt")
            original = {p.name: p.read_bytes() for p in (root / "A").glob("*.pt")}
        print("CPU工程验证阶段", name, "累计秒", time.perf_counter() - started, flush=True)
        subprocess.run([sys.executable, __file__, "--output", str(root), "--phase", name], env=env, check=True)
        if original is not None:
            assert {p.name: p.read_bytes() for p in (root / "A").glob("*.pt")} == original
    print("CPU工程验证完成；4重算/4更新/2提交；A及immutable字节保持；秒", time.perf_counter() - started, flush=True)


if __name__ == "__main__":
    main()
