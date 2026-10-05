"""真实原生组的有限投影敏感性诊断；固定初始化权重，不执行订单或更新参数。"""

import ctypes
from contextlib import ExitStack
import gzip
import hashlib
import json
from pathlib import Path
import platform
import sys
import time

import torch
from torch.distributions import Categorical

from .actor import ActorResult, conditional_choice, consume_actor
from .graphs import CONFIGURATIONS, TwoGraphs
from .native_inputs import NativeComponentConsumer


def _tensor(value, label):
    value = value.detach().cpu()
    if not torch.isfinite(value).all():
        raise ValueError(f"诊断张量含非有限值：{label}")
    return value.tolist()


def _weights_sha256(model):
    digest = hashlib.sha256()
    for name, value in sorted(model.state_dict().items()):
        value = value.detach().cpu().contiguous()
        header = json.dumps([name, str(value.dtype), list(value.shape)], separators=(",", ":")).encode()
        digest.update(len(header).to_bytes(8, "little"))
        digest.update(header)
        digest.update(ctypes.string_at(value.data_ptr(), value.numel() * value.element_size()))
    return digest.hexdigest()


def _trace(consumer, group, row, actor, replay=None):
    """原生 Actor 决定支持与推进；记录当前路径实际使用的完整分布。"""
    log_prob = group.states[row].sum() * 0
    entropy = log_prob
    steps = []
    while not actor.complete():
        support = actor.support()
        prefix = actor.prefix_features()
        indices = actor.price_indices()
        fees = actor.price_fee_features()
        head = actor.head_index()
        legal = torch.as_tensor(support, dtype=torch.bool, device=log_prob.device)
        if legal.sum().item() == 1:
            logits = group.states[row].new_zeros(len(support))
        else:
            blocks = consumer.parameters(group, row, prefix,
                price_fee_features=fees, price_indices=indices)
            logits = (torch.cat([block[head][0] for block in blocks])[:len(support)]
                      if head == 5 else blocks[0][head][0])
        if replay is not None and len(steps) >= len(replay):
            raise ValueError("共同路径在原生 Actor 完成前耗尽")
        selected = None if replay is None else replay[len(steps)]
        choice, conditional_log_prob, conditional_entropy = conditional_choice(
            logits, support, selected=selected)
        distribution = Categorical(logits=logits[legal])
        probabilities = torch.zeros_like(logits).masked_scatter(legal, distribution.probs)
        steps.append({
            "head_index": head, "support": support,
            "validation_state": actor.validation_state(),
            "prefix_features": prefix, "price_indices": indices, "price_fee_features": fees,
            "logits": _tensor(logits, "条件分数"),
            "probabilities": _tensor(probabilities, "条件概率"),
            "greedy_index": int(legal.nonzero().flatten()[logits[legal].argmax()]),
            "selected_index": choice,
            "conditional_log_prob": _tensor(conditional_log_prob, "条件日志概率"),
            "conditional_entropy": _tensor(conditional_entropy, "条件熵"),
        })
        actor.choose(choice)
        log_prob = log_prob + conditional_log_prob
        entropy = entropy + conditional_entropy
    if replay is not None and len(steps) != len(replay):
        raise ValueError("共同路径在原生 Actor 完成后仍有剩余索引")
    result = ActorResult(actor, log_prob, entropy)
    return result, {"row": row, "path": actor.path(), "steps": steps,
                    "intent": actor.validation_intent(),
                    "log_prob": _tensor(log_prob, "路径日志概率"),
                    "conditional_entropy_sum": _tensor(entropy, "路径条件熵和")}


def _gradient_check(model, sampled, replayed, *, components=False):
    named = tuple(model.named_parameters())
    parameters = tuple(value for _, value in named)
    first = torch.autograd.grad(sampled.log_prob, parameters, allow_unused=True, retain_graph=True)
    second = torch.autograd.grad(replayed.log_prob, parameters, allow_unused=True, retain_graph=True)
    usage_equal = True
    equal = True
    compared = nonzero = different = 0
    maximum = 0.0
    by_component = {name: {"used_elements": 0, "nonzero_elements": 0} for name in ("state", "action")}
    for (name, _), left, right in zip(named, first, second, strict=True):
        if left is None or right is None:
            usage_equal &= (left is None) == (right is None)
            continue
        if not torch.isfinite(left).all() or not torch.isfinite(right).all():
            raise ValueError("概率重算梯度含非有限值")
        compared += left.numel()
        nonzero += int(torch.count_nonzero(left))
        summary = by_component[name.split(".", 1)[0]]
        summary["used_elements"] += left.numel()
        summary["nonzero_elements"] += int(torch.count_nonzero(left))
        different += int(torch.count_nonzero(left != right))
        equal &= torch.equal(left, right)
        if left.numel():
            maximum = max(maximum, float((left - right).abs().max()))
    result = {"parameter_usage_equal": usage_equal, "all_equal": usage_equal and equal,
              "compared_elements": compared, "nonzero_elements": nonzero,
              "different_elements": different, "maximum_absolute_difference": maximum}
    if components:
        result["by_component"] = by_component
    return result


class StreamingSensitivity:
    """独立工程诊断：逐组保留数值前史，前32个决定组仅求当组条件梯度。"""

    def __init__(self, output_stream, policy, common_paths_json=None, seed=742001):
        self.stack = ExitStack()
        self.closed = False
        self.started = self.progress_at = time.monotonic()
        self.group_count = self.decision_groups = self.checked_groups = self.state_rows = 0
        self.digest = hashlib.sha256()
        self.output_path = Path(output_stream)
        self.common_paths = []
        self.expected_paths = None
        self.traces = []
        self.checking = False
        self.policy = policy
        self.all_gradient_checks_equal = True
        self.state_nonzero_gradient_groups = self.action_nonzero_gradient_groups = 0
        try:
            if common_paths_json is not None:
                paths = json.loads(common_paths_json)
                if paths["schema"] != "projection_common_paths_v1":
                    raise ValueError("共同路径版本不符")
                self.expected_paths = {row["index"]: row for row in paths["rows"]}
                if len(self.expected_paths) != len(paths["rows"]):
                    raise ValueError("共同路径组序号重复")
            elif policy != "exact_cpu":
                raise ValueError("候选必须显式提供exact基准冻结的共同路径")
            previous_threads = torch.get_num_threads()
            self.stack.callback(torch.set_num_threads, previous_threads)
            torch.set_num_threads(1)
            self.stack.enter_context(torch.random.fork_rng(devices=[]))
            torch.manual_seed(seed)
            configuration = next(config for config in CONFIGURATIONS if config.name == "MAIN-1x256")
            self.model = TwoGraphs(configuration).eval()
            self.consumer = NativeComponentConsumer(self.model)
            self.weights_sha256 = _weights_sha256(self.model)
            self.metadata = {
                "schema": "projection_component_stream_v1", "policy": policy,
                "configuration": configuration.name, "initialization_seed": configuration.seed,
                "sampling_seed": seed, "weights_sha256": self.weights_sha256, "device": "cpu",
                "environment": {"python": sys.version, "torch": torch.__version__,
                                "platform": platform.platform(), "machine": platform.machine(),
                                "num_threads": torch.get_num_threads()},
                "gradient_scope": "前32个DecisionRequired组；进入组前h为detach常量，保留当组state及Actor参数梯度；不是TBPTT/PPO验收",
                "execution_history": "调用方固定共同外生前史；本模块生成意图不提交；所有组连续消费；不更新权重",
            }
            raw = self.stack.enter_context(self.output_path.open("xb"))
            self.writer = self.stack.enter_context(gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0))
            self.handle = self.model.state.register_forward_hook(self._trace_state)
            self.stack.callback(self.handle.remove)
            self._write({"record_type": "header", **self.metadata})
        except BaseException:
            self.close()
            raise

    def _write(self, record):
        encoded = (json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n").encode()
        self.writer.write(encoded)
        self.digest.update(encoded)

    def _trace_state(self, _module, inputs, outputs):
        z, hidden = outputs
        previous = inputs[-1]
        self.traces.append({"z": _tensor(z, "连续z"), "h": _tensor(hidden, "连续h"),
                            "h_prev": _tensor(previous, "实际当组h_prev"),
                            "h_prev_detached": previous.grad_fn is None and not previous.requires_grad,
                            "state_graph_enabled": z.requires_grad})

    def step(self, native):
        if self.closed:
            raise ValueError("流式诊断已经关闭")
        # 只保留上一组数值；detach不重置隐藏状态，不截断公开来源历史。
        self.consumer.hidden = {key: value.detach() for key, value in self.consumer.hidden.items()}
        features = native.features()
        decision_rows = [row for row, value in enumerate(features["rows"]) if value["role"] == "DecisionRequired"]
        if len(decision_rows) > 1:
            raise ValueError("当前有限诊断要求每组最多一个决定行，不能静默漏检")
        self.checking = bool(decision_rows) and self.checked_groups < 32
        self.traces.clear()
        with torch.set_grad_enabled(self.checking):
            group = self.consumer.consume(features)
            states = [dict(trace, row=row, hidden_state_key=features["rows"][row]["hidden_state_key"])
                      for row, trace in enumerate(self.traces)]
            if len(states) != len(features["rows"]):
                raise ValueError("真实状态调用数与组行数不符")
            if any(not state["h_prev_detached"] or state["state_graph_enabled"] != self.checking for state in states):
                raise ValueError("局部梯度边界或当组状态图不符")
            record = {"record_type": "group", "index": self.group_count, "features": features,
                      "states": states, "gradient_checked": self.checking, "common": None, "sample": None}
            hidden_before = {key: value.detach().clone() for key, value in self.consumer.hidden.items()}
            if decision_rows:
                self.decision_groups += 1
            if self.checking:
                row = decision_rows[0]
                position = features["cutoff"]["receipt"]["group"]["group_position"]
                expected = None if self.expected_paths is None else self.expected_paths.get(self.group_count)
                if self.expected_paths is not None and (expected is None or expected["row"] != row
                                                       or expected["group_position"] != position
                                                       or expected["cutoff"] != features["cutoff"]
                                                       or expected["hidden_state_key"] != features["rows"][row]["hidden_state_key"]):
                    raise ValueError("exact共同路径与实际决定组不匹配")
                sampled, sample = _trace(self.consumer, group, row, native.actor(row))
                path = sample["path"] if expected is None else expected["path"]
                _, record["common"] = _trace(self.consumer, group, row, native.actor(row), path)
                before_replay = torch.random.get_rng_state().clone()
                replayed = consume_actor(self.consumer, group, row, native.actor(row), replay=sample["path"])
                sample["replayed_log_prob"] = _tensor(replayed.log_prob, "重算路径日志概率")
                sample["log_prob_equal"] = torch.equal(sampled.log_prob, replayed.log_prob)
                sample["replay_preserved_rng"] = torch.equal(before_replay, torch.random.get_rng_state())
                sample["gradient_check"] = _gradient_check(self.model, sampled, replayed, components=True)
                sample["has_nontrivial_choice"] = any(sum(step["support"]) > 1 for step in sample["steps"])
                self.state_nonzero_gradient_groups += sample["gradient_check"]["by_component"]["state"]["nonzero_elements"] > 0
                self.action_nonzero_gradient_groups += sample["gradient_check"]["by_component"]["action"]["nonzero_elements"] > 0
                self.all_gradient_checks_equal &= (sample["log_prob_equal"] and sample["replay_preserved_rng"]
                                                   and sample["gradient_check"]["all_equal"])
                record["sample"] = sample
                self.common_paths.append({"index": self.group_count, "group_position": position, "row": row,
                                          "cutoff": features["cutoff"],
                                          "hidden_state_key": features["rows"][row]["hidden_state_key"], "path": path})
                self.checked_groups += 1
            record["parameter_calls_preserved_hidden"] = (
                len(self.traces) == len(states) and self.consumer.hidden.keys() == hidden_before.keys()
                and all(torch.equal(self.consumer.hidden[key], value) for key, value in hidden_before.items()))
            if not record["parameter_calls_preserved_hidden"]:
                raise ValueError("参数调用或概率重算意外推进隐藏状态")
        self.consumer.hidden = {key: value.detach() for key, value in self.consumer.hidden.items()}
        record["retained_history_graphs"] = any(value.grad_fn is not None or value.requires_grad
                                               for value in self.consumer.hidden.values())
        self._write(record)
        self.group_count += 1
        self.state_rows += len(states)
        now = time.monotonic()
        if now - self.progress_at >= 10:
            self.writer.flush()
            print(f"流式组件诊断：已消费 {self.group_count} 组，决定组 {self.decision_groups}，"
                  f"局部梯度 {self.checked_groups}/32，已耗时 {now-self.started:.1f} 秒；剩余组数未知",
                  file=sys.stderr, flush=True)
            self.progress_at = now

    def finish(self):
        if self.closed:
            raise ValueError("流式诊断已经关闭")
        if self.expected_paths is not None and set(self.expected_paths) != {row["index"] for row in self.common_paths}:
            raise ValueError("exact共同路径未被完整消费")
        summary = {**self.metadata, "groups": self.group_count, "state_rows": self.state_rows,
                   "decision_groups": self.decision_groups, "gradient_checked_groups": self.checked_groups,
                   "gradient_target_reached": self.checked_groups == 32,
                   "state_nonzero_gradient_groups": self.state_nonzero_gradient_groups,
                   "action_nonzero_gradient_groups": self.action_nonzero_gradient_groups,
                   "all_gradient_checks_equal": self.all_gradient_checks_equal,
                   "weights_unchanged": _weights_sha256(self.model) == self.weights_sha256,
                   "hidden": [{"key": key, "value": _tensor(value, "最终隐藏状态")}
                              for key, value in sorted(self.consumer.hidden.items())],
                   "common_paths": {"schema": "projection_common_paths_v1", "rows": self.common_paths},
                   "elapsed_seconds": time.monotonic() - self.started}
        self._write({"record_type": "end", "groups": self.group_count, "sha256_before_end": self.digest.hexdigest()})
        self.close()
        digest = hashlib.sha256()
        with self.output_path.open("rb") as source:
            for block in iter(lambda: source.read(1 << 20), b""):
                digest.update(block)
        summary["group_stream"] = {"path": str(self.output_path), "compression": "gzip",
                                   "sha256": digest.hexdigest(), "bytes": self.output_path.stat().st_size}
        return json.dumps(summary, ensure_ascii=False, allow_nan=False)

    def close(self):
        if not self.closed:
            self.closed = True
            self.stack.close()


def run(groups, common_paths, seed=742001) -> str:
    """每组最多一个决定行；共同路径为完整原生索引列表或 None，返回 JSON。"""
    if len(groups) != len(common_paths):
        raise ValueError("公共组与共同路径数量不一致")
    started = time.monotonic()
    configuration = next(config for config in CONFIGURATIONS if config.name == "MAIN-1x256")
    previous_threads = torch.get_num_threads()
    torch.set_num_threads(1)
    try:
        with torch.enable_grad(), torch.random.fork_rng(devices=[]):
            torch.manual_seed(seed)
            model = TwoGraphs(configuration).eval()
            weights_sha256 = _weights_sha256(model)
            consumer = NativeComponentConsumer(model)
            report = {"schema": "projection_component_sensitivity_v1",
                      "scope": "单市场有限公共夹具、固定初始化模型、连续共同账户前史；自身采样只形成意图，不执行订单，不更新参数；非收益或正式100ms训练验收",
                      "configuration": configuration.name,
                      "initialization_seed": configuration.seed, "sampling_seed": seed,
                      "environment": {"python": sys.version, "torch": torch.__version__,
                                      "platform": platform.platform(), "machine": platform.machine(),
                                      "num_threads": torch.get_num_threads()},
                      "weights_sha256": weights_sha256, "device": "cpu", "groups": []}
            traces = []

            def trace_state(_module, _inputs, outputs):
                z, hidden = outputs
                traces.append({"z": _tensor(z, "连续 z_t"), "h": _tensor(hidden, "全部层 h_next")})

            handle = model.state.register_forward_hook(trace_state)
            try:
                for index, (native, path) in enumerate(zip(groups, common_paths, strict=True)):
                    traces.clear()
                    features = native.features()
                    group = consumer.consume(features)
                    states = [dict(trace, row=row, hidden_state_key=features["rows"][row]["hidden_state_key"])
                              for row, trace in enumerate(traces)]
                    if len(states) != len(features["rows"]):
                        raise ValueError("真实状态调用数与公共组行数不符")
                    decision_rows = [row for row, role in enumerate(group.roles) if role == "DecisionRequired"]
                    if len(decision_rows) > 1:
                        raise ValueError("当前有限诊断约定每组最多一个决定行")
                    record = {"index": index, "features": features, "states": states,
                              "common": None, "sample": None}
                    hidden_before = {key: value.detach().clone() for key, value in consumer.hidden.items()}
                    if decision_rows:
                        row = decision_rows[0]
                        if path is not None:
                            _, record["common"] = _trace(consumer, group, row, native.actor(row), path)
                        sampled, sample = _trace(consumer, group, row, native.actor(row))
                        before_replay = torch.random.get_rng_state().clone()
                        replayed = consume_actor(consumer, group, row, native.actor(row),
                                                 replay=sampled.actor.path())
                        sample["replayed_log_prob"] = _tensor(replayed.log_prob, "重算路径日志概率")
                        sample["log_prob_equal"] = torch.equal(sampled.log_prob, replayed.log_prob)
                        sample["replay_preserved_rng"] = torch.equal(before_replay, torch.random.get_rng_state())
                        sample["gradient_check"] = _gradient_check(model, sampled, replayed)
                        record["sample"] = sample
                    elif path is not None:
                        raise ValueError("无决定行的公共组不能指定共同动作路径")
                    record["parameter_calls_preserved_hidden"] = (
                        len(traces) == len(states) and consumer.hidden.keys() == hidden_before.keys()
                        and all(torch.equal(consumer.hidden[key], value) for key, value in hidden_before.items()))
                    report["groups"].append(record)
                    print(f"组件投影诊断：公共组 {index + 1}/{len(groups)}，状态行 {len(states)}，"
                          f"已耗时 {time.monotonic() - started:.3f} 秒", file=sys.stderr, flush=True)
            finally:
                handle.remove()
            report["hidden"] = [{"key": key, "value": _tensor(value, "最终隐藏状态")}
                                for key, value in sorted(consumer.hidden.items())]
            report["weights_unchanged"] = _weights_sha256(model) == weights_sha256
            report["elapsed_seconds"] = time.monotonic() - started
            return json.dumps(report, ensure_ascii=False, allow_nan=False)
    finally:
        torch.set_num_threads(previous_threads)


def verify_streaming_fixture(groups, common_paths, output_stream, seed=742001) -> str:
    """复用原有限夹具，仅比较完整历史图与流式局部图的前向数值。"""
    original = json.loads(run(groups, common_paths, seed))
    streaming = StreamingSensitivity(output_stream, "exact_cpu", seed=seed)
    try:
        for native in groups:
            streaming.step(native)
        summary = json.loads(streaming.finish())
    finally:
        streaming.close()
    comparisons = []
    with gzip.open(output_stream, "rt") as source:
        header = json.loads(next(source))
        assert header["record_type"] == "header"
        for expected in original["groups"]:
            actual = json.loads(next(source))
            comparisons.append({
                "index": expected["index"], "features_equal": expected["features"] == actual["features"],
                "state_values_equal": [{name: left[name] == right[name] for name in ("z", "h")}
                    for left, right in zip(expected["states"], actual["states"], strict=True)],
            })
        end = json.loads(next(source))
        assert end["record_type"] == "end" and end["groups"] == len(original["groups"])
        assert not source.read()
    equal = all(row["features_equal"] and all(all(state.values()) for state in row["state_values_equal"])
                for row in comparisons)
    original["streaming_fixture_check"] = {"all_forward_values_equal": equal, "groups": comparisons,
                                           "streaming_summary": summary,
                                           "scope": "仅前向数值兼容；局部梯度不与完整历史梯度比较"}
    if not equal:
        raise ValueError("原有限夹具的流式与完整历史图前向数值不同")
    return json.dumps(original, ensure_ascii=False, allow_nan=False)
