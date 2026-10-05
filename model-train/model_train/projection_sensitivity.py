"""真实原生组的有限投影敏感性诊断；固定初始化权重，不执行订单或更新参数。"""

import ctypes
import hashlib
import json
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


def _gradient_check(model, sampled, replayed):
    parameters = tuple(model.parameters())
    first = torch.autograd.grad(sampled.log_prob, parameters, allow_unused=True, retain_graph=True)
    second = torch.autograd.grad(replayed.log_prob, parameters, allow_unused=True, retain_graph=True)
    usage_equal = True
    equal = True
    compared = nonzero = different = 0
    maximum = 0.0
    for left, right in zip(first, second, strict=True):
        if left is None or right is None:
            usage_equal &= (left is None) == (right is None)
            continue
        if not torch.isfinite(left).all() or not torch.isfinite(right).all():
            raise ValueError("概率重算梯度含非有限值")
        compared += left.numel()
        nonzero += int(torch.count_nonzero(left))
        different += int(torch.count_nonzero(left != right))
        equal &= torch.equal(left, right)
        if left.numel():
            maximum = max(maximum, float((left - right).abs().max()))
    return {"parameter_usage_equal": usage_equal, "all_equal": usage_equal and equal,
            "compared_elements": compared, "nonzero_elements": nonzero,
            "different_elements": different, "maximum_absolute_difference": maximum}


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
