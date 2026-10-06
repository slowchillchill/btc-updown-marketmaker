"""现有逐帧Rust支持与精确前缀上的概率消费。

逐条件CPU回调保留原消费者用途；#181设备阶段循环设计见docs/100ms-gpu-training.md。
本模块当前不实现100ms GPU环境、PPO或活动根恢复。
"""
from dataclasses import dataclass

import torch
from torch.distributions import Categorical


@dataclass
class ActorResult:
    actor: object
    log_prob: torch.Tensor
    conditional_entropy_sum: torch.Tensor


def conditional_choice(logits, support, *, deterministic=False, selected=None):
    indices = torch.as_tensor(support, dtype=torch.bool, device=logits.device).nonzero().flatten()
    legal = logits[indices]
    if len(indices) == 1:
        choice = int(indices[0])
        if selected is not None and selected != choice:
            raise ValueError("replayed index is outside Rust support")
        zero = legal.sum() * 0
        return choice, zero, zero
    distribution = Categorical(logits=legal)
    if selected is not None:
        location = (indices == selected).nonzero().flatten()
        if len(location) != 1:
            raise ValueError("replayed index is outside Rust support")
        local = location[0]
    elif deterministic:
        local = legal.argmax()  # 同分稳定选择原顺序首项。
    else:
        local = distribution.sample()
    return int(indices[local]), distribution.log_prob(local), distribution.entropy()


def consume_actor(consumer, group, row, actor, *, deterministic=False, replay=None):
    """沿同一 Rust 路径重算条件 log_prob；熵和仅代表所访问的条件分布。"""
    log_prob = group.states[row].sum() * 0
    entropy = log_prob
    step = 0
    while not actor.complete():
        support = actor.support()
        selected = replay[step] if replay is not None else None
        legal = [i for i, value in enumerate(support) if value]
        if len(legal) == 1:
            choice = legal[0]
            if selected is not None and selected != choice:
                raise ValueError("replayed index is outside Rust support")
            conditional_log_prob = log_prob * 0
            conditional_entropy = conditional_log_prob
        else:
            outputs = consumer.parameters(group, row, actor.prefix_features(),
                price_fee_features=actor.price_fee_features(), price_indices=actor.price_indices())
            head = actor.head_index()
            logits = (torch.cat([block[head][0] for block in outputs])[:len(support)]
                      if head == 5 else outputs[0][head][0])
            choice, conditional_log_prob, conditional_entropy = conditional_choice(
                logits, support, deterministic=deterministic, selected=selected)
        actor.choose(choice)
        log_prob = log_prob + conditional_log_prob
        entropy = entropy + conditional_entropy
        step += 1
    if replay is not None and step != len(replay):
        raise ValueError("replay has extra indices")
    return ActorResult(actor, log_prob, entropy)


def joint_entropy_sample(consumer, group, row, actor):
    """§9.4 的独立完整动作样本；返回报告值和具有指定梯度的替代项。"""
    result = consume_actor(consumer, group, row, actor, deterministic=False)
    ell = result.log_prob
    return -0.5 * ell.detach(), 0.5 * (ell.detach() + 1) * ell, result
