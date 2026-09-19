"""Rust 支持及精确前缀上的概率消费；不推导可行动作或重建金额。"""
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
        outputs = consumer.parameters(group, row, actor.prefix_features(),
                                      price_fee_features=actor.price_fee_features())
        head = actor.head_index()
        if head == 7:
            count = len(support)
            logits = torch.cat([block[head][0] for block in outputs])[:count]
        else:
            logits = outputs[0][head][0]
        selected = replay[step] if replay is not None else None
        choice, conditional_log_prob, conditional_entropy = conditional_choice(
            logits, support, deterministic=deterministic, selected=selected)
        actor.choose(choice)
        log_prob = log_prob + conditional_log_prob
        entropy = entropy + conditional_entropy
        step += 1
    if replay is not None and step != len(replay):
        raise ValueError("replay has extra indices")
    return ActorResult(actor, log_prob, entropy)
