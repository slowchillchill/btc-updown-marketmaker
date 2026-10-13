"""B有限无折扣GAE；真实dt只保时间故障，奖励时间成本归RewardIntervalMemory。"""
from dataclasses import dataclass

import torch
from torch import Tensor
from torchrl.objectives.value.functional import generalized_advantage_estimate


@dataclass(frozen=True)
class AdvantageStatistics:
    count: Tensor
    mean: Tensor
    m2: Tensor
    valid: Tensor

    @property
    def ready(self):
        return (self.count > 0) & self.valid

    def merge(self, other):
        """同一逻辑批的设备统计；空块不缩小有效决定人口。"""
        count = self.count + other.count
        delta = other.mean - self.mean
        divisor = count.clamp_min(1).to(self.mean.dtype)
        left_count = self.count.to(self.mean.dtype)
        right_count = other.count.to(self.mean.dtype)
        mean = self.mean + delta * (right_count / divisor)
        m2 = self.m2 + other.m2 + delta.square() * (left_count / divisor) * right_count
        return AdvantageStatistics(count, mean, m2, self.valid & other.valid)


@dataclass(frozen=True)
class AdvantageContinuation:
    advantage: Tensor
    present: Tensor
    known: Tensor
    fault: Tensor


@dataclass(frozen=True)
class AdvantageBatch:
    advantage: Tensor
    value_target: Tensor
    present: Tensor
    fault: Tensor
    statistics: AdvantageStatistics
    continuation: AdvantageContinuation

    @property
    def count(self):
        return self.statistics.count

    @property
    def ready(self):
        return self.statistics.ready


@torch.no_grad()
def normalize_advantages(batch: AdvantageBatch, statistics: AdvantageStatistics) -> Tensor:
    """所有物理块先合并整逻辑批统计；minibatch不重新标准化。"""
    divisor = statistics.count.clamp_min(1).to(batch.advantage.dtype)
    std = (statistics.m2 / divisor).sqrt().clamp_min(
        torch.finfo(batch.advantage.dtype).eps)
    return torch.where(batch.present, (batch.advantage - statistics.mean) / std,
                       torch.zeros_like(batch.advantage))


@torch.no_grad()
def dt_advantages(value: Tensor, next_value: Tensor, reward: Tensor, dt_seconds: Tensor,
                  done: Tensor, terminated: Tensor, present: Tensor,
                  reward_known: Tensor, bootstrap_known: Tensor, fault: Tensor, *,
                  target_end: Tensor, continuation: AdvantageContinuation | None = None) -> AdvantageBatch:
    """gamma=lambda=1；普通批末自举，目标末端与经济终止分开。

    物理块从右向左。只在原连续边界将右首优势加入临时末端奖励，保留
    原next_value；因此不要求next_value恰等于右首value。有效轨迹下等价
    承接右首G。原奖励/业务done/terminated不原位改变。Unknown保人口。
    """
    if value.ndim != 3 or value.shape[-1] != 1 or min(value.shape[:2]) < 1:
        raise ValueError('决定列须为非空[R,T,1]')
    floating = (value, next_value, reward, dt_seconds)
    masks = (done, terminated, present, reward_known, bootstrap_known, target_end)
    for column in (*floating, *masks, fault):
        if column.shape != value.shape or column.device != value.device:
            raise ValueError('所有决定列须同shape/设备')
    if not value.is_floating_point() or any(column.dtype != value.dtype for column in floating):
        raise TypeError('值/奖励/秒数须同浮点dtype')
    if any(column.dtype != torch.bool for column in masks) or fault.dtype not in (torch.int32, torch.int64):
        raise TypeError('人口/状态/known须bool，原fault须整数')

    zero = torch.zeros_like(value)
    ended = terminated | target_end
    stopped = done | ended | ~present
    faults = fault | (present & (dt_seconds < 0)).to(fault.dtype)
    known = reward_known & (ended | bootstrap_known)
    values = torch.where(present, value, zero)
    rewards = torch.where(present & reward_known, reward, zero)
    bootstrap = torch.where(present & ~ended & bootstrap_known, next_value, zero)
    if continuation is not None:
        # 真正右邻是同一根的下一物理列；尾补齐本来就是依赖屏障。
        linked = present[:, -1:] & ~stopped[:, -1:] & continuation.present
        rewards = torch.cat((rewards[:, :-1], rewards[:, -1:] + torch.where(
            linked, continuation.advantage, zero[:, -1:])), dim=1)
        known = torch.cat((known[:, :-1], known[:, -1:] & (
            ~linked | continuation.known)), dim=1)
        faults = torch.cat((faults[:, :-1], faults[:, -1:] | torch.where(
            linked, continuation.fault, torch.zeros_like(faults[:, -1:]))), dim=1)
        stopped = torch.cat((stopped[:, :-1], stopped[:, -1:] | linked), dim=1)
    advantage, target = generalized_advantage_estimate(
        1., 1., values, bootstrap, rewards, stopped, ended | ~present, time_dim=1)
    advantage = torch.where(present, advantage, zero)
    target = torch.where(present, target, zero)
    # 对应同一递推的依赖资格；不把Unknown或故障决定删出统计人口。
    next_known = torch.ones_like(known[:, 0])
    next_fault = torch.zeros_like(faults[:, 0])
    dependent_known, dependent_fault = [], []
    for t in reversed(range(value.shape[1])):
        follows = present[:, t] & ~stopped[:, t]
        next_known = ~present[:, t] | (known[:, t] & (~follows | next_known))
        next_fault = faults[:, t] | torch.where(follows, next_fault, torch.zeros_like(next_fault))
        dependent_known.append(next_known)
        dependent_fault.append(next_fault)
        next_fault = torch.where(present[:, t], next_fault, torch.zeros_like(next_fault))
    known = torch.stack(dependent_known[::-1], dim=1)
    faults = torch.stack(dependent_fault[::-1], dim=1)
    count = present.sum(dtype=torch.int64)
    valid = (~present | known).all() & (~present | (faults == 0)).all()
    mean = advantage.sum() / count.clamp_min(1).to(value.dtype)
    centered = torch.where(present, advantage - mean, zero)
    statistics = AdvantageStatistics(count, mean, centered.square().sum(), valid)
    successor = AdvantageContinuation(advantage[:, :1], present[:, :1],
                                      known[:, :1], faults[:, :1])
    return AdvantageBatch(advantage, target, present, faults, statistics, successor)
