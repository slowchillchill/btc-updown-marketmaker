"""按实际奖励区间秒数计算GAE；候选不授予optimizer更新资格。

输入时间轴只含真实决定与尾部补齐。ContextOnly推进GRU，但不在此造转移。
未知奖励保留真实决定人口并令整批not-ready；学习入口必须另行门控step。
"""
from dataclasses import dataclass
import math

import torch
from torch import Tensor
from torchrl.objectives.value.functional import generalized_advantage_estimate


@dataclass(frozen=True)
class AdvantageBatch:
    advantage: Tensor
    value_target: Tensor
    normalized_advantage: Tensor
    present: Tensor
    count: Tensor
    ready: Tensor
    fault: Tensor


@torch.no_grad()
def dt_advantages(value: Tensor, next_value: Tensor, reward: Tensor, dt_seconds: Tensor,
                  done: Tensor, terminated: Tensor, present: Tensor,
                  reward_known: Tensor, bootstrap_known: Tensor, fault: Tensor, *,
                  tau_gamma_seconds: float, tau_lambda_seconds: float) -> AdvantageBatch:
    """整次更新计算一次；minibatch只能索引返回的已标准化优势。

    截断done切断递推，但保留非terminated的next_value。经济终止不要求
    未使用的bootstrap已知。Unknown/补齐可用NaN占位，先设备mask再交TorchRL。
    dt==0合法；负dt表示真实时间倒退。tau为显式工程/获批profile参数，无默认。
    """
    if value.ndim != 3 or value.shape[-1] != 1 or min(value.shape[:2]) < 1:
        raise ValueError('决定列须为非空[R,T,1]')
    floating = (value, next_value, reward, dt_seconds)
    masks = (done, terminated, present, reward_known, bootstrap_known)
    for column in (*floating, *masks, fault):
        if column.shape != value.shape or column.device != value.device:
            raise ValueError('所有决定列须同shape/设备')
    if not value.is_floating_point() or any(column.dtype != value.dtype for column in floating):
        raise TypeError('值/奖励/秒数须同浮点dtype')
    if any(column.dtype != torch.bool for column in masks) or fault.dtype not in (torch.int32, torch.int64):
        raise TypeError('人口/状态/known须bool，原fault须整数')
    for tau in (tau_gamma_seconds, tau_lambda_seconds):
        if not isinstance(tau, (float, int)) or not math.isfinite(tau) or tau <= 0:
            raise ValueError('时间常数须为显式有限正秒数')

    zero = torch.zeros_like(value)
    regression = present & (dt_seconds < 0)
    faults = fault | regression.to(fault.dtype)
    count = present.to(value.dtype).sum()
    ready = (count > 0) & (~present | reward_known).all() & (
        ~present | terminated | bootstrap_known).all() & (~present | (faults == 0)).all()
    elapsed = torch.where(present & ~regression, dt_seconds, zero)
    values = torch.where(present, value, zero)
    rewards = torch.where(present & reward_known, reward, zero)
    bootstrap = torch.where(present & ~terminated & bootstrap_known, next_value, zero)
    advantage, target = generalized_advantage_estimate(
        torch.exp(-elapsed / tau_gamma_seconds), torch.exp(-elapsed / tau_lambda_seconds),
        values, bootstrap, rewards, done | terminated | ~present, terminated | ~present,
        time_dim=1)
    advantage = torch.where(present, advantage, zero)
    target = torch.where(present, target, zero)
    divisor = count.clamp_min(1)
    mean = advantage.sum() / divisor
    centered = torch.where(present, advantage - mean, zero)
    std = (centered.square().sum() / divisor).sqrt().clamp_min(torch.finfo(value.dtype).eps)
    normalized = torch.where(present, centered / std, zero)
    return AdvantageBatch(advantage, target, normalized, present, count, ready, faults)
