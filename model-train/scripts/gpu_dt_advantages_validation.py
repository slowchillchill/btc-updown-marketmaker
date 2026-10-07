#!/usr/bin/env python3
"""有限数学工程样本；显式秒数/tau不是正式训练profile或经济根配置。"""
import argparse
import json
import math
from pathlib import Path
import time

import torch
from model_train.dt_advantages import dt_advantages


def reference(values, following, rewards, elapsed, done, terminated, present, gamma_tau, lambda_tau):
    result = []
    targets = []
    for v, n, r, dt, d, terminal, population in zip(values, following, rewards, elapsed, done, terminated, present):
        previous = 0.0
        advantages = [0.0] * len(v)
        target = [0.0] * len(v)
        for t in reversed(range(len(v))):
            if not population[t]:
                previous = 0.0
                continue
            gamma, lmbda = math.exp(-dt[t] / gamma_tau), math.exp(-dt[t] / lambda_tau)
            delta = r[t] + (0.0 if terminal[t] else gamma * n[t]) - v[t]
            previous = delta + (0.0 if d[t] or terminal[t] else gamma * lmbda * previous)
            advantages[t], target[t] = previous, previous + v[t]
        result.append(advantages)
        targets.append(target)
    selected = [a for row, mask in zip(result, present) for a, p in zip(row, mask) if p]
    mean = sum(selected) / len(selected)
    std = math.sqrt(sum((a - mean) ** 2 for a in selected) / len(selected))
    normalized = [[(a - mean) / std if p else 0.0 for a, p in zip(row, mask)] for row, mask in zip(result, present)]
    return result, targets, normalized


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    started = time.perf_counter()
    nan = float('nan')
    values = [[1., 2., 3., 4.], [2., 1., 5., nan], [nan] * 4]
    following = [[2., 3., 4., 6.], [1., 5., nan, nan], [nan] * 4]
    rewards = [[.5, -.2, 1., 2.], [1., 2., 3., nan], [nan] * 4]
    elapsed = [[0., .1, .7, .05], [.2, .4, .3, nan], [nan] * 4]
    done = [[False, False, False, True], [False, False, True, True], [True] * 4]
    terminal = [[False] * 4, [False, False, True, False], [False] * 4]
    present = [[True] * 4, [True, True, True, False], [False] * 4]
    # 只为本有限验证显式选值，生产函数不保存或提供默认profile。
    gamma_tau, lambda_tau = 1.7, .8
    tensor = lambda data, dtype=torch.float64: torch.tensor(data, dtype=dtype, device='cuda').unsqueeze(-1)
    v, n, r, dt = [tensor(data) for data in (values, following, rewards, elapsed)]
    v.requires_grad_(True)
    n.requires_grad_(True)
    d, term, population = [tensor(data, torch.bool) for data in (done, terminal, present)]
    known = population.clone()
    bootstrap_known = population & ~term
    original_fault = torch.zeros_like(v, dtype=torch.int32)
    original_fault[2] = 8  # 无真实决定的补齐故障不污染有效更新，但原字段保留。
    compute = lambda: dt_advantages(v, n, r, dt, d, term, population, known, bootstrap_known,
        original_fault, tau_gamma_seconds=gamma_tau, tau_lambda_seconds=lambda_tau)
    result = compute()
    assert result.ready.item() and result.count.item() == 7
    assert torch.equal(result.fault, original_fault) and torch.equal(result.present, population)
    expected = reference(values, following, rewards, elapsed, done, terminal, present, gamma_tau, lambda_tau)
    for actual, wanted in zip((result.advantage, result.value_target, result.normalized_advantage), expected):
        torch.testing.assert_close(actual, tensor(wanted), rtol=1e-12, atol=1e-12)
    assert all(not item.requires_grad for item in (result.advantage, result.value_target, result.normalized_advantage))
    assert torch.isfinite(result.value_target).all()
    assert not result.advantage[~population].any() and not result.normalized_advantage[~population].any()
    # 非等长dt及合法零间隔不能被固定100ms替代。
    fixed = dt_advantages(v, n, r, torch.full_like(dt, .1), d, term, population, known, bootstrap_known,
        original_fault, tau_gamma_seconds=gamma_tau, tau_lambda_seconds=lambda_tau)
    assert not torch.allclose(result.advantage[population], fixed.advantage[population])

    unknown_reward = known.clone()
    unknown_reward[0, 1] = False
    unknown_amount = r.clone()
    unknown_amount[0, 1] = nan
    unavailable = dt_advantages(v, n, unknown_amount, dt, d, term, population, unknown_reward, bootstrap_known,
        original_fault, tau_gamma_seconds=gamma_tau, tau_lambda_seconds=lambda_tau)
    assert not unavailable.ready.item() and unavailable.count.item() == 7
    assert torch.equal(unavailable.present, population) and torch.isfinite(unavailable.advantage).all()
    unknown_bootstrap = bootstrap_known.clone()
    unknown_bootstrap[0, -1] = False  # rollout截断不能忽略所需bootstrap。
    bootstrap_amount = n.clone()
    bootstrap_amount[0, -1] = nan
    unavailable = dt_advantages(v, bootstrap_amount, r, dt, d, term, population, known, unknown_bootstrap,
        original_fault, tau_gamma_seconds=gamma_tau, tau_lambda_seconds=lambda_tau)
    assert not unavailable.ready.item() and torch.isfinite(unavailable.value_target).all()

    backwards = dt.clone()
    backwards[0, 0] = -.1
    invalid = dt_advantages(v, n, r, backwards, d, term, population, known, bootstrap_known,
        original_fault, tau_gamma_seconds=gamma_tau, tau_lambda_seconds=lambda_tau)
    assert not invalid.ready.item() and invalid.fault[0, 0].item() != 0
    assert torch.equal(invalid.present, population)
    empty = dt_advantages(v, n, r, dt, d, term, torch.zeros_like(population), known, bootstrap_known,
        original_fault, tau_gamma_seconds=gamma_tau, tau_lambda_seconds=lambda_tau)
    assert not empty.ready.item() and empty.count.item() == 0 and not empty.advantage.any()

    # 同设备捕获/重放不含逐决定CPU读取；改变实际dt重放，再对独立原公式。
    compute()
    graph = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        captured = compute()
    graph.replay()
    torch.cuda.synchronize()
    torch.testing.assert_close(captured.advantage, result.advantage)
    dt[0, 2] = .9
    graph.replay()
    torch.cuda.synchronize()
    elapsed[0][2] = .9
    wanted = reference(values, following, rewards, elapsed, done, terminal, present, gamma_tau, lambda_tau)
    for actual, independent in zip((captured.advantage, captured.value_target, captured.normalized_advantage), wanted):
        torch.testing.assert_close(actual, tensor(independent), rtol=1e-12, atol=1e-12)
    # 实际训练浮点精度复用同七决定，与double标量oracle独立核值及捕获。
    v32, n32 = [item.detach().to(torch.float32).requires_grad_(True) for item in (v, n)]
    r32, dt32 = [item.to(torch.float32) for item in (r, dt)]
    compute32 = lambda: dt_advantages(v32, n32, r32, dt32, d, term, population, known, bootstrap_known,
        original_fault, tau_gamma_seconds=gamma_tau, tau_lambda_seconds=lambda_tau)
    result32 = compute32()
    assert result32.ready.item() and result32.count.item() == 7
    assert all(not item.requires_grad for item in (result32.advantage, result32.value_target, result32.normalized_advantage))
    for actual, independent in zip((result32.advantage, result32.value_target, result32.normalized_advantage), wanted):
        torch.testing.assert_close(actual, tensor(independent, torch.float32), rtol=1e-5, atol=2e-6)
    graph32 = torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph32):
        captured32 = compute32()
    graph32.replay()
    torch.cuda.synchronize()
    torch.testing.assert_close(captured32.advantage, result32.advantage)
    dt32[0, 2] = .25
    graph32.replay()
    torch.cuda.synchronize()
    elapsed32 = [row[:] for row in elapsed]
    elapsed32[0][2] = .25
    wanted32 = reference(values, following, rewards, elapsed32, done, terminal, present, gamma_tau, lambda_tau)
    for actual, independent in zip((captured32.advantage, captured32.value_target, captured32.normalized_advantage), wanted32):
        torch.testing.assert_close(actual, tensor(independent, torch.float32), rtol=1e-5, atol=2e-6)
    report = {'status': 'PASS', 'real_decisions': 7, 'dtype': str(v.dtype),
        'dt_seconds': [[t for t, p in zip(row, mask) if p] for row, mask in zip(elapsed, present)],
        'engineering_tau_seconds': [gamma_tau, lambda_tau],
        'unknown_keeps_population_and_blocks_whole_update': True, 'actual_dt_capture_replay': True,
        'float32_independent_values_capture_replay': True, 'grad_inputs_detached_targets': True,
        'complete_PPO_optimizer_reward_collector_restore': False,
        'elapsed_seconds': time.perf_counter() - started}
    args.output.write_text(json.dumps(report, allow_nan=False, indent=2) + '\n')
    print(json.dumps(report, allow_nan=False), flush=True)


if __name__ == '__main__':
    main()
