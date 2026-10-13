#!/usr/bin/env python3
"""有限无折扣/同批物理分块数学验证；不是完整PPO或正式训练profile。"""
import argparse
import json
import math
from pathlib import Path
import time

import torch
from model_train.dt_advantages import dt_advantages, normalize_advantages, AdvantageStatistics


def reference(values, following, rewards, elapsed, done, terminated, present, target_end):
    result = []
    targets = []
    for v, n, r, dt, d, terminal, population, end in zip(values, following, rewards, elapsed, done, terminated, present, target_end):
        previous = 0.0
        advantages = [0.0] * len(v)
        target = [0.0] * len(v)
        for t in reversed(range(len(v))):
            if not population[t]:
                previous = 0.0
                continue
            delta = r[t] + (0.0 if terminal[t] or end[t] else n[t]) - v[t]
            previous = delta + (0.0 if d[t] or terminal[t] or end[t] else previous)
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
    following = [[2.25, 3.5, 4., 6.], [1., 5., nan, nan], [nan] * 4]
    rewards = [[.5, -.2, 1., 2.], [1., 2., 3., nan], [nan] * 4]
    elapsed = [[0., .1, .7, .05], [.2, .4, .3, nan], [nan] * 4]
    done = [[False, False, False, True], [False, False, True, True], [True] * 4]
    terminal = [[False] * 4, [False, False, True, False], [False] * 4]
    present = [[True] * 4, [True, True, True, False], [False] * 4]
    target_end = [[False] * 4 for _ in values]
    tensor = lambda data, dtype=torch.float64: torch.tensor(data, dtype=dtype, device='cuda').unsqueeze(-1)
    v, n, r, dt = [tensor(data) for data in (values, following, rewards, elapsed)]
    v.requires_grad_(True)
    n.requires_grad_(True)
    d, term, population = [tensor(data, torch.bool) for data in (done, terminal, present)]
    end = tensor(target_end, torch.bool)
    known = population.clone()
    bootstrap_known = population & ~term
    original_fault = torch.zeros_like(v, dtype=torch.int32)
    original_fault[2] = 8  # 无真实决定的补齐故障不污染有效更新，但原字段保留。
    compute = lambda: dt_advantages(v, n, r, dt, d, term, population, known, bootstrap_known,
        original_fault, target_end=end)
    result = compute()
    assert result.ready.item() and result.count.item() == 7
    assert torch.equal(result.fault, original_fault) and torch.equal(result.present, population)
    expected = reference(values, following, rewards, elapsed, done, terminal, present, target_end)
    for actual, wanted in zip((result.advantage, result.value_target, normalize_advantages(result, result.statistics)), expected):
        torch.testing.assert_close(actual, tensor(wanted), rtol=1e-12, atol=1e-12)
    assert all(not item.requires_grad for item in (result.advantage, result.value_target, normalize_advantages(result, result.statistics)))
    assert torch.isfinite(result.value_target).all()
    assert not result.advantage[~population].any() and not normalize_advantages(result, result.statistics)[~population].any()
    # 保真实非等长dt及合法零间隔，但同金额不额外折价。
    fixed = dt_advantages(v, n, r, torch.full_like(dt, .1), d, term, population, known, bootstrap_known,
        original_fault, target_end=end)
    torch.testing.assert_close(result.advantage, fixed.advantage, rtol=0, atol=0)

    unknown_reward = known.clone()
    unknown_reward[0, 1] = False
    unknown_amount = r.clone()
    unknown_amount[0, 1] = nan
    unavailable = dt_advantages(v, n, unknown_amount, dt, d, term, population, unknown_reward, bootstrap_known,
        original_fault, target_end=end)
    assert not unavailable.ready.item() and unavailable.count.item() == 7
    assert torch.equal(unavailable.present, population) and torch.isfinite(unavailable.advantage).all()
    unknown_bootstrap = bootstrap_known.clone()
    unknown_bootstrap[0, -1] = False  # rollout截断不能忽略所需bootstrap。
    bootstrap_amount = n.clone()
    bootstrap_amount[0, -1] = nan
    unavailable = dt_advantages(v, bootstrap_amount, r, dt, d, term, population, known, unknown_bootstrap,
        original_fault, target_end=end)
    assert not unavailable.ready.item() and torch.isfinite(unavailable.value_target).all()

    backwards = dt.clone()
    backwards[0, 0] = -.1
    invalid = dt_advantages(v, n, r, backwards, d, term, population, known, bootstrap_known,
        original_fault, target_end=end)
    assert not invalid.ready.item() and invalid.fault[0, 0].item() != 0
    assert torch.equal(invalid.present, population)
    empty = dt_advantages(v, n, r, dt, d, term, torch.zeros_like(population), known, bootstrap_known,
        original_fault, target_end=end)
    assert not empty.ready.item() and empty.count.item() == 0 and not empty.advantage.any()

    def blocks(kw=None):
        columns = [v, n, r, dt, d, term, population, known, bootstrap_known, original_fault]
        if kw:
            for index, col in kw.items(): columns[index] = col
        right = dt_advantages(*(col[:, 2:] for col in columns), target_end=end[:, 2:])
        left = dt_advantages(*(col[:, :2] for col in columns), target_end=end[:, :2],
                             continuation=right.continuation)
        return left, right, left.statistics.merge(right.statistics)
    left, right, stats = blocks()
    assert stats.ready.item() and stats.count.item() == 7
    for name in ('advantage', 'value_target', 'fault'):
        torch.testing.assert_close(torch.cat([getattr(left,name),getattr(right,name)],dim=1),
                                   getattr(result,name),rtol=1e-12,atol=1e-12)
    padding_stats = empty.statistics.merge(stats)
    assert padding_stats.ready.item() and padding_stats.count.item() == 7
    torch.testing.assert_close(padding_stats.mean,stats.mean,rtol=0,atol=0)
    combined = torch.cat([normalize_advantages(block,stats) for block in (left,right)],dim=1)
    torch.testing.assert_close(combined,normalize_advantages(result,result.statistics),rtol=1e-12,atol=1e-12)
    # 每块单独标准化会错；不得把右首V当作同批的连续G。
    separate = torch.cat([normalize_advantages(block,block.statistics) for block in (left,right)],dim=1)
    assert not torch.allclose(separate,combined)
    truncated = dt_advantages(v[:,:2],n[:,:2],r[:,:2],dt[:,:2],d[:,:2],term[:,:2],
        population[:,:2],known[:,:2],bootstrap_known[:,:2],original_fault[:,:2],target_end=end[:,:2])
    assert not torch.allclose(truncated.advantage,left.advantage)
    bad_known = known.clone();bad_known[0,2] = False
    bad_left,bad_right,bad_stats = blocks({7:bad_known})
    assert not bad_left.ready.item() and not bad_stats.ready.item()
    assert torch.equal(bad_left.present,population[:,:2])
    bad_fault = original_fault.clone();bad_fault[0,2] = 4
    bad_left,bad_right,bad_stats = blocks({9:bad_fault})
    assert not bad_stats.ready.item() and bad_left.continuation.fault[0].item() == 4
    # 同一逻辑批的普通末端保6的Critic自举；有限目标结束单独清余值。
    assert result.value_target[0,-1].item() == 8.
    goal_end = end.clone();goal_end[0,-1] = True
    missing_end = bootstrap_known.clone();missing_end[0,-1] = False
    goal = dt_advantages(v,n,r,dt,d,term,population,known,missing_end,original_fault,target_end=goal_end)
    assert goal.ready.item() and goal.value_target[0,-1].item() == 2.
    assert result.value_target[1,2].item() == 3.  # 真经济终止不消费NaN bootstrap。
    assert not term[0,-1].item() and not end[0,-1].item()  # 输入业务标记未改。

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
    wanted = reference(values, following, rewards, elapsed, done, terminal, present, target_end)
    for actual, independent in zip((captured.advantage, captured.value_target, normalize_advantages(captured, captured.statistics)), wanted):
        torch.testing.assert_close(actual, tensor(independent), rtol=1e-12, atol=1e-12)
    # 实际训练浮点精度复用同七决定，与double标量oracle独立核值及捕获。
    v32, n32 = [item.detach().to(torch.float32).requires_grad_(True) for item in (v, n)]
    r32, dt32 = [item.to(torch.float32) for item in (r, dt)]
    compute32 = lambda: dt_advantages(v32, n32, r32, dt32, d, term, population, known, bootstrap_known,
        original_fault, target_end=end)
    result32 = compute32()
    # 标量统计代表：不分配大轨迹，计数沿真实FP32组件的count dtype。
    assert result32.count.dtype == torch.int64
    unit = torch.ones((), dtype=torch.float32, device='cuda')
    valid = torch.ones((), dtype=torch.bool, device='cuda')
    large = AdvantageStatistics(result32.count.new_tensor(2**24), unit, unit * 0, valid)
    singleton = AdvantageStatistics(result32.count.new_tensor(1), unit, unit * 0, valid)
    merged = large.merge(singleton)
    assert merged.count.dtype == torch.int64 and merged.count.item() == 2**24 + 1
    assert merged.mean.dtype == torch.float32 and merged.m2.dtype == torch.float32
    assert merged.mean.item() == 1. and merged.m2.item() == 0. and merged.ready.item()
    assert result32.ready.item() and result32.count.item() == 7
    assert all(not item.requires_grad for item in (result32.advantage, result32.value_target, normalize_advantages(result32, result32.statistics)))
    for actual, independent in zip((result32.advantage, result32.value_target, normalize_advantages(result32, result32.statistics)), wanted):
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
    wanted32 = reference(values, following, rewards, elapsed32, done, terminal, present, target_end)
    for actual, independent in zip((captured32.advantage, captured32.value_target, normalize_advantages(captured32, captured32.statistics)), wanted32):
        torch.testing.assert_close(actual, tensor(independent, torch.float32), rtol=1e-5, atol=2e-6)
    report = {'status': 'PASS', 'real_decisions': 7, 'dtype': str(v.dtype),
        'module_file': __import__('model_train.dt_advantages',fromlist=['dt_advantages']).__file__,
        'torch_version': torch.__version__, 'torchrl_version': __import__('importlib.metadata',fromlist=['version']).version('torchrl'),
        'dt_seconds': [[t for t, p in zip(row, mask) if p] for row, mask in zip(elapsed, present)],
        'gamma': 1., 'lambda': 1.,
        'int64_count_crosses_float32_exact_integer_boundary': int(merged.count.item()),
        'physical_blocks_match_whole_batch': True, 'whole_batch_statistics': True,
        'ordinary_bootstrap_target_end_economic_termination_separate': True,
        'unknown_keeps_population_and_blocks_whole_update': True, 'actual_dt_capture_replay': True,
        'float32_independent_values_capture_replay': True, 'grad_inputs_detached_targets': True,
        'complete_PPO_optimizer_reward_collector_restore': False,
        'elapsed_seconds': time.perf_counter() - started}
    args.output.write_text(json.dumps(report, allow_nan=False, indent=2) + '\n')
    print(json.dumps(report, allow_nan=False), flush=True)


if __name__ == '__main__':
    main()
