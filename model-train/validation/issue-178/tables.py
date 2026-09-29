"""将正式两轮原始调用的合并统计渲染为报告表格；不重新计时。"""
import argparse
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    rows = json.loads((args.source / 'pooled-summary.json').read_text())
    by = {(r['model'], r['window'], r['variant']): r for r in rows}
    cases = [(model, window) for model in ('MAIN-1x256', 'MAIN-30x1024') for window in ('ordinary', 'high')]
    comparison = ['| 模型 | 窗口 | native基线→最终 ms | native降幅 | 完整服务基线→最终 ms | 服务降幅 |',
                  '|---|---|---:|---:|---:|---:|']
    lines = ['# 正式16次分布与资源', '',
             '每个模型/窗口/版本合并两轮各64组的原始128组。包含每次入口首组；分位数在合并后的原始值上计算，不平均两轮分位数。所有时间单位为毫秒。', '',
             '均值可按互斥墙钟阶段相加，分位数和最大值不可相加。服务CPU是这128次服务的进程CPU，不能与墙钟阶段相加。', '']
    phases = [('native', '原生输入'), ('packing', '打包'), ('state', '状态图'), ('action', '动作准备与选择'),
              ('execute', '原执行请求'), ('remaining_service', '其余服务工作'), ('service', '完整服务'), ('cpu', '服务CPU')]
    for model, window in cases:
        label = '普通' if window == 'ordinary' else '高更新'
        base, final = [by[model, window, variant]['phases_ms'] for variant in ('baseline', 'candidate')]
        values = [f'| {model} | {label} ']
        for phase in ('native', 'service'):
            old, new = base[phase]['mean'], final[phase]['mean']
            values.append(f'| {old:.3f} → {new:.3f} | {(1-new/old)*100:.2f}% ')
        comparison.append(''.join(values) + '|')
        lines += [f'## {model} · {label}', '',
                  '| 阶段 | 基线均值 | P50 | P95 | max | 最终均值 | P50 | P95 | max |',
                  '|---|---:|---:|---:|---:|---:|---:|---:|---:|']
        for phase, name in phases:
            values = [f'{stats[phase][key]:.3f}' for stats in (base, final) for key in ('mean', 'p50', 'p95', 'max')]
            lines.append('| ' + name + ' | ' + ' | '.join(values) + ' |')
        lines.append('')
    lines += ['## 整个独立进程的CPU与峰值RSS', '',
              '每格按r1 / r2排列。进程资源包含图加载、真实公共前史、缓存/hidden恢复、窗口服务和证据生成；RSS不是新增缓存的独占内存。', '',
              '| 模型 | 窗口 | 版本 | user秒 | system秒 | 峰值RSS MiB |',
              '|---|---|---|---:|---:|---:|']
    for model, window in cases:
        for variant in ('baseline', 'candidate'):
            resources = by[model, window, variant]['process_resources_each_round']
            values = [' / '.join(f'{r[key]/scale:.3f}' for r in resources)
                      for key, scale in [('user_seconds', 1), ('system_seconds', 1), ('maximum_rss_kib', 1024)]]
            lines.append('| ' + ' | '.join([model, '普通' if window == 'ordinary' else '高更新',
                                           '基线' if variant == 'baseline' else '最终', *values]) + ' |')
    preparations = json.loads((args.source / 'continuous-preparation.json').read_text())
    lines += ['', '## 各独立进程的入口准备', '',
              '每格为同模型/窗口四次进程的最小–最大值，单位秒。准备总时间含图加载、形状预热、公共前史重放、检查点读取和恢复；分项已包含在总时间内。', '',
              '恢复项合计native暖缓存、模型hidden与打包缓存，三者没有分别计时；既有hidden的历史模型推理未重跑。此表与实际连续native热身重放是不同阶段，不能重复相加到窗口服务。', '',
              '| 模型 | 窗口 | 准备总时间 | 图加载 | 形状预热 | native/hidden/打包恢复 | 恢复历史模型行数 |',
              '|---|---|---:|---:|---:|---:|---:|']
    for model, window in cases:
        rows = [row for row in preparations if model in row['run'] and window in row['run']]
        assert len(rows) == 4
        values = [f'{min(row[key] for row in rows):.3f}–{max(row[key] for row in rows):.3f}'
                  for key in ('preparation_seconds', 'graph_load_seconds', 'graph_shape_warmup_seconds', 'cache_restore_seconds')]
        frames = {row['restored_prior_model_rows'] for row in rows}
        assert len(frames) == 1
        lines.append('| ' + ' | '.join([model, '普通' if window == 'ordinary' else '高更新', *values, str(frames.pop())]) + ' |')
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'comparison.md').write_text('\n'.join(comparison) + '\n')
    (args.output / 'distributions.md').write_text('\n'.join(lines) + '\n')


if __name__ == '__main__':
    main()
