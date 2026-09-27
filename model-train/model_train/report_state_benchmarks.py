"""把配对原始计时整理为 CSV、Markdown 和可筛选的独立 HTML。"""
import argparse
import csv
import html
import json
from pathlib import Path
import statistics


METRICS = ('p50', 'p90', 'p95', 'p99', 'max')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--measurements', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    rows = []
    for path in args.measurements.glob('MAIN-*.json'):
        record = json.loads(path.read_text())
        configuration = record['configuration']
        rows.append({'configuration': configuration['name'], 'layers': configuration['layers'],
            'width': configuration['hidden'], 'threads': record['threads'], 'version': record['version'],
            **record['statistics_ms'], 'maximum_sample_index': record['maximum_sample']['sample_index'],
            'maximum_group_position': record['maximum_sample']['group_position'],
            'maximum_time_utc_ms': record['maximum_sample']['decision_time_utc_ms'],
            'calls': record['sample_count'], 'record': path.name})
    rows.sort(key=lambda row: (row['layers'], row['width'], row['threads'], row['version']))
    by_key = {(row['configuration'], row['threads'], row['version']): row for row in rows}
    expected = {(f'MAIN-{layers}x{width}', threads, version)
        for layers in (1, 2, 3, 10, 20, 30) for width in (256, 512, 768, 1024)
        for threads in (1, 4, 8, 16) for version in ('baseline', 'candidate')}
    assert set(by_key) == expected and len(rows) == 192, '配对矩阵未完整，不能发布完整报告'
    assert all(row['calls'] == 1024 for row in rows), '本报告口径要求固定1024帧'
    with (args.output / '真实帧状态图对照.csv').open('w') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader(); writer.writerows(rows)
    improvements = {}
    for metric in METRICS:
        ratios = [by_key[(row['configuration'], row['threads'], 'baseline')][metric] / row[metric]
                  for row in rows if row['version'] == 'candidate']
        improvements[metric] = {'improved_combinations': sum(ratio > 1 for ratio in ratios),
            'total_combinations': len(ratios), 'median_ratio_across_combinations': statistics.median(ratios),
            'min_ratio': min(ratios), 'max_ratio': max(ratios)}
    (args.output / 'state-matrix-summary.json').write_text(json.dumps(improvements, indent=2) + '\n')
    note = ('同一1,024个真实帧，B=1，16次预热，固定未训练权重，每次调用零 hidden。'
            '单位为毫秒；计时仅含状态图 session.run，读取、补齐、原生投影和动作图均在计时外。'
            '这不是五分钟完整连续回放；样本 P99 和最大值不代表实盘尾部或最坏情况保证。')
    text = '# 全档盘口真实帧状态图对照\n\n' + note + '\n\n'
    text += '96 个组合按基线／候选配对运行，1／4／8／16 线程各自平衡前后顺序。\n\n'
    for version, label in (('candidate', '优化后'), ('baseline', '重新测量的基线')):
        text += f'## {label}\n\n| MAIN 配置 | 线程 | P50 | P90 | P95 | P99 | 最大值 |\n|---|---:|---:|---:|---:|---:|---:|\n'
        for row in rows:
            if row['version'] == version:
                text += f'| {row["layers"]}×{row["width"]} | {row["threads"]} | ' + ' | '.join(f'{row[m]:.3f}' for m in METRICS) + ' |\n'
        text += '\n'
    (args.output / '真实帧状态图对照.md').write_text(text)
    document = '''<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>全档盘口真实帧计算时间</title><style>
:root{color-scheme:light dark;font:15px system-ui;background:#101821;color:#e6edf5}body{max-width:1200px;margin:40px auto;padding:0 20px}h1{font-size:26px}p{line-height:1.8;color:#c3d0dc}label{display:inline-block;margin:12px 24px 20px 0}select{font:inherit;padding:6px}table{width:100%;border-collapse:collapse;font-variant-numeric:tabular-nums}th,td{padding:12px;text-align:right;border-bottom:1px solid #334253}th:first-child,td:first-child{text-align:left}small{display:block;color:#91a5b9;font-size:12px;margin-top:4px}.good{color:#6fe2b2}.bad{color:#ffab9e}.scroll{overflow:auto}a{color:#89bcff}@media(prefers-color-scheme:light){:root{background:#f8fafc;color:#182a3a}p{color:#3b4e61}.good{color:#097052}.bad{color:#ad3524}}</style>
<h1>全档盘口真实帧计算时间</h1><p>NOTE</p>
<label>统计量 <select id="metric"><option value="p90">P90</option><option value="p95">P95</option><option value="p99" selected>P99</option><option value="max">最大值</option><option value="p50">P50</option></select></label>
<label>宽度 <select id="width"><option value="all">全部</option><option>256</option><option>512</option><option>768</option><option>1024</option></select></label>
<p>每格先显示优化后耗时，下方列同次对照基线及基线／优化后比值。大于 1 表示该统计量降低。最大样本身份保留在 CSV 和原始 JSON 中。</p>
<div class="scroll"><table><thead><tr><th>MAIN 配置</th><th>1 线程</th><th>4 线程</th><th>8 线程</th><th>16 线程</th></tr></thead><tbody id="rows"></tbody></table></div>
<script>const data=DATA;const metric=document.querySelector('#metric'),width=document.querySelector('#width');
function render(){const m=metric.value;let output='';for(const l of [1,2,3,10,20,30])for(const w of [256,512,768,1024]){if(width.value!=='all'&&Number(width.value)!==w)continue;output+=`<tr><td>${l}×${w}</td>`;for(const t of [1,4,8,16]){const select=v=>data.find(r=>r.layers===l&&r.width===w&&r.threads===t&&r.version===v);const a=select('candidate')[m],b=select('baseline')[m];output+=`<td class="${a<b?'good':'bad'}">${a.toFixed(3)}<small>基线 ${b.toFixed(3)} · ${(b/a).toFixed(2)}×</small></td>`}output+='</tr>'}document.querySelector('#rows').innerHTML=output}metric.addEventListener('change',render);width.addEventListener('change',render);render();</script></html>'''
    (args.output / '真实帧状态图对照.html').write_text(document.replace('NOTE', html.escape(note)).replace('DATA', json.dumps(rows, ensure_ascii=False)))
    print(json.dumps(improvements, ensure_ascii=False))


if __name__ == '__main__':
    main()
