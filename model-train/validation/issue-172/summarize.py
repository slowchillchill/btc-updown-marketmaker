"""从 #172 逐调用证据复算；只依赖 Python 标准库，不导入模型运行时。"""
import argparse
from collections import Counter, defaultdict
import csv
from fractions import Fraction
import gzip
import hashlib
import json
from pathlib import Path
import statistics
import struct


def read_lines(path):
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        for line in stream:
            yield json.loads(line)


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n')


def digest(value):
    # JSON 的 -0.0 与 0.0 字面值不同；原 f32 序列化可往返其位模式。
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    ensure_ascii=False).encode()).hexdigest()


def stats(values, scale=1e6):
    values = sorted(values)
    if not values:
        return {'count': 0}
    def quantile(q):
        x = (len(values) - 1) * q
        low = int(x)
        high = min(low + 1, len(values) - 1)
        return (values[low] + (values[high] - values[low]) * (x - low)) / scale
    return {'count': len(values), 'mean': statistics.fmean(values) / scale,
            **{name: quantile(q) for name, q in [('p50', .5), ('p90', .9),
                                               ('p95', .95), ('p99', .99)]},
            'max': values[-1] / scale}


def write_csv(path, rows):
    fields = sorted(set().union(*(row.keys() for row in rows)))
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'wt', newline='') as stream:
        writer = csv.DictWriter(stream, fields)
        writer.writeheader()
        writer.writerows({k: json.dumps(v, ensure_ascii=False, separators=(',', ':'))
                         if isinstance(v, (dict, list)) else v for k, v in row.items()}
                        for row in rows)


def native(source, output, labels=None):
    rows = list(read_lines(source / 'measurements.jsonl'))
    groups = defaultdict(list)
    for row in rows:
        if labels:
            row['raw_version'] = row['version']
            row['version'] = labels.get(row['version'], row['version'])
        assert row['next_state_exact']
        for tag in row['tags']:
            key = (tag, row['version'], row['threads'], row['round'])
            groups[key].append(row)
            if tag == 'old20':
                groups[('old20-cold' if row['position'] == 193830 else 'old20-hot',
                        *key[1:])].append(row)
            if tag.startswith('minute'):
                groups[('five-minutes', *key[1:])].append(row)
    summary = []
    for (tag, version, threads, round), values in sorted(groups.items()):
        summary.append({'sample': tag, 'version': version, 'threads': threads,
                        'round': round, 'wall_ms': stats([v['ns'] for v in values]),
                        'process_cpu_ms': stats([v['cpu_delta_ns'] for v in values if v.get('cpu_delta_ns') is not None]),
                        'restore_ms': stats([v['restore_ns'] for v in values]),
                        'positions': [v['position'] for v in values]})
    write_json(output / 'native-summary.json', summary)
    write_csv(output / 'native-calls.csv.gz', rows)


def screen(source, output):
    native(source, output, {'cache': 'parse_once', 'projection': 'time_rules',
                            'combined': 'miss_dedup', 'history': 'scratch_capacity'})


def graphs(source, output):
    rows, summary = [], []
    for path in sorted(source.glob('*-screen.json')):
        data = json.loads(path.read_text())
        for round in data['rounds']:
            group = {'model': data['configuration'], 'threads': data['threads'],
                     'round': round['round'], 'version': round['version']}
            rows.extend({**group, **sample} for sample in round['samples'])
            summary.append({**group, 'state_ms': stats([v['ns'] for v in round['samples']]),
                            'process_cpu_ms': round['cpu_ns'] / 1e6,
                            'preparation_seconds': data['preparation_seconds'],
                            'load_seconds': round['load_seconds'],
                            'f32_bits_exact': data['f32_bits_exact']})
    write_csv(output / 'graph-screen-calls.csv.gz', rows)
    write_json(output / 'graph-screen-summary.json', summary)


def diagnostics(source, output):
    rows = [row for row in read_lines(source / 'measurements.jsonl') if 'diagnostic' in row]
    by_tag = defaultdict(list)
    for row in rows:
        for tag in row['tags']:
            by_tag[tag].append(row)
        if 'old20' in row['tags']:
            by_tag['old20-cold' if row['position'] == 193830 else 'old20-hot'].append(row)
    result = {}
    for tag, samples in by_tag.items():
        result[tag] = {'wall_ms': stats([row['diagnostic_ns'] for row in samples]),
                       'samples': samples}
    # 阶段含嵌套和并行作用域；原始 count/ns 全留，不相加成整链或“CPU 占比”。
    write_json(output / 'diagnostics.json', result)


def reuse(source, output):
    previous = {}
    samples = []
    peak_payload = 0
    for item in read_lines(source / 'groups.jsonl'):
        position = item['position']
        peak_payload = max(peak_payload, sum(len(side) for row in item['group']['rows']
                                           for side in row['book_levels'][4:]) * 32 * 4)
        for row in item['group']['rows']:
            condition = row['hidden_state_key']['condition_id']
            if condition not in item['binance_exact_prices']:
                continue
            sides = item['binance_exact_prices'][condition]
            for side, (prices, values) in enumerate(zip(sides, row['book_levels'][4:])):
                assert len(prices) == len(values), (position, condition, side)
                keys = [Fraction(price) for price in prices]
                assert len(set(keys)) == len(keys), (position, 'duplicate numeric price')
                bits = [struct.pack('<9f', *value) for value in values]
                current = dict(zip(keys, bits))
                identity = (condition, side)
                prior = previous.get(identity)
                if prior and prior[0] + 1 == position:
                    _, old_keys, old = prior
                    common = current.keys() & old.keys()
                    unchanged = sum(current[k] == old[k] for k in common)
                    sample = {'position': position, 'condition': condition, 'side': side,
                              'tags': item['tags'], 'valid_rows': len(current),
                              'common_prices': len(common), 'equal_nine_f32_rows': unchanged,
                              'changed_nine_f32_rows': len(common) - unchanged,
                              'inserted': len(current.keys() - old.keys()),
                              'deleted': len(old.keys() - current.keys()),
                              'same_order': keys == old_keys,
                              'best_price_changed': keys[:1] != old_keys[:1],
                              'whole_side_equal': keys == old_keys and current == old,
                              'embedding_payload_bytes': len(current) * 32 * 4}
                    samples.append(sample)
                previous[identity] = (position, keys, current)
    groups = defaultdict(list)
    for row in samples:
        for tag in row['tags']:
            groups[tag].append(row)
    summary = {}
    for tag, rows in groups.items():
        summary[tag] = {'consecutive_side_pairs': len(rows),
                        **{k: sum(v[k] for v in rows) for k in (
                            'valid_rows', 'common_prices', 'equal_nine_f32_rows',
                            'changed_nine_f32_rows', 'inserted', 'deleted',
                            'whole_side_equal', 'best_price_changed')},
                        'embedding_payload_bytes': stats([v['embedding_payload_bytes'] for v in rows], 1)}
    write_json(output / 'binance-reuse.json', {
        'scope': '仅相邻实际组、同市场同侧按精确有理价格关联；九个 f32 全位相等。候选收益上限证据，未实现缓存。',
        'embedding_width': 32, 'maximum_observed_group_embedding_payload_bytes': peak_payload,
        'payload_excludes': ['键', '索引', '容器节点', '容量', '暂态双份', '归约树'],
        'summary': summary})
    write_csv(output / 'binance-reuse-calls.csv.gz', samples)


def continuous(source, output):
    """目录名固定 r{0,1}-{MAIN-...}-{ordinary,high}-{baseline,candidate}。"""
    summaries, calls, preparations = [], [], []
    expected = {}
    for run in sorted(source.iterdir()):
        if not run.is_dir() or not (run / 'completion.json').exists():
            continue
        round, _, model_tail, segment, version = run.name.split('-')
        model = 'MAIN-' + model_tail
        arrivals = {row['group_position']: row for row in read_lines(run / 'host-time-assignment.jsonl')}
        records = list(read_lines(run / 'measurements.jsonl'))
        samples = [row for row in records if 'service_ns' in row]
        preparation = [row for row in records if 'preparation_seconds' in row]
        assert len(preparation) == 1 and len(samples) == 64, run
        preparations.append({'run': run.name, **preparation[0]})
        completion = json.loads((run / 'completion.json').read_text())
        assert completion['complete'], run
        queue_end = None
        previous = None
        waits, responses = [], []
        signatures = []
        for row in samples:
            group = row['native_group']
            runtimes = row['runtimes']
            assert len(runtimes) == 1
            runtime = runtimes[0]
            timing = runtime['timing']
            actions = [{k: v for k, v in action.items() if k != 'execute_ns'}
                       for action in runtime['actions']]
            signature = digest({'native_group': group, 'hidden': runtime['hidden_sha256'],
                                'actions': actions})
            signatures.append((row['position'], signature))
            arrival = arrivals[row['position']]
            delivered = arrival['assigned_delivery_at_utc_ms'] * 1_000_000
            began = max(delivered, queue_end if queue_end is not None else delivered)
            queue_end = began + row['service_ns']
            waits.append(began - delivered)
            responses.append(queue_end - delivered)
            call = {'run': run.name, 'model': model, 'segment': segment, 'version': version,
                    'round': int(round[1:]), 'position': row['position'], 'now': row['now'],
                    'service_ns': row['service_ns'], 'native_ns': row['native_ns'],
                    'cpu_ns': row['cpu_ns'], 'packing_ns': timing['packing_ns'],
                    'state_ns': timing['state_ns'], 'state_calls': timing['state_calls'],
                    'context_rows': sum(v['role'] == 'ContextOnly' for v in group['rows']),
                    'decision_rows': sum(v['role'] == 'DecisionRequired' for v in group['rows']),
                    'depths': [[len(side) for side in v['book_levels']] for v in group['rows']],
                    'cancel_objects': len(group['cancel']),
                    'object_reduction_ns': timing['object_reduction_ns'],
                    'action_steps': timing['action_steps'], 'actions': actions,
                    'execute_ns': [action['execute_ns'] for action in runtime['actions']],
                    'hidden_sha256': runtime['hidden_sha256'],
                    'native_group_canonical_sha256': digest(group),
                    'complete_result_canonical_sha256': signature,
                    'source_received_ms': arrival['source_closing_received_at_utc_ms'],
                    'assigned_delivery_ms': arrival['assigned_delivery_at_utc_ms'],
                    'source_interval_ms': None if previous is None else arrival['source_closing_received_at_utc_ms'] - previous['source_closing_received_at_utc_ms'],
                    'delivery_interval_ms': None if previous is None else arrival['assigned_delivery_at_utc_ms'] - previous['assigned_delivery_at_utc_ms'],
                    'empty_entry_fifo_wait_ns': waits[-1], 'empty_entry_fifo_response_ns': responses[-1]}
            previous = arrival
            calls.append(call)
        key = (model, segment)
        if key in expected:
            assert expected[key] == signatures, (run.name, '完整输入/hidden/动作/回执不一致')
        expected[key] = signatures
        summaries.append({'run': run.name, 'model': model, 'segment': segment,
                          'version': version, 'round': int(round[1:]),
                          **{field.replace('_ns', '_ms'): stats([row[field] for row in samples])
                             for field in ('service_ns', 'native_ns', 'cpu_ns')},
                          'state_calls': sum(row['runtimes'][0]['timing']['state_calls'] for row in samples),
                          'complete_result_equal': True,
                          'over_4ms': sum(row['service_ns'] > 4_000_000 for row in samples),
                          'over_1ms': sum(row['service_ns'] > 1_000_000 for row in samples),
                          'empty_entry_fifo_wait_ms': stats(waits),
                          'empty_entry_fifo_response_ms': stats(responses)})
    assert len(summaries) == 16, len(summaries)
    write_json(output / 'continuous-summary.json', summaries)
    write_json(output / 'continuous-preparation.json', preparations)
    write_csv(output / 'continuous-calls.csv.gz', calls)
    details(output, output)


def details(source, output):
    """从公开逐调用表复算阶段、实际动作步数与到达间隔，不再读取大原生组。"""
    runs = defaultdict(list)
    with gzip.open(source / 'continuous-calls.csv.gz', 'rt') as stream:
        for row in csv.DictReader(stream):
            runs[row['run']].append(row)
    result = []
    head_names = {0: 'family', 1: 'quantity_branch', 2: 'variant',
                  3: 'price', 4: 'post_only', 5: 'expiry', 7: 'cancel_object'}
    for name, rows in sorted(runs.items()):
        stages = defaultdict(list)
        head_samples = defaultdict(list)
        head_groups = defaultdict(lambda: [0] * len(rows))
        paths = Counter()
        for index, row in enumerate(rows):
            for field in ('native_ns', 'packing_ns', 'state_ns'):
                stages[field].append(int(row[field]))
            stages['object_reduction_ns'].append(sum(json.loads(row['object_reduction_ns'])))
            stages['execute_ns'].append(sum(json.loads(row['execute_ns'])))
            steps = json.loads(row['action_steps'])
            stages['action_steps_ns'].append(sum(step[2] for step in steps))
            for _, head, elapsed in steps:
                head_samples[head].append(elapsed)
                head_groups[head][index] += elapsed
            paths.update(tuple(action['path']) for action in json.loads(row['actions']))
        result.append({
            'run': name, 'groups': len(rows),
            'stage_per_group_ms': {k.removesuffix('_ns'): stats(v) for k, v in stages.items()},
            'heads': {head_names[head]: {
                'calls': len(values), 'calls_per_group': len(values) / len(rows),
                'per_call_ms': stats(values), 'per_group_total_ms': stats(head_groups[head])}
                for head, values in sorted(head_samples.items())},
            'actual_paths': [{'path': list(path), 'groups': count} for path, count in sorted(paths.items())],
            'cancel_population': dict(Counter(int(row['cancel_objects']) for row in rows)),
            'arrivals': {field: {
                'interval_ms': stats([int(row[field]) for row in rows if row[field]], 1),
                'span_ms': int(rows[-1][at]) - int(rows[0][at]),
                'zero_intervals': sum(row[field] == '0' for row in rows),
                'negative_intervals': sum(int(row[field]) < 0 for row in rows if row[field])}
                for field, at in [('source_interval_ms', 'source_received_ms'),
                                  ('delivery_interval_ms', 'assigned_delivery_ms')]},
            'scope': '各阶段来自同组独立时钟；动作步包含支持集、评分和选择。阶段分位数不相加。'
        })
    write_json(output / 'continuous-details.json', result)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', required=True, choices=['native', 'screen', 'graphs', 'diagnostics', 'reuse', 'continuous', 'details'])
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    globals()[args.mode](args.source, args.output)


if __name__ == '__main__':
    main()
