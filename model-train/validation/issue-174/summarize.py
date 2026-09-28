"""从真实逐调用记录复算 #174 全轮次、冷帧、阶段、缓存容量及一致性证据。"""
import argparse
from collections import defaultdict
import importlib.util
import json
from pathlib import Path
import re

spec = importlib.util.spec_from_file_location('issue172_summary', Path(__file__).parent.parent / 'issue-172/summarize.py')
old = importlib.util.module_from_spec(spec)
spec.loader.exec_module(old)


def cache_sizes(cache):
    rows = [side for market in cache.values() for side in market if isinstance(side, dict)]
    fields = ('parsed_rows', 'parsed_capacity', 'parsed_struct_bytes', 'text_capacity_bytes',
              'rational_value_bytes', 'projected_capacity', 'projected_storage_bytes')
    return {**{field: sum(row[field] for row in rows) for field in fields},
            'current_side_strong_refs_max': max((row['current_side_strong_refs'] for row in rows), default=0)}


def resources(path):
    text = path.read_text()
    fields = {'user_seconds': r'User time \(seconds\): ([\d.]+)',
              'system_seconds': r'System time \(seconds\): ([\d.]+)',
              'maximum_rss_kib': r'Maximum resident set size \(kbytes\): (\d+)',
              'exit_status': r'Exit status: (\d+)'}
    return {name: float(re.search(pattern, text)[1]) for name, pattern in fields.items()}


def native(source, output):
    calls, preparations, summaries = [], [], []
    for segment in ('ordinary', 'high'):
        run = source / segment
        completion = json.loads((run / 'completion.json').read_text())
        assert completion['complete'] and completion['groups'] == 64
        rows = list(old.read_lines(run / 'measurements.jsonl'))
        preparations.extend(dict(segment=segment, **r) for r in rows if 'preparation_seconds' in r)
        grouped = defaultdict(list)
        expected = {}
        for row in rows:
            if 'native_ns' not in row:
                continue
            position = row['position']
            signature = old.digest({k: row[k] for k in ('native_group', 'native_business', 'ledger', 'native_bits_sha256')})
            assert expected.setdefault(position, signature) == signature
            assert row['complete_group_bits_business_packing_equal']
            call = {k: row[k] for k in ('position', 'now', 'round', 'variant', 'native_ns', 'native_cpu_ns',
                                       'packing_ns', 'packing_cpu_ns', 'cold_entry', 'native_bits_sha256')}
            call['entry_call'] = call.pop('cold_entry')
            call.update(segment=segment, complete_result_sha256=signature,
                        complete_group_bits_business_packing_equal=True,
                        depths=[[len(s) for s in r['book_levels']] for r in row['native_group']['rows']],
                        cache=cache_sizes(row['cache']) if row['variant'] == 'candidate' else {'log_entries': sum(sum(v) for v in row['cache'].values())})
            calls.append(call)
            for phase in ('all', 'entry' if row['cold_entry'] else 'continuation'):
                grouped[(row['variant'], row['round'], phase)].append(call)
        for (variant, round, phase), values in sorted(grouped.items()):
            summaries.append(dict(segment=segment, variant=variant, round=round, phase=phase,
                                  **{field.removesuffix('_ns') + '_ms': old.stats([r[field] for r in values])
                                     for field in ('native_ns', 'native_cpu_ns', 'packing_ns', 'packing_cpu_ns')}))
        assert len(expected) == 64
        preparations[-1].update(completion=completion, resources=resources(source / (segment + '.resources.txt')))
    assert len(calls) == 512
    old.write_csv(output / 'native-calls.csv.gz', calls)
    old.write_json(output / 'native-summary.json', summaries)
    old.write_json(output / 'native-preparation.json', preparations)
    candidates = [r['cache'] for r in calls if r['variant'] == 'candidate']
    old.write_json(output / 'native-cache-capacity.json', {
        'maximum_observed': {k: max(r[k] for r in candidates) for k in candidates[0]},
        'scope': '各容量字段独立最大值，不相加成同时峰值；BigInt 仅记录值位宽字节，非分配容量。整进程峰值 RSS 包含真实前史和证据，不能当成新增缓存独占堆。'})


def continuous(source, output):
    calls, preparations, summaries = [], [], []
    expected = {}
    for run in sorted(source.iterdir()):
        if not run.is_dir() or not (run / 'completion.json').exists():
            continue
        round, _, tail, segment, variant = run.name.split('-')
        model = 'MAIN-' + tail
        samples = []
        arrivals = {r['group_position']: r for r in old.read_lines(run / 'host-time-assignment.jsonl')}
        signatures = []
        for row in old.read_lines(run / 'measurements.jsonl'):
            if 'preparation_seconds' in row:
                preparations.append(dict(run=run.name, **row))
                continue
            runtime = row['runtimes'][0]
            actions = [{k: v for k, v in action.items() if k != 'execute_ns'} for action in runtime['actions']]
            signature = old.digest({**{k: row[k] for k in ('native_group', 'native_bits_sha256', 'native_business', 'ledger')},
                                    'hidden': runtime['hidden_sha256'], 'z': runtime['z_bits_sha256'], 'actions': actions})
            signatures.append((row['position'], signature))
            timing = runtime['timing']
            group = row['native_group']
            arrival = arrivals[row['position']]
            call = {k: row[k] for k in ('position', 'now', 'service_ns', 'native_ns', 'cpu_ns', 'native_bits_sha256')}
            call.update(run=run.name, model=model, segment=segment, version=variant, round=int(round[1:]),
                        **{k: timing[k] for k in ('packing_ns', 'state_ns', 'state_calls', 'object_reduction_ns', 'action_steps')},
                        actions=actions, execute_ns=[a['execute_ns'] for a in runtime['actions']],
                        complete_result_sha256=signature, hidden_sha256=runtime['hidden_sha256'], z_bits_sha256=runtime['z_bits_sha256'],
                        cache=cache_sizes(row['cache']) if variant == 'candidate' else {'log_entries': sum(sum(v) for v in row['cache'].values())},
                        depths=[[len(s) for s in r['book_levels']] for r in group['rows']], cancel_objects=len(group['cancel']),
                        source_received_ms=arrival['source_closing_received_at_utc_ms'], assigned_delivery_ms=arrival['assigned_delivery_at_utc_ms'])
            previous = samples[-1] if samples else None
            for prefix, field in [('source', 'source_received_ms'), ('delivery', 'assigned_delivery_ms')]:
                call[prefix + '_interval_ms'] = call[field] - previous[field] if previous else None
            samples.append(call)
        assert len(samples) == 64
        key = (model, segment)
        assert expected.setdefault(key, signatures) == signatures, (run.name, '完整结果不一致')
        calls.extend(samples)
        summaries.append(dict(run=run.name, model=model, segment=segment, variant=variant, round=int(round[1:]),
                              **{field.removesuffix('_ns') + '_ms': old.stats([r[field] for r in samples])
                                 for field in ('service_ns', 'native_ns', 'cpu_ns', 'packing_ns', 'state_ns')},
                              complete_group_bits_business_hidden_z_actions_receipts_equal=True,
                              over_4ms=sum(r['service_ns'] > 4_000_000 for r in samples),
                              over_1ms=sum(r['service_ns'] > 1_000_000 for r in samples)))
        completion = json.loads((run / 'completion.json').read_text())
        assert completion['complete'] and completion['groups'] == 64
        preparations[-1].update(completion=completion,
                                resources=resources(source / (run.name + '.resources.txt')))
    assert len(summaries) == 16
    old.write_csv(output / 'continuous-calls.csv.gz', calls)
    old.write_json(output / 'continuous-summary.json', summaries)
    old.write_json(output / 'continuous-preparation.json', preparations)
    old.details(output, output)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['native', 'continuous'], required=True)
    p.add_argument('--source', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=True)
    globals()[a.mode](a.source, a.output)
    for path in a.output.glob('*.json'):
        if '/home/' in path.read_text() or '/tmp/' in path.read_text():
            raise ValueError('公开证据仍含本机路径：' + path.name)


if __name__ == '__main__':
    main()
