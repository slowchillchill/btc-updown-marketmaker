"""合并两轮原始调用计算分布；不平均分位数，不删除入口首组。"""
import argparse
from collections import defaultdict
import csv
import gzip
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location('stats172', Path(__file__).parent.parent / 'issue-172/summarize.py')
stats = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stats)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    args = parser.parse_args()
    grouped = defaultdict(list)
    with gzip.open(args.source / 'continuous-calls.csv.gz', 'rt') as stream:
        for row in csv.DictReader(stream):
            values = {key: int(row[key]) for key in ('service_ns', 'native_ns', 'packing_ns', 'state_ns', 'cpu_ns')}
            values['action_ns'] = sum(json.loads(row['object_reduction_ns'])) + sum(item[2] for item in json.loads(row['action_steps']))
            values['execute_ns'] = sum(json.loads(row['execute_ns']))
            values['remaining_service_ns'] = values['service_ns'] - sum(values[key] for key in ('native_ns', 'packing_ns', 'state_ns', 'action_ns', 'execute_ns'))
            assert values['remaining_service_ns'] >= 0
            grouped[row['model'], row['segment'], row['version']].append(values)
    preparations = json.loads((args.source / 'continuous-preparation.json').read_text())
    result = []
    for (model, segment, variant), rows in sorted(grouped.items()):
        assert len(rows) == 128
        resources = [p['resources'] for p in preparations if model in p['run'] and segment in p['run'] and variant in p['run']]
        assert len(resources) == 2
        result.append({'model': model, 'window': segment, 'variant': variant, 'calls': len(rows),
                       'phases_ms': {key.removesuffix('_ns'): stats.stats([r[key] for r in rows]) for key in rows[0]},
                       'process_resources_each_round': resources,
                       'scope': '两轮各64组的全部128组；入口组保留。CPU为整次服务进程CPU；RSS及进程user/system还包含图加载、公共重放和证据生成。'})
    assert len(result) == 8
    stats.write_json(args.source / 'pooled-summary.json', result)


if __name__ == '__main__':
    main()
