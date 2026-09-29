"""128个有限native筛选组的原数值核对；不冒充模型动作闭环结果。"""
import argparse
import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location('stats172', Path(__file__).parent.parent / 'issue-172/summarize.py')
stats = importlib.util.module_from_spec(spec)
spec.loader.exec_module(stats)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('baseline', 'candidate', 'output'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    rows = {variant: list(stats.read_lines(getattr(args, variant) / 'measurements.jsonl')) for variant in ('baseline', 'candidate')}
    assert len(rows['baseline']) == len(rows['candidate']) == 128
    assert [(r['position'], r['native_bits_sha256']) for r in rows['baseline']] == [(r['position'], r['native_bits_sha256']) for r in rows['candidate']]
    result = []
    for variant in ('baseline', 'candidate'):
        for segment, first in [('ordinary', 201582), ('high', 418584)]:
            selected = [r for r in rows[variant] if first <= r['position'] < first + 64]
            keys = set().union(*(r['components'] for r in selected))
            result.append({'variant': variant, 'window': segment,
                           'native_ms': stats.stats([r['native_ns'] for r in selected]),
                           'nested_or_parallel_component_ms': {key: stats.stats([r['components'].get(key, 0) for r in selected]) for key in sorted(keys) if key.endswith('_ns')},
                           'work_counts': {key: stats.stats([r['components'].get(key, 0) for r in selected], scale=1) for key in sorted(keys) if not key.endswith('_ns')},
                           'both_variants_128_groups_native_bits_equal': True,
                           'scope': '带探针、无模型交易的真实前史续推；只用于候选排序。部分后补探针仅候选有值，不能把基线缺测当零成本；闭环同探针结果另列。'})
    stats.write_json(args.output, result)


if __name__ == '__main__':
    main()
