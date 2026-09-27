"""有界真实原生连续段的三端独立递推；不把固定前缀评分当作自主交易。"""
import argparse
from collections import Counter
import copy
import json
from pathlib import Path
import subprocess
import time

import onnxruntime
import torch
from .graphs import Configuration, TwoGraphs, STATE_INPUTS
from .native_inputs import NativeComponentConsumer
from .validate_graphs import call_ort, compare, session, sha


def groups(path):
    with path.open() as stream:
        for line in stream:
            yield json.loads(line)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--groups', type=Path, required=True)
    parser.add_argument('--graphs', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--threads', type=int, default=1)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    configuration = Configuration(**json.loads((args.graphs / 'benchmark_configuration.json').read_text())['configuration'])
    torch.set_num_threads(args.threads)
    torch.set_num_interop_threads(1)
    suite = args.output / 'native-suite.json'
    # Rust 原入口独立持有整段 hidden；输入不包含 Python 状态或归约结果。
    with suite.open('w') as stream:
        stream.write(json.dumps({'layers': configuration.layers, 'hidden': configuration.hidden})[:-1] + ',"groups":[')
        for index, record in enumerate(groups(args.groups)):
            group = record['group']
            for row in group['rows']:
                prefix = [0.] * 60
                prefix[7] = 1.
                row['fixture_prefixes'] = [prefix]
            if index:
                stream.write(',')
            json.dump(group, stream)
        stream.write(']}')
    library = next((Path(onnxruntime.__file__).parent / 'capi').glob('libonnxruntime.so.*')).resolve()
    raw = args.output / 'rust.jsonl'
    started = time.monotonic()
    with raw.open('w') as output, (args.output / 'rust.stderr').open('w') as errors:
        subprocess.run([str(args.binary.resolve()), str(library), str(args.graphs.resolve()),
                        str(args.threads), '--native', str(suite.resolve())],
                       stdout=output, stderr=errors, check=True)
    model = TwoGraphs(configuration).eval()
    model.load_state_dict(torch.load(args.graphs / 'initial_weights.pt', weights_only=True, map_location='cpu'))
    ort_model = copy.deepcopy(model)
    ort_model.state.forward = call_ort(session(args.graphs / 'state_encoder.onnx', args.threads), STATE_INPUTS)
    consumers = [NativeComponentConsumer(model), NativeComponentConsumer(ort_model)]
    traces = [[], []]
    for index, item in enumerate((model, ort_model)):
        item.state.register_forward_hook(lambda module, values, outputs, index=index: traces[index].append(outputs))
    roles, operations = Counter(), Counter()
    stats = {}
    rows = group_count = 0
    first = last = None
    with raw.open() as stream, torch.no_grad():
        metadata = json.loads(next(stream))
        rust_states = (rust_record for line in stream if (rust_record := json.loads(line))['label'].endswith('/state'))
        for group_index, record in enumerate(groups(args.groups)):
            group = record['group']
            traces[0].clear(); traces[1].clear()
            for consumer in consumers:
                consumer.consume_batch([group], prepare_parameters=[False])
            for kind in ('pre_frame_hidden_ops', 'post_frame_hidden_ops'):
                for operation in group[kind]:
                    operations[next(iter(operation))] += 1
            assert len(traces[0]) == len(traces[1]) == len(group['rows'])
            for row_index, (row, reference, actual) in enumerate(zip(group['rows'], *traces, strict=True)):
                label = {'group_index': group_index, 'row_index': row_index, 'role': row['role']}
                compare(reference, actual, {**label, 'runtime': 'PythonORT'}, stats)
                rust = next(rust_states)
                assert rust['label'] == f'native/{group_index}/{row_index}/state'
                compare(reference, tuple(torch.tensor(values).reshape(want.shape)
                    for values, want in zip(rust['values'], reference, strict=True)), {**label, 'runtime': 'RustORT'}, stats)
                roles[row['role']] += 1
                rows += 1
                first = record['decision_time_utc_ms'] if first is None else first
                last = record['decision_time_utc_ms']
            assert consumers[0].hidden.keys() == consumers[1].hidden.keys()
            group_count += 1
        assert next(rust_states, None) is None
    result = {
        'complete': True, 'configuration': configuration.name, 'threads': args.threads,
        'rows': rows, 'groups': group_count, 'roles': dict(roles), 'operations': dict(operations),
        'first_state_time_utc_ms': first, 'last_state_time_utc_ms': last,
        'remaining_hidden_keys': len(consumers[0].hidden), 'comparison': stats,
        'groups_sha256': sha(args.groups), 'rust_trace_sha256': sha(raw),
        'weights_sha256': sha(args.graphs / 'initial_weights.pt'),
        'state_graph_sha256': sha(args.graphs / 'state_encoder.onnx'),
        'rust_binary_sha256': sha(args.binary), 'rust_runtime': metadata,
        'wall_seconds': time.monotonic() - started,
        'scope': '完整顺序消费指定真实段及其原生隐藏操作；三端各自递推。Rust 原验证入口另执行固定 Family 前缀评分，未选择/提交自主动作。尚未出现的生命周期操作不记作已覆盖。',
    }
    (args.output / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'rust_runtime'}, ensure_ascii=False))


if __name__ == '__main__':
    main()
