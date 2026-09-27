"""固定真实帧的配对状态图抽测；不把抽样零 hidden 当作连续回放。"""
import argparse
from collections import Counter
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import resource
import shutil
import subprocess
import sys
import time

import numpy as np
import torch
from .graphs import Configuration, STATE_INPUTS, STATE_OUTPUTS, TwoGraphs
from .validate_graphs import compare, session, sha


class SampledInputs:
    def __init__(self, directory):
        self.metadata = json.loads((directory / 'metadata.json').read_text())
        self.frames = json.loads((directory / 'frames.json').read_text())
        self.data = np.memmap(directory / 'inputs.f32', dtype='<f4', mode='r')
        self.hashes = {name: sha(directory / name) for name in ('metadata.json', 'frames.json', 'inputs.f32')}

    def feed(self, frame, configuration):
        values = self.data[frame['offset_f32']:frame['offset_f32'] + frame['count_f32']]
        depth = max(max(frame['depths']), 1)
        book = np.zeros((1, 6, depth, 9), dtype=np.float32)
        mask = np.zeros((1, 6, depth), dtype=np.bool_)
        cursor = 1029
        for side, count in enumerate(frame['depths']):
            book[0, side, :count] = values[cursor:cursor + count * 9].reshape(count, 9)
            mask[0, side, :count] = True
            cursor += count * 9
        assert cursor == len(values)
        return dict(zip(STATE_INPUTS, (
            np.array(values[:596]).reshape(1, 596), np.array(values[596:1029]).reshape(1, 433),
            book, mask, np.zeros((configuration.layers, 1, configuration.hidden), dtype=np.float32))))


def environment():
    return {
        'python': platform.python_version(), 'platform': platform.platform(),
        'packages': {name: importlib.metadata.version(name) for name in ('torch', 'onnxruntime', 'onnx', 'numpy')},
        'affinity': sorted(os.sched_getaffinity(0)), 'load_average': os.getloadavg(),
        'governors': sorted({p.read_text().strip() for p in Path('/sys/devices/system/cpu').glob('cpu*/cpufreq/scaling_governor')}),
        'boost': Path('/sys/devices/system/cpu/cpufreq/boost').read_text().strip() if Path('/sys/devices/system/cpu/cpufreq/boost').exists() else None,
        'frequency_policy': '未调整频率、绑核或系统后台服务',
        'peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024,
        'cpu_model': next(line.split(':', 1)[1].strip() for line in Path('/proc/cpuinfo').read_text().splitlines() if line.startswith('model name')),
    }


def measure(args):
    directory = args.graphs / args.config
    provenance = json.loads((directory / 'benchmark_configuration.json').read_text())
    config = Configuration(**provenance['configuration'])
    samples = SampledInputs(args.packed)
    frames = samples.frames[:args.limit] if args.limit else samples.frames
    # 参照固定为单线程，避免验证阶段另建的 Torch 工作池干扰随后 ORT 正式计时。
    # 被测 ORT 仍按请求的 1/4/8/16 线程运行；多线程三端验收另由原验证入口完成。
    torch.set_num_threads(1)
    torch.set_num_interop_threads(1)
    graph = session(directory / 'state_encoder.onnx', args.threads)
    selected = sorted(set(np.linspace(0, len(frames) - 1, min(16, len(frames)), dtype=int).tolist()))
    model = TwoGraphs(config).eval()
    model.load_state_dict(torch.load(directory / 'initial_weights.pt', map_location='cpu', weights_only=True))
    stats = {}
    with torch.no_grad():
        for index in selected:
            feed = samples.feed(frames[index], config)
            expected = model.state(*(torch.from_numpy(feed[name]) for name in STATE_INPUTS))
            compare(expected, tuple(torch.from_numpy(value) for value in graph.run(None, feed)),
                    {'configuration': config.name, 'sample_index': index}, stats)
    del model
    before = environment()
    for index in range(16):
        graph.run(None, samples.feed(frames[index % len(frames)], config))
    durations, preparation = [], []
    started = time.time()
    for frame in frames:
        begin = time.perf_counter_ns()
        feed = samples.feed(frame, config)
        preparation.append(time.perf_counter_ns() - begin)
        begin = time.perf_counter_ns()
        outputs = graph.run(None, feed)
        durations.append(time.perf_counter_ns() - begin)
        if not all(np.isfinite(value).all() for value in outputs):
            raise ArithmeticError(f'非有限状态输出：{frame["group_position"]}')
    maximum = int(np.argmax(durations))
    result = {
        'configuration': provenance['configuration'], 'threads': args.threads, 'version': args.version,
        'source_commit': provenance['source_commit'], 'weights_sha256': sha(directory / 'initial_weights.pt'),
        'model_source_files_sha256': provenance.get('source_files_sha256'),
        'benchmark_source_sha256': sha(Path(__file__)),
        'graph_files': {p.name: {'sha256': sha(p), 'bytes': p.stat().st_size}
                        for p in directory.glob('state_encoder.onnx*')},
        'input_files_sha256': samples.hashes, 'sample_count': len(frames), 'warmup_calls': 16,
        'validation_sample_indices': selected, 'validation': stats,
        'validation_pytorch_intra_threads': 1,
        'samples_ns': durations, 'preparation_ns': preparation,
        'statistics_ms': {name: float(value) for name, value in zip(
            ('p50', 'p90', 'p95', 'p99', 'max'), np.percentile(np.array(durations) / 1e6, (50, 90, 95, 99, 100), method='linear'))},
        'maximum_sample': {'sample_index': maximum, **frames[maximum]},
        'environment_before': before, 'environment_after': environment(),
        'session': {'provider': 'CPUExecutionProvider', 'intra_threads': args.threads, 'inter_threads': 1,
                    'execution': 'SEQUENTIAL', 'optimization': 'ALL', 'memory_pattern': False, 'spinning': False},
        'started_unix_seconds': started, 'wall_seconds': time.time() - started,
        'scope': '同一固定真实帧、B1、未训练同权重、逐帧零 hidden；计时仅 session.run（包含 Python 调用边界），不含读取/补齐/原生投影/动作图，不是五分钟完整连续回放。',
        'quantile_method': 'numpy linear', 'complete': True,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'configuration': config.name, 'threads': args.threads, 'version': args.version,
                      'statistics_ms': result['statistics_ms']}, ensure_ascii=False), flush=True)


def matrix(args):
    configurations = [f'MAIN-{layers}x{width}' for layers in (1, 2, 3, 10, 20, 30) for width in (256, 512, 768, 1024)]
    args.output.mkdir(parents=True, exist_ok=True)
    for index, (configuration, threads) in enumerate((c, t) for c in configurations for t in (1, 4, 8, 16)):
        versions = [('baseline', args.baseline), ('candidate', args.candidate)]
        if (index // 4 + index % 4) % 2:
            versions.reverse()
        for version, graphs in versions:
            label = f'{configuration}-t{threads}-{version}'
            output = args.output / f'{label}.json'
            if output.exists():
                raise FileExistsError(output)
            command = [sys.executable, '-m', 'model_train.benchmark_state', 'measure',
                       '--graphs', str(graphs), '--packed', str(args.packed), '--config', configuration,
                       '--threads', str(threads), '--version', version, '--output', str(output)]
            print(json.dumps({'started': label, 'unix_seconds': time.time()}), flush=True)
            with (args.output / f'{label}.log').open('w') as log:
                subprocess.run(command, check=True, stdout=log, stderr=subprocess.STDOUT,
                               env=dict(os.environ, OMP_NUM_THREADS=str(threads), MKL_NUM_THREADS=str(threads)))
            print(json.dumps({'completed': label, 'unix_seconds': time.time()}), flush=True)


def export_state(args):
    import onnx
    from .book import lower_pairwise_sum
    from .graph_fixtures import fixture, book_fixture
    for directory in sorted(args.graphs.glob(args.config)):
        provenance = json.loads((directory / 'benchmark_configuration.json').read_text())
        config = Configuration(**provenance['configuration'])
        destination = args.output / config.name
        destination.mkdir(parents=True)
        model = TwoGraphs(config).eval()
        model.load_state_dict(torch.load(directory / 'initial_weights.pt', map_location='cpu', weights_only=True))
        fast, slow, *_ = fixture(2)
        inputs = fast, slow, *book_fixture(2, 7), torch.zeros(config.layers, 2, config.hidden)
        B = torch.export.Dim('B', min=1)
        N = torch.export.Dim('N', min=1)
        program = torch.onnx.export(model.state, inputs, dynamo=True, opset_version=18, optimize=True,
            input_names=list(STATE_INPUTS), output_names=list(STATE_OUTPUTS),
            dynamic_shapes=({0:B}, {0:B}, {0:B,2:N}, {0:B,2:N}, {1:B}))
        onnx.save_model(lower_pairwise_sum(program.model_proto), destination / 'state_encoder.onnx',
            save_as_external_data=True, all_tensors_to_one_file=True, location='state_encoder.onnx.data', size_threshold=1024)
        for pattern in ('initial_weights.pt', 'action_decoder.onnx*'):
            for path in directory.glob(pattern):
                shutil.copyfile(path, destination / path.name)
        provenance['source_commit'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
        provenance['source_files_sha256'] = {path.name: sha(path) for path in (
            Path(__file__).with_name('book.py'), Path(__file__).with_name('components.py'))}
        provenance['export_scope'] = '沿用同一初始化权重，仅重新导出状态图；动作图字节保持原产物'
        (destination / 'benchmark_configuration.json').write_text(json.dumps(provenance, ensure_ascii=False, indent=2) + '\n')
        graph = onnx.load(destination / 'state_encoder.onnx', load_external_data=False)
        metadata = {
            'input_contract_version': provenance['input_contract_version'],
            'feature_schema_sha256': provenance['feature_schema_sha256'],
            'configuration': provenance['configuration'], 'environment': environment(),
            'initial_weights_sha256': sha(destination / 'initial_weights.pt'),
            'scope': provenance['export_scope'],
            'state_graph': {'nodes': len(graph.graph.node),
                'operators': sorted({node.op_type for node in graph.graph.node}),
                'opsets': [(op.domain, op.version) for op in graph.opset_import]},
        }
        (destination / 'export_metadata.json').write_text(json.dumps(metadata, ensure_ascii=False, indent=2) + '\n')


def profile(args):
    """独立分析器会话；Loop 容器和其子事件分别列出，避免重复计入。"""
    import onnx
    import onnxruntime as ort
    directory = args.graphs / args.config
    provenance = json.loads((directory / 'benchmark_configuration.json').read_text())
    config = Configuration(**provenance['configuration'])
    inputs = SampledInputs(args.packed)
    feed = inputs.feed(inputs.frames[0], config)
    args.output.mkdir(parents=True, exist_ok=True)
    options = ort.SessionOptions()
    options.intra_op_num_threads = args.threads
    options.inter_op_num_threads = 1
    options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
    options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    options.enable_mem_pattern = False
    options.enable_profiling = True
    options.profile_file_prefix = str(args.output / 'ort')
    for key in ('session.intra_op.allow_spinning', 'session.inter_op.allow_spinning'):
        options.add_session_config_entry(key, '0')
    runtime = ort.InferenceSession(str(directory / 'state_encoder.onnx'), sess_options=options,
                                   providers=['CPUExecutionProvider'])
    for _ in range(4):
        runtime.run(None, feed)
    path = Path(runtime.end_profiling())
    graph = onnx.load(directory / 'state_encoder.onnx', load_external_data=False)
    namespaces = {node.name: next((p.value for p in node.metadata_props if p.key == 'namespace'), '')
                  for node in graph.graph.node}
    categories, operators, loop_containers = Counter(), Counter(), Counter()
    activations, outputs = [], []
    for event in json.loads(path.read_text()):
        if event.get('cat') != 'Node' or not event['name'].endswith('_kernel_time'):
            continue
        operator = event['args'].get('op_name', 'unknown')
        name = event['name'].removesuffix('_kernel_time')
        namespace = namespaces.get(name, '')
        if operator == 'Loop':
            loop_containers[name] += event['dur']
            continue
        if name.startswith('book_sum_'):
            category = 'book_reduction'
        elif 'BookEncoder' in namespace:
            category = 'book_encoding' if any(x in namespace for x in ('BookLinear', 'AlignedLinear', 'SiLU')) else 'book_layout_and_reduction'
        elif 'GRU/gru' in namespace or operator == 'GRU':
            category = 'gru'
        elif '/core:' in namespace:
            category = 'core_features'
        else:
            category = 'unattributed_or_runtime_fusion'
        categories[category] += event['dur']
        operators[operator] += event['dur']
        activations.append(int(event['args'].get('activation_size', 0)))
        outputs.append(int(event['args'].get('output_size', 0)))
    result = {'configuration': config.name, 'threads': args.threads, 'calls': 4,
              'profile_file': path.name, 'node_duration_us': dict(categories), 'operator_duration_us': dict(operators),
              'loop_container_duration_us_excluded_from_totals': dict(loop_containers),
              'largest_reported_node_input_activation_bytes': max(activations, default=0),
              'largest_reported_node_output_bytes': max(outputs, default=0),
              'environment': environment(), 'input_files_sha256': inputs.hashes,
              'scope': '独立诊断会话，包含冷调用与分析器开销；时间不是正式分位数，节点输入激活字节不是全图峰值内存。未能按源命名空间归因的融合节点单独列出。'}
    (args.output / 'summary.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(result, ensure_ascii=False))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    one = sub.add_parser('measure')
    one.add_argument('--graphs', type=Path, required=True)
    one.add_argument('--config', required=True)
    one.add_argument('--threads', type=int, choices=(1, 4, 8, 16), required=True)
    one.add_argument('--version', required=True)
    one.add_argument('--limit', type=int)
    all_configs = sub.add_parser('matrix')
    all_configs.add_argument('--baseline', type=Path, required=True)
    all_configs.add_argument('--candidate', type=Path, required=True)
    exporter = sub.add_parser('export')
    exporter.add_argument('--graphs', type=Path, required=True)
    exporter.add_argument('--config', default='MAIN-*')
    exporter.add_argument('--output', type=Path, required=True)
    diagnostic = sub.add_parser('profile')
    diagnostic.add_argument('--graphs', type=Path, required=True)
    diagnostic.add_argument('--config', required=True)
    diagnostic.add_argument('--threads', type=int, choices=(1, 4, 8, 16), required=True)
    for command in (one, all_configs, diagnostic):
        command.add_argument('--packed', type=Path, required=True)
        command.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    {'measure': measure, 'matrix': matrix, 'export': export_state, 'profile': profile}[args.command](args)


if __name__ == '__main__':
    main()
