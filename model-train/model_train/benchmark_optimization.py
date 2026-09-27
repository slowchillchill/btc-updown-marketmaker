"""#170 的独立 ORT 配对、内存和最大值诊断；真实帧抽测，不是连续回放。"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import subprocess
import sys
import time

import numpy as np
import onnxruntime as ort


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def rss():
    return next(int(line.split()[1]) * 1024 for line in Path('/proc/self/status').read_text().splitlines() if line.startswith('VmRSS:'))


def usage():
    u = resource.getrusage(resource.RUSAGE_SELF)
    return {'cpu_s': u.ru_utime + u.ru_stime, 'voluntary': u.ru_nvcsw, 'involuntary': u.ru_nivcsw,
            'minor': u.ru_minflt, 'major': u.ru_majflt, 'peak_rss_bytes': u.ru_maxrss * 1024, 'rss_bytes': rss()}


def feed(data, frame, cfg):
    values = data[frame['offset_f32']:frame['offset_f32'] + frame['count_f32']]
    n = max(1, max(frame['depths']))
    book = np.zeros((1, 6, n, 9), dtype=np.float32)
    mask = np.zeros((1, 6, n), dtype=np.bool_)
    cursor = 1029
    for side, count in enumerate(frame['depths']):
        book[0, side, :count] = values[cursor:cursor + count * 9].reshape(count, 9)
        mask[0, side, :count] = True
        cursor += count * 9
    return dict(zip(('fast_features', 'slow_features', 'book_levels', 'book_mask', 'h_prev'), (
        values[:596].copy().reshape(1, 596), values[596:1029].copy().reshape(1, 433), book, mask,
        np.zeros((cfg['layers'], 1, cfg['hidden']), dtype=np.float32))))


def run(args):
    if args.mode == 'memory' and 'torch' in sys.modules:
        raise RuntimeError('内存诊断必须通过 python model_train/benchmark_optimization.py 启动，避免包初始化加载 PyTorch')
    directory = args.graphs / args.config
    cfg = json.loads((directory / 'benchmark_configuration.json').read_text())['configuration']
    frames = json.loads((args.packed / 'frames.json').read_text())
    data = np.memmap(args.packed / 'inputs.f32', dtype='<f4', mode='r')
    deepest = int(np.argmax([max(f['depths']) for f in frames]))
    most = int(np.argmax([sum(f['depths']) for f in frames]))
    indices = list(range(len(frames))) if args.mode == 'matrix' else sorted(set(
        np.linspace(0, len(frames) - 1, 128, dtype=int).tolist() + [74, 380, 944, deepest, most]))
    if args.mode == 'outlier':
        source = json.loads(args.diagnostic_from.read_text())
        assert (source['configuration'], source['threads'], source['version']) == (args.config, args.threads, args.version)
        durations = np.array(source['samples_ns'])
        ordinary = int(np.argmin(abs(durations - np.median(durations))))
        indices = [source['sample_indices'][int(np.argmax(durations))]] * 16 + [source['sample_indices'][ordinary]] * 16
    prepared = time.perf_counter()
    resident_indices = list(range(len(frames))) if args.mode == 'outlier' else indices
    resident_feeds = [feed(data, frames[i], cfg) for i in resident_indices]
    feeds = [resident_feeds[i] for i in indices] if args.mode == 'outlier' else resident_feeds
    preparation = time.perf_counter() - prepared
    stages = {'before_load': usage()}
    options = ort.SessionOptions()
    options.intra_op_num_threads = args.threads
    options.inter_op_num_threads = 1
    options.execution_mode = ort.ExecutionMode.ORT_SEQUENTIAL
    options.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
    options.enable_mem_pattern = False
    for key in ('session.intra_op.allow_spinning', 'session.inter_op.allow_spinning'):
        options.add_session_config_entry(key, '0')
    started = time.perf_counter()
    session = ort.InferenceSession(str(directory / 'state_encoder.onnx'), sess_options=options, providers=['CPUExecutionProvider'])
    load_seconds = time.perf_counter() - started
    stages['after_load'] = usage()
    warm = sorted(set(np.linspace(0, len(frames) - 1, 14, dtype=int).tolist() + [deepest, most]))
    warm = (warm * 16)[:16]
    started = time.perf_counter()
    for i in warm:
        session.run(None, feed(data, frames[i], cfg))
    warmup_seconds = time.perf_counter() - started
    stages['after_warmup'] = usage()
    samples, counters = [], []
    began = time.perf_counter()
    reference = args.reference
    if reference:
        ref = ort.InferenceSession(str(reference / args.config / 'state_encoder.onnx'), sess_options=options, providers=['CPUExecutionProvider'])
    for i, f in zip(indices, feeds):
        before = usage() if args.mode == 'outlier' else None
        begin = time.perf_counter_ns()
        result = session.run(None, f)
        samples.append(time.perf_counter_ns() - begin)
        if before:
            after = usage()
            counters.append({key: after[key] - before[key] for key in ('cpu_s', 'voluntary', 'involuntary', 'minor', 'major')})
        assert all(np.isfinite(v).all() for v in result)
        if reference:
            for actual, expected in zip(result, ref.run(None, f)):
                np.testing.assert_allclose(actual, expected, atol=1e-5, rtol=1e-4)
        del result
    stages['after_inference'] = usage()
    summary = {'configuration': args.config, 'threads': args.threads, 'version': args.version,
        'mode': args.mode, 'round': args.round, 'sample_indices': indices, 'samples_ns': samples,
        'warmup_indices': warm, 'sample_count': len(samples), 'resident_sample_count': len(resident_feeds),
        'preparation_seconds': preparation,
        'load_seconds': load_seconds, 'warmup_seconds': warmup_seconds,
        'wall_seconds': time.perf_counter() - began, 'stages': stages, 'counters': counters,
        'statistics_ms': dict(zip(('p50', 'p90', 'p95', 'p99', 'max'), np.percentile(np.array(samples) / 1e6, [50, 90, 95, 99, 100]).tolist())),
        'graphs_sha256': {p.name: sha(p) for p in directory.glob('state_encoder.onnx*')},
        'weights_sha256': sha(directory / 'initial_weights.pt'),
        'benchmark_source_sha256': sha(Path(__file__)),
        'diagnostic_source_sha256': sha(args.diagnostic_from) if args.diagnostic_from else None,
        'inputs_sha256': {n: sha(args.packed / n) for n in ('inputs.f32', 'frames.json', 'metadata.json')},
        'environment': {'python': sys.version, 'ort': ort.__version__, 'numpy': np.__version__,
                        'torch_imported': 'torch' in sys.modules, 'affinity': sorted(os.sched_getaffinity(0)), 'load': os.getloadavg()},
        'scope': '预构造五输入、B1、同一未训练权重、逐帧零 hidden，16 次覆盖最大形状预热；仅 session.run。Torch 是否经包初始化导入见 environment.torch_imported；不执行 Torch 推理；显式 reference 也由 ORT 执行。RSS 包含输入/模型/运行时；生命周期峰值不能相减为激活内存。'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps({'output': args.output.name, 'statistics_ms': summary['statistics_ms']}, ensure_ascii=False), flush=True)


def matrix(args):
    configs = [f'MAIN-{l}x{w}' for l in (1, 2, 3, 10, 20, 30) for w in (256, 512, 768, 1024)]
    if args.mode != 'matrix':
        configs = ['MAIN-1x256', 'MAIN-30x1024']
    for round_index in (0, 1):
        settings = [(c, t) for c in configs for t in ((1, 4, 8, 16) if args.mode == 'matrix' else (8,))]
        if round_index:
            settings.reverse()
        for index, (config, threads) in enumerate(settings):
            versions = [('baseline', args.baseline), ('candidate', args.candidate)]
            if (configs.index(config) * 4 + (1, 4, 8, 16).index(threads) + round_index) % 2:
                versions.reverse()
            for version, directory in versions:
                label = f'{config}-t{threads}-{version}-r{round_index}.json'
                command = [sys.executable, '-m', 'model_train.benchmark_optimization', 'run', '--mode', args.mode,
                    '--config', config, '--threads', str(threads), '--graphs', str(directory), '--packed', str(args.packed),
                    '--output', str(args.output / label), '--version', version, '--round', str(round_index)]
                subprocess.run(command, check=True)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('command', choices=['run', 'matrix'])
    p.add_argument('--mode', choices=['matrix', 'screen', 'memory', 'outlier'], default='screen')
    for name in ('graphs', 'packed', 'output', 'baseline', 'candidate', 'reference', 'diagnostic-from'):
        p.add_argument('--' + name, type=Path)
    p.add_argument('--config'); p.add_argument('--threads', type=int, default=8)
    p.add_argument('--version', default='candidate'); p.add_argument('--round', type=int, default=0)
    args = p.parse_args()
    (run if args.command == 'run' else matrix)(args)


if __name__ == '__main__':
    main()
