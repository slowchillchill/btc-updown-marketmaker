"""串行运行 #172 已冻结的正式样本；不调用 Python 模型或并行性能任务。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess


def now():
    return datetime.now(timezone.utc).isoformat()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['native', 'continuous'], required=True)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--config', type=Path, required=True)
    p.add_argument('--recording', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--selection', type=Path)
    p.add_argument('--graphs', type=Path)
    p.add_argument('--packed', type=Path)
    p.add_argument('--checkpoints', type=Path)
    p.add_argument('--ort-library', type=Path)
    a = p.parse_args()
    a.output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    for name in ('NATIVE_DIAGNOSTIC', 'NATIVE_SCREEN', 'NATIVE_RESOURCE',
                 'NATIVE_CONTINUOUS', 'NATIVE_SELECT'):
        env.pop(name, None)
    env.update(RAYON_NUM_THREADS='8', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1')
    if a.mode == 'native':
        if a.selection is None:
            p.error('native 必须提供 --selection')
        env['NATIVE_SELECTION'] = str(a.selection.resolve())
        cases = [('native-formal', {})]
    else:
        for name in ('graphs', 'packed', 'checkpoints', 'ort_library'):
            if getattr(a, name) is None:
                p.error('continuous 必须提供 --' + name.replace('_', '-'))
        env.update(FRAME_GRAPHS=str(a.graphs.resolve()), FRAME_PACKED=str(a.packed.resolve()),
                   FRAME_CHECKPOINTS=str(a.checkpoints.resolve()), ORT_DYLIB_PATH=str(a.ort_library.resolve()))
        env.pop('NATIVE_SELECTION', None)
        configurations = [(model, segment, first, version)
                          for model in ('MAIN-1x256', 'MAIN-30x1024')
                          for segment, first in [('ordinary', 201582), ('high', 418584)]
                          for version in ('baseline', 'candidate')]
        cases = [(f'r{round}-{model}-{segment}-{version}',
                  {'FRAME_MODEL': model, 'FRAME_MODE': version, 'FRAME_FIRST': str(first)})
                 for round in (0, 1)
                 for model, segment, first, version in (configurations if round == 0 else configurations[::-1])]
    binary_sha = hashlib.file_digest(a.binary.open('rb'), 'sha256').hexdigest()
    manifest = a.output / 'run-order.jsonl'
    if manifest.exists():
        raise FileExistsError('保留既有运行；请使用新的输出目录')
    with manifest.open('w') as stream:
        for name, extra in cases:
            destination = a.output / name
            if destination.exists():
                raise FileExistsError(destination)
            command = ['/usr/bin/time', '-v', '-o', str(a.output / (name + '.resources.txt')),
                       str(a.binary.resolve()), str(a.config.resolve()), str(a.recording.resolve()),
                       str(destination.resolve()), 'decisions', '9999999']
            row = {'run': name, 'started_utc': now(), 'binary_sha256': binary_sha,
                   'model': extra.get('FRAME_MODEL'), 'native_threads': 8,
                   'ort_threads': 4 if extra.get('FRAME_MODEL') == 'MAIN-1x256' else 16 if extra else None}
            print(json.dumps(row), flush=True)
            with (a.output / (name + '.log')).open('w') as log:
                result = subprocess.run(command, env={**env, **extra}, stdout=log, stderr=subprocess.STDOUT)
            row.update(completed_utc=now(), exit_code=result.returncode)
            stream.write(json.dumps(row) + '\n')
            stream.flush()
            print(json.dumps(row), flush=True)
            if result.returncode:
                raise SystemExit(result.returncode)


if __name__ == '__main__':
    main()
