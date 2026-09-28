"""串行运行 #176 的16次独立完整服务，保留实际执行文件。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def now():
    return datetime.now(timezone.utc).isoformat()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--mode', choices=['continuous'], required=True)
    for name in ('candidate-binary', 'config', 'recording', 'output', 'checkpoints'):
        p.add_argument('--' + name, type=Path, required=True)
    for name in ('baseline-binary', 'graphs', 'packed', 'ort-library'):
        p.add_argument('--' + name, type=Path)
    a = p.parse_args()
    a.output.mkdir(parents=True)
    binaries = a.output / 'binaries'
    binaries.mkdir()
    frozen = {}
    for variant in ('candidate', 'baseline'):
        path = getattr(a, variant + '_binary')
        if path is not None:
            target = binaries / variant
            shutil.copy2(path, target)
            with target.open('rb') as stream:
                frozen[variant] = (target.resolve(), hashlib.file_digest(stream, 'sha256').hexdigest())
    env = dict(os.environ)
    env.update(RAYON_NUM_THREADS='8', OMP_NUM_THREADS='1', MKL_NUM_THREADS='1',
               FRAME_CHECKPOINTS=str(a.checkpoints.resolve()))
    for name in ('baseline_binary', 'graphs', 'packed', 'ort_library'):
        if getattr(a, name) is None:
            p.error('continuous 必须提供 --' + name.replace('_', '-'))
    env.update(FRAME_PACKED=str(a.packed.resolve()),
               ORT_DYLIB_PATH=str(a.ort_library.resolve()))
    configurations = [(model, segment, first, version)
                      for model in ('MAIN-1x256', 'MAIN-30x1024')
                      for segment, first in [('ordinary', 201582), ('high', 418584)]
                      for version in ('baseline', 'candidate')]
    cases = [(f'r{round}-{model}-{segment}-{version}', version,
              {'FRAME_MODEL': model, 'FRAME_MODE': version, 'FRAME_FIRST': str(first), 'FRAME_GRAPHS': str((a.graphs / version).resolve())})
             for round in (1, 2)
             for model, segment, first, version in (configurations if round == 1 else configurations[::-1])]
    with (a.output / 'run-order.jsonl').open('w') as stream:
        for name, variant, extra in cases:
            binary, binary_sha = frozen[variant]
            command = ['/usr/bin/time', '-v', '-o', str(a.output / (name + '.resources.txt')),
                       str(binary), str(a.config.resolve()), str(a.recording.resolve()),
                       str((a.output / name).resolve()), 'decisions', '9999999']
            row = {'run': name, 'started_utc': now(), 'binary_sha256': binary_sha,
                   'model': extra.get('FRAME_MODEL'), 'native_threads': 8,
                   'ort_threads': (4 if extra['FRAME_MODEL'] == 'MAIN-1x256' else 16) if 'FRAME_MODEL' in extra else None}
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
