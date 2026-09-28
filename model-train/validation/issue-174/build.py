"""串行编译独立驱动；共享构建缓存时，先保存候选文件再构建基线。"""
import argparse
import hashlib
import json
from pathlib import Path
import os
import platform
import shutil
import subprocess


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--harnesses', type=Path, required=True)
    p.add_argument('--build', type=Path, required=True)
    a = p.parse_args()
    root, build = a.harnesses.resolve(), a.build.resolve()
    binaries = root / 'binaries'
    binaries.mkdir()
    toolchain = {'rustc': subprocess.check_output(['rustc', '-vV'], text=True).strip(),
                 'cargo': subprocess.check_output(['cargo', '--version'], text=True).strip(),
                 'host': {'system': platform.system(), 'release': platform.release(), 'architecture': platform.machine()}}
    (root / 'toolchain.json').write_text(json.dumps(toolchain, indent=2) + '\n')
    records = []
    for variant in ('candidate', 'baseline'):
        manifest = root / (variant + '-harness/model-train/native-inputs/Cargo.toml')
        commands = []
        if variant == 'candidate':
            commands.append(('oracle', ['cargo', 'test', '--release', '--lib', '--locked', '--offline', '--manifest-path', str(manifest), 'incremental_dependencies_match_independent_latest_main']))
        command = ['cargo', 'build', '--release', '--locked', '--offline', '--features', 'ort', '--manifest-path', str(manifest), '--example', 'frame_latency']
        if variant == 'candidate':
            command += ['--example', 'native_incremental']
        commands.append((variant, command))
        for label, command in commands:
            with (root / (label + '.log')).open('w') as log:
                result = subprocess.run(command, env=dict(os.environ, CARGO_TARGET_DIR=str(build)), stdout=log, stderr=subprocess.STDOUT)
            if result.returncode:
                print('\n'.join((root / (label + '.log')).read_text().splitlines()[-60:]), flush=True)
                raise SystemExit(result.returncode)
        files = [('frame_latency', variant + '-frame')]
        if variant == 'candidate':
            files += [('native_incremental', 'native_incremental')]
        for example, name in files:
            target = binaries / name
            shutil.copy2(build / ('release/examples/' + example), target)
            with target.open('rb') as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            records.append(dict(variant=variant, example=example, binary=name, sha256=digest))
    (root / 'binaries.json').write_text(json.dumps(records, indent=2) + '\n')
    print(json.dumps(records), flush=True)


if __name__ == '__main__':
    main()
