"""最终源码的串行回归与共享输入三端验证；命令记录只保存可公开路径。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'output', 'build', 'graphs'):
        p.add_argument('--' + name, type=Path, required=True)
    a = p.parse_args()
    source, output, build, graphs = (getattr(a, name).resolve() for name in ('source', 'output', 'build', 'graphs'))
    output.mkdir(parents=True)
    train = source / 'model-train'
    python = train / '.venv/bin/python'
    libdir = subprocess.check_output([str(python), '-c', 'import sysconfig; print(sysconfig.get_config_var("LIBDIR"))'], text=True).strip()
    base = subprocess.check_output([str(python), '-c', 'import sys; print(sys.base_prefix)'], text=True).strip()
    site = subprocess.check_output([str(python), '-c', 'import site; print(site.getsitepackages()[0])'], text=True).strip()
    ort = subprocess.check_output([str(python), '-c', 'import onnxruntime; from pathlib import Path; print(Path(onnxruntime.__file__).parent/"capi/libonnxruntime.so.1.28.0")'], text=True).strip()
    env = dict(os.environ, PYO3_PYTHON=str(python), PYTHONHOME=base, PYTHONPATH=str(train) + ':' + site, CARGO_TARGET_DIR=str(build),
               LD_LIBRARY_PATH=libdir + (':' + os.environ['LD_LIBRARY_PATH'] if os.environ.get('LD_LIBRARY_PATH') else ''),
               OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', RAYON_NUM_THREADS='8', ORT_DYLIB_PATH=ort,
               NATIVE_MAIN_GRAPH=str(graphs / 'MAIN-1x256'), NATIVE_BASELINE_GRAPH=str(graphs / 'BASELINE-2x256'),
               NATIVE_COMPONENT_WEIGHTS=str(graphs / 'MAIN-1x256/initial_weights.pt'),
               NATIVE_VALIDATION_OUTPUT=str(output / 'three-runtime'), CONTINUOUS_OUTPUT=str(output / 'continuous'),
               NATIVE_INPUT_OWNERSHIP_OUTPUT=str(output / 'ownership.json'), NATIVE_CANCEL_GRAPH=str(graphs / 'MAIN-1x256'))
    (output / 'continuous').mkdir()
    records = []
    public = lambda value: str(value).replace(str(source), '.').replace(str(output), 'target/issue-174-performance/validation').replace(str(build), 'target/issue-174-performance/build').replace(str(graphs), 'target/issue-170-performance/final-graphs')

    def run(label, command, cwd=source, extra=None):
        row = dict(label=label, args=[public(v) for v in command], cwd=public(cwd),
                   started_utc=datetime.now(timezone.utc).isoformat())
        with (output / (label + '.log')).open('w') as log:
            result = subprocess.run([str(v) for v in command], cwd=cwd, env={**env, **(extra or {})}, stdout=log, stderr=subprocess.STDOUT)
        row.update(exit_code=result.returncode, completed_utc=datetime.now(timezone.utc).isoformat(),
                   raw_log_sha256=sha(output / (label + '.log')))
        records.append(row)
        (output / 'commands.json').write_text(json.dumps(records, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps(row, ensure_ascii=False), flush=True)
        if result.returncode:
            raise SystemExit(result.returncode)

    native = ['--manifest-path', 'model-train/native-inputs/Cargo.toml']
    run('native-clippy', ['cargo', 'clippy', *native, '--all-targets', '--features', 'continuous-validation', '--locked', '--offline', '--', '-D', 'warnings', '-A', 'clippy::needless_borrow', '-A', 'clippy::too_many_arguments'])
    run('wheel-build', [train / '.venv/bin/maturin', 'build', '--release', '--features', 'python', '--locked', '--offline', '--interpreter', python, '--out', output / 'wheels'], train)
    wheels = list((output / 'wheels').glob('*.whl'))
    assert len(wheels) == 1
    run('wheel-install', ['uv', 'tool', 'run', '--from', 'uv==0.12.9', 'uv', 'pip', 'install', '--python', python, '--offline', '--no-deps', '--reinstall', wheels[0]], output)
    run('retained-ownership', ['cargo', 'test', *native, '--features', 'continuous-validation', '--lib', '--locked', '--offline', 'retained_groups_actors_and_packing_survive_remap_depth_changes_and_retirement', '--', '--nocapture'])
    run('ownership', ['cargo', 'test', *native, '--features', 'continuous-validation', '--lib', '--locked', '--offline', 'native_shared_inputs_preserve_delayed_backward_and_detached_aliases', '--', '--ignored', '--nocapture'])
    for name in ('MAIN-1x256', 'MAIN-30x1024'):
        run('shared-three-runtime-' + name, ['cargo', 'test', *native, '--features', 'continuous-validation', '--lib', '--locked', '--offline', 'full_book_three_runtime_and_original_execution', '--', '--ignored', '--nocapture'],
            extra={'ACTOR_GRAPH_DIRECTORY': str(graphs / name), 'NATIVE_DEEP_EXECUTION_OUTPUT': str(output / (name + '-shared-three-runtime.json'))})
    run('actual-cancel-race', ['cargo', 'test', *native, '--features', 'continuous-validation', '--lib', '--locked', '--offline', 'actual_graph_pair_intent_execution_and_frozen_cancel_race', '--', '--ignored', '--nocapture'])
    run('python-full', [python, '-m', 'unittest', 'discover', '-s', 'tests', '-v'], train)
    run('native-tests', ['cargo', 'test', *native, '--features', 'continuous-validation', '--lib', '--tests', '--locked', '--offline'])
    run('rust-ort-tests', ['cargo', 'test', '--manifest-path', 'model-train/rust-ort-validation/Cargo.toml', '--locked', '--offline'])
    run('rust-ort-clippy', ['cargo', 'clippy', '--manifest-path', 'model-train/rust-ort-validation/Cargo.toml', '--all-targets', '--locked', '--offline', '--', '-D', 'warnings'])
    run('workspace-fmt', ['cargo', 'fmt', '--all', '--', '--check'])
    run('native-fmt', ['cargo', 'fmt', *native, '--', '--check'])
    run('workspace-tests', ['cargo', 'test', '--workspace', '--lib', '--bins', '--tests', '--examples', '--locked', '--offline', '--', '--skip', 'four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second'])
    identity = {'commands': records, 'wheel': {'filename': wheels[0].name, 'sha256': sha(wheels[0])},
                'source_sha256': {str(path.relative_to(source)): sha(path)
                                  for directory, suffix in ((train / 'native-inputs/src', '*.rs'), (train / 'model_train', '*.py'))
                                  for path in sorted(directory.glob(suffix))},
                'explicitly_skipped': 'four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second'}
    (output / 'checks.json').write_text(json.dumps(identity, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__':
    main()
