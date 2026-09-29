"""首轮两项P2的定向验收与规定回归；不重跑性能矩阵或重导出模型。"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for key in ('source', 'build', 'output'):
        parser.add_argument('--' + key, type=Path, required=True)
    args = parser.parse_args()
    source, build, output = (getattr(args, key).resolve() for key in ('source', 'build', 'output'))
    output.mkdir(parents=True, exist_ok=False)
    train = source / 'model-train'
    python = train / '.venv/bin/python'
    base, site, libdir = json.loads(subprocess.check_output([str(python), '-c',
        'import json,sys,site,sysconfig; print(json.dumps([sys.base_prefix,site.getsitepackages()[0],sysconfig.get_config_var("LIBDIR")]))'], text=True))
    env = dict(os.environ, PYO3_PYTHON=str(python), PYTHONHOME=base,
        PYTHONPATH=str(train) + ':' + site, LD_LIBRARY_PATH=libdir, CARGO_TARGET_DIR=str(build),
        OMP_NUM_THREADS='1', MKL_NUM_THREADS='1', RAYON_NUM_THREADS='8',
        NATIVE_ACTION_PROBABILITY_OUTPUT=str(output / 'probability-and-optimizer.json'))
    def digest(path):
        with path.open('rb') as stream:
            return hashlib.file_digest(stream, 'sha256').hexdigest()
    def public(value):
        return str(value).replace(str(output), 'target/issue-176-performance/' + output.name).replace(str(build), 'target/issue-176-build').replace(str(source), '.')
    records = []
    sources = ('model-train/model_train/probability_validation.py',
        'model-train/native-inputs/src/buy_tests.rs', 'model-train/validation/issue-176/review_checks.py')
    def run(label, command):
        row = {'label': label, 'args': [public(v) for v in command], 'cwd': '.',
            'started_utc': datetime.now(timezone.utc).isoformat(),
            'source_files': {name: digest(source / name) for name in sources}}
        path = output / (label + '.log')
        with path.open('w') as log:
            result = subprocess.run(command, cwd=source, env=env, stdout=log, stderr=subprocess.STDOUT)
        row.update(exit_code=result.returncode, completed_utc=datetime.now(timezone.utc).isoformat(),
            raw_log_sha256=digest(path))
        records.append(row)
        (output / 'commands.json').write_text(json.dumps(records, ensure_ascii=False, indent=2) + '\n')
        print(json.dumps(row, ensure_ascii=False), flush=True)
        if result.returncode:
            print('\n'.join(path.read_text().splitlines()[-70:]), flush=True)
            raise SystemExit(result.returncode)
    native = ['--manifest-path', 'model-train/native-inputs/Cargo.toml']
    run('joint-and-optimizer', ['cargo', 'test', *native, '--features', 'continuous-validation', '--lib',
        '--locked', '--offline', 'small_domain_joint_probability_entropy_and_singleton_optimizer', '--', '--ignored', '--nocapture'])
    run('native-tests', ['cargo', 'test', *native, '--features', 'continuous-validation', '--lib', '--tests', '--locked', '--offline'])
    run('workspace-tests', ['cargo', 'test', '--workspace', '--lib', '--bins', '--tests', '--examples',
        '--locked', '--offline', '--', '--skip', 'four_source_production_path_sustains_two_to_five_thousand_native_messages_per_second'])
    run('native-clippy', ['cargo', 'clippy', *native, '--all-targets', '--features', 'continuous-validation',
        '--locked', '--offline', '--', '-D', 'warnings', '-A', 'clippy::needless_borrow', '-A', 'clippy::too_many_arguments'])
    run('workspace-fmt', ['cargo', 'fmt', '--all', '--', '--check'])
    run('native-fmt', ['cargo', 'fmt', *native, '--', '--check'])


if __name__ == '__main__':
    main()
