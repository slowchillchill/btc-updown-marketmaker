"""冻结本轮实际使用的源码、图、权重、输入与依赖身份；公开输出只含相对名称。"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess

BASE = 'e57503821391a9943ce694eabab00ec0fdff1141'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'graphs', 'packed', 'recording', 'config', 'selection', 'ort-library', 'output'):
        p.add_argument('--' + name, type=Path, required=True)
    a = p.parse_args()
    source = a.source.resolve()
    schema = sha(source / 'model-train/native-inputs/feature_schema.json')
    models = {}
    for name in ('MAIN-1x256', 'MAIN-30x1024'):
        directory = a.graphs / name
        metadata = json.loads((directory / 'benchmark_configuration.json').read_text())
        assert metadata['input_contract_version'] == 'native_inputs_v5'
        assert metadata['feature_schema_sha256'] == schema
        assert metadata['weights_sha256'] == sha(directory / 'initial_weights.pt')
        proofs = {}
        for filename, expected in metadata['source_files_sha256'].items():
            relative = 'model-train/model_train/' + filename
            baseline = subprocess.check_output(['git', 'show', BASE + ':' + relative], cwd=source)
            assert hashlib.sha256(baseline).hexdigest() == expected == sha(source / relative)
            proofs[relative] = expected
        models[name] = {'metadata': metadata, 'source_matches_current_baseline': proofs,
                        'files': {file.name: {'sha256': sha(file), 'bytes': file.stat().st_size}
                                  for file in sorted(directory.iterdir())
                                  if file.name == 'initial_weights.pt' or '.onnx' in file.name}}
    config = json.loads(a.config.read_text())
    # 路径只是本机定位。公开语义使用对应产物内容身份，不复制路径绑定。
    source_name = lambda path: '/'.join(Path(path).parts[-3:])
    for item in config['files']:
        item['path'] = source_name(item['path'])
    config['source_root'] = Path(config['source_root']).name
    overrides = config['source_overrides']
    config['source_overrides'] = {}
    for path, override in overrides.items():
        override['source_root'] = Path(override['source_root']).name
        config['source_overrides'][source_name(path)] = override
    for key in ('rule_snapshot', 'finality_path'):
        path = Path(config[key])
        config[key] = {'artifact_role': key, 'sha256': sha(path)}
    identity = {'baseline': BASE, 'input_contract_version': 'native_inputs_v5',
                'feature_schema_sha256': schema, 'models': models,
                'native_source_files_sha256': {
                    str(path.relative_to(source)): sha(path)
                    for path in sorted((source / 'model-train/native-inputs/src').glob('*.rs'))},
                'configuration_without_local_paths': config,
                'record_completion': json.loads((a.recording / 'record-completion.json').read_text()),
                'record_completion_sha256': sha(a.recording / 'record-completion.json'),
                'packed': {name: sha(a.packed / name) for name in ('inputs.f32', 'frames.json', 'metadata.json')},
                'selection_sha256': sha(a.selection),
                'ort_library': {'filename': a.ort_library.name, 'sha256': sha(a.ort_library)},
                'locks': {name: sha(source / name) for name in
                          ('Cargo.lock', 'model-train/uv.lock', 'model-train/native-inputs/Cargo.lock',
                           'model-train/rust-ort-validation/Cargo.lock')},
                'host': {'kernel': platform.release(), 'system': platform.system(), 'architecture': platform.machine()},
                'graph_metadata_note': '历史 source_commit 单独保留；当前源码身份由固定基线下的源文件摘要、原权重及实际图文件共同核对，未把历史字段改称当前导出提交。'}
    serialized = json.dumps(identity, ensure_ascii=False, indent=2) + '\n'
    # 公开证据本身的边界检查；不增加生产业务接受条件。
    if '/home/' in serialized or '/tmp/' in serialized:
        raise ValueError('源元数据仍有本机路径，应明确投影其公开字段后输出')
    a.output.write_text(serialized)


if __name__ == '__main__':
    main()
