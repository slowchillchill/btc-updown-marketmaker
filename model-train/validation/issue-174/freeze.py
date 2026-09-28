"""核对复用图/权重/检查点与当前基线，冻结可公开的内容身份。"""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess

BASELINE = '43765885dedd2593abdbdde868da3d64ed6fb646'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'graphs', 'packed', 'recording', 'config', 'checkpoints', 'ort-library', 'harnesses', 'output'):
        p.add_argument('--' + name, type=Path, required=True)
    a = p.parse_args()
    # #172 的公开证据已保存完整录制/规则/依赖身份。逐项核对实际复用品，再引用该证据，避免重复私有路径。
    prior_path = a.source / 'model-train/validation/issue-172/evidence/provenance.json'
    prior = json.loads(prior_path.read_text())
    assert sha(a.recording / 'record-completion.json') == prior['record_completion_sha256']
    for name, digest in prior['packed'].items():
        assert sha(a.packed / name) == digest
    assert sha(a.ort_library) == prior['ort_library']['sha256']
    config = json.loads(a.config.read_text())
    for key in ('rule_snapshot', 'finality_path'):
        assert sha(Path(config[key])) == prior['configuration_without_local_paths'][key]['sha256']
    projected = json.loads(json.dumps(config))
    source_name = lambda path: '/'.join(Path(path).parts[-3:])
    for item in projected['files']:
        item['path'] = source_name(item['path'])
    projected['source_root'] = Path(projected['source_root']).name
    overrides = projected['source_overrides']
    projected['source_overrides'] = {}
    for path, override in overrides.items():
        override['source_root'] = Path(override['source_root']).name
        projected['source_overrides'][source_name(path)] = override
    for key in ('rule_snapshot', 'finality_path'):
        projected[key] = {'artifact_role': key, 'sha256': sha(Path(config[key]))}
    assert projected == prior['configuration_without_local_paths']
    schema = sha(a.source / 'model-train/native-inputs/feature_schema.json')
    assert schema == prior['feature_schema_sha256']
    models = {}
    for name in ('MAIN-1x256', 'MAIN-30x1024'):
        directory = a.graphs / name
        metadata = json.loads((directory / 'benchmark_configuration.json').read_text())
        assert metadata == prior['models'][name]['metadata']
        proof = {}
        for filename, digest in metadata['source_files_sha256'].items():
            relative = 'model-train/model_train/' + filename
            before = subprocess.check_output(['git', 'show', BASELINE + ':' + relative], cwd=a.source)
            assert hashlib.sha256(before).hexdigest() == digest
            after = (a.source / relative).read_bytes()
            if filename == 'book.py':
                # 唯一新增类是宿主打包；逐个原定义的完整 AST 与基线相同，图内编码/归约没有改写。
                original = ast.parse(before)
                current = ast.parse(after)
                current.body = [node for node in current.body if not isinstance(node, ast.ClassDef) or node.name != 'PackedBookInputs']
                assert ast.dump(current) == ast.dump(original)
                proof[relative] = {'baseline_sha256': digest, 'current_sha256': hashlib.sha256(after).hexdigest(),
                                   'original_definitions_ast_equal': True, 'host_only_addition': 'PackedBookInputs'}
            else:
                assert hashlib.sha256(after).hexdigest() == digest
                proof[relative] = {'baseline_and_current_sha256': digest}
        files = {}
        for filename, expected in prior['models'][name]['files'].items():
            path = directory / filename
            assert sha(path) == expected['sha256'] and path.stat().st_size == expected['bytes']
            files[filename] = expected
        models[name] = dict(files=files, source_proof=proof, metadata=metadata)
    checkpoints = {str(first): {'filename': f'entry-{first}.json', 'sha256': sha(a.checkpoints / f'entry-{first}.json'),
                                'bytes': (a.checkpoints / f'entry-{first}.json').stat().st_size}
                   for first in (201582, 418584)}
    prior_entries = json.loads((a.source / 'model-train/validation/issue-172/evidence/preparation/prehistory.json').read_text())['entries']
    for entry in prior_entries:
        assert checkpoints[str(entry['position'])]['sha256'] == entry['sha256']
        assert checkpoints[str(entry['position'])]['bytes'] == entry['bytes']
    sources = json.loads((a.harnesses / 'source-identities.json').read_text())
    assert sources['baseline'] == BASELINE
    for variant, files in sources['variants'].items():
        for relative, digest in files.items():
            assert sha(a.harnesses / (variant + '-harness') / relative) == digest
    identity = {'baseline': BASELINE, 'input_contract_version': 'native_inputs_v5', 'feature_schema_sha256': schema,
                'prior_identity': {'relative_path': str(prior_path.relative_to(a.source)), 'sha256': sha(prior_path)},
                'configuration_sha256': sha(a.config), 'record_completion_sha256': sha(a.recording / 'record-completion.json'),
                'packed': prior['packed'], 'ort_library': prior['ort_library'], 'models': models,
                'checkpoints': checkpoints, 'source_identities_sha256': sha(a.harnesses / 'source-identities.json'),
                'locks': {name: sha(a.source / name) for name in ('Cargo.lock', 'model-train/uv.lock', 'model-train/native-inputs/Cargo.lock')},
                'checkpoint_scope': '前序真实无交易顺序恢复已独立核对；新缓存不在检查点内，首次 consume 冷建计时。交易实例独立推进，未回滚账户或 hidden。'}
    serialized = json.dumps(identity, ensure_ascii=False, indent=2) + '\n'
    assert '/home/' not in serialized and '/tmp/' not in serialized
    a.output.write_text(serialized)


if __name__ == '__main__':
    main()
