"""导出可复算报告需要的源码、运行身份和验证摘要；私有原始日志留在产物目录。"""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
import tarfile


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def test_results(path):
    # 业务测试会启动子进程测试；每个顶层 Running 目标只计最后一条结果。
    targets = []
    for section in re.split(r'^     Running ', path.read_text(), flags=re.M)[1:]:
        results = re.findall(r'^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out;', section, re.M)
        assert results, section.splitlines()[0]
        targets.append(dict(target=section.splitlines()[0].split(' (')[0],
                            **dict(zip(('passed', 'failed', 'ignored', 'measured', 'filtered'), map(int, results[-1])))))
    return dict(targets=targets, total={field: sum(row[field] for row in targets)
                                      for field in ('passed', 'failed', 'ignored', 'measured', 'filtered')})


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('source', 'artifact-root', 'harnesses', 'validation', 'native', 'continuous', 'output'):
        p.add_argument('--' + name, type=Path, required=True)
    a = p.parse_args()
    source, output = a.source.resolve(), a.output.resolve()
    output.mkdir(parents=True, exist_ok=True)

    def public(value):
        if isinstance(value, str):
            return value.replace(str(source) + '/', '').replace(str(a.artifact_root.resolve()) + '/', 'target/')
        if isinstance(value, dict):
            return {public(k): public(v) for k, v in value.items()}
        if isinstance(value, list):
            return [public(v) for v in value]
        return value

    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(public(value), ensure_ascii=False, indent=2) + '\n'
        assert '/home/' not in text, path
        path.write_text(text)

    for name in ('source-identities.json', 'binaries.json', 'provenance.json'):
        write(output / 'source' / name, json.loads((a.harnesses / name).read_text()))
    toolchain = a.harnesses / 'toolchain.json'
    if not toolchain.exists():
        toolchain = a.harnesses.parent / 'toolchain.json'  # 首轮在生成目录的公共父目录冻结。
    write(output / 'source/toolchain.json', json.loads(toolchain.read_text()))
    identity = json.loads((a.harnesses / 'source-identities.json').read_text())
    archives = []
    for variant, files in identity['variants'].items():
        archive_path = output / 'source' / (variant + '-overlay.tar.gz')
        saved = []
        with archive_path.open('wb') as target, gzip.GzipFile(fileobj=target, mode='wb', filename='', mtime=0) as zipped, tarfile.open(fileobj=zipped, mode='w') as archive:
            for relative, digest in files.items():
                path = a.harnesses / (variant + '-harness') / relative
                data = path.read_bytes()
                assert hashlib.sha256(data).hexdigest() == digest, relative
                before = subprocess.run(['git', 'show', identity['baseline'] + ':' + relative], cwd=source, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
                if before.returncode == 0 and before.stdout == data:
                    continue
                assert b'/home/' not in data, relative
                member = tarfile.TarInfo(relative)
                member.size = len(data)
                member.mode = 0o644
                archive.addfile(member, io.BytesIO(data))
                saved.append(relative)
        archives.append(dict(variant=variant, archive=archive_path.name, sha256=sha(archive_path),
                             baseline=identity['baseline'], overlay_files=saved))
    write(output / 'source/archives.json', archives)

    checks = json.loads((a.validation / 'checks.json').read_text())
    for relative, digest in checks['source_sha256'].items():
        assert sha(source / relative) == digest, ('已验证源码随后变化', relative)
    write(output / 'validation/checks.json', checks)
    write(output / 'validation/ownership.json', json.loads((a.validation / 'ownership.json').read_text()))
    results = {name: test_results(a.validation / (name + '.log'))
               for name in ('workspace-tests', 'native-tests', 'ownership', 'actual-cancel-race',
                            'shared-three-runtime-MAIN-1x256', 'shared-three-runtime-MAIN-30x1024')}
    python_log = (a.validation / 'python-full.log').read_text()
    assert re.search(r'^OK$', python_log, re.M)
    results['python-full'] = {'passed': int(re.search(r'^Ran (\d+) tests', python_log, re.M)[1])}
    results['independent-baseline-oracle'] = test_results(a.harnesses / 'oracle.log')
    results['explicitly_skipped'] = checks['explicitly_skipped']
    native_retained = (a.validation / 'retained-ownership.log').read_text()
    metrics = re.search(r'retained_ownership groups=(\d+) actors=(\d+) projected_versions=(\d+) projected_capacity_bytes=(\d+)', native_retained)
    assert metrics
    results['retained-native-inputs'] = dict(zip(('groups', 'actors', 'projected_versions', 'projected_capacity_bytes'), map(int, metrics.groups())))
    results['retained-native-inputs']['validation'] = test_results(a.validation / 'retained-ownership.log')
    supplementary = a.validation / 'retained-ownership-command.json'
    results['retained-native-inputs']['command'] = (json.loads(supplementary.read_text()) if supplementary.exists()
                                                    else next(row for row in checks['commands'] if row['label'] == 'retained-ownership'))
    write(output / 'validation/results.json', results)
    for model in ('MAIN-1x256', 'MAIN-30x1024'):
        original = json.loads((a.validation / (model + '-shared-three-runtime.json')).read_text())
        # 原三端宿主还附带其他夹具的通用参数；仅导出本次真实调用的深度与所用依赖。
        value = {key: item for key, item in original.items() if key != 'provenance'}
        value['provenance'] = {key: original['provenance'][key] for key in
                               ('graphs', 'mapped_ort', 'native_extension_sha256', 'sources', 'ort_sessions')}
        value['raw_evidence_sha256'] = sha(a.validation / (model + '-shared-three-runtime.json'))
        write(output / 'validation' / (model + '-three-runtime.json'), value)
    for name, directory in (('native', a.native), ('continuous', a.continuous)):
        order = [json.loads(line) for line in (directory / 'run-order.jsonl').read_text().splitlines()]
        assert all(row['exit_code'] == 0 for row in order)
        write(output / name / 'run-order.json', order)


if __name__ == '__main__':
    main()
