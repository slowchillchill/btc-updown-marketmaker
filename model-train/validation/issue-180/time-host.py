"""步骤10：准确源码副本上的有限真实模型观察/完成/发送消费者。"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import behavior


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source = args.source.resolve()
    output = args.output.resolve()
    behavior.prepare(source, args.revision, output, True)
    fixture_path = output / 'model-train/native-inputs/src/continuous_tests.rs'
    fixture = fixture_path.read_text()
    old = '.filter(|at| *at == START || (FIRST-30000..=FIRST+20000).contains(at) || [FIRST+270000,FIRST+271000,FIRST+280000,FIRST+300000,FIRST+301000,END].contains(at))'
    fixture = behavior.replace_once(fixture, 'for at in (START..=END).step_by(1000)' + old,
        'for at in [START,FIRST-30000,FIRST-29000,FIRST-20000,FIRST,FIRST+20,FIRST+270000,FIRST+271000,FIRST+280000,FIRST+300000,FIRST+301000,END]')
    fixture += '\n' + Path(__file__).with_suffix('.rs').read_text()
    fixture_path.write_text(fixture)
    # 同一窄 trace 输出的线程互斥仅用于诊断，避免模型和宿主写入交错。
    runtime_path = output / 'model-train/native-inputs/src/continuous_runtime.rs'
    runtime = runtime_path.read_text()
    runtime = behavior.replace_once(runtime, 'pub(super) fn issue180_trace(value: serde_json::Value) {',
        '''pub(super) fn issue180_trace(value: serde_json::Value) {
    static TRACE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = TRACE_LOCK.lock().unwrap();''')
    runtime_path.write_text(runtime)
    manifest_path = output.parent / (output.name + '-freeze.json')
    manifest = json.loads(manifest_path.read_text())
    manifest['purpose'] = '有限100ms真实模型消费者；不运行容量窗口'
    manifest['diagnostic_files'] = {
        str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in [fixture_path, runtime_path, output / 'model-train/native-inputs/src/root.rs']
    }
    manifest['time_host_tool_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    manifest['time_host_fixture_sha256'] = hashlib.sha256(Path(__file__).with_suffix('.rs').read_bytes()).hexdigest()
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')
    # 更新原诊断补丁，实际附加消费者和锁必须包含在摘要内。
    import difflib
    patch = ''
    for path in [fixture_path, runtime_path, output / 'model-train/native-inputs/src/root.rs']:
        relative = str(path.relative_to(output))
        original = subprocess.check_output(['git', 'show', manifest['revision'] + ':' + relative], cwd=source, text=True)
        patch += ''.join(difflib.unified_diff(original.splitlines(True), path.read_text().splitlines(True), fromfile='a/' + relative, tofile='b/' + relative))
    (output.parent / (output.name + '-diagnostic.patch')).write_text(patch)
    manifest['diagnostic_patch_sha256'] = hashlib.sha256(patch.encode()).hexdigest()
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    main()
