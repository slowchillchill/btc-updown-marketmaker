"""建立当前主线和候选的离线测量副本，生产路径不加入诊断接口。"""
import argparse
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tomllib

BASELINE = 'bdb54b4e46adf29a8b607171338a6efce701d301'


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--variant', choices=['baseline', 'candidate'], required=True)
    args = parser.parse_args()
    source, out = args.source.resolve(), args.output.resolve()
    project = source / 'model-train/validation/issue-178'
    previous = module('previous174', project.parent / 'issue-174/prepare.py')
    actor = module('actor176', project.parent / 'issue-176/instrument.py')
    probe = module('probe178', project / 'instrument.py')
    out.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(subprocess.check_output(['git', 'archive', BASELINE], cwd=source))) as tar:
        tar.extractall(out, filter='data')
    if args.variant == 'candidate':
        for folder in ('model-train/native-inputs', 'shared-run', 'ledger'):
            shutil.copytree(source / folder, out / folder, dirs_exist_ok=True,
                            ignore=shutil.ignore_patterns('target', '__pycache__'))
    native = out / 'model-train/native-inputs'
    manifest, lock = native / 'Cargo.toml', native / 'Cargo.lock'
    version = next(p['version'] for p in tomllib.loads(lock.read_text())['package'] if p['name'] == 'libc')
    manifest.write_text(manifest.read_text().replace('[features]\n', '[features]\nbench-profile = ["shared-run/bench-profile"]\n').replace(
        '[dev-dependencies]\n', '[dev-dependencies]\nlibc = "=' + version + '"\n'))
    text = lock.read_text(); start = text.index('name = "model-native-inputs"'); end = text.index('[[package]]', start)
    lock.write_text(text[:start] + text[start:end].replace(' "ledger",\n', ' "ledger",\n "libc",\n') + text[end:])
    src = native / 'src'
    market = previous.market_helpers((src / 'market.rs').read_text(), True)
    for name in ('SidePrevious', 'ParsedLevel', 'SideInputs', 'Sample', 'MarketHistory'):
        marker = ('pub(crate) struct ' if name in ('SidePrevious', 'Sample', 'MarketHistory') else 'struct ') + name
        market = market.replace(marker, '#[derive(serde::Serialize, serde::Deserialize)]\n' + marker)
    (src / 'market.rs').write_text(market)
    with (src/'market.rs').open('a') as stream:
        stream.write('''
impl MarketHistory {
    pub(crate) fn bench_published_books(&self)->BookLevels {
        std::array::from_fn(|i|self.sides[i].inputs.as_ref().map_or_else(Arc::default,|side|side.book.clone()))
    }
}
''')
    root = previous.root_helpers((src / 'root.rs').read_text(), 'market', True)
    root = root.replace('pub struct HiddenKey {', '#[derive(serde::Deserialize)]\npub struct HiddenKey {')
    start = root.index('    pub fn baseline(&self)'); end = root.index('    pub fn clear_added_inputs', start)
    root = root[:start] + root[end:]
    root += '''
impl NativeInputRoot {
    pub fn bench_warm_checkpoint(&self)->serde_json::Value {
        let mut value=self.bench_business();
        value["complete_histories"]=serde_json::to_value(&self.histories).unwrap();
        value
    }
    pub fn bench_restore_warm(&mut self,value:&serde_json::Value) {
        self.restore_checkpoint(value);
        self.histories=serde_json::from_value(value["complete_histories"].clone()).unwrap();
    }
    pub fn bench_previous_books(&self,rows:&serde_json::Value)->Vec<(HiddenKey,crate::BookLevels)> {
        rows.as_array().unwrap().iter().map(|row| {
            let key:HiddenKey=serde_json::from_value(row["hidden_state_key"].clone()).unwrap();
            let books=self.histories[&key.condition_id.0].bench_published_books();
            (key,books)
        }).collect()
    }
}
'''
    (src / 'root.rs').write_text(root)
    scorer=actor.instrument((src / 'ort_actor.rs').read_text(), True)
    scorer=scorer.replace('    pub hidden: usize,','    pub hidden: usize,\n    pub bench_prefix: Vec<f32>,')
    scorer=scorer.replace('            hidden,','            hidden,\n            bench_prefix: Vec::new(),')
    # 复制已有36维图输入作为字段观察；不为证据重新执行精确前缀投影。
    scorer=scorer.replace('            let name = match head {','            self.bench_prefix.clone_from(&prefix);\n            let name = match head {')
    (src / 'ort_actor.rs').write_text(scorer)
    probe.apply(src)
    (src / 'bench_profile.rs').write_text((project / 'profile.rs').read_text())
    with (src / 'lib.rs').open('a') as stream:
        stream.write('\npub mod bench_profile;\n')
    shared=out/'shared-run'
    text=(shared/'Cargo.toml').read_text()
    text=text.replace('[features]\n','[features]\nbench-profile = []\n') if '[features]\n' in text else text+'\n[features]\nbench-profile = []\n'
    (shared/'Cargo.toml').write_text(text)
    probe.shared(shared/'src')
    (shared/'src/bench_profile.rs').write_text((project/'profile.rs').read_text())
    with (shared/'src/lib.rs').open('a') as stream:stream.write('\npub mod bench_profile;\n')
    (native / 'examples/common').mkdir(exist_ok=True)
    runtime = (project.parent / 'issue-176/runtime.rs').read_text()
    runtime=runtime.replace('self.scorer.profile.clear();','self.scorer.profile.clear();\n                    self.scorer.bench_prefix.clear();')
    runtime=runtime.replace('"row":i,"head":head,"support_ns"','"row":i,"head":head,"prefix_features":self.scorer.bench_prefix,"support_ns"')
    runtime += '''
impl Runtime {
    pub fn restore_books(&mut self,rows:Vec<(model_native_inputs::HiddenKey,model_native_inputs::BookLevels)>) {
        self.books.clear();
        for (key,book) in rows {
            let mut packed=PackedBooks::default();packed.update([(&key,&book)]);self.books.push(packed);
        }
    }
}
'''
    (native / 'examples/common/frame_runtime.rs').write_text(runtime)
    tail = (project / 'frame.rs').read_text().replace('VARIANT_NAME', args.variant)
    host = previous.host((native / 'examples/replay_projection.rs').read_text(), tail, True)
    (native / 'examples/frame_latency.rs').write_text(host)
    identities = {str(p.relative_to(out)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for folder in (native, out / 'shared-run', out / 'ledger')
                  for p in folder.rglob('*') if p.is_file() and p.suffix in ('.rs', '.toml', '.lock')}
    (out / 'source-identities.json').write_text(json.dumps({'baseline': BASELINE, 'variant': args.variant,
                                                         'files': identities}, indent=2) + '\n')
    print(out)


if __name__ == '__main__':
    main()
