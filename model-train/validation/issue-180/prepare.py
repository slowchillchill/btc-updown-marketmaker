"""从准确工具提交建立全仓在线诊断副本；生产路径不加入诊断接口。"""
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

BASELINE = None


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    result = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--support-failure', action='store_true')
    parser.set_defaults(variant='candidate')
    args = parser.parse_args()
    source, out = args.source.resolve(), args.output.resolve()
    global BASELINE
    BASELINE = subprocess.check_output(['git','rev-parse',args.revision],cwd=source,text=True).strip()
    out.mkdir(parents=True)
    with tarfile.open(fileobj=io.BytesIO(subprocess.check_output(['git', 'archive', BASELINE], cwd=source))) as tar:
        tar.extractall(out, filter='data')
    project = out / 'model-train/validation/issue-178'
    previous = module('previous174', project.parent / 'issue-174/prepare.py')
    actor = module('actor176', project.parent / 'issue-176/instrument.py')
    probe = module('probe178', project / 'instrument.py')
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
    (src / 'ort_actor.rs').write_text(scorer)
    # 只保留 F6 共享读取/既有 Actor 范围计数，不启用逐账本价位 rational_key/全特征探针。
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
    tail = (out / 'model-train/validation/issue-180/frame.rs').read_text()
    host = previous.host((native / 'examples/replay_projection.rs').read_text(), tail, True)
    baseline_tail=(out/'model-train/validation/issue-180/protocol.rs').read_text()
    baseline_host=previous.host((native/'examples/replay_projection.rs').read_text(),baseline_tail,False)
    baseline_host=baseline_host.replace('    let mut driver = FinitePublicConsumptionV1::new(','''    let comparison=std::rc::Rc::new(std::cell::RefCell::new(BufWriter::new(File::create(out.join("comparison.jsonl"))?)));
    let first:u64=std::env::var("FRAME_FIRST")?.parse()?;
    let source=source.map(|item| {if let Ok(item)=&item {if item.prepared.reference.group_position>=first {line(&mut *comparison.borrow_mut(),&serde_json::json!({"reference":item.prepared.reference,"group":item.prepared.group})).unwrap();}}item});
    let mut driver = FinitePublicConsumptionV1::new(''')
    baseline_host=baseline_host.replace('    driver.stop_and_drain()?;','    comparison.borrow_mut().flush()?;\n    driver.stop_and_drain()?;')
    (native/'examples/frame_protocol_original.rs').write_text(baseline_host)
    host = host.replace('#[path="common/frame_runtime.rs"] mod frame_runtime;', '#[path="common/frame_runtime.rs"] mod frame_runtime;\n#[path="common/frame_online.rs"] mod frame_online;')
    host=host.replace('    finite_in_flight_groups: usize,','    finite_in_flight_groups: usize,\n    output:data_pipeline_training::gate_a::GateAOutputConfig,\n    record_checkpoint_inputs:u64,')
    a=host.index('    let rec = root.join("recording");'); b=host.index('    let variant =',a)
    host=host[:a]+'''    let source=frame_online::Source::start(&root.join("recording"),&out.join("dpt"), cfg.header.clone(), cfg.canonical.decode_batch_rows,cfg.canonical.max_decode_batch_bytes,cfg.output.clone(),cfg.record_checkpoint_inputs)?;
    let control=source.control.clone();
    let mut driver=FinitePublicConsumptionV1::new(execution,source,"offline-host-monotone-closing-received-high-watermark-v1".into());
'''+host[b:]
    a=host.index('struct ReceivedTimes {');b=host.index('fn retry',a);host=host[:a]+host[b:]
    host=host.replace('HiddenOp, InputRole, NativeInputRoot','NativeInputRoot').replace('    collections::{BTreeMap, BTreeSet},\n','')
    (native / 'examples/frame_latency.rs').write_text(host)
    (native / 'examples/common/frame_online.rs').write_text((out/'model-train/validation/issue-180/online.rs').read_text())
    manifest.write_text(manifest.read_text().replace('[dev-dependencies]\n','[dev-dependencies]\ndata-collect = { path = "../../data-collect" }\n'))
    text=lock.read_text();a=text.index('name = "model-native-inputs"');b=text.index('[[package]]',a)
    lock.write_text(text[:a]+text[a:b].replace(' "data-pipeline-core",',' "data-collect",\n "data-pipeline-core",')+text[b:])
    online=out/'data-pipeline/training/src/public_online.rs';t=online.read_text()
    t=t.replace('    materials: PublicOnlineMaterialsV1,','    materials: PublicOnlineMaterialsV1,\n    bench_reserve_ns:u128,',1)
    t=t.replace('let (permit, frame) = tokio::select!', 'let (permit, frame, bench_reserve_ns) = tokio::select!',1)
    t=t.replace('                let permit = self.background.reserve().await?;', '                let began=std::time::Instant::now();\n                let permit = self.background.reserve().await?;\n                let reserve_ns=began.elapsed().as_nanos();',1)
    t=t.replace('((permit, frame))','((permit, frame, reserve_ns))',1)
    t=t.replace('            materials: PublicOnlineMaterialsV1 {','            bench_reserve_ns,\n            materials: PublicOnlineMaterialsV1 {',1)
    t=t.replace('impl<\'a, S: AsyncRead + AsyncWrite + Unpin> PublicOnlinePendingV1<\'a, S> {','impl<\'a, S: AsyncRead + AsyncWrite + Unpin> PublicOnlinePendingV1<\'a, S> {\n    pub fn bench_resources(&self)->serde_json::Value {serde_json::json!({"reserve_wait_ns":self.bench_reserve_ns,"recording_inflight":self.session.config.recording_in_flight_inputs.get()-self.session.background.capacity.available_permits()})}',1)
    online.write_text(t)
    if args.support_failure:
        module('capture180',out/'model-train/validation/issue-180/capture.py').instrument(out)
    # 每个变化都是诊断副本差异；冻结精确原文件及补丁。
    pristine=out.parent/(out.name+'-pristine')
    pristine.mkdir()
    with tarfile.open(fileobj=io.BytesIO(subprocess.check_output(['git','archive',BASELINE],cwd=source))) as tar:tar.extractall(pristine,filter='data')
    patch=subprocess.run(['diff','-ruN',str(pristine),str(out)],capture_output=True)
    if patch.returncode not in (0,1):raise RuntimeError(patch.stderr.decode())
    (out/'diagnostic.patch').write_bytes(patch.stdout.replace(str(pristine).encode(),b'a').replace(str(out).encode(),b'b'))
    shutil.rmtree(pristine)

    identities = {str(p.relative_to(out)): hashlib.sha256(p.read_bytes()).hexdigest()
                  for folder in (native, out / 'shared-run', out / 'ledger', out/'data-pipeline/training')
                  for p in folder.rglob('*') if p.is_file() and p.suffix in ('.rs', '.toml', '.lock')}
    (out / 'source-identities.json').write_text(json.dumps({'baseline': BASELINE, 'variant': args.variant,
                                                         'files': identities}, indent=2) + '\n')
    print(out)


if __name__ == '__main__':
    main()
