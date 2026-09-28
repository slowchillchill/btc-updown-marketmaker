"""建立 #172 离线副本；复用 #170 的无交易恢复夹具，生产不增加快照或诊断接口。"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('--source', type=Path, required=True)
p.add_argument('--output', type=Path, required=True)
a = p.parse_args()
w, out = a.source.resolve(), a.output.resolve()
dst = out / 'native-harness'
if not dst.exists():
    subprocess.run([sys.executable, str(w/'model-train/model_train/prepare_native_benchmark.py'),
                    '--source', str(w), '--output', str(out)], check=True)
src = dst/'model-train/native-inputs/src'
base = subprocess.check_output(['git', 'show', 'e57503821391a9943ce694eabab00ec0fdff1141:model-train/native-inputs/src/market.rs'], cwd=w, text=True)
root = subprocess.check_output(['git','show','e57503821391a9943ce694eabab00ec0fdff1141:model-train/native-inputs/src/root.rs'],cwd=w,text=True)
(src/'market_tests.rs').write_text(subprocess.check_output(['git','show','e57503821391a9943ce694eabab00ec0fdff1141:model-train/native-inputs/src/market_tests.rs'],cwd=w,text=True))
# 基线固定在获批起点；候选为同一文件的有界差异，不复用 #170 的旧版本性能。
variants = {name: base for name in ('baseline', 'cache', 'projection', 'combined', 'history', 'buffer', 'diagnostic')}
for name in variants:
    candidate = w/f'model-train/validation/issue-172/{name}.patch'
    if candidate.exists():
        path = out/f'variants/{name}.rs'
        path.write_text(base)
        subprocess.run(['patch', '--fuzz=0', str(path), str(candidate)], check=True)
        variants[name] = path.read_text()
# 诊断副本才读取时钟和累积计数。所有正式版本不带这些语句。
def wrap_function(s, signature, label):
    start = s.index(signature)
    body = s.index('{', start)
    return s[:body+1] + f'\n    let _diagnostic = crate::diagnostic::Scope::new("{label}");' + s[body+1:]
exact = (w/'model-train/native-inputs/src/exact.rs').read_text()
for signature, label in [('pub(crate) fn decimal(', 'parse_decimal'), ('pub fn signed_log_f32(', 'exact_log'), ('pub(crate) fn sqrt_log_f32(', 'exact_sqrt_log')]:
    exact = wrap_function(exact, signature, label)
exact=exact.replace('Scope::new("exact_log");','Scope::new("exact_log");crate::diagnostic::log_input(value);')
(src/'exact_diagnostic.rs').write_text(exact)
s = variants['diagnostic'].replace('crate::exact::', 'crate::exact_diagnostic::')
for signature, label in [('pub(crate) fn side(', 'side_inclusive'), ('pub(crate) fn rules(', 'rules'), ('fn history(', 'history'), ('pub(crate) fn encode(', 'market_inclusive'), ('pub(crate) fn t5(', 't5'), ('fn reference(', 'reference'), ('fn lifecycle(', 'lifecycle'), ('fn events(', 'events')]:
    s = wrap_function(s, signature, label)
s = s.replace('let total = sum(levels.iter().map(|(_, q)| q));', 'let total = { let _d=crate::diagnostic::Scope::new("side_total"); sum(levels.iter().map(|(_, q)| q)) };')
s = s.replace('let distances: Vec<_> = levels', 'let distance_start=std::time::Instant::now();\n        let distances: Vec<_> = levels')
s = s.replace('        let start = book.len();', '        crate::diagnostic::elapsed("distance_keys",distance_start);\n        let start = book.len();')
s = s.replace('        book[start..]\n', '        let projection_start=std::time::Instant::now();\n        book[start..]\n')
s = s.replace('                        previous\n                            .logs', '                        let _d=crate::diagnostic::Scope::new("cache_lookup_and_log");\n                        previous\n                            .logs', 1)
s = s.replace('.unwrap_or_else(|| log(value))', '.unwrap_or_else(|| {crate::diagnostic::miss(value);log(value)})')
s = s.replace('        // 重建而非累加历史键', '        crate::diagnostic::elapsed("row_projection_wall",projection_start);\n        let rebuild_start=std::time::Instant::now();\n        // 重建而非累加历史键')
s = s.replace('            .collect();\n    }\n    *previous = next;', '            .collect();\n        crate::diagnostic::elapsed("cache_rebuild",rebuild_start);\n    }\n    *previous = next;')
s=s.replace('let _diagnostic = crate::diagnostic::Scope::new("side_inclusive");','let _diagnostic = crate::diagnostic::Scope::new("side_inclusive");let side_id=crate::diagnostic::begin_side();')
s=s.replace('crate::diagnostic::miss(value)','crate::diagnostic::miss(side_id,value)')
variants['diagnostic'] = s
for name, market in variants.items():
    (out/f'variants/{name}.rs').write_text(market)
    # 仅副本开放字段以准确比较共同状态。
    for field in ('total:', 'best:', 'logs:', 'value:', 'identity:', 'at:', 'sides:', 'history_features:'):
        market = market.replace('\n    '+field, '\n    pub(crate) '+field)
    market = market.replace('pub(crate) value: Option<(R, Vec<DirectObservationAnchorV1>)>', 'value: Option<(R, Vec<DirectObservationAnchorV1>)>')
    market += '''\nimpl MarketHistory {
    pub(crate) fn assert_matches(&self, h:&crate::market_baseline::MarketHistory) {
        for i in 0..6 {
            assert_eq!(self.sides[i].total,h.sides[i].total);assert_eq!(self.sides[i].best,h.sides[i].best);
            assert_eq!(self.sides[i].logs.iter().map(|(k,v)|(k,v.to_bits())).collect::<Vec<_>>(),h.sides[i].logs.iter().map(|(k,v)|(k,v.to_bits())).collect::<Vec<_>>());
            assert_eq!(self.series[i].len(),h.series[i].len());
            for (a,b) in self.series[i].iter().zip(&h.series[i]) {assert_eq!(a.value,b.value);assert_eq!(a.identity,b.identity);assert_eq!(a.at,b.at);}
            assert_eq!(self.history_features[i].map(f32::to_bits),h.history_features[i].map(f32::to_bits));
        }
    }
'''
    if name != 'baseline':
        market += '''pub(crate) fn from_baseline(h:&crate::market_baseline::MarketHistory)->Self {
            Self { sides:std::array::from_fn(|i| SidePrevious {total:h.sides[i].total.clone(),best:h.sides[i].best.clone(),logs:h.sides[i].logs.clone()}),
                series:std::array::from_fn(|i|h.series[i].iter().map(|s| Sample {value:s.value.clone(),identity:s.identity.clone(),at:s.at}).collect()),history_features:h.history_features }
        }
'''
    market += '}\n'
    if 'scratch_distances:' in market:
        market=market.replace('logs:h.sides[i].logs.clone()}', 'logs:h.sides[i].logs.clone(),scratch_distances:Vec::with_capacity(h.sides[i].logs.len()),scratch_times:Vec::with_capacity(h.sides[i].logs.len())}')
    (src/f'market_{name}.rs').write_text(market)
    r = root
    root_patch=w/f'model-train/validation/issue-172/{name}.root.patch'
    if root_patch.exists():
        path=out/f'variants/root_{name}.rs';path.write_text(r)
        subprocess.run(['patch','--fuzz=0',str(path),str(root_patch)],check=True);r=path.read_text()
    begin = r.index('#[derive(Clone, Debug, PartialEq, Eq, Serialize)]')
    end = r.index('pub struct NativeInputRoot')
    r = r[:begin] + 'use crate::root::{HiddenKey,HiddenOp,InputRole,AccountInputRow,NativeActionContext,NativeInputGroup,MarketBinding};\n#[derive(Clone)]\n' + r[end:]
    r = r.replace('crate::market::', f'crate::market_{name}::')
    for field in ('snapshot:', 'trajectory:', 'account:', 'started_at:', 'reset:', 'markets:', 'hidden:', 'histories:'):
        r = r.replace('\n    '+field, '\n    pub(crate) '+field)
    r = r.replace(f'let (fast, book) = crate::market_{name}::encode(', f'let history=self.histories.entry(id.0.clone()).or_default();\n            let (fast, book) = crate::bench_pool::get().install(|| crate::market_{name}::encode(')
    r = r.replace('self.histories.entry(id.0.clone()).or_default(),\n            );','history,\n            ));')
    if name == 'diagnostic':
        r = r.replace('let before = shared.action_before(driver)?;', 'let account_start=std::time::Instant::now();let before=shared.action_before(driver)?;crate::diagnostic::elapsed("account_freeze",account_start);')
        r = r.replace('let slow = crate::account::encode(read, current.as_ref(), &old, &self.markets, now)?;', 'let account_start=std::time::Instant::now();let slow=crate::account::encode(read,current.as_ref(),&old,&self.markets,now)?;crate::diagnostic::elapsed("account_encode",account_start);')
    if name != 'baseline':
        r += '''\nimpl NativeInputRoot {pub fn from_baseline(r:&crate::root_baseline::NativeInputRoot)->Self {Self {snapshot:r.snapshot.clone(),trajectory:r.trajectory.clone(),account:r.account.clone(),started_at:r.started_at,reset:r.reset,markets:r.markets.clone(),hidden:r.hidden.clone(),histories:r.histories.iter().map(|(k,h)|(k.clone(),crate::market_'''+name+'''::MarketHistory::from_baseline(h))).collect()}}}\n'''
    r += '''\nimpl NativeInputRoot {pub fn assert_matches(&self,r:&crate::root_baseline::NativeInputRoot) {
        assert_eq!(self.trajectory,r.trajectory);assert_eq!(self.account,r.account);assert_eq!(self.started_at,r.started_at);
        assert!(std::sync::Arc::ptr_eq(&self.snapshot,&r.snapshot));
        assert_eq!(serde_json::to_value(self.markets.iter().map(|(k,v)|(k,(&v.market,&v.assets))).collect::<BTreeMap<_,_>>()).unwrap(),serde_json::to_value(r.markets.iter().map(|(k,v)|(k,(&v.market,&v.assets))).collect::<BTreeMap<_,_>>()).unwrap());
        assert_eq!(self.reset,r.reset);assert_eq!(self.hidden,r.hidden);assert_eq!(self.histories.len(),r.histories.len());
        for (k,h) in &self.histories {h.assert_matches(&r.histories[k]);}
    }}\n'''
    if name == 'baseline':
        r += '''impl NativeInputRoot {pub fn bench_changes(&self,before:&Self)->(usize,usize,usize) {
            let mut counts=(0,0,0);
            for (id,h) in &self.histories {for i in 0..6 {
                let old=before.histories.get(id);
                let key=|q:&std::collections::VecDeque<crate::market_baseline::Sample>|q.back().map(|s|(s.identity.clone(),s.at));
                counts.0+=usize::from(key(&h.series[i])!=old.and_then(|p|key(&p.series[i])));
                counts.1+=h.sides[i].logs.len();
                if let Some(old)=old {counts.2+=h.sides[i].logs.keys().filter(|k|old.sides[i].logs.contains_key(*k)).count();}
            }}counts
        }}\n'''
    if 'rule_tail:' in r:
        r=r.replace('reset:r.reset,markets:', 'reset:r.reset,rule_tail:r.histories.keys().next().map(|k|crate::market_'+name+'::static_rules(&r.snapshot.public_rule_context(r.markets[k].market.clone()))),markets:')
        r=r.replace('assert_eq!(self.reset,r.reset);', 'if let Some(tail)=self.rule_tail { let k=r.histories.keys().next().unwrap();assert_eq!(tail.map(f32::to_bits),crate::market_'+name+'::static_rules(&r.snapshot.public_rule_context(r.markets[k].market.clone())).map(f32::to_bits));}assert_eq!(self.reset,r.reset);')
    (src/f'root_{name}.rs').write_text(r)
# 原共享组类型保持一致。
(src/'root.rs').write_text(root.replace('pub(crate) struct MarketBinding {','#[derive(Clone)]\npub(crate) struct MarketBinding {'))
(src/'market.rs').write_text(base)
lib = (src/'lib.rs').read_text()
if 'mod market_diagnostic;' not in lib:
    lib += '\nmod market_diagnostic;pub mod root_diagnostic;mod exact_diagnostic;pub mod diagnostic;\n'
if 'mod market_buffer;' not in lib: lib += '\nmod market_buffer;pub mod root_buffer;\n'
(src/'lib.rs').write_text(lib)
(src/'diagnostic.rs').write_text((w/'model-train/validation/issue-172/diagnostic.rs').read_text())
# 重新从已验证的 #170 驱动补丁生成，避免每次准备叠加变化。
example = dst/'model-train/native-inputs/examples/benchmark_projection.rs'
example.write_text((w/'model-train/native-inputs/examples/replay_projection.rs').read_text())
subprocess.run(['patch','--fuzz=0',str(example),str(w/'model-train/validation/issue-170/benchmark-example.patch')],check=True)
s = example.read_text()
s = s.replace('let resource_diagnostic=std::env::var_os("NATIVE_RESOURCE").is_some();', 'let resource_diagnostic=true;\n    let diagnosing=std::env::var_os("NATIVE_DIAGNOSTIC").is_some();\n    let screen=std::env::var_os("NATIVE_SCREEN").is_some();')
s = s.replace('            for round in 0..2 {', '''            if diagnosing {
                let mut diagnostic=model_native_inputs::root_diagnostic::NativeInputRoot::from_baseline(&before);
                shared.bench_restore(shared_before.as_ref().unwrap());
                model_native_inputs::diagnostic::reset();
                let start=Instant::now();let actual=diagnostic.consume(&mut shared,&mut driver).unwrap().unwrap();let elapsed=start.elapsed().as_nanos();
                diagnostic.assert_matches(&native);assert_eq!(serde_json::to_value(actual)?,expected);
                line(&mut benchmark,&serde_json::json!({"position":position,"now":now,"tags":tags,"diagnostic_ns":elapsed,"diagnostic":model_native_inputs::diagnostic::take()}))?;
            }
            for round in 0..if diagnosing {0}else{2} {''')
s = s.replace('                if tags.iter().all(|tag| tag.starts_with("stress")) { settings=vec![("baseline",2),("history",2)]; }', '                if screen {settings=vec![("baseline",2),("cache",2),("projection",2),("combined",2),("buffer",2)];}')
s = s.replace('        if let Some(group) = group {\n', '''        if position%10000==0 {eprintln!("position={position} elapsed_s={:.1}",start.elapsed().as_secs_f64());}
        if let Some(group) = group {
''')
s=s.replace('if resource_diagnostic { settings=vec![', 'if false { settings=vec![')
s=s.replace('"resource_diagnostic":resource_diagnostic', '"process_cpu_clock_enabled":resource_diagnostic')
s=s.replace('("combined",2)];','("combined",2),("buffer",2)];')
s=s.replace('settings=vec![("baseline",2),("history",2)];','settings=vec![("baseline",2),("history",2),("cache",2),("projection",2),("combined",2),("buffer",2)];')
s=s.replace('"cache"=>measure!', '"buffer"=>measure!(model_native_inputs::root_buffer::NativeInputRoot::from_baseline(&before)),\n                        "cache"=>measure!')
for original,label in [('cache','parse_once'),('projection','time_rules'),('combined','miss_dedup'),('history','candidate'),('buffer','scratch_capacity')]:
    s=s.replace('"'+original+'"','"'+label+'"')
example.write_text(s)
(out/'source-identities.json').write_text(json.dumps({'baseline':'e57503821391a9943ce694eabab00ec0fdff1141','variants':{n:hashlib.sha256(v.encode()).hexdigest() for n,v in variants.items()},'generator_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},indent=2)+'\n')
print(dst)
# 下列序列化只供已验证的无交易段入口准备复用；交易中的执行器和账户从不回滚。
p=src/'market_baseline.rs';s=p.read_text();s+='''
impl MarketHistory {
    pub(crate) fn checkpoint(&self)->serde_json::Value {
        serde_json::json!({"sides":self.sides.iter().map(|s|(&s.total,&s.best,s.logs.iter().map(|(k,v)|(k,v.to_bits())).collect::<Vec<_>>())).collect::<Vec<_>>(),
            "series":self.series.iter().map(|q|q.iter().map(|s|(&s.value,&s.identity,s.at)).collect::<Vec<_>>()).collect::<Vec<_>>(),"features":self.history_features.map(|v|v.map(f32::to_bits))})
    }
    pub(crate) fn restore(v:&serde_json::Value)->Self {
        type Side=(Option<R>,Option<R>,Vec<(R,u32)>);
        let sides:Vec<Side>=serde_json::from_value(v["sides"].clone()).unwrap();
        let series:Vec<Vec<(R,Vec<DirectObservationAnchorV1>,i64)>>=serde_json::from_value(v["series"].clone()).unwrap();
        let features:[[u32;4];6]=serde_json::from_value(v["features"].clone()).unwrap();
        Self{sides:sides.into_iter().map(|(total,best,logs)|SidePrevious{total,best,logs:logs.into_iter().map(|(k,v)|(k,f32::from_bits(v))).collect()}).collect::<Vec<_>>().try_into().ok().unwrap(),
            series:series.into_iter().map(|q|q.into_iter().map(|(value,identity,at)|Sample{value,identity,at}).collect()).collect::<Vec<_>>().try_into().ok().unwrap(),history_features:features.map(|v|v.map(f32::from_bits))}
    }
}
''';p.write_text(s)
p=src/'root_baseline.rs';s=p.read_text();s+='''
impl NativeInputRoot {
    pub fn checkpoint(&self)->serde_json::Value {serde_json::json!({"trajectory":self.trajectory,"account":self.account,"started_at":self.started_at,"reset":self.reset,
        "markets":self.markets.iter().map(|(k,v)|(k,(&v.market,&v.assets))).collect::<BTreeMap<_,_>>(),"hidden":self.hidden,"histories":self.histories.iter().map(|(k,h)|(k,h.checkpoint())).collect::<BTreeMap<_,_>>()})}
    pub fn restore_checkpoint(&mut self,v:&serde_json::Value) {
        assert_eq!(v["trajectory"],self.trajectory);assert_eq!(v["account"],serde_json::to_value(&self.account).unwrap());assert_eq!(v["started_at"],self.started_at);
        self.reset=v["reset"].as_bool().unwrap();self.hidden=serde_json::from_value(v["hidden"].clone()).unwrap();
        let markets:BTreeMap<String,(MarketIdentityV2,Vec<AssetIdentityV1>)>=serde_json::from_value(v["markets"].clone()).unwrap();
        self.markets=markets.into_iter().map(|(k,(market,assets))|(k,MarketBinding{market,assets})).collect();
        let histories:BTreeMap<String,serde_json::Value>=serde_json::from_value(v["histories"].clone()).unwrap();
        self.histories=histories.into_iter().map(|(k,v)|(k,crate::market_baseline::MarketHistory::restore(&v))).collect();
    }
}
''';p.write_text(s)
# 独立宿主复用实际录制驱动，既不导入 Python，也不反序列化业务组。
example=dst/'model-train/native-inputs/examples/frame_latency.rs'
s=(w/'model-train/native-inputs/examples/replay_projection.rs').read_text()
s=s.replace('use model_native_inputs::{HiddenOp, InputRole, NativeInputRoot};','use model_native_inputs::{HiddenOp,InputRole};\nuse model_native_inputs::root_baseline::NativeInputRoot;\n#[path="frame_runtime.rs"] mod frame_runtime;')
s=s.replace('    let start = Instant::now();\n    loop {', '''    let mode=std::env::var("FRAME_MODE").unwrap();
    let preparing=mode=="prepare";
    let first=std::env::var("FRAME_FIRST").ok().map(|s|s.parse::<u64>().unwrap()).unwrap_or(0);
    let last=if preparing {418647}else{first+63};
    let checkpoints=PathBuf::from(std::env::var("FRAME_CHECKPOINTS").unwrap());
    let graphs=PathBuf::from(std::env::var("FRAME_GRAPHS").unwrap());
    let names=if preparing {vec![("MAIN-1x256",4),("MAIN-30x1024",16)]}else {let model=std::env::var("FRAME_MODEL").unwrap();if model=="MAIN-1x256" {vec![("MAIN-1x256",4)]}else{vec![("MAIN-30x1024",16)]}};
    let mut runtimes=names.iter().map(|(name,threads)| frame_runtime::Runtime::new(&graphs.join(name),*threads)).collect::<Vec<_>>();
    let executor=tokio::runtime::Builder::new_current_thread().build().unwrap();
    let mut candidate=None;
    let mut restored=None;
    let selection:BTreeMap<u64,Vec<String>>=std::env::var("NATIVE_SELECTION").ok().map(|p|serde_json::from_reader(File::open(p).unwrap()).unwrap()).unwrap_or_default();
    let mut measurements=BufWriter::new(File::create(out.join("measurements.jsonl"))?);
    let start = Instant::now();
    let preparation_cpu=process_cpu_ns();
    loop {''')
start=s.index('        let consume_start = Instant::now();')
end=s.index('        if let Some(group) = group {',start)
s=s[:start]+'''        let position=cutoff.receipt().group().group_position;
        if position>last {break;}
        if position%10000==0 {eprintln!("position={position} elapsed_s={:.1}",start.elapsed().as_secs_f64());}
        if !preparing && position<first {shared.action_before(&mut driver)?;continue;}
        if preparing && [201582,418584].contains(&position) {
            let value=serde_json::json!({"position":position,"native":native.checkpoint(),"ledger":shared.ledger().projection_v2(),"runtimes":runtimes.iter().map(|r|r.checkpoint()).collect::<Vec<_>>(),"elapsed_seconds":start.elapsed().as_secs_f64(),"cpu_ns":process_cpu_ns()-preparation_cpu});
            fs::create_dir_all(&checkpoints)?;json(checkpoints.join(format!("entry-{position}.json")),&value)?;
            // 当前顺序基线直接对照独立反序列化结果，不仅比较自产摘要。
            let loaded:serde_json::Value=serde_json::from_slice(&fs::read(checkpoints.join(format!("entry-{position}.json")))?)?;
            let mut roundtrip=native.clone();roundtrip.restore_checkpoint(&loaded["native"]);roundtrip.assert_matches(&native);restored=Some((roundtrip,position+63));
            for (runtime,v) in runtimes.iter_mut().zip(loaded["runtimes"].as_array().unwrap()) {let original=runtime.checkpoint();runtime.restore(v);assert_eq!(runtime.checkpoint(),original);}
        }
        if !preparing && position==first {
            let loaded:serde_json::Value=serde_json::from_slice(&fs::read(checkpoints.join(format!("entry-{first}.json")))?)?;
            assert_eq!(loaded["position"],position);assert_eq!(loaded["ledger"],serde_json::to_value(shared.ledger().projection_v2())?);
            native.restore_checkpoint(&loaded["native"]);
            runtimes[0].restore(&loaded["runtimes"][if names[0].0=="MAIN-1x256"{0}else{1}]);
            if mode=="candidate" {candidate=Some(model_native_inputs::root_history::NativeInputRoot::from_baseline(&native));}
            line(&mut measurements,&serde_json::json!({"preparation_seconds":start.elapsed().as_secs_f64(),"preparation_cpu_ns":process_cpu_ns()-preparation_cpu,"graph_load_seconds":runtimes[0].load_seconds,"graph_warmup_seconds":runtimes[0].warmup_seconds,"entry_position":position,"restored_prior_model_rows":runtimes[0].frames,"ledger_equal":true,"native_threads":8,"ort_threads":names[0].1}))?;
        }
        let selected=preparing && selection.contains_key(&position);
        let native_before=selected.then(||native.clone());
        let shared_before=(selected || restored.as_ref().is_some_and(|(_,last)|position<=*last)).then(||shared.bench_snapshot());
        let cpu_before=process_cpu_ns();let consume_start=Instant::now();
        let group=if let Some(candidate)=&mut candidate {candidate.consume(&mut shared,&mut driver)}else{native.consume(&mut shared,&mut driver)}.map_err(|e|format!("consume: {e:?}"))?;
        let native_consume_ns=consume_start.elapsed().as_nanos();
        if let Some((roundtrip,last))=&mut restored {
            if position<=*last {
                let ledger=serde_json::to_value(shared.ledger().projection_v2())?;
                shared.bench_restore(shared_before.as_ref().unwrap());
                let actual=roundtrip.consume(&mut shared,&mut driver).unwrap();
                roundtrip.assert_matches(&native);assert_eq!(serde_json::to_value(&actual)?,serde_json::to_value(&group)?);assert_eq!(serde_json::to_value(shared.ledger().projection_v2())?,ledger);
                line(&mut measurements,&serde_json::json!({"roundtrip_sequential_equal":true,"position":position}))?;
            }
        }
        if selected {
            let mut diagnostic=model_native_inputs::root_diagnostic::NativeInputRoot::from_baseline(native_before.as_ref().unwrap());
            let ledger=serde_json::to_value(shared.ledger().projection_v2())?;
            shared.bench_restore(shared_before.as_ref().unwrap());model_native_inputs::diagnostic::reset();
            let cpu=process_cpu_ns();let at=Instant::now();let actual=diagnostic.consume(&mut shared,&mut driver).unwrap();let wall=at.elapsed().as_nanos();let cpu=process_cpu_ns()-cpu;
            diagnostic.assert_matches(&native);assert_eq!(serde_json::to_value(actual)?,serde_json::to_value(&group)?);assert_eq!(serde_json::to_value(shared.ledger().projection_v2())?,ledger);
            line(&mut measurements,&serde_json::json!({"position":position,"tags":selection[&position],"now":cutoff.decision_time().logical_time_utc_ms(),"diagnostic_ns":wall,"diagnostic_cpu_ns":cpu,"diagnostic":model_native_inputs::diagnostic::take()}))?;
        }
        if let Some(group)=group {
            if selected {
                let mut keys=BTreeMap::new();
                for row in &group.rows {
                    let decision=driver.decision().unwrap();let frames=decision.frames();let frame=frames.iter().find(|f|f.observation.market.condition_id==row.hidden_state_key.condition_id).unwrap();
                    if let Some(depth)=&frame.observation.binance_depth.value {
                        keys.insert(row.hidden_state_key.condition_id.0.clone(),[&depth.bids,&depth.asks].map(|side|side.iter().filter(|v|contracts::economics::EconomicRational::from_decimal(rust_decimal::Decimal::from_str_exact(&v.quantity_btc.0).unwrap())>contracts::economics::EconomicRational::zero()).map(|v|v.price_usdt_per_btc.0.clone()).collect::<Vec<_>>()));
                    }
                }
                line(&mut writer,&serde_json::json!({"position":position,"tags":selection[&position],"group":&group,"binance_exact_prices":keys}))?;
            }
            let group=Arc::new(group);
            let mut outputs=Vec::new();
            for runtime in &mut runtimes {
                let (actors,timing,_)=runtime.advance(group.clone(),!preparing);
                let mut actions=Vec::new();
                for actor in actors {
                    let began=Instant::now();
                    driver.wait_for_recording_capacity()?;
                    let receipt=executor.block_on(actor.execute(&mut shared,&mut driver.decision().unwrap(),RequestContextV2{global_request_id:format!("model-{position}"),caller_correlation_id:None})).map_err(|e|format!("execute: {e:?}"))?;
                    actions.push((actor,receipt,began.elapsed().as_nanos()));
                }
                outputs.push((timing,actions));
            }
            let service_ns=consume_start.elapsed().as_nanos();let cpu_after=process_cpu_ns();
            if !preparing {
                let outputs=outputs.into_iter().zip(&runtimes).map(|((timing,actions),runtime)|serde_json::json!({"timing":timing,"hidden_sha256":runtime.hidden_digest(),"actions":actions.into_iter().map(|(actor,receipt,execute_ns)|serde_json::json!({"path":actor.path,"intent":format!("{:?}",actor.intent().unwrap()),"receipt":format!("{receipt:?}"),"execute_ns":execute_ns})).collect::<Vec<_>>()})).collect::<Vec<_>>();
                line(&mut measurements,&serde_json::json!({"position":position,"now":cutoff.decision_time().logical_time_utc_ms(),"service_ns":service_ns,"native_ns":native_consume_ns,"cpu_ns":cpu_after-cpu_before,"native_group":&*group,"runtimes":outputs}))?;
            }
''' + s[end+len('        if let Some(group) = group {'):]
# 不再将全前史或每个大业务组写到旧输出文件；正式组在 measurements 中保留。
s=s.replace('            if group\n                .rows', '            if false && group\n                .rows')
s=s.replace('                line(\n                    &mut meta,', '                if false {line(\n                    &mut meta,')
s=s.replace('"slow_len":row.slow.len()}),\n                )?;', '"slow_len":row.slow.len()}),\n                )?;}')
s=s.replace('            if !group.rows.is_empty()', '            if false && (!group.rows.is_empty()')
s=s.replace('                || !group.post_frame_hidden_ops.is_empty()\n            {', '                || !group.post_frame_hidden_ops.is_empty())\n            {')
s=s.replace('"group":group}', '"group":&*group}')
s=s.replace('    writer.flush()?;', '    measurements.flush()?;\n    writer.flush()?;')
s=s.replace('从录制起点完整恢复公共事实、按原根开始时间消费历史及全部原生输出；显式有界、无交易账户，不是五分钟完整回放或全交易链计时','无交易前史与段入口派生状态准备另列；正式段独立账户/执行器及hidden，包含原生构造、打包、状态与适用自主动作；有限片段，不是在线容量验收')
s=s.replace('\"complete\": true, \"bounded_only\": true,', '\"complete\": true, \"bounded_only\": true, \"mode\":mode, \"models\":names, \"actual_runtime_frames\":runtimes.iter().map(|r|r.frames).collect::<Vec<_>>(),')
s+='''
fn process_cpu_ns()->u128 {let mut v=libc::timespec{tv_sec:0,tv_nsec:0};assert_eq!(unsafe{libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID,&mut v)},0);v.tv_sec as u128*1_000_000_000+v.tv_nsec as u128}
'''
example.write_text(s)
(example.parent/'frame_runtime.rs').write_text((w/'model-train/validation/issue-172/runtime.rs').read_text())
(example.parent/'action_fetch.rs').write_text((w/'model-train/validation/issue-172/action_fetch.rs').read_text())
# 独立实际业务对象夹具使用相同纯 Rust 宿主；没有 Python 嵌入或状态/对象造假。
runtime=(w/'model-train/validation/issue-172/runtime.rs').read_text()
runtime=runtime.replace('use model_native_inputs::','use crate::')
runtime=runtime.replace('model_native_inputs::pad_books','crate::pad_books')
(src/'frame_runtime_probe.rs').write_text(runtime)
(src/'frame_probe.rs').write_text((w/'model-train/validation/issue-172/frame_probe.rs').read_text())
lib=(src/'lib.rs').read_text()
if 'mod frame_probe;' not in lib:
    lib+='\n#[cfg(all(test, feature="ort", not(feature="continuous-validation")))] mod frame_probe;\n'
(src/'lib.rs').write_text(lib)
