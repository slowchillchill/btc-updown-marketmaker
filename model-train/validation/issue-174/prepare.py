"""从本轮准确 main 建立独立基线/候选；只在离线副本开放入口恢复和共同状态对照。"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tomllib

BASELINE = '43765885dedd2593abdbdde868da3d64ed6fb646'
DEPENDENCIES = ('ledger', 'shared-run', 'execution-normalization', 'sim-system')


def market_helpers(source, candidate):
    restore_side = ('SidePrevious { total, best, inputs:None }' if candidate else
                    'SidePrevious {total,best,logs:serde_json::from_value::<Vec<(R,u32)>>(v[2].clone()).unwrap().into_iter().map(|(k,b)|(k,f32::from_bits(b))).collect()}')
    source += '''
impl MarketHistory {
    pub(crate) fn bench_business(&self)->serde_json::Value {
        serde_json::json!({"sides":self.sides.iter().map(|s|(&s.total,&s.best)).collect::<Vec<_>>(),
            "series":self.series.iter().map(|q|q.iter().map(|s|(&s.value,&s.identity,s.at)).collect::<Vec<_>>()).collect::<Vec<_>>(),
            "features":self.history_features.map(|r|r.map(f32::to_bits))})
    }
    pub(crate) fn restore(v:&serde_json::Value)->Self {
        let sides = v["sides"].as_array().unwrap().iter().map(|v| {
            let total=serde_json::from_value(v[0].clone()).unwrap(); let best=serde_json::from_value(v[1].clone()).unwrap();
            RESTORE_SIDE
        }).collect::<Vec<_>>().try_into().ok().unwrap();
        let series:Vec<Vec<(R,Vec<DirectObservationAnchorV1>,i64)>>=serde_json::from_value(v["series"].clone()).unwrap();
        let features:[[u32;4];6]=serde_json::from_value(v["features"].clone()).unwrap();
        Self{sides,series:series.into_iter().map(|q|q.into_iter().map(|(value,identity,at)|Sample{value,identity,at}).collect()).collect::<Vec<_>>().try_into().ok().unwrap(),history_features:features.map(|v|v.map(f32::from_bits))}
    }
}
'''.replace('RESTORE_SIDE', restore_side)
    if candidate:
        source += '''
impl MarketHistory {
    pub(crate) fn clear_added_inputs(&mut self) {for side in &mut self.sides {side.inputs=None;}}
    pub(crate) fn bench_cache(&self)->serde_json::Value {
        serde_json::json!(self.sides.iter().map(|s|s.inputs.as_ref().map(|v|serde_json::json!({
            "parsed_rows":v.levels.len(),"parsed_capacity":v.levels.capacity(),"parsed_struct_bytes":v.levels.capacity()*std::mem::size_of::<ParsedLevel>(),
            "text_capacity_bytes":v.prefix.capacity()+v.levels.iter().map(|p|p.price_text.capacity()+p.quantity_text.capacity()+p.path.capacity()).sum::<usize>(),
            "rational_value_bytes":v.levels.iter().flat_map(|p|[&p.price,&p.quantity]).map(|n|(n.numerator().magnitude().bits()+7)/8+(n.denominator().magnitude().bits()+7)/8).sum::<u64>(),
            "projected_capacity":v.book.capacity(),"projected_storage_bytes":v.book.capacity()*std::mem::size_of::<[f32;9]>(),"current_side_strong_refs":std::sync::Arc::strong_count(&v.book)
        }))).collect::<Vec<_>>())
    }
}
'''
    else:
        source += '''impl MarketHistory { pub(crate) fn bench_cache(&self)->serde_json::Value {serde_json::json!(self.sides.iter().map(|s|s.logs.len()).collect::<Vec<_>>())} }'''
    return source


def root_helpers(source, market, candidate):
    if market == 'market_baseline':
        # 未变化的账户/撤单编码器要求原市场元数据类型；不转换特征或候选缓存。
        source = source.replace('pub(crate) struct MarketBinding {\n    pub market: MarketIdentityV2,\n    pub assets: Vec<AssetIdentityV1>,\n}', 'use crate::root::MarketBinding;')
    source = source.replace('pub(crate) struct MarketBinding {', '#[derive(Clone)]\npub(crate) struct MarketBinding {')
    source = source.replace('pub struct NativeInputRoot {', '#[derive(Clone)]\npub struct NativeInputRoot {')
    source = source.replace('crate::market::', f'crate::{market}::')
    source += '''
impl NativeInputRoot {
    pub fn restore_checkpoint(&mut self,v:&serde_json::Value) {
        assert_eq!(v["trajectory"],self.trajectory);assert_eq!(v["account"],serde_json::to_value(&self.account).unwrap());assert_eq!(v["started_at"],self.started_at);
        self.reset=v["reset"].as_bool().unwrap();self.hidden=serde_json::from_value(v["hidden"].clone()).unwrap();
        let markets:BTreeMap<String,(MarketIdentityV2,Vec<AssetIdentityV1>)>=serde_json::from_value(v["markets"].clone()).unwrap();
        self.markets=markets.into_iter().map(|(k,(market,assets))|(k,MarketBinding{market,assets})).collect();
        self.histories=v["histories"].as_object().unwrap().iter().map(|(k,v)|(k.clone(),crate::MARKET::MarketHistory::restore(v))).collect();
        self.rule_tail=self.histories.keys().next().map(|k|crate::MARKET::static_rules(&self.snapshot.public_rule_context(self.markets[k].market.clone())));
    }
    pub fn bench_business(&self)->serde_json::Value {serde_json::json!({"trajectory":self.trajectory,"account":self.account,"started_at":self.started_at,"reset":self.reset,
        "rule_tail":self.rule_tail.map(|v|v.map(f32::to_bits)),"markets":self.markets.iter().map(|(k,v)|(k,(&v.market,&v.assets))).collect::<BTreeMap<_,_>>(),
        "hidden":self.hidden,"histories":self.histories.iter().map(|(k,h)|(k,h.bench_business())).collect::<BTreeMap<_,_>>()})}
    pub fn bench_cache(&self)->serde_json::Value {serde_json::json!(self.histories.iter().map(|(k,h)|(k,h.bench_cache())).collect::<BTreeMap<_,_>>())}
}
impl NativeInputGroup {
    pub fn bench_f32_bits(&self)->Vec<u32> {
        self.rows.iter().flat_map(|r|r.fast.iter().chain(&r.slow).chain(&r.rule).chain(r.book_levels.iter().flat_map(|s|s.iter().flatten())))
            .chain(self.cancel.iter().flatten()).map(|v|v.to_bits()).collect()
    }
}
'''.replace('MARKET', market)
    if candidate:
        source += '''impl NativeInputRoot {
            pub fn baseline(&self)->crate::root_baseline::NativeInputRoot {crate::root_baseline::NativeInputRoot::new(self.trajectory.clone(),self.account.clone(),self.started_at,self.snapshot.clone())}
            pub fn clear_added_inputs(&mut self) {for history in self.histories.values_mut(){history.clear_added_inputs();}}
        }'''
    return source


def host(source, tail, frame):
    start = source.index('// 仅显式诊断启用：')
    end = source.index('#[derive(Deserialize)]', start)
    source = source[:start] + source[end:]
    start = source.index('    let mut writer = BufWriter::new(File::create(out.join("groups.jsonl"))?);')
    end = source.index('\nfn main()', start)
    source = source[:start] + tail + '\n' + source[end:]
    source = source.replace('    stop_role: &str,\n    limit: u64,', '    _stop_role: &str,\n    _limit: u64,')
    if frame:
        source = source.replace('use contracts::', '#[path="common/frame_runtime.rs"] mod frame_runtime;\nuse contracts::', 1)
    source += '''
fn process_cpu_ns()->u128 {let mut v=libc::timespec{tv_sec:0,tv_nsec:0};assert_eq!(unsafe{libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID,&mut v)},0);v.tv_sec as u128*1_000_000_000+v.tv_nsec as u128}
'''
    return source


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source, output = args.source.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    project = source/'model-train/validation/issue-174'
    # 旧恢复依赖的真实业务源在本轮基线上未变；复用的是离线入口恢复，不是旧原生基线。
    subprocess.run(['git','diff','--exit-code','e57503821391a9943ce694eabab00ec0fdff1141',BASELINE,'--',*DEPENDENCIES],cwd=source,check=True,stdout=subprocess.DEVNULL)
    archive = subprocess.check_output(['git','archive',BASELINE],cwd=source)
    baseline_market = subprocess.check_output(['git','show',BASELINE+':model-train/native-inputs/src/market.rs'],cwd=source,text=True)
    baseline_root = subprocess.check_output(['git','show',BASELINE+':model-train/native-inputs/src/root.rs'],cwd=source,text=True)
    identities = {'baseline':BASELINE,'variants':{}}
    for variant in ('baseline','candidate'):
        destination = output/f'{variant}-harness'
        destination.mkdir()
        with tarfile.open(fileobj=io.BytesIO(archive)) as tar: tar.extractall(destination,filter='data')
        with tarfile.open(source/'model-train/validation/issue-172/evidence/source/formal-overlay.tar.gz') as tar:
            for member in tar.getmembers():
                if member.name.split('/')[0] in DEPENDENCIES: tar.extract(member,destination,filter='data')
        native = destination/'model-train/native-inputs'
        if variant == 'candidate': shutil.copytree(source/'model-train/native-inputs',native,dirs_exist_ok=True,ignore=shutil.ignore_patterns('target'))
        manifest = native/'Cargo.toml'; lock= native/'Cargo.lock'
        version=next(p['version'] for p in tomllib.loads(lock.read_text())['package'] if p['name']=='libc')
        manifest.write_text(manifest.read_text().replace('[dev-dependencies]\n','[dev-dependencies]\nlibc = "='+version+'"\n'))
        locked=lock.read_text(); start=locked.index('name = "model-native-inputs"'); end=locked.index('[[package]]',start)
        lock.write_text(locked[:start]+locked[start:end].replace(' "ledger",\n',' "ledger",\n "libc",\n')+locked[end:])
        src=native/'src'
        (src/'market.rs').write_text(market_helpers((src/'market.rs').read_text(),variant=='candidate'))
        (src/'root.rs').write_text(root_helpers((src/'root.rs').read_text(),'market',variant=='candidate'))
        if variant == 'candidate':
            (src/'market_baseline.rs').write_text(market_helpers(baseline_market,False))
            (src/'root_baseline.rs').write_text(root_helpers(baseline_root,'market_baseline',False))
            with (src/'lib.rs').open('a') as f:f.write('\npub mod market_baseline;pub mod root_baseline;\n')
            with (src/'market_tests.rs').open('a') as f:f.write('''
#[test]
fn incremental_dependencies_match_independent_latest_main() {
    let mut candidate=crate::market::MarketHistory::default();
    let mut baseline=crate::market_baseline::MarketHistory::default();
    let mut cleared=crate::market::MarketHistory::default();
    for (i,o) in incremental_dependency_observations().into_iter().enumerate() {
        let now=33000+i as i64;
        let (a,books)=crate::market::encode(&o,&[0.;30],now,&mut candidate);
        let (b,reference)=crate::market_baseline::encode(&o,&[0.;30],now,&mut baseline);
        let bits=|v:&[f32]|v.iter().map(|f|f.to_bits()).collect::<Vec<_>>();
        assert_eq!(bits(&a),bits(&b),"fast {i}");
        for (a,b) in books.iter().zip(&reference) {assert_eq!(a.iter().flatten().map(|v|v.to_bits()).collect::<Vec<_>>(),b.iter().flatten().map(|v|v.to_bits()).collect::<Vec<_>>(),"book {i}");}
        assert_eq!(candidate.bench_business(),baseline.bench_business(),"business {i}");
        cleared.clear_added_inputs();
        let (c,book_c)=crate::market::encode(&o,&[0.;30],now,&mut cleared);
        assert_eq!(bits(&a),bits(&c));
        assert_eq!(serde_json::to_value(books).unwrap(),serde_json::to_value(book_c).unwrap());
        assert_eq!(candidate.bench_business(),cleared.bench_business());
    }
}
''')
            (native/'examples/native_incremental.rs').write_text(host((native/'examples/replay_projection.rs').read_text(),(project/'native.rs').read_text(),False))
        (native/'examples/frame_latency.rs').write_text(host((native/'examples/replay_projection.rs').read_text(),(project/'frame.rs').read_text().replace('VARIANT_NAME',variant),True))
        runtime=(project/('runtime.rs' if variant=='candidate' else '../issue-172/runtime.rs')).read_text()
        (native/'examples/common').mkdir(exist_ok=True)
        (native/'examples/common/frame_runtime.rs').write_text(runtime)
        identities['variants'][variant]={str(p.relative_to(destination)):hashlib.sha256(p.read_bytes()).hexdigest()
            for folder in (native,*(destination/d for d in DEPENDENCIES)) for p in sorted(folder.rglob('*')) if p.is_file() and p.suffix in ('.rs','.toml','.lock')}
    (output/'source-identities.json').write_text(json.dumps(identities,ensure_ascii=False,indent=2)+'\n')
    print(output)


if __name__ == '__main__': main()
