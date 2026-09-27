from pathlib import Path
import shutil,subprocess,tomllib
import argparse
parser=argparse.ArgumentParser(description="#170 专用离线基准副本；完整根前态转换不进入生产接口")
parser.add_argument('--source',type=Path,default=Path(__file__).resolve().parents[2])
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--baseline',default='040c7cb00b98b4897f077f8f9dc4ad6156bcc4b6')
args=parser.parse_args();w=args.source.resolve();out=args.output.resolve();dst=out/'native-harness'
if dst.exists():raise FileExistsError(dst)
out.mkdir(parents=True,exist_ok=True)
variants=out/'variants';variants.mkdir()
base=subprocess.check_output(['git','show',args.baseline+':model-train/native-inputs/src/market.rs'],cwd=w,text=True)
variants.joinpath('baseline.rs').write_text(base)
patches=w/'model-train/validation/issue-170'
for name,steps in [('cache',['cache']),('projection',['projection']),('combined',['cache','projection']),('history',['cache','projection','history'])]:
    destination=variants/(name+'.rs');destination.write_text(base)
    for step in steps:subprocess.run(['patch','--fuzz=0',str(destination),str(patches/(step+'.patch'))],check=True)

dst.mkdir(exist_ok=True)
for item in w.iterdir():
 if item.name not in ('target','.git','model-train','wallets') and not (dst/item.name).exists(): (dst/item.name).symlink_to(item)
(dst/'model-train').mkdir(exist_ok=True)
shutil.copytree(w/'model-train/native-inputs',dst/'model-train/native-inputs',ignore=shutil.ignore_patterns('target'),dirs_exist_ok=True)
# 仅基准副本使用已锁定的 libc 读取全进程 CPU 时钟，生产依赖和接口不变。
manifest=dst/'model-train/native-inputs/Cargo.toml';lock=manifest.with_name('Cargo.lock')
version=next(p['version'] for p in tomllib.loads(lock.read_text())['package'] if p['name']=='libc')
manifest.write_text(manifest.read_text().replace('[dev-dependencies]\n','[dev-dependencies]\nlibc = "='+version+'"\n'))
locked=lock.read_text();start=locked.index('name = "model-native-inputs"');end=locked.index('[[package]]',start)
package=locked[start:end].replace(' "ledger",\n',' "ledger",\n "libc",\n')
lock.write_text(locked[:start]+package+locked[end:])
src=dst/'model-train/native-inputs/src';root=(src/'root.rs').read_text();root=root.replace('pub(crate) struct MarketBinding {','#[derive(Clone)]\npub(crate) struct MarketBinding {');(src/'root.rs').write_text(root)
base=(out/'variants/baseline.rs').read_text()
for name in ('baseline','cache','projection','combined','history'):
 s=(out/f'variants/{name}.rs').read_text()
 # 私有基准副本开放内部字段供完整调用前状态转换；生产没有快照接口。
 for f in ('total:','best:','logs:','value:','identity:','at:','sides:','history_features:'):
  s=s.replace('\n    '+f,'\n    pub(crate) '+f)
 if name!='baseline':
  s+='''\nimpl MarketHistory {
    pub(crate) fn from_baseline(h: &crate::market_baseline::MarketHistory) -> Self {
        Self {
            sides: std::array::from_fn(|i| SidePrevious { total: h.sides[i].total.clone(), best: h.sides[i].best.clone(), logs: h.sides[i].logs.clone() }),
            series: std::array::from_fn(|i| h.series[i].iter().map(|s| Sample {value:s.value.clone(),identity:s.identity.clone(),at:s.at}).collect()),
'''
  if name=='history': s+='''            history_features: std::array::from_fn(|i| { let mut features=[0.;4]; let mut q=h.series[i].clone(); crate::market_baseline::history(&mut features,&mut q,None,0); features }),
'''
  s+='''        }
    }
}
'''
 s=s.replace('pub(crate) value: Option<(R, Vec<DirectObservationAnchorV1>)>', 'value: Option<(R, Vec<DirectObservationAnchorV1>)>')
 s=s.replace('\nfn history(', '\npub(crate) fn history(')
 s+='''\nimpl MarketHistory {
    pub(crate) fn assert_matches(&self,h:&crate::market_baseline::MarketHistory) {
        for i in 0..6 {
            assert_eq!(self.sides[i].total,h.sides[i].total); assert_eq!(self.sides[i].best,h.sides[i].best);
            let a:Vec<_>=self.sides[i].logs.iter().map(|(k,v)|(k,v.to_bits())).collect();
            let b:Vec<_>=h.sides[i].logs.iter().map(|(k,v)|(k,v.to_bits())).collect(); assert_eq!(a,b);
            assert_eq!(self.series[i].len(),h.series[i].len());
            for (a,b) in self.series[i].iter().zip(&h.series[i]) {
                assert_eq!(a.value,b.value);assert_eq!(a.identity,b.identity);assert_eq!(a.at,b.at);
            }
'''
 if name=='history':s+='''            let mut q=h.series[i].clone();let mut v=[0.;4];crate::market_baseline::history(&mut v,&mut q,None,0);
            assert_eq!(self.history_features[i].map(f32::to_bits),v.map(f32::to_bits));
'''
 s+='''        }
    }
}
'''
 (src/f'market_{name}.rs').write_text(s)
 r=root;begin=r.index('#[derive(Clone)]\npub(crate) struct MarketBinding');end=r.index('pub struct NativeInputRoot',begin)
 r=r[:begin]+'use crate::root::MarketBinding;\n#[derive(Clone)]\n'+r[end:]
 r=r.replace('crate::market::','crate::market_'+name+'::')
 for f in ('snapshot:', 'trajectory:', 'account:', 'started_at:', 'reset:', 'markets:', 'hidden:', 'histories:'):
  r=r.replace('\n    '+f,'\n    pub(crate) '+f)
 if name!='baseline':
  r+='''\nimpl NativeInputRoot {
    pub fn from_baseline(r: &crate::root_baseline::NativeInputRoot) -> Self {
        Self { snapshot:r.snapshot.clone(), trajectory:r.trajectory.clone(), account:r.account.clone(), started_at:r.started_at, reset:r.reset, markets:r.markets.clone(), hidden:r.hidden.clone(), histories:r.histories.iter().map(|(k,h)| (k.clone(),crate::market_'''+name+'''::MarketHistory::from_baseline(h))).collect() }
    }
}
'''
 # 所有副本复用公开组类型，以便调用同一逐位比较器。
 begin=r.index('#[derive(Clone, Debug, PartialEq, Eq, Serialize)]');end=r.index('use crate::root::MarketBinding;')
 r=r[:begin]+'use crate::root::{HiddenKey,HiddenOp,InputRole,AccountInputRow,NativeActionContext,NativeInputGroup};\n'+r[end:]
 r=r.replace('let (fast, book) = crate::market_'+name+'::encode(', 'let history = self.histories.entry(id.0.clone()).or_default();\n            let (fast, book) = crate::bench_pool::get().install(|| crate::market_'+name+'::encode(')
 r=r.replace('self.histories.entry(id.0.clone()).or_default(),\n            );','history,\n            ));')
 r+='''\nimpl NativeInputRoot { pub fn assert_matches(&self,r:&crate::root_baseline::NativeInputRoot) {
    assert_eq!(self.reset,r.reset); assert_eq!(self.hidden,r.hidden); assert_eq!(self.histories.len(),r.histories.len());
    for (k,h) in &self.histories {h.assert_matches(&r.histories[k]);}
} }
'''
 (src/f'root_{name}.rs').write_text(r)
lib=(src/'lib.rs').read_text();lib+='\n'+''.join(f'mod market_{n}; pub mod root_{n};\n' for n in ('baseline','cache','projection','combined','history'));(src/'lib.rs').write_text(lib)
print(dst)

(src/'bench_pool.rs').write_text("""use std::sync::{OnceLock,atomic::{AtomicUsize,Ordering}};
static POOLS:OnceLock<Vec<rayon::ThreadPool>>=OnceLock::new();
static INDEX:AtomicUsize=AtomicUsize::new(2);
pub fn set(index:usize) { INDEX.store(index,Ordering::Relaxed); let _=get(); }
pub fn get()-> &'static rayon::ThreadPool { &POOLS.get_or_init(|| [1,4,8,16].into_iter().map(|n| rayon::ThreadPoolBuilder::new().num_threads(n).build().unwrap()).collect())[INDEX.load(Ordering::Relaxed)] }
""")
with (src/'lib.rs').open('a') as f:f.write('pub mod bench_pool;\n')
shared=dst/'shared-run'
if shared.is_symlink():
 shared.unlink();shutil.copytree(w/'shared-run',shared,ignore=shutil.ignore_patterns('target'))
p=shared/'src/group.rs';s=p.read_text()
if 'BENCH_CLOSED' not in s:
 s=s.replace('        for record in self.observation.take_closed_records() {','        for record in self.observation.take_closed_records() {\n            BENCH_CLOSED.fetch_add(1,std::sync::atomic::Ordering::Relaxed);')
 s+='\nstatic BENCH_CLOSED:std::sync::atomic::AtomicUsize=std::sync::atomic::AtomicUsize::new(0);\npub fn bench_closed_count()->usize { BENCH_CLOSED.load(std::sync::atomic::Ordering::Relaxed) }\n';p.write_text(s)
p=shared/'src/registry.rs';s=p.read_text()
if 'BENCH_MARKET' not in s:
 s=s.replace('.or_insert_with(|| rules.clone());','.or_insert_with(|| { BENCH_MARKET.fetch_add(1,std::sync::atomic::Ordering::Relaxed);rules.clone() });')
 s+='\nstatic BENCH_MARKET:std::sync::atomic::AtomicUsize=std::sync::atomic::AtomicUsize::new(0);\npub fn bench_market_count()->usize { BENCH_MARKET.load(std::sync::atomic::Ordering::Relaxed) }\n';p.write_text(s)
p=shared/'src/lib.rs';s=p.read_text()
if 'bench_closed_count' not in s:
 s+='\npub use group::bench_closed_count;\npub use registry::bench_market_count;\n';p.write_text(s)

subprocess.run(['patch','--fuzz=0',str(dst/'model-train/native-inputs/examples/replay_projection.rs'),str(patches/'benchmark-example.patch')],check=True)
(dst/'model-train/native-inputs/examples/replay_projection.rs').rename(dst/'model-train/native-inputs/examples/benchmark_projection.rs')


# 仅离线副本：恢复完整消费前内存状态，保留原记录写入器/回调，不把应用工作移出计时。
for directory in ('ledger','execution-normalization','sim-system'):
 target=dst/directory
 if target.is_symlink():
  target.unlink();shutil.copytree(w/directory,target,ignore=shutil.ignore_patterns('target'))
p=dst/'execution-normalization/src/consumption.rs';s=p.read_text().replace('#[derive(Debug)]\npub struct NoeConsumptionSource', '#[derive(Clone, Debug)]\npub struct NoeConsumptionSource');p.write_text(s)
p=dst/'sim-system/src/run_v2/noe_projection/memory.rs';s=p.read_text()
if 'bench_fork' not in s:
 s=s.replace('struct ObservationState<','#[derive(Clone)]\nstruct ObservationState<').replace('struct PendingFeedback<','#[derive(Clone)]\nstruct PendingFeedback<')
 s+='''
impl<P: MemoryOrderEvidence> MemoryNoeObservation<P> {
    pub fn bench_fork(&self)->Self {Self(Arc::new(Mutex::new(self.0.lock().unwrap().clone())))}
    pub fn bench_restore_consumer(&self,source:&Self) {
        let source=source.0.lock().unwrap();let mut state=self.0.lock().unwrap();
        state.consumption=source.consumption.clone();state.feedback=source.feedback.clone();state.delivered=source.delivered;
    }
}
''';p.write_text(s)
for file,typename in [('funds.rs','FundsStateV1'),('economics.rs','EconomicsState')]:
 p=dst/'ledger/src'/file;s=p.read_text().replace('#[derive(Default)]\npub'+('(super)' if typename=='FundsStateV1' else '(crate)')+' struct '+typename, '#[derive(Clone, Default)]\npub'+('(super)' if typename=='FundsStateV1' else '(crate)')+' struct '+typename);p.write_text(s)
p=dst/'ledger/src/core.rs';s=p.read_text()
if 'bench_snapshot' not in s:
 s=s.replace('enum AdmissionPolicy {','#[derive(Clone)]\nenum AdmissionPolicy {')
 fields=s.split('pub struct LedgerWriterV1 {',1)[1].split('\n}',1)[0]
 names=[line.strip().split(':')[0] for line in fields.splitlines() if ':' in line]
 assignments=',\n'.join(name+':self.'+name+'.clone()' for name in names if name not in ('durable','record_sink'))
 s+='\nimpl LedgerWriterV1 {\n    pub fn bench_snapshot(&self)->Self {Self { '+assignments+',durable:None,record_sink:None }}\n    pub fn bench_restore(&mut self,source:&Self) { let durable=self.durable.take();let sink=self.record_sink.take();*self=source.bench_snapshot();self.durable=durable;self.record_sink=sink; }\n}\n';p.write_text(s)
p=shared/'src/group.rs';s=p.read_text()
if 'pub fn bench_snapshot' not in s:
 s=s.replace('    pub fn action_before(', '''    pub fn bench_snapshot(&self)->Self {Self {ledger:self.ledger.bench_snapshot(),registry:self.registry.clone(),observation:self.observation.bench_fork(),pending:self.pending.clone()}}
    pub fn bench_restore(&mut self,source:&Self) {self.ledger.bench_restore(&source.ledger);self.registry=source.registry.clone();self.observation.bench_restore_consumer(&source.observation);self.pending=source.pending.clone();}
    pub fn action_before(''');p.write_text(s)

p=src/'root_baseline.rs';s=p.read_text()
s+='''
impl NativeInputRoot {
    pub fn bench_changes(&self,before:&Self)->(usize,usize,usize) {
        let mut updated=0;let mut inputs=0;let mut retained=0;
        for (id,h) in &self.histories {
            let prior=before.histories.get(id);
            for i in 0..6 {
                let key=|q:&std::collections::VecDeque<crate::market_baseline::Sample>|q.back().map(|s|(s.identity.clone(),s.at));
                if key(&h.series[i])!=prior.and_then(|p|key(&p.series[i])) {updated+=1;}
                inputs+=h.sides[i].logs.len();
                if let Some(prior)=prior {retained+=h.sides[i].logs.keys().filter(|k|prior.sides[i].logs.contains_key(*k)).count();}
            }
        }
        (updated,inputs,retained)
    }
}
''';p.write_text(s)
