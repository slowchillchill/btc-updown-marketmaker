"""真实阶段追加对照事前冻结旧入口；仅观察原候选与共同安装，不驱动经济。"""
from dataclasses import fields,is_dataclass,replace
from pathlib import Path
import importlib.util,json,os,sys,time
import torch
from queues import ExactInt,Numbers
from public_ledger import PublicLedgerState

_compare_artifact=None


def own(value):
    if isinstance(value,torch.Tensor):return value.detach().to('cpu',copy=True)
    if is_dataclass(value):return {f.name:own(getattr(value,f.name)) for f in fields(value)}
    if isinstance(value,(tuple,list)):return [own(v) for v in value]
    if isinstance(value,dict):return {k:own(v) for k,v in value.items()}
    if isinstance(value,frozenset):return sorted(value)
    if value is None or type(value) in (bool,int,float,str):return value
    raise TypeError('追加诊断读面不是基础状态: '+type(value).__name__)


def same(left,right,path='public'):
    if isinstance(left,ExactInt):
        width=max(left.words.size(1),right.words.size(1))
        for name in ('words','sign','fault'):
            assert torch.equal(getattr(left.with_capacity(width),name),getattr(right.with_capacity(width),name)),path+'.'+name
    elif isinstance(left,torch.Tensor):
        if path.endswith('.positions') and left.ndim==right.ndim==2 and left.dtype==right.dtype==torch.int64:
            width=max(left.size(1),right.size(1))
            left=torch.nn.functional.pad(left,(0,width-left.size(1)));right=torch.nn.functional.pad(right,(0,width-right.size(1)))
        matches=torch.equal(left,right)
        if _compare_artifact is not None and (not matches or path.endswith('.lifecycle.fault')):
            row=dict(path=path,left_shape=list(left.shape),right_shape=list(right.shape),
                left=left.cpu().tolist(),right=right.cpu().tolist(),equal=matches,
                differences=None if left.shape!=right.shape else (left!=right).nonzero().cpu().tolist())
            with (_compare_artifact/'comparison-context.jsonl').open('a') as output:output.write(json.dumps(row)+'\n')
        assert matches,path
    elif is_dataclass(left):
        for f in fields(left):same(getattr(left,f.name),getattr(right,f.name),path+'.'+f.name)
    elif isinstance(left,dict):
        assert left.keys()==right.keys(),path
        for k in left:same(left[k],right[k],path+'.'+str(k))
    elif isinstance(left,(tuple,list)):
        assert len(left)==len(right),path
        for i,(a,b) in enumerate(zip(left,right,strict=True)):same(a,b,path+'.'+str(i))
    else:assert left==right,path


def runtime_view(runtime):
    frontier=runtime.hidden_frontier
    return dict(sim={name:own(getattr(runtime,name)) for name in
        ('orders','account','hidden','fills','calendars','market_controls','resolution_obligations')},
        hidden_frontier={name:own(getattr(frontier,name)) for name in
            ('next_bucket','next_step','next_reference','applying','closed_at','has_closed','closed_bucket','index_position','fault')},
        input_sequence=None if runtime.input_frontier is None else own(runtime.input_frontier.next_sequence))


_clocks=[]
_base=None
_legacy=None


def observe(clock,artifact,*,resources=False):
    global _base,_legacy
    artifact=Path(artifact);artifact.mkdir(parents=True,exist_ok=True)
    observations=[];entered={};held=[];probe=[]
    for phase,handler in tuple(clock.handlers.items()):
        def tracked(runtime,projection,choice,handler=handler):
            entered['objects']=(clock.runtime,clock.projection,clock.public)
            entered['view']=(runtime_view(runtime),own(projection),own(clock.public),own(clock.source.position))
            entered['at']=choice.at.cpu().tolist()
            return handler(runtime,projection,choice)
        tracked.__dict__.update(getattr(handler,'__dict__',{}))
        clock.handlers[phase]=tracked
    _clocks.append((clock,artifact,resources,observations,entered,held,probe))
    if _base is not None:return observations
    _base=PublicLedgerState.append_batches
    path=Path(os.environ['B_GPU_PUBLIC_APPEND_BASELINE'])
    spec=importlib.util.spec_from_file_location('public_append_baseline',path)
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    _legacy=module.PublicLedgerState.append_batch
    def append(self,bank,batches):
        global _compare_artifact
        matches=[item for item in _clocks if item[0].public is self and item[4]]
        if not matches:return _base(self,bank,batches)
        c,a,resource,rows,entry,old,precision=matches[0]
        _compare_artifact=a
        for live,saved in old:same(own(live),saved,'prior_output')
        original=own(self)
        assert all(a is b for a,b in zip((c.runtime,c.projection,c.public),entry['objects'],strict=True))
        for live,saved in zip((runtime_view(c.runtime),own(c.projection),own(c.public),own(c.source.position)),entry['view'],strict=True):
            same(live,saved,'uninstalled_stage')
        torch.cuda.synchronize();start=time.perf_counter();actual=_base(self,bank,batches)
        torch.cuda.synchronize();elapsed=time.perf_counter()-start
        expected=self
        for batch in batches:expected=_legacy(expected,bank,batch)
        same(expected,actual);same(own(self),original,'input')
        if resource and batches and not precision:
            # 原精度机制探针；不改变已返回经济候选，不冒称材料自然溢出。
            from transport import select_batch_columns
            zero=select_batch_columns(batches[0],torch.empty((self.ledger.fault.numel(),0),
                dtype=torch.int64,device=batches[0].rows.device))
            zero=replace(zero,fault=torch.ones_like(zero.fault))
            mix=(zero,)+tuple(batches)
            legacy=self
            for batch in mix:legacy=_legacy(legacy,bank,batch)
            same(legacy,_base(self,bank,mix),'zero_fault_mixed')
            precision.append(dict(zero_fault_preserved=True))
        if resource and batches and not entry.get('resource_complete'):
            marker=ExactInt.from_i64_tensor(torch.ones(1,dtype=torch.int64,device=self.ledger.fault.device))
            marker=ExactInt(marker.words.clone(),marker.sign.clone(),marker.fault.clone());marker.words[:,3]=1<<30
            requests=[];record=Numbers.record_resource_request
            def request(required):
                requests.append(required.clone());record(required)
            Numbers.record_resource_request=request
            try:marker*marker
            finally:Numbers.record_resource_request=record
            required=int(torch.stack(requests).max().cpu())
            assert required==(8 if marker.words.size(1)==4 else 0),(marker.words.size(1),required)
            precision.append(dict(width=marker.words.size(1),required=required,
                fits=required<=marker.words.size(1),objects_not_installed=True))
            if marker.words.size(1)>=8:entry['resource_complete']=True
        row=dict(at=entry['at'],natural_batches=len(batches),widths=[int(batch.emitted.sum(1).max().cpu()) for batch in batches],
            new_records=actual.pending_noe.active.size(1)-self.pending_noe.active.size(1),
            append_seconds_including_fixed_prepare_decode_selection_directory_growth=elapsed,
            old_input_output_and_full_old_append_equal=True,common_install_after_prepare=True)
        rows.append(row);old[:]=[(actual,own(actual))]
        (a/'public-append-report.json').write_text(json.dumps(dict(calls=rows,precision=precision,
            scope='whole append candidate only; whole endpoint and later original receive separately timed'),indent=2)+'\n')
        print(json.dumps(dict(stage='public_stage_append',**row)),flush=True)
        return actual
    PublicLedgerState.append_batches=append
    return observations
