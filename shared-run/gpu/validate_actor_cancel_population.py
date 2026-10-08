"""取消发送目录/旧冻结身份的短组件核对。

请求正文和完成输入来自原CPU真实来源；登记沿实际GPU接口。完成解码只核
公共读面，不据借用的反馈证明新发送时延或SIM撮合。完整取消后端证据另复用。
"""
from pathlib import Path
import importlib.util,json,os,sys,time
from dataclasses import replace,fields,is_dataclass
from types import SimpleNamespace
import torch
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('shared-run/gpu','sim-system/gpu','ledger/gpu','execution-normalization/gpu',
    'model-train/native-inputs/gpu','data-pipeline/gpu')]
from validate_actor_send_batch import T,run,integers,values,SubmissionState,PublicSubmission
from actor_send import ActorRequestSubmission,SubmittedActorRequests
from action_selection import ActionControl
from registry import PublicCancellationState
from mutation_requests import MutationRequests
from input_frontier import InputCommitFrontier
from rules import PublicRuleGpu
from latency import EmpiricalDelaySampler,byte_column
from validate_cancel_source import prepare_cancel_source
from validate_registry import position_key
from cancel_projection import cancellation_rows_from_closed
from consumption import ClosedRecords


def check(source,codec,prefix_source):
    started=time.perf_counter();roots=2;yes=T([True]*roots,torch.bool);zero=T([0]*roots)
    single,pair=source['cancellation']['stages'][:2]
    identity,offsets=byte_column([s['request_id'].encode() for s in (single,pair)])
    sampler=EmpiricalDelaySampler.build((ROOT/'sim-system/config/latency-profiles'/source['profile_sha256']/'profile.json').read_bytes(),
        source['profile_sha256'],[source['session_seed'].encode()]*roots)
    runtime=SubmissionState(run(lambda:MutationRequests.empty(integers([0]*roots),capacity=2,action_capacity=3)),
        run(lambda:InputCommitFrontier.open(roots,source['records'][0]['position']['source_watermark']['run_id'],'session-control-v2')))
    public=PublicSubmission(None,run(lambda:PublicCancellationState.open(integers([]),T([[],[]],torch.bool))))
    population=SubmittedActorRequests();condition=source['initial'][0]['market']['condition_id']
    tokens=tuple(e['payload']['event']['asset']['token_id'] for e in source['initial'])
    # 两份原Accepted明确包含sim-1/2；旧冻结只持sim-1，新冻结持两项。
    accepted=[]
    for record in source['records']:
        control=record['record'].get('Evidence',{}).get('evidence',{}).get('Control',{}).get('result',{})
        item=control.get('Submission',{}).get('outcome',{}).get('Accepted')
        if item:accepted.append(item['order']['order_identity'])
    assert len(accepted)==2
    raw_ids={}
    def original_ids(value):
        if isinstance(value,dict):
            if 'platform_order_id' in value:raw_ids[value['platform_order_id']]=int(value['platform_order_id'].removeprefix('sim-'))
            for entry in value.values():original_ids(entry)
        elif isinstance(value,list):
            for entry in value:original_ids(entry)
    original_ids(source['effects'])
    assert raw_ids=={'sim-1':1,'sim-2':2}
    # 原连续入口的真实公开冻结：C1500仅原FOK身份1，C1700已有1/2/3。
    prefix_root=Path(prefix_source)
    observations=[json.loads(line) for line in (prefix_root/'continuous-progress.jsonl').read_text().splitlines()]
    earlier=next(row for row in observations if row['stage']=='continuous_freeze' and row['at']==[1500])
    later=next(row for row in observations if row['stage']=='actual_public_freeze_diagnostic' and row['at']==[1700])
    original=json.loads((prefix_root/'original-actor-send.json').read_text())
    first_id=original['effects'][0]['outcome']['Accepted']['platform_order_id']
    assert earlier['accepted']==[True] and earlier['order_count']==1
    assert first_id=='sim-1'
    current=[value for value,on in zip(later['intents']['identities'],later['intents']['present'][0],strict=True) if on]
    assert current==[1,2,3]
    old={first_id:1};new={f'sim-{value}':value for value in current}
    def sender(keys,at,orders):
        return ActorRequestSubmission(ActionControl(zero,T(keys),T([[0,1]]*roots),T([at]*roots)),T([[0,1]]*roots),
            sampler,PublicRuleGpu(source['rule']),identity,offsets,((condition,),)*roots,(tokens,)*roots,
            ({},)*roots,((source['market_end'],),)*roots,codec,order_sequences=orders,population=population)
    def action(families):
        return SimpleNamespace(family=T(families),legs=SimpleNamespace(present=T([[False]*2]*roots,torch.bool)),fault=T([0]*roots,torch.int32))
    early=sender([1,0],pair['sent_at'],(new,new))
    runtime,public,first,fit=early(runtime,public,((0,),(pair['request'],)),action([4,0]),T([True,False],torch.bool),cutoff=object())
    assert first.registered.cpu().tolist()==[True,False] and fit.all().cpu().item()
    assert values(public.cancellations.requests)==[1,-1]
    assert public.cancellations.registered.cpu().tolist()==[[True],[False]]
    prefix=values(runtime.input_frontier.next_sequence);old_refs=population.cancellation.item_refs[0].clone()
    first_target=values(population.cancellation.intents.order_sequences)[:2]
    # 较旧冻结晚发送，仅知道sim-1。不得借它重解码较新冻结的历史sim-2。
    late=sender([1,0],pair['sent_at']+1,(old,old))
    runtime,public,second,fit=late(runtime,public,((1,),(single['request'],)),action([0,3]),T([False,True],torch.bool),cutoff=object())
    assert second.registered.cpu().tolist()==[False,True] and fit.all().cpu().item()
    assert public.cancellations.registered.cpu().tolist()==[[True],[True]]
    assert values(public.cancellations.requests)==[1,0]
    assert values(runtime.input_frontier.next_sequence)==[prefix[0],prefix[1]+1]
    catalog=population.cancellation
    assert torch.equal(catalog.item_refs[0,:1],old_refs)
    assert values(catalog.intents.order_sequences)[:2]==first_target
    assert catalog.item_refs.cpu().tolist()==[[[0,1]],[[2,-1]]]
    assert catalog.requests==((pair['request'],),(single['request'],))
    # 独立真实Completed输入走原NOE→公共取消投影。它不提供新发送的匹配结果。
    records=source['cancellation']['records']
    references=[r for r in records if 'CancellationRequestCompleted' in r['record'].get('Evidence',{}).get('evidence',{}).get('Control',{}).get('result',{})
        and r['record']['Evidence']['source_reference']['record']['DirectResponse']['association']['Request']['global_request_id'] in {s['request_id'] for s in (single,pair)}]
    requests={s['request_id']:i for i,s in enumerate((single,pair))}
    metadata={'contexts':{r['position']['context_reference']:0 for r in references},'runs':{source['records'][0]['position']['source_watermark']['run_id']:0},
        'references':{position_key(r):(0,0) for r in references}}
    bank,batches,index,_=prepare_cancel_source(references,requests,None,metadata)
    def duplicate(value):
        if isinstance(value,torch.Tensor):return value.repeat((roots,)+(1,)*(value.dim()-1))
        if is_dataclass(value):return replace(value,**{f.name:duplicate(getattr(value,f.name)) for f in fields(value)})
        if hasattr(value,'__dict__'):return SimpleNamespace(**{k:duplicate(v) for k,v in vars(value).items()})
        raise TypeError(type(value))
    batches=[duplicate(b) for b in batches]
    by_request={r['record']['Evidence']['source_reference']['record']['DirectResponse']['association']['Request']['global_request_id']:r for r in references}
    ordered=[by_request[pair['request_id']],by_request[single['request_id']]]
    closed=ClosedRecords(T([[index[position_key(r)]] for r in ordered]),
        run(lambda:integers([r['visibility']['Feedback']['delivery_sequence'] for r in ordered])),
        T([[r['visibility']['Feedback']['available_at_utc_ms']] for r in ordered]),T([[True]]*roots,torch.bool),T([0]*roots,torch.int32))
    _,completed=run(lambda:cancellation_rows_from_closed(bank,batches,closed))
    claimed=run(lambda:public.cancellations.receive(completed))
    frozen=run(lambda:claimed.freeze_global(T([max(s['closed_at'] for s in (single,pair))]*roots)))
    assert values(frozen.pending_count)==[0,0] and not frozen.fault.any().cpu().item()
    assert frozen.has_latest.cpu().tolist()==[True,True]
    assert torch.equal(claimed.registered,public.cancellations.registered)
    # 同根旧冻结晚发A：全局历史宽1、root0已有1/root1已有1；
    # 只向root0追加，不得重解码root0的B或把root1 padding当逻辑人口。
    public=replace(public,cancellations=claimed)
    next_sender=sender([0,0],pair['sent_at']+2,(old,old))
    runtime,public,third,fit=next_sender(runtime,public,((0,),(single['request'],)),action([3,0]),T([True,False],torch.bool),cutoff=object())
    assert third.registered.cpu().tolist()==[True,False] and fit.all().cpu().item()
    catalog=population.cancellation
    assert catalog.requests==((pair['request'],single['request']),(single['request'],))
    assert catalog.item_refs.cpu().tolist()==[[[0,1],[3,-1]],[[2,-1],[-1,-1]]]
    assert values(public.cancellations.requests)==[1,0,0,-1]
    assert public.cancellations.registered.cpu().tolist()==[[True,True],[True,False]]
    assert torch.equal(public.cancellations.completions.received,claimed.completions.received)
    assert values(runtime.input_frontier.next_sequence)==[3,2]
    assert values(first.input_sequence)==[1,0] and first.registered.cpu().tolist()==[True,False]
    current=run(lambda:public.cancellations.freeze_global(T([max(s['closed_at'] for s in (single,pair))]*roots)))
    assert values(current.pending_count)==[1,0]  # 第三次已登记、尚未领取对应完成，不能冒充完成。
    return dict(passed=True,roots=roots,newer_body_sent_before_older_body=True,stable_refs=catalog.item_refs.cpu().tolist(),
        requests=values(public.cancellations.requests),first_receipt_sequence=values(first.input_sequence),
        prefix_source=str(prefix_root.name),old_public_ids=list(old),new_public_ids=list(new),
        next_input=values(runtime.input_frontier.next_sequence),claimed_pending_before_third=values(frozen.pending_count),current_pending=values(current.pending_count),
        scope='actual GPU cancel registration/catalog append; independent original Completed NOE public consumer; no SIM matching or new transport timing proof',
        elapsed_seconds=time.perf_counter()-started)

if __name__=='__main__':
    source=json.loads(Path(sys.argv[1]).read_text())[0]
    spec=importlib.util.spec_from_file_location('model_native_inputs',Path(os.environ['CARGO_TARGET_DIR'])/'debug/libmodel_native_inputs.so')
    codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    print(json.dumps(check(source,codec,sys.argv[2])),flush=True)
