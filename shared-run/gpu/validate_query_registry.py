"""原连续SIM工程场景的V3 NOE→公开门→Registry普通Query对照。

主序列复用原Query切片的真实delivery起点；附加重排明确为原CPU空Registry
observe反例，不冒称完整收到流或连续SIM宿主。数量预期只来自原CPU读面。
"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import json
import sys
import time
sys.path=[p for p in sys.path if Path(p).resolve()!=Path(__file__).resolve().parent]
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','execution-normalization/gpu','ledger/gpu','shared-run/gpu')]
from queues import Numbers
from lifecycle import OrderQueryResult,map_order_query
from projection import Projection
from core import EVIDENCE
from feedback import FeedbackEmitter
from consumption import PendingRecords,ReceiveGates,ClosedRecords
from registry import PublicRegistryEvents,PublicRegistryState,FrozenIntentOrders,identity_matches
from registry_projection import public_rows_from_closed
from validate_registry import event_columns,position_key,amounts
from validate_base_execution import integers,rational,values


def body(reference):return reference['record']['Evidence']
def query(reference):return body(reference)['evidence'].get('Control',{}).get('result',{}).get('OrderQuery')
def delivery(reference):return reference['visibility']['Feedback']['delivery_sequence']


def prepare(queries,catalog,identities):
    t=lambda v:torch.tensor(v,dtype=torch.int64,device='cuda')
    zero=t([0]);yes=zero==0
    state=Numbers.run_exact_batch(lambda:Projection.open(FeedbackEmitter.open(1),zero,zero,zero,zero,zero,
        integers([0]),zero,t([-1]),zero,zero))
    batches=[];slots={};states=('Open','Matched','VenueDustTerminal','MarketResolvedCanceled','Canceled','Expired','Delayed','Unmatched')
    for reference in queries:
        p=reference['position'];b=body(reference);source=b['source_reference']['record']['DirectResponse']
        private=b['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
        result=query(reference)['result'];found=result['Found']['order']
        quantities=found['quantities'];kind=0 if 'Base' in quantities else 1
        measurement=quantities['Base'] if kind==0 else quantities['QuoteBudget']
        matched=measurement['matched_base_quantity_shares'] if kind==0 else measurement['actual_matched_base_quantity_shares']
        opened=measurement['active_unfilled_base_quantity_shares'] if kind==0 else '0'
        value=Numbers.run_exact_batch(lambda:OrderQueryResult(zero,integers([identities[found['order_identity']]]),
            t([catalog['conditions'][found['condition_id']]]),t([catalog['tokens'][found['token_id']]]),
            t([states.index(found['state'])]),t([-1]),t([kind]),rational([matched]),rational([opened]),t([-1]),zero,
            integers([0]),integers([0])))
        request=catalog['requests'][source['association']['Request']['global_request_id']]
        state=replace(state,request_slots=t([request]),context_slots=t([catalog['contexts'][p['context_reference']]]),
            run_slots=t([catalog['runs'][p['source_watermark']['run_id']]]))
        mapped=Numbers.run_exact_batch(lambda:map_order_query(value,integers([private]),t([b['source_order']['ordinal']]),zero,t([request]),yes))
        state,batch=Numbers.run_exact_batch(lambda:state._finish(state.emitter,[(mapped,EVIDENCE,integers([private]),
            integers([p['source_watermark']['projection_batch_sequence']]),t([source['response_received_at_utc_ms']]),zero,
            t([source['platform_responded_at_utc_ms']]),yes)],zero.to(torch.int32),record_ordinal_base=integers([p['record_ordinal']])))
        assert batch.fault.cpu().tolist()==[0] and batch.emitted.cpu().tolist()==[[True]]
        assert batch.item_ordinals.cpu().tolist()==[[-1]] and batch.request_slots.cpu().tolist()==[[request]]
        row=int(batch.rows.cpu()[0,0]);slots[position_key(reference)]=row
        catalog['references'][position_key(reference)]=(row,reference)
        assert state.normalizer.bank.source_order[row].cpu().tolist()==[2,private&0xffffffff,private>>32,b['source_order']['ordinal']]
        assert values(batch.record_ordinals)==[p['record_ordinal']]
        batches.append(batch)
    return state.normalizer.bank,batches,slots


def validate(source,artifact):
    started=time.perf_counter();run=Numbers.run_exact_batch
    t=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    catalog={kind:{} for kind in ('references','contexts','runs','requests','conditions','tokens')};identities={}
    references={}
    for frame in source:
        for order in frame['orders']:
            identities.setdefault(order['identity'],len(identities))
            for field in ('accepted','formed','latest_lifecycle','latest_direct','latest_cancel'):
                reference=order[field]
                if reference is not None:references.setdefault(position_key(reference),reference)
            for reference in order['trades']:references.setdefault(position_key(reference),reference)
    for reference in references.values():
        p=reference['position'];b=body(reference)
        catalog['contexts'].setdefault(p['context_reference'],len(catalog['contexts']))
        catalog['runs'].setdefault(p['source_watermark']['run_id'],len(catalog['runs']))
        r=b['source_reference']['record'].get('DirectResponse')
        if r is not None:
            association=next(iter(r['association'].values()))
            catalog['requests'].setdefault(association['global_request_id'],len(catalog['requests']))
        result=query(reference)
        if result is not None:
            q=result['result']['Found']['order']
            catalog['conditions'].setdefault(q['condition_id'],len(catalog['conditions']))
            catalog['tokens'].setdefault(q['token_id'],len(catalog['tokens']))
        for result in b['evidence'].values():
            if 'TradeLifecycle' in b['evidence'] or 'TradeLifecycleV3' in b['evidence']:
                result=result['result'];result=result.get('result',result)
                identities.setdefault(result['trade_identity'],len(identities))
    queries=sorted((r for r in references.values() if query(r) is not None),key=delivery)
    # 此原两个Query响应之间没有漏导出的反馈；冷切片承接真实已交付前缀。
    # 若来源改变，须补实际材料，不能将全流delivery压成片内编号。
    assert [delivery(r) for r in queries]==list(range(delivery(queries[0]),delivery(queries[0])+len(queries)))
    bank,batches,query_slots=prepare(queries,catalog,identities)
    next_slot=len(queries)
    for key,reference in references.items():
        if key not in query_slots:catalog['references'][key]=(next_slot,reference);next_slot+=1
    # 完整身份目录可以预分配；present只来自截至实际收到的来源人口。
    orders=list(dict.fromkeys(o['identity'] for f in source for o in f['orders']))
    state=run(lambda:PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')))
    active=torch.cat([b.emitted for b in batches],1)
    pending=run(lambda:PendingRecords.from_columns(active,torch.cat([b.received_at for b in batches],1),
        integers([body(r)['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence'] for r in queries]),
        integers([body(r)['source_order']['ordinal'] for r in queries]),torch.cat([b.rows for b in batches],1),
        integers([delivery(queries[0])-1])))
    terminal_frame=next(f for f in source if f['stage']=='terminal_before_lifecycle')
    terminal_sources={o['identity']:o['latest_direct'] for o in terminal_frame['orders']
        if o['latest_direct'] is not None and query(o['latest_direct']) is not None}
    assert terminal_sources and all(query(r)['result']['Found']['order']['state']=='Expired' for r in terminal_sources.values())
    seen=set();rank=0;old=None;old_values=None;reports=[]
    for index,frame in enumerate(source,1):
        reordered=frame['stage'].startswith('reordered_')
        if frame['stage']=='reordered_terminal_only':
            state=run(lambda:PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')));seen=set();rank=0
        current={}
        for order in frame['orders']:
            for name in ('accepted','formed','latest_lifecycle','latest_direct','latest_cancel'):
                r=order[name]
                if r is not None:current[position_key(r)]=r
            for r in order['trades']:current[position_key(r)]=r
        incoming=sorted((r for key,r in current.items() if key not in seen),key=lambda r:(delivery(r),r['position']['record_ordinal']))
        other=[r for r in incoming if query(r) is None]
        if other:
            events=run(lambda:event_columns(other,identities,rank,catalog['requests'],catalog))
            state=run(lambda:state.receive(events));rank+=len(other)
        if reordered:
            selected=[r for r in incoming if query(r) is not None]
            closed=ClosedRecords(t([[query_slots[position_key(r)] for r in selected]]),integers([delivery(r) for r in selected]),
                t([[r['visibility']['Feedback']['available_at_utc_ms'] for r in selected]]),
                t([[True]*len(selected)],torch.bool),t([0],torch.int32))
        else:
            gates=ReceiveGates(t([frame['cutoff']-1]),t([frame['cutoff']]),t([True],torch.bool),t([True],torch.bool))
            pending,closed=run(lambda:pending.take_closed(gates))
        events,_=run(lambda:public_rows_from_closed(bank,batches,closed,rank+closed.emitted.to(torch.int64).cumsum(1)-1))
        for column,wanted in enumerate(closed.emitted.cpu().tolist()[0]):
            if not wanted:continue
            r=queries[int(closed.record_refs.cpu()[0,column])]
            assert values(closed.delivered_sequences)[column]==delivery(r)
            assert int(closed.available_at.cpu()[0,column])==r['visibility']['Feedback']['available_at_utc_ms']
            assert events.query_states.cpu()[0,column] in range(9)
        state=run(lambda:state.receive(events));rank+=int(closed.emitted.sum().cpu())
        seen.update(current)
        same,_=run(lambda:identity_matches(integers([identities[o] for o in orders]),state.events.orders,1,len(orders),state.events.received.size(1)))
        present=(same&state.events.received[:,None,:]).any(2)
        actual={o['identity']:o for o in frame['orders']}
        origin=t([[actual.get(o,{}).get('origin_known',False) for o in orders]],torch.bool)
        intents=run(lambda:FrozenIntentOrders(present,integers([identities[o] for o in orders]),origin,
            rational([actual.get(o,{}).get('original_target') or '0' for o in orders]),torch.zeros_like(present),origin))
        cutoff=t([frame['cutoff']])
        if reordered:
            from commitments import SubmittedLegs
            empty=t([[]],torch.bool);columns=t([[]])
            legs=SubmittedLegs(empty,empty,columns,columns,rational([]),empty)
            formation=run(lambda:state.freeze_order_formation(intents,legs,cutoff,t([[True]],torch.bool)))
            assert formation.fault.cpu().tolist()==[0]
            assert not formation.processing_known.any().cpu().item(),frame['stage']
            assert not formation.source_known.any().cpu().item(),frame['stage']
            for slot,identity in enumerate(orders):
                if identity in actual:assert formation.market.cpu().tolist()[0][slot]==catalog['conditions'][actual[identity]['condition']]

        compute=lambda:(state.freeze_orders(intents,cutoff),state.freeze_order_sources(intents,cutoff))
        Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
        graph=torch.cuda.CUDAGraph()
        with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
        graph.replay();assert captured.fits.cpu().item()
        read,sources=run(compute);captured_read,captured_sources=captured.value
        assert read.fault.cpu().tolist()==[0] and sources.fault.cpu().tolist()==[0]
        for name in ('matched','active','commitment'):
            assert amounts(getattr(read,name))==amounts(getattr(captured_read,name))
        for name in ('active_known','active_applicable','active_applicability_known','matched_known','cancellable','cancellable_known'):
            assert torch.equal(getattr(read,name),getattr(captured_read,name))
        assert torch.equal(sources.known_query.record_refs,captured_sources.known_query.record_refs)
        assert torch.equal(sources.known_query.present,captured_sources.known_query.present)
        for at,identity in enumerate(orders):
            assert bool(present.cpu()[0,at])==(identity in actual),(frame['stage'],identity,'人口')
            if identity not in actual:continue
            expected=actual[identity]
            assert bool(read.active_applicability_known.cpu()[0,at])==(expected['base_applicable'] is not None)
            if expected['base_applicable'] is not None:assert bool(read.active_applicable.cpu()[0,at])==expected['base_applicable']
            for name,known in (('matched','matched_known'),('active','active_known'),('cancellable','cancellable_known')):
                oracle=expected[name];is_known=isinstance(oracle,dict) and 'Known' in oracle
                assert bool(getattr(read,known).cpu()[0,at])==is_known,(frame['stage'],identity,name,oracle)
                if is_known:
                    if name=='cancellable':assert bool(read.cancellable.cpu()[0,at])==oracle['Known']
                    else:assert amounts(getattr(read,name))[at]==Fraction(oracle['Known'])
            reference=expected['latest_direct']
            assert bool(sources.latest_direct.present.cpu()[0,at])==(reference is not None)
            if reference is not None:
                key=position_key(reference)
                assert int(sources.latest_direct.record_refs.cpu()[0,at])==catalog['references'][key][0]
                assert values(sources.latest_direct.delivery_sequences)[at]==delivery(reference)
                assert values(sources.latest_direct.record_ordinals)[at]==reference['position']['record_ordinal']
                assert int(sources.latest_direct.platform_at.cpu()[0,at])==body(reference)['source_reference']['record']['DirectResponse']['platform_responded_at_utc_ms']
            # 预期来自原CPU终态检查点的实际Direct引用，不由GPU首次结果定义。
            reference=None if frame['stage']=='before_query' else terminal_sources.get(identity)
            assert bool(sources.known_query.present.cpu()[0,at])==(reference is not None),(frame['stage'],identity,'known_query present')
            if reference is not None:
                row=query_slots[position_key(reference)]
                assert int(sources.known_query.record_refs.cpu()[0,at])==row,(frame['stage'],identity,'known_query position')
                snapshot=query(reference)['result']['Found']['order']
                # 原canonical来源位置携带location；不另复制整份Query快照。
                assert snapshot['condition_id']==expected['condition'] and snapshot['token_id']==expected['token']
                assert values(sources.known_query.private_sequences)[at]==body(reference)['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
                assert values(sources.known_query.batch_sequences)[at]==reference['position']['source_watermark']['projection_batch_sequence']
                assert values(sources.known_query.record_ordinals)[at]==reference['position']['record_ordinal']
                assert int(sources.known_query.context_slots.cpu()[0,at])==catalog['contexts'][reference['position']['context_reference']]
                assert int(sources.known_query.run_slots.cpu()[0,at])==catalog['runs'][reference['position']['source_watermark']['run_id']]
                assert values(sources.known_query.delivery_sequences)[at]==delivery(reference)
                assert int(sources.known_query.available_at.cpu()[0,at])==reference['visibility']['Feedback']['available_at_utc_ms']
                direct=body(reference)['source_reference']['record']['DirectResponse']
                assert int(sources.known_query.platform_at.cpu()[0,at])==direct['platform_responded_at_utc_ms']
                assert int(sources.known_query.received_at.cpu()[0,at])==direct['response_received_at_utc_ms']
        if frame['stage']=='terminal_before_lifecycle':
            old=(read,sources)
            old_values=(amounts(read.active),sources.latest_direct.record_refs.clone(),sources.known_query.record_refs.clone())
        elif frame['stage']=='late_open_after_terminal':
            for at,identity in enumerate(orders):
                if identity in terminal_sources:
                    assert int(sources.latest_direct.record_refs.cpu()[0,at])!=query_slots[position_key(terminal_sources[identity])]
        if old is not None:
            assert amounts(old[0].active)==old_values[0]
            assert torch.equal(old[1].latest_direct.record_refs,old_values[1]) and torch.equal(old[1].known_query.record_refs,old_values[2])
        reports.append({'stage':frame['stage'],'cutoff':frame['cutoff'],'orders':len(actual),'elapsed_seconds':time.perf_counter()-started})
        print(json.dumps(reports[-1]),flush=True)
    report={'status':'PASS','stages':reports,'actual_V3_NOE_Query_public_gate_and_Registry':True,
        'Query_quantity_not_Submission_measurement':True,'full_SIM_Query_sender_or_cancel_population':False}
    (artifact/'report.json').write_text(json.dumps(report,indent=2)+'\n')


if __name__=='__main__':validate(json.loads(Path(sys.argv[1]).read_text()),Path(sys.argv[2]))
