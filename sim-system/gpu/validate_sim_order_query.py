"""同一原GTC账户取消后的显式单笔Base Query→NOE→公开Registry/Ledger。

原SIM规范入口导出Found/NF/实际RetentionAdvance后的OOR；不证明Quote、列表
查询或自主连续时钟。来源目录只预分配身份，业务present仍由真实公开记录形成。
"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import json
import os
import sys
import time
import torch
from queues import Numbers
from validate_base_execution import integers,rational,values
from latency import byte_column
from pending_order_query import BaseQueryTarget,OrderQueryHistory
from scheduling import ScheduledTime


def semantic_from_reference(record):
    from fields import encode_fields
    from validate_projection import entity_sequence
    t=lambda v:torch.tensor(v,dtype=torch.int64,device='cuda')
    zero=t([0]);q=record['record']['Evidence']['evidence']['Control']['result']['OrderQuery']['result']
    found=q.get('Found',{}).get('order') if isinstance(q,dict) else None
    outcome=0 if found else 1 if q=='NotFound' else 2
    retained=q['OutOfRetention']['OrderHistory']['retained_from_utc_ms'] if outcome==2 else 0
    quantities=found['quantities']['Base'] if found else {}
    states=('Open','Matched','VenueDustTerminal','MarketResolvedCanceled','Canceled','Expired','Delayed','Unmatched')
    return encode_fields([zero,t([outcome]),zero,zero,integers([entity_sequence(found['order_identity'],'Order') if found else 0]),
        t([states.index(found['state']) if found else 0]),t([-1]),t([0 if found else 2]),
        rational([quantities.get('matched_base_quantity_shares','0')]),rational([quantities.get('active_unfilled_base_quantity_shares','0')]),
        t([0 if outcome==2 else -1]),t([retained]),integers([0]),integers([0])])


def advance_live(source,runtime,noe,sampler,asset_tokens):
    """原同账户活动订单直接响应；此请求未登记Ledger关联，不确认预留。"""
    t=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    live=source['live_query'];yes=t([True],torch.bool);zero=t([0]);fault=t([0],torch.int32)
    ids,offsets=byte_column([key.encode() for key in (source['post_request_key'],source['cancel_request_key'],live['request'])])
    requests,receipt=runtime.requests.run_registration(sampler,ids,offsets,t([2]),t([1]),integers([live['input_sequence']]),
        t([live['sent_at']]),t([[2]]),t([[0]]),yes[:,None],~yes[:,None],zero,yes,fault)
    assert receipt.arrived_at.cpu().tolist()==[live['arrived_at']]
    runtime=replace(runtime,requests=requests).install_order_query_history(OrderQueryHistory.empty(zero))
    choice=runtime.choose(ScheduledTime.absent(zero),yes,t([live['arrived_at']]),yes)
    target=BaseQueryTarget(t([2]),integers([1]),fault)
    result_refs=t([2])
    compute=lambda:runtime.query_base(choice,target,result_refs,noe,sampler,asset_tokens)
    held_runtime,held_noe=runtime,noe
    candidate=Numbers.device_exact_batch(compute,word_capacity=4,device='cuda')
    # 仅在资源出口读取fits；失败不安装任何SIM/NOE候选，然后同输入冷增长重算。
    if candidate.fits.cpu().item():runtime,noe,_,_=candidate.value
    else:
        assert runtime is held_runtime and noe is held_noe and not runtime.requests.done.all().cpu().item()
    # 独立足容量重算从同一原来源开始，不重复推进private/请求状态。
    runtime,noe=held_runtime,held_noe
    next_runtime,next_noe,batch,effect=Numbers.run_exact_batch(compute)
    assert effect.processed.cpu().item() and effect.state.cpu().tolist()==[0]
    quantities=live['result']['quantities']
    assert quantities['measurement']=='BASE'
    assert Fraction(quantities['active_unfilled_base_quantity_shares'])>0
    assert [Fraction(n,d) for n,d in zip(values(effect.matched.numerator),values(effect.matched.denominator))]==[Fraction(quantities['matched_base_quantity_shares'])]
    assert [Fraction(n,d) for n,d in zip(values(effect.active_quantity.numerator),values(effect.active_quantity.denominator))]==[Fraction(quantities['active_unfilled_base_quantity_shares'])]
    return next_runtime,next_noe,batch,{'initial_word_capacity':4,'required_words':int(candidate.required_words.cpu()),
        'initial_fits':bool(candidate.fits.cpu()),'word_growth_triggered':not bool(candidate.fits.cpu())}


def check(source,runtime,noe,sampler,asset_tokens,state,live_resources):
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'ledger/gpu'),str(root/'shared-run/gpu')]
    from query_submission import submit_order_query
    from fields import encode_fields
    from consumption import ReceiveGates
    from base_runtime_storage import BaseRuntimeStorage
    from validate_base_orders import compare
    from ledger_drain import LedgerDrainStorage
    from registry import FrozenIntentOrders
    from validate_order_query import exact,BALANCES
    run=Numbers.run_exact_batch
    t=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    zero=t([0]);yes=t([True],torch.bool);fault=t([0],torch.int32)
    payload=source['order_queries'];frames=payload['frames']
    keys=[source['post_request_key'],source['cancel_request_key'],source['live_query']['request'],*[f['request'] for f in frames],payload['reservation']]
    identities,offsets=byte_column([key.encode() for key in keys])
    request=len(keys)-1
    assert state.ledger.query_associations is not None
    assert (state.ledger.query_associations.events<0).all().cpu().item()
    account,decision=run(lambda:state.ledger.reserve_action(t([[request]]),zero[:,None],
        rational([payload['principal']]),rational(['0']),yes[:,None]))
    assert decision.fault.cpu().tolist()==[0]
    state=replace(state,ledger=account)
    assert runtime.order_query_history is not None
    def check_account(original,label):
        projection=original['projection']
        assert values(state.ledger.current_entry_sequence)==[projection['ledger_entry_seq']],label
        assert values(state.ledger.current_local_prefix)==[projection['ledger_local_prefix']],label
        balances=projection['platform_confirmed_projection']
        assert exact(state.ledger.platform.balances)==[Fraction(balances[n]) for n in BALANCES],label
        funds=run(state.ledger.funds)
        assert exact(funds.actual_available)==[Fraction(original['cash']['actual_available_pusd'])],label
        assert exact(funds.maker_available)==[Fraction(original['cash']['maker_available_pusd'])],label
        assert exact(state.ledger.accepted_principal.amounts)==[Fraction(balances['cumulative_accepted_principal_pusd'])],label
    check_account(payload['reserved'],'reserved')
    retries=0;reports=[];started=time.perf_counter();old=None;old_values=None
    artifact=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])
    for index,frame in enumerate(frames):
        print(json.dumps({'stage':'order_query_start','index':index+1,'total':len(frames),
            'elapsed_seconds':time.perf_counter()-started}),flush=True)
        if frame['retention'] is not None:
            assert frame['input_sequence']==frame['retention_input_sequence']+1
            retention=frame['retention']
            runtime=runtime.advance_order_retention(t([retention['order_history_retained_from_utc_ms']]),yes,yes)
        check_account(frame['before'],'before_send')
        prior_runtime,prior_account=runtime,state.ledger
        key=t([index+3]);method=t([1]);input_sequence=integers([frame['input_sequence']]);sent=t([frame['sent_at']])
        item=t([index+3]);event=t([index]);original_request=t([request])
        def send():
            return submit_order_query(runtime,state.ledger,sampler,identities,offsets,key,method,
                input_sequence,sent,item,event,original_request,yes,fault)
        next_runtime,account,receipt,association,enough=run(send)
        if not enough.cpu().item():
            assert torch.equal(next_runtime.requests.occupied,prior_runtime.requests.occupied)
            assert torch.equal(next_runtime.requests.keys,prior_runtime.requests.keys)
            assert values(next_runtime.requests.input_sequences)==values(prior_runtime.requests.input_sequences)
            assert values(account.current_entry_sequence)==values(prior_account.current_entry_sequence)
            assert values(account.current_local_prefix)==values(prior_account.current_local_prefix)
            assert torch.equal(account.query_associations.events,prior_account.query_associations.events)
            runtime=replace(runtime,requests=runtime.requests.resized(int(receipt.required_requests.cpu()[0])))
            state=replace(state,ledger=state.ledger.resized_query_associations(int(association.required_capacity.cpu()[0])))
            next_runtime,account,receipt,association,enough=run(send);retries+=1
        assert enough.cpu().item() and receipt.registered.cpu().item() and association.changed.cpu().item()
        warmed=Numbers.device_exact_batch(send,word_capacity=16,device='cuda')
        send_graph=torch.cuda.CUDAGraph()
        with torch.cuda.graph(send_graph):captured_send=Numbers.device_exact_batch(send,word_capacity=16,device='cuda')
        send_graph.replay();torch.cuda.synchronize()
        assert warmed.fits.cpu().item() and captured_send.fits.cpu().item()
        compare(captured_send.value,(next_runtime,account,receipt,association,enough),'common_query_send_capture')
        assert receipt.arrived_at.cpu().tolist()==[frame['arrived_at']]
        runtime=next_runtime;state=replace(state,ledger=account)
        check_account(frame['after_send'],'after_send')
        # 重复同一发送/本地事件不会重新推进任一域，也不创建自动重试请求。
        repeated,account,receipt,association,enough=run(send)
        assert receipt.duplicate.cpu().item() and not association.changed.cpu().item()
        assert values(account.current_entry_sequence)==values(state.ledger.current_entry_sequence)
        compare(repeated.requests,runtime.requests,'same_query_send_idempotent')
        target=BaseQueryTarget(item,integers([1 if frame['target']=='sim-1' else 999]),fault)
        choice=runtime.choose(ScheduledTime.absent(zero),yes,t([frame['arrived_at']]),yes)
        assert choice.enabled(5).cpu().tolist()==[True] and choice.at.cpu().tolist()==[frame['arrived_at']]
        result_refs=item
        compute=lambda:runtime.query_base(choice,target,result_refs,noe,sampler,asset_tokens)
        warm=Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
        graph=torch.cuda.CUDAGraph()
        with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
        graph.replay();torch.cuda.synchronize()
        assert warm.fits.cpu().item() and captured.fits.cpu().item()
        actual=run(compute)
        compare(captured.value,actual,'actual_query_candidate_capture')
        runtime,noe,batch,effect=actual
        assert effect.fault.cpu().tolist()==[0] and effect.processed.cpu().tolist()==[True]
        assert batch.emitted.cpu().tolist()==[[True]] and not batch.economic_enabled.any().cpu().item()
        record,=frame['records'];body=record['record']['Evidence']
        source_order=body['source_order'];private=source_order['sequence']['PrivateFeedback']['private_feedback_sequence']
        reference=body['source_reference']['record']['DirectResponse']
        assert reference['association']['Request']['global_request_id']==frame['request']
        assert batch.request_slots.cpu().tolist()==[[index+3]] and batch.item_ordinals.cpu().tolist()==[[-1]]
        assert values(batch.private_sequences)==[private]
        assert values(batch.batch_sequences)==[record['position']['source_watermark']['projection_batch_sequence']]
        assert values(batch.record_ordinals)==[record['position']['record_ordinal']]
        assert batch.received_at.cpu().tolist()==[[reference['response_received_at_utc_ms']]]
        assert batch.platform_at.cpu().tolist()==[[reference['platform_responded_at_utc_ms']]]
        row=int(batch.rows.cpu()[0,0])
        assert noe.normalizer.bank.source_order[row].cpu().tolist()==[2,private&0xffffffff,private>>32,source_order['ordinal']]
        logical=run(lambda:encode_fields([zero,t([5]),key]))
        actual_logical=noe.normalizer.bank.logical_keys[row:row+1]
        width=max(actual_logical.size(1),logical.size(1))
        assert torch.equal(torch.nn.functional.pad(actual_logical,(0,width-actual_logical.size(1))),
            torch.nn.functional.pad(logical,(0,width-logical.size(1))))
        assert record['position']['context_reference']==source['prior_records'][0]['position']['context_reference']
        assert record['position']['source_watermark']['run_id']==source['prior_records'][0]['position']['source_watermark']['run_id']
        assert batch.context_slots.cpu().tolist()==[[0]] and batch.run_slots.cpu().tolist()==[[0]]
        q=body['evidence']['Control']['result']['OrderQuery']['result']
        found=q.get('Found',{}).get('order') if isinstance(q,dict) else None
        outcome=0 if found else 1 if q=='NotFound' else 2
        if found:
            assert found['condition_id']==source['initial'][0]['market']['condition_id'] and found['token_id']=='1'
        # 独立原NOE正文直接编码；不取GPU结果作为expected或复用被测mapper。
        semantic=run(lambda:semantic_from_reference(record))
        actual=noe.normalizer.bank.semantic[row:row+1];width=max(actual.size(1),semantic.size(1))
        assert torch.equal(torch.nn.functional.pad(actual,(0,width-actual.size(1))),
            torch.nn.functional.pad(semantic,(0,width-semantic.size(1))))
        state=run(lambda:state.append_batch(noe.normalizer.bank,batch))
        cutoff=t([frame['cutoff']]);gates=ReceiveGates.unset(1).set_receive(cutoff,yes).set_public(cutoff,yes)
        state,delivery=run(lambda:state.receive(noe.normalizer.bank,gates,torch.ones_like(state.ledger.columns.enabled)))
        assert delivery.fault.cpu().tolist()==[0] and delivery.closed.emitted.cpu().sum().item()==1
        emitted=delivery.closed.emitted.cpu().tolist()[0]
        delivered=[value for value,present in zip(values(delivery.closed.delivered_sequences),emitted) if present]
        available=[value for value,present in zip(delivery.closed.available_at.cpu().tolist()[0],emitted) if present]
        assert delivered==[record['visibility']['Feedback']['delivery_sequence']]
        assert available==[record['visibility']['Feedback']['available_at_utc_ms']]
        assert not (state.waiting.received&~state.waiting.consumed).any().cpu().item()
        check_account(frame['after'],'after_receive')
        intents=FrozenIntentOrders(yes[:,None],integers([1]),yes[:,None],rational(['5']),~yes[:,None],yes[:,None])
        frozen=run(lambda:state.freeze_account(intents,cutoff))
        assert frozen.orders.cancellable.cpu().tolist()==[[False]] and frozen.orders.matched_known.cpu().tolist()==[[True]]
        expected_quantity=source['checkpoints'][-1]['state']['quantities']['Base']
        assert exact(frozen.orders.matched)==[Fraction(expected_quantity['matched_base_units'],1_000_000)]
        assert frozen.orders.active_known.cpu().tolist()==[[True]] and exact(frozen.orders.active)==[Fraction(0)]
        if old is None:
            old=(BaseRuntimeStorage(runtime,runtime).snapshot(),LedgerDrainStorage(state.ledger,state.waiting,word_capacity=16).snapshot().ledger,frozen)
            old_values=(values(old[0].orders.read(t([0])).quantity.open_base_units),values(old[1].current_entry_sequence),exact(old[2].funds.actual_available),
                old[0].order_query_history.retained_from.clone(),old[0].order_query_history.has_retained.clone(),old[0].orders.last_updated_at.clone())
        assert values(old[0].orders.read(t([0])).quantity.open_base_units)==old_values[0]
        assert values(old[1].current_entry_sequence)==old_values[1]
        assert exact(old[2].funds.actual_available)==old_values[2]
        assert torch.equal(old[0].order_query_history.retained_from,old_values[3])
        assert torch.equal(old[0].order_query_history.has_retained,old_values[4])
        assert torch.equal(old[0].orders.last_updated_at,old_values[5])
        reports.append({'outcome':outcome,'entry':values(state.ledger.current_entry_sequence)[0],
            'cutoff':frame['cutoff'],'elapsed_seconds':time.perf_counter()-started})
        print(json.dumps({'stage':'order_query_done','index':index+1,'total':len(frames),**reports[-1]}),flush=True)
    assert [r['outcome'] for r in reports]==[0,1,2] and retries>=2
    report={'status':'PASS','frames':reports,'common_sender_capacity_retries':retries,
        'actual_live_query_resources':live_resources,
        'single_Base_Order_actual_SIM_NOE_PublicRegistry_Ledger':True,'Quote_Orders_continuous_host':False}
    (artifact/'query-report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report
