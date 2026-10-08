"""真实混合请求形成→完整取消pending→NOE→同一Ledger/Registry的原CPU对照。

原CPU来源只提供真实请求/规则/公开门及独立期望；目标展开、处理顺序、
结果原因、资源续接与经济效果由实际生产入口生成。不是完整Actor发送宿主。
"""
from dataclasses import replace
from fractions import Fraction
import importlib.util,json,os,sys,time
from pathlib import Path
import torch
from validate_pending_post import (check,T,run,integers,rational,values,amounts,units,checkpoint,
    asset_layout,original_semantic,ROOT,PendingPostIntents,ReceiveGates,ScheduledTime)
from queues import Numbers
from cancel_submission import cancellation_catalog,submit_cancellation
from registry import PublicCancellationState
from pending_cancel_intents import CancelFailureScenario
from cancel_results import CancelResults
from latency import byte_column
from post_submission import submit_post_orders
from commitments import SubmittedLegs,bind_submission_receipt
from intent_terms import SubmittedBuyTerms,freeze_intent_orders
from cancel_objects import CancelObjectDirectory


def receive_closed(public,bank,gates,progress,label,principal_conditions):
    # 首次Registry领取一次；后续同waiting/Ledger一轮一轮继续。每轮建立新cursor。
    public,delivery=run(lambda:public.receive_first(bank,gates))
    round_number=0;principal_grown=False
    while True:
        remaining=int((public.waiting.received & ~public.waiting.consumed).sum().cpu())
        fault=delivery.fault|public.waiting.fault|public.ledger.fault
        progress(dict(stage='public_receive_round',request=label,round=round_number,remaining=remaining,
            fault=fault.cpu().tolist(),registry_fault=public.registry.events.fault.cpu().tolist(),
            ledger_fault=public.ledger.fault.cpu().tolist(),waiting_fault=public.waiting.fault.cpu().tolist(),
            order_capacity=public.ledger.orders.occupied.size(1),required_orders=public.ledger.orders.capacity_required.cpu().tolist(),
            trade_capacity=public.ledger.trades.occupied.size(1),required_trades=public.ledger.trades.capacity_required.cpu().tolist(),
            required_order_funds=public.ledger.order_funds.capacity_required.cpu().tolist(),
            principal_conditions=public.ledger.accepted_principal.conditions.cpu().tolist(),
            required_principal_conditions=public.ledger.accepted_principal.required_conditions.cpu().tolist()))
        if not remaining or fault.any().cpu().item():break
        ledger,waiting=run(lambda:public.waiting.drain(public.ledger,torch.ones_like(public.ledger.columns.enabled),passes=1))
        public=replace(public,ledger=ledger,waiting=waiting);round_number+=1
        next_remaining=int((waiting.received & ~waiting.consumed).sum().cpu())
        if next_remaining==remaining:
            progress(dict(stage='public_receive_no_exit',request=label,round=round_number,remaining=next_remaining,
                fault=(ledger.fault|waiting.fault).cpu().tolist(),received=waiting.received.cpu().tolist(),
                consumed=waiting.consumed.cpu().tolist(),schemas=ledger.lifecycle.schemas.cpu().tolist(),
                required_orders=ledger.orders.capacity_required.cpu().tolist(),required_trades=ledger.trades.capacity_required.cpu().tolist(),
                required_order_funds=ledger.order_funds.capacity_required.cpu().tolist()))
            required=ledger.accepted_principal.required_conditions.cpu().tolist()
            if required==[1] and not principal_grown:
                assert ledger.accepted_principal.conditions.cpu().tolist()==[0]
                assert principal_conditions.cpu().tolist()==[0,1]
                old_amounts=amounts(ledger.accepted_principal.amounts);old_entry=values(ledger.current_entry_sequence);old_consumed=waiting.consumed.clone()
                grown=run(lambda:ledger.extended_principal_conditions(principal_conditions))
                assert amounts(grown.accepted_principal.amounts)==[*old_amounts,Fraction(0)]
                assert values(grown.current_entry_sequence)==old_entry and torch.equal(waiting.consumed,old_consumed)
                progress(dict(stage='principal_directory_continuation',request=label,required=required,
                    conditions=grown.accepted_principal.conditions.cpu().tolist(),consumed=int(waiting.consumed.sum().cpu())))
                public=replace(public,ledger=grown);principal_grown=True
                continue
            break
    return public,replace(delivery,fault=delivery.fault|public.waiting.fault|public.ledger.fault)


def assert_target(actual,original,item_ids):
    assert actual.present.cpu().tolist()==[True]
    assert values(actual.order_sequence)==[int(original['platform_order_id'].removeprefix('sim-'))]
    assert values(actual.target_ordinal)==[original['target_ordinal']]
    assert actual.processed_at.cpu().tolist()==[original['effective_processing_at_utc_ms']]
    refs=actual.input_item_refs;on=refs.present.cpu().tolist()[0]
    actual_refs=[(item,ordinal) for item,ordinal,present in zip(refs.fields[0].cpu().tolist(),refs.fields[1].cpu().tolist(),on,strict=True) if present]
    assert actual_refs==[(item_ids[r['item_id']],r['batch_item_ordinal']) for r in original['input_item_refs']]
    outcome=original['outcome']
    if 'Canceled' in outcome:
        assert actual.outcome.cpu().tolist()==[0] and actual.reason.cpu().tolist()==[-1]
        body=outcome['Canceled']
        assert values(actual.canceled_base_units)==[units(body['canceled_base_quantity_shares'])]
        assert values(actual.released_quote_units)==[units(body['released_quote_amount_pusd'])]
        sequence=body['account_transition_seq']
        assert actual.account_sequence_known.cpu().tolist()==[sequence is not None]
        if sequence is not None:assert values(actual.account_sequence)==[sequence]
    else:
        assert actual.outcome.cpu().tolist()==[1]
        assert actual.reason.cpu().tolist()==[['NotFoundOrNotOwned','NoCancellableRemainder','TemporaryFailureNoEffect'].index(outcome['NotCanceledNoEffect'])]


def formation(source,runtime,public,receipt):
    yes=T([[True,True]],torch.bool)
    legs=SubmittedLegs(yes,torch.zeros_like(yes),T([[0,0]]),T([[0,1]]),rational(['5','1.66']),yes,
        T([[0,0]]),T([[0,1]]),yes,
        SubmittedBuyTerms(T([[0,2]]),torch.zeros_like(yes),rational(['0.1','0.6']),rational(['0','1']),T([[0,0]])))
    legs=run(lambda:bind_submission_receipt(legs,T([0]),receipt))
    directory,fault=run(lambda:freeze_intent_orders(legs,public.submission_bindings,integers([1,2]),yes))
    assert fault.cpu().tolist()==[0]
    result=run(lambda:public.registry.freeze_order_formation(directory,legs,T([source['closed_at']]),T([[True,True]],torch.bool)))
    assert result.fault.cpu().tolist()==[0] and result.processing_known.cpu().tolist()==[[True,True]]
    assert result.source_known.cpu().tolist()==[[True,True]]
    expected=[]
    for ordinal in range(2):
        platform=next(e['outcome']['Accepted']['platform_order_id'] for e in source['effects'] if e['key']['ordinal']==ordinal)
        accepted=next(r for r in source['records'] if 'Submission' in r['record'].get('Evidence',{}).get('evidence',{}).get('Control',{}).get('result',{}) and json.loads(r['record']['Evidence']['evidence']['Control']['result']['Submission']['outcome']['Accepted']['order']['order_identity'].split(':',1)[1])[-1]['Order']['source_order_id']==platform)
        identity=accepted['record']['Evidence']['evidence']['Control']['result']['Submission']['outcome']['Accepted']['order']['order_identity']
        formed=next(r for r in source['records'] if r['record'].get('Evidence',{}).get('evidence',{}).get('OrderLifecycle',{}).get('result',{}).get('order_identity')==identity and isinstance(r['record']['Evidence']['evidence']['OrderLifecycle']['result']['event'],dict) and 'Accepted' in r['record']['Evidence']['evidence']['OrderLifecycle']['result']['event'])
        expected.append(dict(accepted=accepted,formed=formed,origin=dict(ordinal=ordinal)))
    for slot,order in enumerate(expected):
        accepted=order['accepted']['record']['Evidence']['evidence']['Control']['result']['Submission']['outcome']['Accepted']
        assert result.processing_at.cpu().tolist()[0][slot]==accepted['effective_processing_at_utc_ms']
        assert values(result.input_sequences)[slot]==source['input_sequence']
        assert result.item_ordinals.cpu().tolist()[0][slot]==order['origin']['ordinal']
        assert values(result.source_sequences)[slot]==order['formed']['record']['Evidence']['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
        assert result.market.cpu().tolist()[0][slot]==0
    assert result.processing_at.cpu().tolist()[0][0]!=result.processing_at.cpu().tolist()[0][1]
    markets=CancelObjectDirectory(T([[True,True]],torch.bool),T([[1000,1000]]),T([[0,1]]),T([[True,True]],torch.bool),
        T([[]],torch.bool),T([[]]).reshape(1,0,2),T([[]]))
    frozen=run(lambda:public.freeze_account(directory,T([source['closed_at']]),submitted_legs=legs,
        cancel_markets=markets,request_identity_order=T([[0,0]])))
    assert frozen.fault.cpu().tolist()==[0]
    assert frozen.cancel_directory.paired_members[frozen.cancel_directory.paired_present].cpu().tolist()==[[0,1]]
    return result,frozen


def after_posts(source,runtime,public,noe,sampler,rule,slots,owners,receipt,progress,codec):
    started=time.perf_counter();yes=T([True],torch.bool);zero=T([0]);stages=source['cancellation']['stages']
    frozen,account_frozen=formation(source,runtime,public,receipt)
    frozen_times=frozen.processing_at.clone();frozen_sequences=values(frozen.source_sequences)
    ids=[source['request_id'],*[stage['request_id'] for stage in stages]]
    late=next(stage['late'] for stage in stages if stage['late'] is not None)
    ids.append(late['request']['request']['context']['global_request_id']);late_key=len(ids)-1
    identity,offsets=byte_column([value.encode() for value in ids])
    item_ids={}
    for stage in stages:
        body=stage['request']['request']
        for item in body.get('items',[{'item':body['item']}] if 'item' in body else []):
            item_ids.setdefault(item['item']['item_id'],len(item_ids))
    conditions,markets,tokens=asset_layout(source)
    catalog=run(lambda:cancellation_catalog((tuple(stage['request'] for stage in stages),),
        ({condition:i for i,condition in enumerate(conditions)},),(item_ids,),
        ({f'sim-{i}':i for i in range(1,12)},),codec.cancel_targets_batch_v2,'cuda'))
    public=replace(public,cancellations=run(lambda:PublicCancellationState.open(integers(list(range(1,len(stages)+1))),
        T([[False]*len(stages)],torch.bool))))
    runtime=replace(runtime,requests=runtime.requests.resized(len(ids),9),cancel_results=CancelResults(1))
    scenario=CancelFailureScenario(T([[ids.index(value['global_request_id']) for value in source['cancellation']['scenario']['targets']]]),
        run(lambda:integers([int(value['platform_order_id'].removeprefix('sim-')) for value in source['cancellation']['scenario']['targets']])),
        run(lambda:integers([source['cancellation']['scenario_input_sequence']]*2)),T([[True,True]],torch.bool),T([0],torch.int32))
    reports=[];capacity_checked=False;result_ref=0
    for stage_index,stage in enumerate(stages):
        key=stage_index+1
        method,refs,ordinals,present=catalog.columns(T([stage_index]))
        pad=runtime.requests.action_capacity-refs.size(1)
        refs=torch.nn.functional.pad(refs,(0,pad),value=-1);ordinals=torch.nn.functional.pad(ordinals,(0,pad));present=torch.nn.functional.pad(present,(0,pad))
        def sending(world,public):return submit_cancellation(world,public,sampler,identity,offsets,T([key]),method,
            integers([stage['input_sequence']]),T([stage['sent_at']]),refs,ordinals,present,yes,T([0],torch.int32))
        runtime,public,receipt,enough=run(lambda:sending(runtime,public))
        assert receipt.fault.cpu().tolist()==[0] and enough.cpu().item()
        assert receipt.arrived_at.cpu().tolist()==[stage['receipt']['arrived_at']]
        assert receipt.scheduled_count.cpu().tolist()==[stage['receipt']['count']]
        if stage['late'] is not None:
            late=stage['late'];posts=late['request']['request']['items'];count=len(posts)
            old=public.ledger;capacity=2+count;pad=capacity-old.reservations.capacity
            public=replace(public,ledger=replace(old,reservations=old.reservations.resized(capacity),
                global_requests=torch.nn.functional.pad(old.global_requests,(0,pad),value=-1),
                item_ordinals=torch.nn.functional.pad(old.item_ordinals,(0,pad),value=-1)))
            admission,reserved=run(lambda:public.ledger.reserve_action(T([list(range(2,2+count))]),T([[conditions.index(p['item']['order']['condition_id']) for p in posts]]),
                rational([r['requested_principal_pusd'] for r in late['reserve_intents']]),rational([r['immediate_fee_reserve_pusd'] for r in late['reserve_intents']]),T([[True]*count],torch.bool)))
            assert reserved.fault.cpu().tolist()==[0]
            public=replace(public,ledger=admission)
            runtime,ledger,late_receipt,enough=run(lambda:submit_post_orders(runtime,public.ledger,sampler,identity,offsets,T([late_key]),T([1]),
                integers([late['input_sequence']]),T([late['sent_at']]),T([list(range(count))]),T([[p['batch_item_ordinal'] for p in posts]]),
                T([[True]*count],torch.bool),T([[False]*count],torch.bool),zero,T([list(range(2,2+count))]),T([list(range(2,2+count))]),yes,T([0],torch.int32)))
            public=replace(public,ledger=ledger);checkpoint(public.ledger,late['sent_ledger'])
            bids=[slots.index((tokens[p['item']['order']['token_id']],'bids',Fraction('0.1'))) for p in posts]
            post_intents=PendingPostIntents((tuple(json.dumps(p['item']) for p in posts),),T([[tokens[p['item']['order']['token_id']] for p in posts]]),
                T([[conditions.index(p['item']['order']['condition_id']) for p in posts]]),T([bids]),T([[300000]*count]),T([0],torch.int32))
        batches=[];effect=0
        while True:
            choice=runtime.choose(ScheduledTime.absent(zero),yes,T([stage['closed_at']]),yes)
            assert choice.fault.cpu().tolist()==[0]
            if not choice.present.cpu().item():break
            assert choice.phase.cpu().tolist()==[5]
            pending=runtime.requests.next_pending();at=int(choice.at.cpu()[0])
            if int(pending.request_key.cpu()[0])==late_key:
                asset=post_intents.assets.gather(1,pending.item_ref[:,None])[:,0]
                ask_lists=[[i for i,(a,s,_) in enumerate(slots) if a==index and s=='asks'] for index in range(len(markets))]
                ask=T(ask_lists)[asset];paths=T([[0]*(ask.size(1)*runtime.hidden.queues.capacity)])
                runtime=runtime.advance_role_time(choice,T([300000]*len(markets)))
                choice=runtime.choose(ScheduledTime.absent(zero),yes,T([stage['closed_at']]),yes)
                runtime,noe,batch,actual=runtime.post_pending(choice,post_intents,rule,codec.post_terms_batch_v2,ask,paths,
                    run(lambda:rational([source['taker_rebate_rate']])),owners,pending.item_ref+2,noe,sampler,T(list(range(len(markets)))))
                assert actual.fault.cpu().tolist()==[0]
            else:
                work=runtime.prepare_cancel(choice,catalog.intents)
                if stage_index==2 and not capacity_checked:
                    small=replace(runtime,cancel_results=runtime.cancel_results.resized(result_ref))
                    before=(values(runtime.account.available),values(runtime.account.next_sequence),runtime.requests.done.clone(),runtime.hidden.queues.kind.clone())
                    candidate,projected,batch,effects=small.cancel_pending(small.choose(ScheduledTime.absent(zero),yes,T([stage['closed_at']]),yes),work,owners,T([result_ref]),noe,sampler,T(list(range(len(markets)))),scenario)
                    assert effects.fault.cpu().tolist()==[2]
                    assert values(candidate.account.available)==before[0] and values(candidate.account.next_sequence)==before[1]
                    assert torch.equal(candidate.requests.done,before[2]) and torch.equal(candidate.hidden.queues.kind,before[3])
                    assert values(projected.emitter.next_private_sequence)==values(noe.emitter.next_private_sequence)
                    assert not batch.emitted.any().cpu().item()
                    capacity_checked=True
                runtime=replace(runtime,cancel_results=runtime.cancel_results.resized(result_ref+1))
                choice=runtime.choose(ScheduledTime.absent(zero),yes,T([stage['closed_at']]),yes)
                runtime,noe,batch,effects=runtime.cancel_pending(choice,work,owners,T([result_ref]),noe,sampler,T(list(range(len(markets)))),scenario)
                assert effects.fault.cpu().tolist()==[0],effects.fault.cpu().tolist()
                actual=runtime.cancel_results.groups[-1].targets
                actual=[t for t in actual if t.present.cpu().item()]
                expected=stage['effects'][effect]['body']['cancel']
                assert len(actual)==len(expected)
                for target,original in zip(actual,expected,strict=True):assert_target(target,original,item_ids)
                if effects.response.emitted.cpu().item():
                    complete=[t for t in effects.targets if t.present.cpu().item()]
                    originals=sorted([t for e in stage['effects'] if 'cancel' in e['body'] for t in e['body']['cancel']],key=lambda t:t['target_ordinal'])
                    assert len(complete)==len(originals)
                    for target,original in zip(complete,originals,strict=True):assert_target(target,original,item_ids)
                result_ref+=1
            original=stage['effects'][effect]
            assert at==original['at'] and int(pending.ordinal.cpu()[0])==original['ordinal']
            cp=original['account']
            for name in ('cash','available','principal','fee'):assert values(getattr(runtime.account,name))==[units(cp[name])]
            assert values(runtime.account.next_sequence)==[cp['next_sequence']]
            effect+=1;batches.append(batch)
            progress(dict(stage='actual_pending_committed',request=stage['request_id'],pending=effect,total=len(stage['effects']),processing_at=at))
            public=run(lambda:public.append_batch(noe.normalizer.bank,batch))
        assert effect==len(stage['effects'])
        originals={(r['position']['source_watermark']['projection_batch_sequence'],r['position']['record_ordinal']):r for r in stage['records']}
        seen=[]
        for batch in batches:
            for column,on in enumerate(batch.emitted.cpu().tolist()[0]):
                if not on:continue
                position=(values(batch.batch_sequences)[column],values(batch.record_ordinals)[column]);original=originals[position];seen.append(position)
                expected=run(lambda:original_semantic(original,source));actual=noe.normalizer.bank.semantic[int(batch.rows[0,column].cpu()):int(batch.rows[0,column].cpu())+1]
                width=max(actual.size(1),expected.size(1))
                matches=torch.equal(torch.nn.functional.pad(actual,(0,width-actual.size(1))),torch.nn.functional.pad(expected,(0,width-expected.size(1))))
                if column%8==0:progress(dict(stage='NOE_compared',request=stage['request_id'],compared=len(seen),total=len(originals)))
                if not matches:progress(dict(stage='NOE_semantic_failure',position=position,actual=actual.cpu().tolist(),expected=expected.cpu().tolist()))
                assert matches,position
        assert len(seen)==len(originals)
        gates=ReceiveGates.unset(1).set_receive(T([stage['closed_at']]),yes).set_public(T([stage['closed_at']]),yes)
        public,delivery=receive_closed(public,noe.normalizer.bank,gates,progress,stage['request_id'],T(list(range(len(conditions)))))
        assert delivery.fault.cpu().tolist()==[0] and not (public.waiting.received & ~public.waiting.consumed).any().cpu().item()
        checkpoint(public.ledger,stage['ledger'])
        before=(values(public.ledger.current_entry_sequence),amounts(public.ledger.platform.balances),runtime.requests.done.clone())
        runtime,public,again,enough=run(lambda:sending(runtime,public))
        assert again.duplicate.cpu().item() and not again.registered.cpu().item()
        public,repeated=receive_closed(public,noe.normalizer.bank,gates,progress,stage['request_id']+'-duplicate',T(list(range(len(conditions)))))
        assert not repeated.closed.emitted.any().cpu().item()
        assert values(public.ledger.current_entry_sequence)==before[0] and amounts(public.ledger.platform.balances)==before[1] and torch.equal(runtime.requests.done,before[2])
        assert torch.equal(frozen.processing_at,frozen_times) and values(frozen.source_sequences)==frozen_sequences
        reports.append(dict(request=stage['request_id'],actual_targets=sum(len(e['body'].get('cancel',[])) for e in stage['effects']),actual_NOE=len(seen)))
        progress(dict(stage='cancel_request_completed',**reports[-1]))
    assert capacity_checked
    return dict(stages=reports,original_full_response_and_NOE_Ledger=True,processing_population_and_lex_order=True,
        single_pending_capacity_retry=True,elapsed_seconds=time.perf_counter()-started)


def main():
    source=json.load(sys.stdin);spec=importlib.util.spec_from_file_location('model_native_inputs',os.environ['B_GPU_NATIVE_SDK_LIBRARY'])
    codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    artifact=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR']);artifact.mkdir(parents=True,exist_ok=True)
    def progress(value):
        with (artifact/'pending-cancel-progress.jsonl').open('a') as out:out.write(json.dumps(value)+'\n')
    reports=[check(case,codec.post_terms_batch_v2,progress,lambda *args:after_posts(*args,codec)) for case in source]
    print(json.dumps(dict(cases=reports,full_Actor_send_host_delivered=False)))


if __name__=='__main__':main()
