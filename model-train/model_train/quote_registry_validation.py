"""真实Captured正文及实际已领取CPU Registry前缀的短设备对照。"""
from dataclasses import replace
from types import SimpleNamespace
from fractions import Fraction
import json
import os
from pathlib import Path
import time
import torch
from .actor_selection_validation import (make_parts,public_state,source_ledger,compare_selected,
    CapturedDeviceActor,ActorSelection,Numbers,integers,values,exact)
from registry import PublicRegistryState
from registry_projection import public_rows_from_closed
from intent_terms import freeze_intent_orders
from core import EvidenceBank,OUT_EVIDENCE
from validate_projection import expected_semantic
from validate_registry import event_columns,position_key,platform_order_id

T=lambda data,dtype=torch.int64:torch.tensor(data,dtype=dtype,device='cuda')
run=Numbers.run_exact_batch


def select(native,source):
    policy,inputs,random,control=make_parts(source,2)
    with torch.no_grad():
        policy.action.actor.variant.weight.zero_();policy.action.actor.variant.bias.copy_(T([0.,40.],torch.float32))
        # 冷工程参数只偏好约4份。原prefix16/17为lo/upper、hi/upper；原20=1、0=0。
        # 两个LayerNorm仍执行；零和输入消去均值，成对SiLU(x)-SiLU(-x)=x保线性。
        # head j 的2j*(target-lo)-j²*(hi-lo)/16只偏好所在段，合法mask/随机仍由原Actor处理。
        desired=400/int(source['pair_upper'])
        prefix=policy.action.prefix
        prefix[1].weight.zero_();prefix[1].bias.zero_()
        prefix[1].weight[0,16]=1.;prefix[1].weight[0,20]=-desired;prefix[1].weight[0,0]=desired-1.
        prefix[1].weight[1].copy_(-prefix[1].weight[0])
        prefix[1].weight[2,17]=1.;prefix[1].weight[2,16]=-1.
        prefix[1].weight[3].copy_(-prefix[1].weight[2])
        prefix[3].weight.zero_();prefix[3].bias.zero_()
        prefix[3].weight[0,0]=1.;prefix[3].weight[0,1]=-1.
        prefix[3].weight[1,2]=1.;prefix[3].weight[1,3]=-1.
        decoder=policy.action.actor.decoder[1];decoder.weight.zero_();decoder.bias.zero_()
        for row,column in ((0,48),(2,49)):
            decoder.weight[row,column]=1.;decoder.weight[row,50]=-1.
            decoder.weight[row+1].copy_(-decoder.weight[row])
        quantity=policy.action.actor.quantity;quantity.weight.zero_();quantity.bias.zero_()
        for j in range(16):
            quantity.weight[j,0]=-2*j*100000.;quantity.weight[j,1]=2*j*100000.
            quantity.weight[j,2]=-j*j/16*100000.;quantity.weight[j,3]=j*j/16*100000.
        policy.action.actor.price.bias[98]=40.  # 原0.01 tick的0.99价格；仍由原完整支持确定资格。
    captured=CapturedDeviceActor(policy,inputs,random,path_capacity=32,word_capacity=16)
    publication=ActorSelection(random);current,_=captured.publish_and_reserve(publication,public_state(source_ledger(source)),control)
    artifact=Path(os.environ['B_GPU_QUOTE_REGISTRY_DIR'])
    choices=publication.candidate.choices[0,:int(publication.candidate.length.cpu()[0])].cpu().tolist()
    original=native.actor(0);supports=[]
    for choice in choices:
        supports.append(dict(choice=choice,support=original.support(),prefix=original.prefix_features()))
        original.choose(choice)
    diagnostic=dict(family=publication.action.family.cpu().tolist(),variants=publication.action.legs.terms.variants.cpu().tolist(),
        choices=choices,quantity_hundredths=values(publication.candidate.prefix.quantity),
        prices=publication.candidate.prefix.prices.cpu().tolist(),targets=[str(q) for q in exact(publication.action.legs.target)],
        principal=[str(q) for q in exact(publication.action.principal)],fee=[str(q) for q in exact(publication.action.fee)],
        original=original.validation_selection('quote-registry'),supports=supports,counter=random.counter.cpu().tolist())
    (artifact/'captured-source.json').write_text(json.dumps(source,indent=2)+'\n')
    (artifact/'captured-selection-diagnostic.json').write_text(json.dumps(diagnostic,indent=2)+'\n')
    torch.save({key:value.detach().cpu() for key,value in policy.state_dict().items()},artifact/'engineering-policy.pt')
    print(json.dumps(dict(event='actual_captured_selection',**{key:diagnostic[key] for key in ('family','variants','choices','quantity_hundredths','prices','targets')})),flush=True)
    reference=compare_selected(native,source,publication,expected_variants=[[2,2]])
    committed=random.counter.clone()
    decision=publication.reservation.decision
    capacity=dict(capacity=current.ledger.reservations.capacity,required_capacity=decision.required_capacity.cpu().tolist(),
        fault=decision.fault.cpu().tolist(),outcomes=decision.outcomes.cpu().tolist(),
        required_market_conditions=decision.required_market_conditions.cpu().tolist(),
        complete=publication.reservation.complete.cpu().tolist(),counter=committed.cpu().tolist())
    (artifact/'published-reservation-capacity.json').write_text(json.dumps(capacity,indent=2)+'\n')
    print(json.dumps(dict(event='published_reservation_capacity',**capacity)),flush=True)
    assert capacity['capacity']==1 and capacity['required_capacity']==[2] and capacity['fault']==[0]
    assert capacity['complete']==[False] and capacity['required_market_conditions']==[-1]
    current=replace(current,ledger=replace(current.ledger,reservations=current.ledger.reservations.resized(2)))
    current,_=captured.publish_and_reserve(publication,current,control)
    assert torch.equal(committed,random.counter)
    assert publication.reservation.complete.cpu().tolist()==[True]
    assert random.counter[0,0].cpu().item()>0
    assert all(Fraction(leg['order']['FokMarket']['worst_price_pusd_per_share'])==Fraction('0.99')
        for leg in reference['intent']['SubmitPairedBuy']['orders'])
    counter=random.counter.clone();again,_=captured.publish_and_reserve(publication,current,control)
    assert again is current and torch.equal(counter,random.counter)
    return dict(choices=publication.candidate.choices[0,:int(publication.candidate.length.cpu()[0])].cpu().tolist(),
        publication=publication,current=current,counter=counter,started=time.perf_counter())


def production_rows(references,rank):
    # 仅冷解码实际已领取强类型引用。业务数量/目标来自原正文与CPU Registry，而非此转换器。
    identities={}
    for reference in references:
        body=reference['record']['Evidence']['evidence'];kind,value=next(iter(body.items()))
        value=value['result']
        if kind=='Control':
            accepted=value['Submission']['outcome']['Accepted']
            assert 'QuoteBudget' in accepted['measurement']
            value=accepted['order']
        elif kind=='TradeLifecycleV3':value=value['result']
        for name in ('order_identity','trade_identity'):
            if name in value:identities[value[name]]=int(json.loads(value[name].split(':',1)[1])[-1][
                'Order' if name=='order_identity' else 'Trade']['source_'+('order' if name=='order_identity' else 'trade')+'_id'].removeprefix('sim-'))
    catalog=dict(contexts={r['position']['context_reference']:0 for r in references},
        runs={r['position']['source_watermark']['run_id']:0 for r in references},
        references={position_key(r):(i,r) for i,r in enumerate(references)})
    metadata=run(lambda:event_columns(references,identities,rank,{'quote-registry':9},catalog))
    semantics=[run(lambda r=r:expected_semantic(r)) for r in references]
    width=max(row.size(1) for row in semantics)
    semantic=torch.cat([torch.nn.functional.pad(row,(0,width-row.size(1))) for row in semantics])
    schemas=[]
    for reference in references:
        kind=next(iter(reference['record']['Evidence']['evidence']))
        schemas.append({'Control':27,'OrderLifecycle':7,'TradeLifecycleV3':8}[kind])
    size=len(references);z=T([0]*size);zero=T([0],torch.int32);rows=T([list(range(size))]);on=rows>=0
    bank=EvidenceBank(T([[0]]*size),T([[0]]*size),semantic,T([[0]]*size),z,
        T([[0,0,0,0]]*size),z,torch.zeros_like(z,dtype=torch.int32),T(schemas))
    batch=SimpleNamespace(rows=rows,emitted=on,normalized=SimpleNamespace(outcomes=T([[OUT_EVIDENCE]*size])),
        private_sequences=metadata.private_sequences,record_ordinals=metadata.record_ordinals,
        batch_sequences=metadata.batch_sequences,context_slots=metadata.context_slots,request_slots=metadata.request_slots,
        item_ordinals=metadata.item_ordinals,run_slots=metadata.run_slots,platform_at=metadata.platform_at,received_at=metadata.received_at)
    closed=SimpleNamespace(record_refs=rows,emitted=on,fault=zero,delivered_sequences=metadata.delivery_sequences,available_at=metadata.available_at)
    return run(lambda:public_rows_from_closed(bank,[batch],closed,metadata.receive_rank))


def check(selected,source):
    publication=selected['publication'];legs=replace(publication.action.legs,receipt_known=publication.action.legs.present.clone())
    state=None;seen=set();rank=0;checks=[]
    for frame in source['frames']:
        incoming=[]
        for order in frame['raw_orders']:
            for reference in [order['accepted'],order['formed'],*order['trades']]:
                if reference is not None and position_key(reference) not in seen:
                    seen.add(position_key(reference));incoming.append(reference)
        incoming.sort(key=lambda r:(r['visibility']['Feedback']['delivery_sequence'],r['position']['record_ordinal']))
        if incoming:
            events,binding=production_rows(incoming,rank)
            state=PublicRegistryState(events) if state is None else run(lambda:state.receive(events))
            bindings=binding if 'bindings' not in locals() else run(lambda:bindings.receive(binding))
            rank+=len(incoming)
        identities=integers([int(platform_order_id(o['identity']).removeprefix('sim-')) for o in frame['orders']])
        present=T([[True]*len(frame['orders'])],torch.bool)
        directory,fault=run(lambda:freeze_intent_orders(legs,bindings,identities,present))
        assert fault.cpu().tolist()==[0]
        view=run(lambda:state.freeze_orders(directory,T([frame['at']])))
        assert view.fault.cpu().tolist()==[0]
        targets=exact(directory.original_target);commitments=exact(view.commitment)
        for i,order in enumerate(frame['orders']):
            assert directory.origin_known.cpu().tolist()[0][i]
            assert order['base_applicability'] is False and view.active_applicability_known.cpu().tolist()[0][i]
            assert not view.active_applicable.cpu().tolist()[0][i] and order['active']=='NotApplicable'
            oracle=order['commitment'];expected=Fraction(0) if oracle is None else Fraction(oracle['Known'])
            assert view.commitment_known.cpu().tolist()[0][i] and commitments[i]==expected,(frame['stage'],commitments,oracle)
            if frame['stage'] in ('accepted_incomplete_coverage','covered_pending_finality','covered_pending_applied'):assert commitments[i]==targets[i] and expected>0
            else:assert expected==0
        checks.append(dict(stage=frame['stage'],at=frame['at'],target_shares=[str(q) for q in targets],commitment_shares=[str(q) for q in commitments],newly_received=len(incoming)))
        print(json.dumps(dict(event='quote_registry_prefix',**checks[-1])),flush=True)
    return json.dumps(dict(actual_captured_target_and_original_cpu=True,quote_base_not_applicable=True,
        coverage_and_finality_preserve_original_target=True,all_final_zero=True,principal_fee_reserve_and_rng_unchanged=True,
        checks=checks,elapsed_seconds=time.perf_counter()-selected['started']))
