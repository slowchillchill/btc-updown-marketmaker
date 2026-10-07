"""真实Native配对请求/receipt/公开引用的连续承诺读面对照。

意图和回执只从实际引入前缀开始可见；原slow只作oracle。此有限来源适配器
不代表完整NOE流或GPU请求登记宿主，也不使用派生commitment初始化。
"""
from pathlib import Path
from dataclasses import replace
import torch
from queues import Numbers
from validate_base_execution import integers,rational,values
from registry import PublicRegistryEvents,PublicRegistryState,FrozenIntentOrders,BASE_ACCEPTED,QUOTE_ACCEPTED
from commitments import SubmittedLegs,SubmissionBindings,freeze_leg_commitments
from input_commitments import commitment_inputs,write_commitment_fields
from validate_registry import position_key,event_columns


def validate_commitments(groups):
    device='cuda'
    submitted={item['request']:item for group in groups for item in group['submitted_sources']}
    request_slots={name:index for index,name in enumerate(submitted)}
    legs=[(request,leg) for request,item in submitted.items() for leg in item['legs']]
    conditions=sorted({leg['condition'] for _,leg in legs})
    condition_slots={name:index for index,name in enumerate(conditions)}
    bindings={entry['market']['condition_id']:entry['assets'] for group in groups for entry in group['bound_markets']}
    reference_catalog={'contexts':{},'runs':{},'references':{}}
    identities={}
    origins={}
    all_references=[]
    for group in groups:
        for order in group['public_order_sources']:
            identities.setdefault(order['identity'],len(identities)+1)
            if order['origin'] is not None:
                origins[order['identity']]=order['origin']
            for name in ['accepted','formed','latest_lifecycle','latest_direct']:
                if order[name] is not None:
                    all_references.append(order[name])
            for ref in order['trades']:
                evidence=ref['record']['Evidence']['evidence']
                result=evidence.get('TradeLifecycleV3',evidence.get('TradeLifecycle'))['result']
                result=result.get('result',result)
                identities.setdefault(result['trade_identity'],len(identities)+1)
                all_references.append(ref)
    for ref in all_references:
        p=ref['position']
        reference_catalog['contexts'].setdefault(p['context_reference'],len(reference_catalog['contexts']))
        reference_catalog['runs'].setdefault(p['source_watermark']['run_id'],len(reference_catalog['runs']))
        key=position_key(ref)
        if key not in reference_catalog['references']:
            reference_catalog['references'][key]=(len(reference_catalog['references']),ref)
    orders=list(origins)
    state=PublicRegistryState(PublicRegistryEvents.empty(1,device))
    associations=SubmissionBindings.empty(1,device)
    seen=set()
    checked=0
    unbound=0
    frozen=[]
    for group in groups:
        introductions={item['request'] for item in group['submitted_sources']}
        incoming=[]
        public=set()
        for row in group['public_order_sources']:
            public.add(row['identity'])
            candidates=[row[name] for name in ['accepted','formed','latest_lifecycle','latest_direct'] if row[name] is not None]+row['trades']
            for ref in candidates:
                key=position_key(ref)
                if key not in seen:
                    seen.add(key)
                    incoming.append(ref)
        incoming.sort(key=lambda ref:(ref['visibility']['Feedback']['delivery_sequence'],ref['position']['record_ordinal']))
        rank=len(seen)-len(incoming)
        target=[]
        origin_known=[]
        receipts=[]
        for name in orders:
            origin=origins[name]
            target.append(next(leg['target'] for request,leg in legs if request==origin['request'] and leg['item']==origin['item']))
            introduced=origin['request'] in introductions
            origin_known.append(introduced)
            receipts.append(introduced)
        directory=FrozenIntentOrders(torch.tensor([[name in public for name in orders]],dtype=torch.bool,device=device),
            integers([identities[name] for name in orders]),torch.tensor([origin_known],dtype=torch.bool,device=device),
            rational(target),torch.zeros((1,len(orders)),dtype=torch.bool,device=device),torch.tensor([receipts],dtype=torch.bool,device=device))
        cutoff=torch.tensor([group['cutoff']['decision_time']['logical_time_utc_ms']],device=device)
        def receive():
            events=event_columns(incoming,identities,rank,request_slots,reference_catalog)
            source=SubmissionBindings(events.received & ((events.kinds==BASE_ACCEPTED)|(events.kinds==QUOTE_ACCEPTED)),
                events.request_slots,events.item_ordinals,events.private_sequences,events.receive_rank,events.orders,
                torch.ones_like(events.received),events.fault)
            return state.receive(events),associations.receive(source)
        state,associations=Numbers.run_exact_batch(receive)
        population=torch.tensor([[request in introductions for request,_ in legs]],dtype=torch.bool,device=device)
        metadata=[]
        for request,leg in legs:
            side=next((asset['outcome']['index'] for asset in bindings[leg['condition']] if asset['token_id']==leg['token']),-1)
            metadata.append(side)
        def freeze():
            submitted_legs=SubmittedLegs(population,population,
                torch.tensor([[request_slots[request] for request,_ in legs]],device=device),
                torch.tensor([[leg['item'] for _,leg in legs]],device=device),rational([leg['target'] for _,leg in legs]),
                torch.ones_like(population),torch.tensor([[condition_slots[leg['condition']] for _,leg in legs]],device=device),
                torch.tensor([metadata],device=device),torch.tensor([[side>=0 for side in metadata]],device=device))
            read=state.freeze_orders(directory,cutoff)
            return freeze_leg_commitments(submitted_legs,associations,directory,read)
        commitments=Numbers.run_exact_batch(freeze)
        assert commitments.fault.cpu().tolist()==[0]
        unbound+=int((commitments.present & ~commitments.has_order).any().cpu())
        frozen.append((commitments,values(commitments.remaining.numerator),values(commitments.remaining.denominator)))
        if not group['rows']:
            continue
        scopes=[[group['current']] if group['current'] is not None else [],group['old']]
        width=max(1,max(len(scope) for scope in scopes))
        scope_slots=torch.tensor([[[condition_slots[name] for name in scope]+[-1]*(width-len(scope)) for scope in scopes]],device=device)
        present=torch.tensor([[[True]*len(scope)+[False]*(width-len(scope)) for scope in scopes]],device=device)
        applicable=torch.tensor([[group['current'] is not None,True]],device=device)
        native=Numbers.run_exact_batch(lambda:commitment_inputs(commitments,scope_slots,present,applicable))
        assert native.fault.cpu().tolist()==[0]
        slow=write_commitment_fields(torch.zeros((1,433),device=device),native)
        torch.testing.assert_close(slow[0,98:110].cpu(),torch.tensor(group['rows'][0]['slow'][98:110]),rtol=1e-6,atol=1e-7)
        checked+=1
    for snapshot,numerator,denominator in frozen:
        assert values(snapshot.remaining.numerator)==numerator and values(snapshot.remaining.denominator)==denominator
    return {'original_Native_98_to_109_checked_prefixes':checked,'actual_receipt_without_public_order_prefixes':unbound,
        'derived_commitment_initialization':False,'continuous_public_submission_and_order_state':True,
        'scope_from_same_original_Native_freeze':True,'full_GPU_request_or_root':False}
