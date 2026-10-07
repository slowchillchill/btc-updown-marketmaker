#!/usr/bin/env python3
"""原NativeActor实际fixture逐前缀对照；取消输入为原完整冻结人口，不冒称GPU资格构建。"""
import argparse
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
import json
import os
import sys
import time
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','execution-normalization/gpu','ledger/gpu',
    'data-pipeline/gpu','shared-run/gpu','model-train/native-inputs/gpu')]
from queues import Numbers
from validate_base_execution import rational,integers,values
from rules import PublicRuleGpu
from cancel_objects import CancelObjectsRead
from actor_context import FrozenActorContext,publish_actor_context,_owned
from actor_support import DeviceNativeActor


def context(source):
    original=source['actor_context'];device='cuda'
    rules=PublicRuleGpu(original['rules'])
    assert rules.context['immediate_limit_min_principal_pusd']==original['immediate_limit_min_principal_pusd']
    def build():
        actual=rational([original['actual_available_pusd']]);maker=rational([original['maker_available_pusd']])
        comparison=actual.compare(maker)
        available=type(actual)(actual.numerator.select(comparison.order<=0,maker.numerator),
            actual.denominator.select(comparison.order<=0,maker.denominator))
        cash=(available.numerator*available.numerator.constant(1000000)).divrem(available.denominator)[0]
        cash=cash.select(cash.sign>0,cash.constant(0))
        ticks=rational(original['ticks']);units=(ticks.numerator*ticks.numerator.constant(10000)).divrem(ticks.denominator)[0]
        cap=[0,0]
        if original['single_side'] is not None:cap[original['single_side']]=int(original['cap_centishares'])
        intents=original['cancel_intents'];names=['CancelOrder','CancelOrders','CancelMarketOrders']
        kinds=[next(i for i,name in enumerate(names) if name in intent) for intent in intents]
        # 原冻结对象顺序与成员身份作为输入；不从GPU首次结果建立expected。
        objects=CancelObjectsRead(torch.ones((1,len(intents)),dtype=torch.bool,device=device),
            torch.tensor([kinds],dtype=torch.int64,device=device),
            torch.empty((1,len(intents),0),dtype=torch.bool,device=device),
            torch.empty((1,0),dtype=torch.int64,device=device),
            torch.full((1,len(intents)),-1,dtype=torch.int64,device=device),
            torch.full((1,len(intents)),-1,dtype=torch.int64,device=device),torch.zeros(1,dtype=torch.int32,device=device))
        return FrozenActorContext(torch.tensor([source.get('cutoff',0)],device=device),torch.zeros((1,3),dtype=torch.int64,device=device),
            torch.arange(2,device=device)[None,:],cash,integers(cap),
            torch.tensor([[Fraction(value)>0 for value in original['repair_gap_shares']]],device=device),
            torch.ones((1,2),dtype=torch.bool,device=device),torch.ones((1,2),dtype=torch.bool,device=device),units,
            torch.tensor([original['buy_enabled']],device=device),torch.ones(1,dtype=torch.bool,device=device),
            objects,rules,torch.zeros(1,dtype=torch.int32,device=device))
    result=Numbers.run_exact_batch(build)
    assert values(result.cash_micro)==[int(original['cash_micro'])]
    return result


def mask_for(support,stage):
    return getattr(support,{'Family':'family','QuantityBranch':'quantity','Variant(A)':'variant',
        'Variant(B)':'variant','Price(A)':'price','Price(B)':'price','CancelObject':'cancel'}[stage])


def check_accepted_only(path):
    """实际Accepted-only冷切片→公共账户/市场freeze→publisher；非完整收到流。"""
    from public_device_page import PreparedPublicPage,build_public_device_page
    from validate_portfolio_nav import cold_ledger
    from validate_registry import event_columns,position_key
    from registry import PublicRegistryEvents,PublicRegistryState,FrozenIntentOrders,BASE_ACCEPTED,QUOTE_ACCEPTED
    from commitments import SubmittedLegs,SubmissionBindings
    from public_ledger import PublicLedgerState
    from consumption import PendingRecords
    from closed import ClosedAdmission
    from cancel_objects import OrderFormation,CancelObjectDirectory,freeze_cancel_objects
    from input_account import account_inputs_at_prefix
    from input_history import SourceHistoryInputs
    from input_100ms import MarketInputs100ms,MarketFreezeMemory
    source=json.loads(path.read_text());artifact=path.parent;run=Numbers.run_exact_batch
    t=lambda values,dtype=torch.int64:torch.tensor(values,dtype=dtype,device='cuda')
    zero=t([0]);yes=t([True],torch.bool);cutoff=t([source['cutoff']]);start=zero-10000
    assert source['accepted_received']<source['cutoff']<=source['first_trade_received']
    assert not source['checkpoint']['successful_trades']
    build_public_device_page(Path(os.environ['B_GPU_COLUMNS_BINARY']),Path(source['compact']),artifact/'public-page')
    prepared=PreparedPublicPage(artifact/'public-page',Path(source['compact']))
    pm,market,reference,binance=prepared.install();assert len(pm.markets)==1
    history=SourceHistoryInputs(pm,market,reference,prepared.columns)
    memory=history.empty(1,start);before=int((pm.causal_visible_at<=cutoff[0]-100).sum().cpu())
    through=int((pm.causal_visible_at<=cutoff[0]).sum().cpu())
    if before:
        groups=torch.arange(before,device='cuda')
        memory,error=run(lambda:history.consume(groups,pm.causal_visible_at[groups],zero,start,memory))
        assert not error.any().cpu().item()
    memory=run(lambda:history.freeze(memory)).memory
    if through>before:
        groups=torch.arange(before,through,device='cuda')
        memory,error=run(lambda:history.consume(groups,pm.causal_visible_at[groups],zero,start,memory))
        assert not error.any().cpu().item()
    encoder=MarketInputs100ms(pm,market,reference,binance,history,
        PublicRuleGpu(source['actor_context']['rules']),prepared.columns,pm.causal_visible_at)
    endpoint=MarketFreezeMemory(zero+before-1,cutoff-100,None,None)
    while True:
        frozen=run(lambda:encoder.freeze(cutoff,zero,start,memory,endpoint))
        if frozen.resources_fit.all().cpu().item():break
        encoder.grow_resources(frozen.required.max(0).values.cpu().tolist())
    assert frozen.role.cpu().tolist()==[1] and frozen.fault.cpu().tolist()==[0]
    orders=source['public_order_sources'];assert len(orders)==2 and all(not order['trades'] for order in orders)
    identities={order['identity']:index for index,order in enumerate(orders)}
    requests={row['request']:index for index,row in enumerate(source['receipts'])}
    references={}
    for order in orders:
        for field in ('accepted','formed','latest_lifecycle','latest_direct'):
            value=order[field]
            if value is not None:references[position_key(value)]=value
    references=sorted(references.values(),key=lambda r:(r['visibility']['Feedback']['delivery_sequence'],r['position']['record_ordinal']))
    catalog={'references':{position_key(r):(i,r) for i,r in enumerate(references)},
        'contexts':{r['position']['context_reference']:0 for r in references},
        'runs':{r['position']['source_watermark']['run_id']:0 for r in references}}
    events=run(lambda:event_columns(references,identities,0,requests,catalog))
    registry=run(lambda:PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')).receive(events))
    bindings=run(lambda:SubmissionBindings.empty(1,'cuda').receive(SubmissionBindings(
        events.received&((events.kinds==BASE_ACCEPTED)|(events.kinds==QUOTE_ACCEPTED)),events.request_slots,
        events.item_ordinals,events.private_sequences,events.receive_rank,events.orders,
        events.received,events.fault,yes)))
    origin=[order['origin']['request'] for order in orders]
    original_intents={row['request']:row for row in source['receipts']}
    targets=[original_intents[key]['intent']['SubmitSingleBuy']['order']['order']['GtcLimit']['size_shares'] for key in origin]
    intents=run(lambda:FrozenIntentOrders(yes[:,None].expand(1,2),integers(list(range(2))),
        yes[:,None].expand(1,2),rational(targets),~yes[:,None].expand(1,2),yes[:,None].expand(1,2)))
    outcomes={asset['token_id']:asset['outcome']['index'] for asset in pm.assets}
    sides=[outcomes[original_intents[key]['intent']['SubmitSingleBuy']['order']['token_id']] for key in origin]
    legs=run(lambda:SubmittedLegs(yes[:,None].expand(1,2),yes[:,None].expand(1,2),t([[requests[key] for key in origin]]),
        zero[:,None].expand(1,2),rational(targets),yes[:,None].expand(1,2),zero[:,None].expand(1,2),t([sides]),yes[:,None].expand(1,2)))
    ledger=cold_ledger(source['checkpoint'],pm)
    empty=ledger.columns.enabled&False
    pending=run(lambda:PendingRecords.from_columns(empty,ledger.columns.row_refs,integers([0]*empty.numel()),
        integers([0]*empty.numel()),ledger.columns.row_refs,integers([0])))
    state=PublicLedgerState(pending,registry,ClosedAdmission.empty(ledger.columns),ledger,submission_bindings=bindings)
    account=run(lambda:state.freeze_account(intents,cutoff,submitted_legs=legs))
    args={name:yes for name in ('success_prefix_known','settlement_prefix_known','rebate_prefix_known',
        'economic_prefix_known','economic_liability_prefix_known')}
    native=run(lambda:account_inputs_at_prefix(account,market,zero,frozen.prefixes,start,zero[:,None].expand(1,2),
        yes[:,None].expand(1,2),t([asset['outcome']['index'] for asset in pm.assets]),row_role=frozen.role,
        executed_prefix_known=yes,ledger_prefix_known=~yes,**args))
    repair=native.inputs.repair
    assert repair.gap_known.cpu().tolist()==[[True,True]] and values(repair.gap_exact.numerator)==[0,0]
    assert not repair.cap_known[0,1].cpu().item() and not account.orders.commitment_known[0,1].cpu().item()
    sources=account.order_sources.formed
    formation=run(lambda:OrderFormation(account.orders.present,t([[0,1]]),zero[:,None].expand(1,2),
        intents.receipt_known,t([[original_intents[key]['receipt']['platform_arrived_at_utc_ms'] for key in origin]]),
        integers([original_intents[key]['receipt']['input_sequence'] for key in origin]),zero[:,None].expand(1,2),
        sources.present,sources.private_sequences,account.fault))
    frame=market.frame(frozen.prefixes)
    directory=CancelObjectDirectory(frame.admitted,market.times[:,0][None,:],zero[:,None],yes[:,None],
        torch.empty((1,0),dtype=torch.bool,device='cuda'),torch.empty((1,0,2),dtype=torch.int64,device='cuda'),
        torch.empty((1,0),dtype=torch.int64,device='cuda'))
    objects=run(lambda:freeze_cancel_objects(account.orders,formation,directory))
    context=run(lambda:publish_actor_context(account,native,encoder,frozen,zero,zero,torch.zeros((1,3),dtype=torch.int64,device='cuda'),objects))
    assert context.fault.cpu().tolist()==[0] and values(context.cash_micro)==[int(source['actor_context']['cash_micro'])]
    actor=DeviceNativeActor(context);support=run(lambda:actor.support(actor.initial()))
    assert support.fault.cpu().tolist()==[0] and support.family.cpu().tolist()==[source['actor_paths']['family_support']]
    assert support.family[0,0].cpu().item() and support.family[0,2].cpu().item()
    report={'cutoff':source['cutoff'],'source':'actual_GTC_receipts_and_Accepted_only',
        'gap_known':repair.gap_known.cpu().tolist(),'public_cap_known':repair.cap_known.cpu().tolist(),
        'original_family_support':support.family.cpu().tolist(),'actual_publish_actor_context':True,
        'continuous_received_stream':False}
    (artifact/'GPU-report.json').write_text(json.dumps(report,indent=2)+'\n');return report


def check(source,label):
    started=time.perf_counter();actor=DeviceNativeActor(context(source));reference=source['actor_paths']
    initial=actor.initial();family=Numbers.run_exact_batch(lambda:actor.support(initial))
    assert family.fault.cpu().tolist()==[0]
    assert family.family.cpu().tolist()==[reference['family_support']]
    checked=0
    for trace in reference['traces']:
        state=actor.initial()
        for index,step in enumerate(trace['steps']):
            print(json.dumps({'stage':'actor_support','source':label,'family':trace['family'],
                'prefix':index+1,'prefixes':len(trace['steps']),'elapsed_seconds':time.perf_counter()-started}),flush=True)
            support=Numbers.run_exact_batch(lambda:actor.support(state))
            assert support.fault.cpu().tolist()==[0],(label,step['stage'],support.fault.cpu().tolist())
            assert mask_for(support,step['stage']).cpu().tolist()==[step['mask']],(label,step['stage'])
            choice=torch.tensor([step['choice']],dtype=torch.int64,device='cuda')
            state=Numbers.run_exact_batch(lambda:actor.choose(state,choice));checked+=1
            assert state.fault.cpu().tolist()==[0]
        assert state.stage.cpu().tolist()==[7]
        if trace['quantity_centishares'] is not None:
            assert values(state.quantity)==[int(trace['quantity_centishares'])]
            assert state.variants.cpu().tolist()==[[{'GtcLimit':0,'FokMarket':1}.get(v,-1) for v in trace['variants']]]
            assert state.prices.cpu().tolist()==[[int(v) if v is not None else -1 for v in trace['price_indices']]]
        elif trace['family']>=3:
            assert state.cancel_index.cpu().tolist()==[trace['steps'][-1]['choice']]
    return {'source':label,'original_population':len(source['actor_context']['cancel_intents']),
        'family_support':reference['family_support'],'prefixes':checked,'elapsed_seconds':time.perf_counter()-started}


def check_mixed(source):
    # 原既有500百份、GTC/FOK混合路径，包含1pUSD费门槛的非连续价格支持反例。
    actor=DeviceNativeActor(context(source));path=source['a_price_support']['prefix_path']
    state=actor.initial()
    for choice in path:
        coordinate=torch.tensor([choice],device='cuda')
        state=Numbers.run_exact_batch(lambda:actor.choose(state,coordinate))
        assert state.fault.cpu().tolist()==[0]
    assert values(state.quantity)==[500]
    support=Numbers.run_exact_batch(lambda:actor.support(state))
    assert support.price.cpu().tolist()==[source['a_price_support']['mask']]
    def compute():
        support=actor.support(state)
        selected=actor.choose(state,torch.full_like(state.stage,18))
        support_b=actor.support(selected)
        selected=actor.choose(selected,torch.full_like(state.stage,19))
        return support,support_b,selected
    warm=Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
    assert warm.fits.cpu().item()
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(compute,word_capacity=16,device='cuda')
    graph.replay();torch.cuda.synchronize()
    first,second,selected=captured.value
    assert captured.fits.cpu().item() and selected.fault.cpu().tolist()==[0]
    assert first.price.cpu().tolist()==[source['a_price_support']['mask']]
    assert second.price[0].nonzero().flatten().cpu().tolist()[0]==19
    assert selected.stage.cpu().tolist()==[7] and selected.variants.cpu().tolist()==[[0,1]]
    assert selected.prices.cpu().tolist()==[[19,20]] and values(selected.quantity)==[500]
    saved=_owned(selected)
    # 下一次实际重放覆盖同一发布源；这是组件暂存覆写，不是自然业务路径。
    state.quantity.words.copy_(integers([400]).with_capacity(state.quantity.words.shape[1]).words)
    graph.replay();torch.cuda.synchronize()
    assert values(captured.value[2].quantity)==[400]
    assert values(saved.quantity)==[500] and saved.prices.cpu().tolist()==[[19,20]]
    return {'quantity_centishares':500,'variants':['GTC','FOK'],'prices':[19,20],
        'CUDA_capture_replay':True,'owned_selected_prefix':True,'required_words':int(captured.required_words.cpu())}


def main():
    parser=argparse.ArgumentParser();parser.add_argument('--sources',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();started=time.perf_counter()
    original=json.loads((args.sources/'buy.json').read_text())
    reports=[check(original,'original-buy')]
    for source in sorted(args.sources.glob('cancel-*.json')):reports.append(check(json.loads(source.read_text()),source.name))
    assert len(reports)==3 and {row['original_population'] for row in reports}=={1,257,259}
    for source in sorted(args.sources.glob('repair-*.json')):reports.append(check(json.loads(source.read_text()),source.name))
    assert len(reports)==5 and all(row['family_support'][1] for row in reports[3:])
    encoding=json.loads((args.sources/'encoding.json').read_text());actor=DeviceNativeActor(context(encoding))
    prefix=Numbers.run_exact_batch(lambda:actor.choose(actor.initial(),torch.full_like(actor.context.cutoff,2)))
    assert prefix.fault.cpu().tolist()==[0]
    assert values(prefix.root_upper)==[int(encoding['root_upper'])] and int(encoding['root_upper'])>actor.encoding_limit
    assert values(prefix.hi)==values(prefix.root_upper)
    mask=Numbers.run_exact_batch(lambda:actor.support(prefix)).quantity
    assert mask.cpu().tolist()==[encoding['quantity_support']]
    report={'status':'PASS','sources':reports,'mixed_path':check_mixed(original),
        'accepted_only':check_accepted_only(args.sources/'accepted-only/reference.json'),
        'encoding_original_cash_root_preserved':True,'elapsed_seconds':time.perf_counter()-started,
        'complete_model_sampler_intent_host':False}
    args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report),flush=True)


if __name__=='__main__':main()
