"""原259/257对象→公共来源/原意图关联→Native184的有限设备组件验证。

订单资金从原同cutoff公共OrderFunds冷读面安装；不宣称推进新的资金业务。
NOE引用是原实际公开引用切片，不冒充完整首次领取流或连续账户宿主。
"""
import argparse,json,sys,time
from pathlib import Path
from dataclasses import replace
from types import SimpleNamespace
from fractions import Fraction
import torch
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','ledger/gpu','execution-normalization/gpu',
    'data-pipeline/gpu','shared-run/gpu','model-train/native-inputs/gpu')]
from queues import Numbers
from validate_base_execution import integers,rational
from validate_registry import (position_key,event_columns,cancel_object_columns,compare_objects,
    scope_columns,platform_order_id,completion_columns)
from registry import PublicRegistryState,PublicRegistryEvents,PublicCancellationState,BASE_ACCEPTED,QUOTE_ACCEPTED
from commitments import SubmittedLegs,SubmissionBindings
from intent_terms import SubmittedBuyTerms,freeze_intent_orders
from cancel_requests import CancellationScopes,CancellationTargets,freeze_cancel_snapshot,freeze_scoped_cancel_requests
from cancel_objects import freeze_cancel_objects
from validate_cancel_source import prepare_cancel_source,receive_cancel_source
from cancel_projection import cancellation_order_events
from order_funds import OrderFunds,OrderFundsRead
from input_cancel import cancel_inputs
from model_train.gpu_policy import Policy100ms,ModelShape100ms


def leg_terms(intent):
    kind,body=next(iter(intent.items()))
    orders=[body['order']] if kind=='SubmitSingleBuy' else body['orders']
    result=[]
    for item,leg in enumerate(orders):
        name,terms=next(iter(leg['order'].items()))
        variant=['GtcLimit','GtdLimit','FokMarket','FakMarket'].index(name)
        price=Fraction(terms['limit_price_pusd_per_share'] if variant<2 else terms['worst_price_pusd_per_share'])
        spend=Fraction(terms.get('spend_pusd','0'))
        target=Fraction(terms['size_shares']) if variant<2 else spend/price
        result.append(dict(item=item,token=leg['token_id'],target=target,price=price,spend=spend,
            variant=variant,post=terms.get('post_only',False),expiry=terms.get('expires_at_utc_sec',0)))
    return result


def validate(frames,output):
    started=time.perf_counter();device='cuda';run=Numbers.run_exact_batch
    tensor=lambda data,dtype=torch.int64:torch.tensor(data,dtype=dtype,device=device)
    # 工程模型只验证现有184参数组件，不形成正式profile或完整采样。
    torch.manual_seed(181184);model=Policy100ms(ModelShape100ms(1,32)).cuda().eval()
    object_head=model.stages()['action_objects']
    # 冷字典可以预分配，但每个cutoff的present/receipt只来自当时实际公开来源。
    universe=sorted({platform_order_id(o['identity']) for f in frames for o in f['raw_orders']})
    catalog=list(dict.fromkeys(o['identity'] for f in frames for o in f['raw_orders']))
    identities={key:int(platform_order_id(key).removeprefix('sim-')) for key in catalog}
    references={};contexts={};runs={}
    for frame in frames:
        for order in frame['raw_orders']:
            for ref in [order[k] for k in ('accepted','formed','latest_lifecycle','latest_direct','latest_cancel')]+order['trades']:
                if ref is None:continue
                references.setdefault(position_key(ref),ref)
                payload=ref['record']['Evidence']['evidence']
                if 'TradeLifecycleV3' in payload or 'TradeLifecycle' in payload:
                    value=payload.get('TradeLifecycleV3',payload.get('TradeLifecycle'))['result'];value=value.get('result',value)
                    identities[value['trade_identity']]=int(json.loads(value['trade_identity'].split(':',1)[1])[-1]['Trade']['source_trade_id'].removeprefix('sim-'))
        for obj in frame['cancel_objects']:
            latest=obj['latest']
            if isinstance(latest,dict) and latest.get('Known') is not None:
                references.setdefault(position_key(latest['Known']),latest['Known'])
    for ref in references.values():
        contexts.setdefault(ref['position']['context_reference'],len(contexts))
        runs.setdefault(ref['position']['source_watermark']['run_id'],len(runs))
    ref_catalog=dict(contexts=contexts,runs=runs,references={key:(i,ref) for i,(key,ref) in enumerate(references.items())})
    cancel_refs=[r for r in references.values() if any(name in r['record']['Evidence']['evidence'].get('Control',{}).get('result',{})
        for name in ('Cancellation','CancellationRequestCompleted'))]
    request_ids={r['request']:i+1 for f in frames for i,r in enumerate(f['cancel_source_requests'])}
    source=prepare_cancel_source(cancel_refs,request_ids,universe,ref_catalog)
    next_slot=len(cancel_refs)
    for key,ref in references.items():
        if ref not in cancel_refs:ref_catalog['references'][key]=(next_slot,ref);next_slot+=1
    buys={r['request']:r for f in frames for r in f['buy_source_receipts']};buy_ids={r:i for i,r in enumerate(buys)}
    legs=[(request,t) for request,receipt in buys.items() for t in leg_terms(receipt['intent'])]
    blank=tensor([[]],torch.bool)
    cancel=run(lambda:PublicCancellationState(integers(list(request_ids.values())),tensor([[False]*len(request_ids)],torch.bool),
        completion_columns(None,request_ids,ref_catalog)))
    targets=run(lambda:CancellationTargets(blank,integers([]),CancellationScopes(blank,tensor([[]]),
        torch.zeros((1,0,len(universe)),dtype=torch.bool,device=device)),tensor([[]]),tensor([[]]),tensor([0],torch.int32)))
    registry=PublicRegistryState(PublicRegistryEvents.empty(1,device));bindings=SubmissionBindings.empty(1,device)
    seen=set();reports=[];saved=None;old_values=None
    for ordinal,frame in enumerate(frames,1):
        print(json.dumps(dict(stage='cutoff_started',cutoff=frame['logical_at'],index=ordinal,total=len(frames),
            elapsed_seconds=time.perf_counter()-started)),flush=True)
        markets=[m['market'] for m in frame['bound_markets']];market_ids={m['condition_id']:i for i,m in enumerate(markets)}
        assets={m['market']['condition_id']:m['assets'] for m in frame['bound_markets']}
        raw={o['identity']:o for o in frame['raw_orders']}
        incoming=[]
        for o in frame['raw_orders']:
            for ref in [o[k] for k in ('accepted','formed','latest_lifecycle','latest_direct','latest_cancel')]+o['trades']:
                if ref is not None and position_key(ref) not in seen:seen.add(position_key(ref));incoming.append(ref)
        for o in frame['cancel_objects']:
            latest=o['latest'];ref=latest.get('Known') if isinstance(latest,dict) else None
            if ref is not None and position_key(ref) not in seen:seen.add(position_key(ref));incoming.append(ref)
        incoming.sort(key=lambda r:(r['visibility']['Feedback']['delivery_sequence'],r['position']['record_ordinal']))
        canceled=[r for r in incoming if r in cancel_refs];ordinary=[r for r in incoming if r not in cancel_refs]
        cancel=cancel.register(tensor([[name in {r['request'] for r in frame['cancel_source_requests']} for name in request_ids]],torch.bool))
        cancel,targets,cancel_rows=receive_cancel_source(source,canceled,cancel,targets,
            integers([int(x.removeprefix('sim-')) for x in universe]),tensor([[x in {platform_order_id(o['identity']) for o in frame['raw_orders']} for x in universe]],torch.bool))
        rank=len(seen)-len(incoming)
        def receive():
            events=event_columns(ordinary,identities,rank,buy_ids,ref_catalog)
            events_cancel=cancellation_order_events(cancel_rows,torch.arange(rank,rank+len(canceled),device=device)[None,:])
            next_registry=registry.receive(events).receive(events_cancel)
            next_bindings=bindings.receive(SubmissionBindings(events.received&((events.kinds==BASE_ACCEPTED)|(events.kinds==QUOTE_ACCEPTED)),
                events.request_slots,events.item_ordinals,events.private_sequences,events.receive_rank,events.orders,torch.ones_like(events.received),events.fault))
            return next_registry,next_bindings
        registry,bindings=run(receive)
        cutoff=tensor([frame['logical_at']]);population=tensor([[name in raw for name in catalog]],torch.bool)
        terms=run(lambda:SubmittedBuyTerms(tensor([[t['variant'] for _,t in legs]]),tensor([[t['post'] for _,t in legs]],torch.bool),
            rational([t['price'] for _,t in legs]),rational([t['spend'] for _,t in legs]),tensor([[t['expiry'] for _,t in legs]])))
        introduced={r['request'] for r in frame['buy_source_receipts']}
        submitted=run(lambda:SubmittedLegs(tensor([[r in introduced for r,_ in legs]],torch.bool),tensor([[r in introduced for r,_ in legs]],torch.bool),
            tensor([[buy_ids[r] for r,_ in legs]]),tensor([[t['item'] for _,t in legs]]),rational([t['target'] for _,t in legs]),
            tensor([[True]*len(legs)],torch.bool),terms=terms))
        intents,error=run(lambda:freeze_intent_orders(submitted,bindings,integers([identities[k] for k in catalog]),population))
        assert error.cpu().tolist()==[0]
        known=intents.origin_known.cpu().tolist()[0];at=intents.term_leg_slots.cpu().tolist()[0]
        for index,key in enumerate(catalog):
            original=raw.get(key,{}).get('origin')
            assert known[index]==(original is not None),(frame['stage'],'实际Accepted原意图关联',key)
            if original is not None:
                request,term=legs[at[index]]
                assert (request,term['item'])==(original['request'],original['item'])
                assert buys[request]['intent']==original['intent'],'同身份原正文保持'
        orders=run(lambda:registry.freeze_orders(intents,cutoff));sources=run(lambda:registry.freeze_order_sources(intents,cutoff))
        adapted=dict(frame,public_market_sources=markets,
            cancel_objects=[dict(kind=o['kind'],members=o['members'],**({'condition':o['condition']} if o['kind']==2 else {})) for o in frame['cancel_objects']],
            buy_source_receipts=[dict(r,terms=[dict(t,target=str(t['target'])) for t in leg_terms(r['intent'])]) for r in frame['buy_source_receipts']])
        formation,directory=run(lambda:cancel_object_columns(adapted,catalog,identities,registry.events))
        objects=run(lambda:freeze_cancel_objects(orders,formation,directory));compare_objects(adapted,objects,directory,catalog)
        # 资金输入是原公开OrderFunds冻结读面，不从Native184的expected逆填。
        original_funds={f['identity']:f for f in frame['order_funds']}
        funds=run(lambda:OrderFunds.empty(1,max(1,len(original_funds)),1,0,device))
        keys=list(original_funds)
        assert keys and all(k in identities for k in keys),'该原夹具有非空公开资金目录'
        funds=run(lambda:replace(funds,keys=integers([identities[k] for k in keys]),occupied=tensor([[True]*len(keys)],torch.bool)))
        def read_amount(name):return rational([original_funds[k][name].get('Known','0') if isinstance(original_funds[k][name],dict) else '0' for k in keys])
        funds_read=run(lambda:OrderFundsRead(read_amount('principal'),read_amount('fee'),tensor([['Known' in original_funds[k]['principal'] and 'Known' in original_funds[k]['fee'] for k in keys]],torch.bool),
            integers([original_funds[k]['last_sequence'] or 0 for k in keys]),
            tensor([[original_funds[k]['last_sequence'] is not None for k in keys]],torch.bool),tensor([0],torch.int32)))
        locations=tensor([[market_ids.get(raw.get(k,{}).get('condition'),-1) for k in catalog]])
        sides=tensor([[next((a['outcome']['index'] for a in assets.get(raw.get(k,{}).get('condition'),[]) if a['token_id']==raw.get(k,{}).get('token')),-1) for k in catalog]])
        request_scopes=scope_columns([next((r['scope'] for r in frame['cancel_source_requests'] if r['request']==name),{'orders':[]}) for name in request_ids],universe,market_ids)
        membership=torch.zeros((1,objects.present.shape[1],len(universe)),dtype=torch.bool,device=device)
        for index,key in enumerate(catalog):membership[:,:,universe.index(platform_order_id(key))]=objects.members[:,:,index]
        scopes=CancellationScopes(objects.kinds==2,objects.markets,membership)
        universe_locations=tensor([[market_ids.get(next((o['condition'] for o in frame['raw_orders'] if platform_order_id(o['identity'])==k),None),-1) for k in universe]])
        # 此夹具每个仍必要Pair至少有一腿未闭合，原快照保留其另一腿关联。
        snapshot=run(lambda:freeze_cancel_snapshot(cancel,request_scopes,scopes,objects.present,universe_locations,universe_locations>=0,
            tensor([[k in {platform_order_id(o['identity']) for o in frame['raw_orders']} for k in universe]],torch.bool),targets,cutoff))
        requests=run(lambda:freeze_scoped_cancel_requests(snapshot.state,request_scopes,scopes,universe_locations,snapshot.location_known,snapshot.targets,cutoff))
        conditions=tensor([[list(range(len(markets)))]]).reshape(1,1,len(markets))
        current=[market_ids[frame['current']]] if frame['current'] is not None else []
        old=[market_ids[k] for k in frame['old']];scope_width=max(1,len(current),len(old))
        scope_conditions=tensor([[current+[-1]*(scope_width-len(current)),old+[-1]*(scope_width-len(old))]])
        scope_present=scope_conditions>=0
        def encode():return cancel_inputs(objects,directory,intents,orders,sources,funds,funds_read,source[0],requests,cutoff,
            locations,sides,conditions[:,0],scope_conditions,scope_present)
        eager=run(encode);expected=tensor([frame['expected']],torch.float32)
        assert eager.fault.cpu().tolist()==[0]
        torch.testing.assert_close(eager.values[:,eager.present[0]],expected,rtol=1e-6,atol=1e-7)
        expected_padded=torch.zeros_like(eager.values);expected_padded[:,eager.present[0]]=expected
        actual_features=eager.values.detach().clone().requires_grad_(True)
        original_features=expected_padded.detach().clone().requires_grad_(True)
        gradients=[];embeddings=[]
        for features in (actual_features,original_features):
            encoded=object_head(features,eager.present);embeddings.append(encoded)
            gradients.append(torch.autograd.grad(encoded.square().sum(),(features,)+tuple(object_head.parameters())))
        torch.testing.assert_close(*embeddings,rtol=1e-5,atol=1e-6)
        for actual,original in zip(*gradients):torch.testing.assert_close(actual,original,rtol=1e-5,atol=1e-6)
        def encode_model():
            read=encode()
            return read,object_head(read.values,read.present)
        with torch.no_grad():warm=Numbers.device_exact_batch(encode_model,word_capacity=16,device=device)
        assert warm.fits.cpu().item()
        graph=torch.cuda.CUDAGraph()
        with torch.no_grad(),torch.cuda.graph(graph):captured=Numbers.device_exact_batch(encode_model,word_capacity=16,device=device)
        graph.replay();read,encoded=captured.value
        assert captured.fits.cpu().item() and read.fault.cpu().tolist()==[0]
        torch.testing.assert_close(read.values,eager.values,rtol=0,atol=0)
        torch.testing.assert_close(encoded,embeddings[0],rtol=0,atol=0)
        owned=read.snapshot();original=owned.values.clone()
        old_fields=(owned.present.clone(),owned.physical_slots.clone(),owned.fault.clone())
        # 组件级覆写同一个实际发布源，时间值和人口都改变；不是新增业务事件。
        cutoff.add_(100);objects.present.zero_();graph.replay();read,encoded=captured.value
        assert captured.fits.cpu().item() and read.fault.cpu().tolist()==[0]
        assert not torch.equal(read.values,original) and not read.present.any().cpu().item()
        assert torch.equal(owned.values,original)
        for field,wanted in zip((owned.present,owned.physical_slots,owned.fault),old_fields):assert torch.equal(field,wanted)
        if saved is not None:
            for field,wanted in zip((saved.values,saved.present,saved.physical_slots,saved.fault),old_values):assert torch.equal(field,wanted)
        saved=owned;old_values=(original,*old_fields)
        reports.append(dict(cutoff=frame['logical_at'],object_count=int(eager.present.sum().cpu()),
            physical_width=eager.present.shape[1],all184_original_native=True,actual_accepted_origin=True,
            capture_and_owned_replay=True,model_input_parameter_gradients=True,continuous_account=False,
            word_capacity=16,required_words=int(captured.required_words.cpu()),fits=bool(captured.fits.cpu())))
        print(json.dumps(dict(stage='cutoff_finished',**reports[-1],elapsed_seconds=time.perf_counter()-started)),flush=True)
    report=dict(status='PASS',checkpoints=reports,elapsed_seconds=time.perf_counter()-started,
        funds_boundary='actual_frozen_public_order_funds_cold_read',complete_cancel_sampler=False,
        engineering_model=dict(layers=1,hidden=32,seed=181184),legacy_GTD_FAK_numeric_fixture=False)
    output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report),flush=True)


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--sources',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();validate([json.loads((args.sources/f'cancel-input-{at}.json').read_text()) for at in (36000,56000)],args.output)
