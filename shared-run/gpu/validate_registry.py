"""原SharedExecutionV2公开引用的有限连续订单读面对照。

冷适配器仅解码原引用和冻结意图，不用原matched/active/commitment初始化状态。
未导出的中间反馈、完整取消请求和全根时钟不在此组件的覆盖范围内。
"""
from dataclasses import replace
from pathlib import Path
from fractions import Fraction
import json
import sys
import time
import math
import torch

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT/'sim-system/gpu'), str(ROOT/'ledger/gpu'), str(ROOT/'execution-normalization/gpu'), str(Path(__file__).resolve().parent)]
sys.path.append(str(ROOT/'model-train/native-inputs/gpu'))
sys.path.append(str(ROOT/'data-pipeline/gpu'))
from registry import (PublicRegistryEvents, FrozenIntentOrders, PublicRegistryState,
    BASE_ACCEPTED, QUOTE_ACCEPTED, ORDER_LIFECYCLE, TRADE, CANCEL_TARGET,
    PublicCancellationState, CancellationCompletions)
from validate_base_execution import integers, rational, values
from commitments import SubmittedLegs,SubmissionBindings,freeze_leg_commitments
from cancel_objects import OrderFormation,CancelObjectDirectory,freeze_cancel_objects,sim_identity_order
from cancel_requests import CancellationScopes,CancellationTargets,freeze_scoped_cancel_requests,freeze_cancel_snapshot
from registry import identity_matches
from queues import Numbers
from input_orders import order_inputs


def amounts(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def position_key(reference):
    position = reference['position']
    watermark = position['source_watermark']
    return (position['context_reference'], watermark['run_id'], watermark['projection_batch_sequence'], position['record_ordinal'])


def event_columns(references, identities, rank_start, requests, catalog):
    # 这是验证器冷边界的原强类型JSON解码，不是生产逐事件CPU业务循环。
    rows = []
    for reference in references:
        evidence = reference['record']['Evidence']
        source = evidence['source_order']
        row = dict(kind=ORDER_LIFECYCLE, order=0, trade=0, private=source['sequence']['PrivateFeedback']['private_feedback_sequence'],
            delivery=reference['visibility']['Feedback']['delivery_sequence'], batch=reference['position']['source_watermark']['projection_batch_sequence'], available=reference['visibility']['Feedback']['available_at_utc_ms'], ordinal=reference['position']['record_ordinal'],
            platform=0,received_at=0,request=-1,item=-1,matched='0', opened='0', quantity='0', role=0, disposition=0, formed_matched=False, formation=False, canceled=False, closed=False)
        original_source=evidence['source_reference']['record']
        if 'DirectResponse' in original_source:
            times=original_source['DirectResponse']
            row.update(platform=times['platform_responded_at_utc_ms'],received_at=times['response_received_at_utc_ms'])
        elif 'OrderLifecycle' in original_source:
            times=original_source['OrderLifecycle']
            row.update(platform=times['platform_event_at_utc_ms'],received_at=times['feedback_received_at_utc_ms'])
        payload = evidence['evidence']
        if 'Control' in payload:
            control = payload['Control']['result']
            if 'Submission' in control:
                accepted = control['Submission']['outcome']['Accepted']
                row['order'] = identities[accepted['order']['order_identity']]
                association=evidence['source_reference']['record']['DirectResponse']['association']['SubmissionItem']
                row.update(request=requests[association['global_request_id']],item=association['batch_item_ordinal'])
                measurement = accepted['measurement']
                if 'Base' in measurement:
                    row.update(kind=BASE_ACCEPTED, matched=measurement['Base']['matched_base_quantity_shares'],
                        opened=measurement['Base']['open_base_quantity_shares'])
                else:
                    row.update(kind=QUOTE_ACCEPTED, matched=measurement['QuoteBudget']['actual_matched_base_quantity_shares'])
            elif 'Cancellation' in control:
                outcome = control['Cancellation']
                row.update(kind=CANCEL_TARGET, order=identities[outcome['order_identity']], canceled=outcome['outcome']=='Canceled')
            else:
                raise ValueError('该订单组件冷适配器未包含此Control')
        elif 'OrderLifecycle' in payload:
            result = payload['OrderLifecycle']['result']
            event = result['event']
            row.update(order=identities[result['order_identity']], formation=isinstance(event,dict) and 'Accepted' in event, formed_matched=isinstance(event,dict) and event.get('Accepted',{}).get('state')=='Matched',
                closed=event in ['CallerCanceled','GtdExpired','VenueDustTerminal'] if isinstance(event,str) else 'MarketResolvedCanceled' in event)
        elif 'TradeLifecycleV3' in payload or 'TradeLifecycle' in payload:
            result = payload.get('TradeLifecycleV3',payload.get('TradeLifecycle'))['result']
            result = result.get('result',result)
            row.update(kind=TRADE,order=identities[result['order_identity']],trade=identities[result['trade_identity']],
                quantity=result['base_quantity_shares'],role=0 if result['liquidity_role']=='Taker' else 1,
                disposition=['PendingUnapplied','PendingApplied','FinalSuccess','FinalFailureUnapplied','FinalFailureCompensated'].index(result['economic_disposition']))
        else:
            raise ValueError('该订单组件冷适配器未包含此证据')
        rows.append(row)
    active = [True]*len(rows)
    if not rows:
        rows=[dict(platform=0,received_at=0,kind=0,order=0,trade=0,private=0,delivery=0,batch=0,available=0,ordinal=0,request=-1,item=-1,matched='0',opened='0',quantity='0',role=0,disposition=0,formed_matched=False,formation=False,canceled=False,closed=False)]
        active=[False]
    tensor = lambda name,dtype=torch.int64:torch.tensor([[row[name] for row in rows]],dtype=dtype,device='cuda')
    integer = lambda name:integers([row[name] for row in rows])
    ratio = lambda name:rational([row[name] for row in rows])
    return PublicRegistryEvents(torch.tensor([active],dtype=torch.bool,device='cuda'), tensor('kind'),
        integer('order'),integer('trade'),integer('private'),integer('delivery'),integer('ordinal'),integer('batch'),
        torch.tensor([[catalog['contexts'][r['position']['context_reference']] for r in references] if references else [0]],device='cuda'),
        torch.tensor([[catalog['runs'][r['position']['source_watermark']['run_id']] for r in references] if references else [0]],device='cuda'),tensor('request'),tensor('item'),
        torch.tensor([[catalog['references'][position_key(r)][0] for r in references] if references else [-1]],device='cuda'),tensor('available'),
        torch.arange(rank_start,rank_start+len(rows),device='cuda')[None,:],ratio('matched'),ratio('opened'),ratio('quantity'),
        tensor('role'),tensor('disposition'),tensor('formed_matched',torch.bool),tensor('formation',torch.bool),tensor('canceled',torch.bool),
        tensor('closed',torch.bool),tensor('platform'),tensor('received_at'),torch.zeros(1,dtype=torch.int32,device='cuda'))


def completion_columns(reference,requests,catalog):
    active=reference is not None
    if active:
        evidence=reference['record']['Evidence']
        completed=evidence['evidence']['Control']['result']['CancellationRequestCompleted']['result']
        source=evidence['source_reference']['record']['DirectResponse']
        request=source['association']['Request']['global_request_id']
        values=[completed[name] for name in ['canceled_count','not_canceled_no_effect_count','evidence_unknown_no_effect_count']]
        delivery=reference['visibility']['Feedback']['delivery_sequence']
        ordinal=reference['position']['record_ordinal']
        available=reference['visibility']['Feedback']['available_at_utc_ms']
        batch=reference['position']['source_watermark']['projection_batch_sequence']
        private=evidence['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
        responded=source['platform_responded_at_utc_ms']
        received=source['response_received_at_utc_ms']
    else:
        request=None;values=[0,0,0];delivery=ordinal=available=responded=received=batch=private=0
    tensor=lambda value,dtype=torch.int64:torch.tensor([[value]],dtype=dtype,device='cuda')
    return CancellationCompletions(tensor(active,torch.bool),integers([requests.get(request,0)]),integers([delivery]),
        integers([ordinal]),integers(values),tensor(available),tensor(responded),tensor(received),torch.zeros(1,dtype=torch.int32,device='cuda'),
        tensor(catalog["references"][position_key(reference)][0] if active else -1),
        tensor(catalog["contexts"].get(reference["position"]["context_reference"],0) if active else 0),
        tensor(catalog["runs"].get(reference["position"]["source_watermark"]["run_id"],0) if active else 0),
        integers([batch]),integers([private]))


def cancel_object_columns(frame,catalog,identities,events):
    # 原输入source、receipt和formed NOE是冷解码依据；cancel_objects只作后验对照。
    raw={row['identity']:row for row in frame['raw_orders']}
    markets=frame['public_market_sources']
    market_ids={market['condition_id']:index for index,market in enumerate(markets)}
    business_sequences=integers([int(platform_order_id(identity).removeprefix('sim-')) for identity in catalog])
    identity_rank,identity_fault=sim_identity_order(business_sequences,torch.ones((1,len(catalog)),dtype=torch.bool,device='cuda'))
    receipts={receipt['request']:receipt for receipt in frame['buy_source_receipts']}
    processed=[];times=[];sequences=[];items=[];formed=[];source_sequence=[];locations=[]
    for identity in catalog:
        order=raw.get(identity)
        origin=None if order is None else order['origin']
        accepted=None if order is None else order['accepted']
        has_processing=accepted is not None and origin is not None and origin['request'] in receipts
        processed.append(has_processing)
        times.append(accepted['record']['Evidence']['evidence']['Control']['result']['Submission']['outcome']['Accepted']['effective_processing_at_utc_ms'] if has_processing else 0)
        sequences.append(receipts[origin['request']]['input_sequence'] if has_processing else 0)
        items.append(origin['item'] if has_processing else 0)
        source=None if order is None else order['formed']
        formed.append(source is not None)
        source_sequence.append(source['record']['Evidence']['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence'] if source is not None else 0)
        locations.append(market_ids.get(None if order is None else order['condition'],-1))
    tensor=lambda values,dtype=torch.int64:torch.tensor([values],dtype=dtype,device='cuda')
    same,error=identity_matches(integers([identities[key] for key in catalog]),events.orders,1,len(catalog),events.received.size(1))
    evidence=(same&events.received[:,None,:]).any(2)
    formation=OrderFormation(evidence,identity_rank,tensor(locations),
        tensor(processed,torch.bool),tensor(times),integers(sequences),tensor(items),tensor(formed,torch.bool),integers(source_sequence),events.fault|identity_fault)
    pairs=[]
    for request in sorted(receipts):
        receipt=receipts[request]
        if len(receipt['terms'])!=2:continue
        members=[]
        for ordinal in range(2):
            member=next((identity for identity,order in raw.items() if order['origin'] is not None and
                order['origin']['request']==request and order['origin']['item']==ordinal and order['accepted'] is not None),None)
            members.append(catalog.index(member) if member is not None else -1)
        pairs.append(members)
    pair_members=torch.tensor([pairs],dtype=torch.int64,device='cuda').reshape(1,len(pairs),2)
    directory=CancelObjectDirectory(tensor([True]*len(markets),torch.bool),tensor([market['window']['start_at_utc_ms'] for market in markets]),
        tensor([sorted(market_ids).index(market['condition_id']) for market in markets]),tensor([True]*len(markets),torch.bool),
        (pair_members>=0).all(2),pair_members,tensor(list(range(len(pairs)))))
    return formation,directory


def compare_objects(frame,result,directory,catalog):
    assert result.fault.cpu().tolist()==[0],frame['stage']
    ranks=result.formation_ranks.cpu().tolist()[0]
    population=result.present.cpu().tolist()[0]
    members=result.members.cpu().tolist()[0]
    kinds=result.kinds.cpu().tolist()[0]
    pair_slots=result.origin_pairs.cpu().tolist()[0]
    market_slots=result.markets.cpu().tolist()[0]
    pair_members=directory.paired_members.cpu().tolist()[0]
    actual=[]
    for index,active in enumerate(population):
        if not active:continue
        kind=kinds[index]
        if kind==1:
            selected=[catalog[item] for item in pair_members[pair_slots[index]]]
        else:
            selected=[catalog[item] for item in sorted(range(len(catalog)),key=lambda item:ranks[item]) if members[index][item]]
        row={'kind':kind,'members':selected}
        if kind==2:row['condition']=frame['public_market_sources'][market_slots[index]]['condition_id']
        actual.append(row)
    oracle=[{key:value for key,value in row.items() if key!='origin'} for row in frame['cancel_objects']]
    assert actual==oracle,(frame['stage'],'完整撤单对象/形成顺序',actual,oracle)


def platform_order_id(identity):
    return json.loads(identity.split(':',1)[1])[-1]['Order']['source_order_id']


def scope_columns(scopes,universe,markets):
    tensor=lambda values,dtype=torch.int64:torch.tensor([values],dtype=dtype,device='cuda')
    return CancellationScopes(tensor(['market' in scope for scope in scopes],torch.bool),
        tensor([markets.get(scope.get('market'),-1) for scope in scopes]),
        torch.tensor([[[order in scope.get('orders',[]) for order in universe] for scope in scopes]],dtype=torch.bool,device='cuda').reshape(1,len(scopes),len(universe)))


def scoped_freeze(frame,cancel,request_ids,target_rows,universe,objects,catalog,view,legs_view,leg_keys,device_targets=None):
    markets={market['condition_id']:index for index,market in enumerate(frame['public_market_sources'])}
    source_requests={source['request']:source['scope'] for source in frame['cancel_source_requests']}
    requests=scope_columns([source_requests.get(name,{'orders':[]}) for name in request_ids],universe,markets)
    queries=scope_columns([query['scope'] for query in frame['scoped_cancellation_requests']],universe,markets)
    location={platform_order_id(order['identity']):markets.get(order['condition'],-1) for order in frame['raw_orders']}
    tensor=lambda values,dtype=torch.int64:torch.tensor([values],dtype=dtype,device='cuda')
    order_markets=tensor([location.get(order,-1) for order in universe])
    known=order_markets>=0
    target_scopes=[];request_keys=[];outcomes=[];available=[]
    for reference in target_rows:
        evidence=reference['record']['Evidence']
        control=evidence['evidence']['Control']['result']['Cancellation']
        request=evidence['source_reference']['record']['DirectResponse']['association']['CancellationTarget']['global_request_id']
        target_scopes.append({'orders':[platform_order_id(control['order_identity'])]})
        request_keys.append(request_ids[request])
        outcomes.append(['Canceled','NotCanceledNoEffect','EvidenceUnknownNoEffect'].index(control['outcome']))
        available.append(reference['visibility']['Feedback']['available_at_utc_ms'])
    targets=CancellationTargets(tensor([True]*len(target_rows),torch.bool),integers(request_keys),
        scope_columns(target_scopes,universe,markets),tensor(outcomes),tensor(available),torch.zeros(1,dtype=torch.int32,device='cuda'))
    if device_targets is not None:targets=device_targets
    cutoff=torch.tensor([frame['logical_at']],device='cuda')
    full=freeze_scoped_cancel_requests(cancel,requests,queries,order_markets,known,targets,cutoff)
    # 原action_snapshot先保留unfinished/承诺所需BUY请求的全部outcomes，再补needed撤单位置。
    raw={row['identity']:row for row in frame['raw_orders']}
    origin=[raw[key]['origin']['request'] if key in raw and raw[key]['origin'] is not None else None for key in catalog]
    same=torch.tensor([[[request==order_request for request,_,_ in leg_keys] for order_request in origin]],dtype=torch.bool,device='cuda')
    needed_buy=legs_view.present|(same&view.unfinished[:,:,None]).any(1)
    retained=view.unfinished|(same&needed_buy[:,None,:]).any(2)
    membership=torch.zeros((1,objects.members.size(1),len(universe)),dtype=torch.bool,device='cuda')
    locations=torch.zeros((1,len(universe)),dtype=torch.bool,device='cuda')
    for index,key in enumerate(catalog):
        column=universe.index(platform_order_id(key))
        membership[:,:,column]=objects.members[:,:,index]
        locations[:,column]=retained[:,index]
    scope_markets=objects.markets.clone()
    object_scopes=CancellationScopes(objects.kinds==2,scope_markets,membership)
    snapshot=freeze_cancel_snapshot(cancel,requests,object_scopes,objects.present,order_markets,known,locations,targets,cutoff)
    frozen=freeze_scoped_cancel_requests(snapshot.state,requests,queries,order_markets,snapshot.location_known,snapshot.targets,cutoff)
    return full,frozen


def compare_response_reference(reference,index,read,catalog):
    # 冷ref字典绑定context/run；完整业务position各列独立保持，无hash或压缩序号。
    source=reference['record']['Evidence']['source_reference']['record']['DirectResponse']
    assert values(read.batch_sequences)[index]==reference['position']['source_watermark']['projection_batch_sequence']
    assert values(read.record_ordinals)[index]==reference['position']['record_ordinal']
    assert values(read.private_sequences)[index]==source['private_feedback_sequence']
    assert values(read.delivery_sequences)[index]==reference['visibility']['Feedback']['delivery_sequence']
    slot=read.record_refs.cpu().flatten().tolist()[index]
    expected_slot,body=catalog["references"][position_key(reference)]
    assert slot==expected_slot and body==reference, "完整共享引用正文与位置"
    assert read.context_slots.cpu().flatten().tolist()[index]==catalog["contexts"][reference["position"]["context_reference"]]
    assert read.run_slots.cpu().flatten().tolist()[index]==catalog["runs"][reference["position"]["source_watermark"]["run_id"]]
    assert read.available_at.cpu().flatten().tolist()[index]==reference['visibility']['Feedback']['available_at_utc_ms']
    assert read.responded_at.cpu().flatten().tolist()[index]==source['platform_responded_at_utc_ms']
    assert read.received_at.cpu().flatten().tolist()[index]==source['response_received_at_utc_ms']


def compare_scoped(frame,read,catalog,oracle_key='scoped_cancellation_requests'):
    assert read.fault.cpu().tolist()==[0]
    pending=values(read.pending_count)
    counts=values(read.response_counts)
    pending_known=read.pending_known.cpu().tolist()[0]
    latest_known=read.latest_known.cpu().tolist()[0]
    latest=read.has_latest.cpu().tolist()[0]
    counts_known=read.counts_known.cpu().tolist()[0]
    for index,query in enumerate(frame[oracle_key]):
        assert pending_known[index]==('Known' in query['pending']),(frame['stage'],'作用域pending Known')
        if pending_known[index]:assert pending[index]==query['pending']['Known']
        assert latest_known[index]==('Known' in query['latest']),(frame['stage'],'作用域latest Known',index,query.get('scope'),query['latest'],latest_known,latest)
        if latest_known[index]:
            assert latest[index]==(query['latest']['Known'] is not None)
            if latest[index]:compare_response_reference(query['latest']['Known'],index,read.latest_response,catalog)
        for outcome,oracle in enumerate(query['counts']):
            if oracle=='NotApplicable':assert not latest[index]
            else:
                assert counts_known[index][outcome]==('Known' in oracle),(frame['stage'],'逐目标counts Known')
                if counts_known[index][outcome]:assert counts[index*3+outcome]==oracle['Known']


def compare_order_sources(frame,sources,orders,catalog):
    assert sources.fault.cpu().tolist()==[0]
    raw={order['identity']:order for order in frame['raw_orders']}
    for column,name,source_kind in [(sources.formed,'formed','OrderLifecycle'),(sources.latest_direct,'latest_direct','DirectResponse')]:
        present=column.present.cpu().tolist()[0]
        slots=column.record_refs.cpu().tolist()[0]
        platform=column.platform_at.cpu().tolist()[0]
        received=column.received_at.cpu().tolist()[0]
        for index,identity in enumerate(orders):
            expected=raw.get(identity,{}).get(name)
            assert present[index]==(expected is not None),(frame['stage'],name,identity)
            if expected is None:
                assert slots[index]==-1
                continue
            slot,body=catalog['references'][position_key(expected)]
            assert slots[index]==slot and body==expected,(frame['stage'],name,'完整来源引用')
            source=expected['record']['Evidence']['source_reference']['record'][source_kind]
            assert platform[index]==source['platform_event_at_utc_ms' if source_kind=='OrderLifecycle' else 'platform_responded_at_utc_ms']
            assert received[index]==source['feedback_received_at_utc_ms' if source_kind=='OrderLifecycle' else 'response_received_at_utc_ms']
            position=expected['position']
            assert values(column.batch_sequences)[index]==position['source_watermark']['projection_batch_sequence']
            assert values(column.record_ordinals)[index]==position['record_ordinal']
            assert values(column.delivery_sequences)[index]==expected['visibility']['Feedback']['delivery_sequence']
            assert values(column.private_sequences)[index]==source['private_feedback_sequence']
            assert column.available_at.cpu().tolist()[0][index]==expected['visibility']['Feedback']['available_at_utc_ms']
            assert column.context_slots.cpu().tolist()[0][index]==catalog['contexts'][position['context_reference']]
            assert column.run_slots.cpu().tolist()[0][index]==catalog['runs'][position['source_watermark']['run_id']]


def compare_native_order_times(frame, view, sources, catalog):
    # 只解码原condition目录；原unfinished/formed仅作为投影后的对照依据。
    raw = {order['identity']:order for order in frame['raw_orders']}
    conditions = [market['condition_id'] for market in frame['public_market_sources']]
    scopes = [conditions, *[[condition] for condition in conditions]]
    members = torch.tensor([[[raw.get(identity, {}).get('condition') in scope for identity in catalog]
                             for scope in scopes]], dtype=torch.bool, device='cuda')
    unknown = any(order['condition'] is None for order in frame['orders'])
    known = torch.tensor([[not (unknown and scope) for scope in scopes]], dtype=torch.bool, device='cuda')
    result = Numbers.run_exact_batch(lambda: order_inputs(view, sources,
        torch.tensor([frame['logical_at']], dtype=torch.int64, device='cuda'), members, known))
    assert result.fault.cpu().tolist() == [0]
    def projected(milliseconds):
        return math.copysign(math.log1p(abs(milliseconds)/1000), milliseconds)
    now = frame['logical_at']
    for name, kind, features in [('formed','OrderLifecycle',result.formed_time),
                                ('latest_direct','DirectResponse',result.latest_direct_time)]:
        expected = []
        for identity in catalog:
            order = raw.get(identity)
            reference = None if order is None else order[name]
            row = [0.0]*6
            if order is not None:
                row[5] = 1.0
            if reference is not None:
                times = reference['record']['Evidence']['source_reference']['record'][kind]
                at = times['platform_event_at_utc_ms' if kind=='OrderLifecycle' else 'platform_responded_at_utc_ms']
                received = times['feedback_received_at_utc_ms' if kind=='OrderLifecycle' else 'response_received_at_utc_ms']
                row = [projected(now-at), projected(now-received), projected(received-at), 1., 1., 1.]
            expected.append(row)
        torch.testing.assert_close(features.cpu(), torch.tensor([expected], dtype=torch.float32), rtol=1e-6, atol=1e-7)
    expected = []
    expected_active, expected_cancel, expected_count = [], [], []
    for scope in scopes:
        selected = [order for order in frame['orders'] if order['condition'] in scope]
        def quantity_triple(value):
            number = float(value)
            return [math.copysign(math.log1p(abs(number)),number), 1., 1.]
        if unknown and scope:
            expected_active.append([0.,0.,1.])
            expected_cancel.append([0.,0.,1.])
            expected_count.append([0.,0.,1.])
        else:
            unavailable = any(order['active']!='NotApplicable' and 'Known' not in order['active'] for order in selected)
            total = sum((Fraction(order['active']['Known']) for order in selected if isinstance(order['active'],dict) and 'Known' in order['active']), Fraction(0))
            expected_active.append([0.,0.,1.] if unavailable else quantity_triple(total))
            unavailable = any('Known' not in order['cancellable'] for order in selected)
            expected_cancel.append([0.,0.,1.] if unavailable else quantity_triple(sum(order['cancellable']['Known'] for order in selected)))
            expected_count.append(quantity_triple(len(selected)))
        if unknown and scope:
            expected.append([[0.,0.,1.]]*2)
        elif not selected:
            expected.append([[0.,0.,0.]]*2)
        elif any(order['formed'] is None for order in selected):
            expected.append([[0.,0.,1.]]*2)
        else:
            ages = [now-order['formed']['record']['Evidence']['source_reference']['record']['OrderLifecycle']['platform_event_at_utc_ms']
                    for order in selected]
            expected.append([[projected(max(ages)),1.,1.], [projected(min(ages)),1.,1.]])
    torch.testing.assert_close(result.ages.cpu(), torch.tensor([expected], dtype=torch.float32), rtol=1e-6, atol=1e-7)
    for actual, expected in [(result.active,expected_active),(result.cancellable_count,expected_cancel),(result.order_count,expected_count)]:
        torch.testing.assert_close(actual.cpu(), torch.tensor([expected], dtype=torch.float32), rtol=1e-6, atol=1e-7)
    return result


def check_family(source):
    # 冷共享引用目录保存原完整正文；设备slot仅定位，不替代原context/run/业务position。
    catalog_refs={"references":{},"contexts":{},"runs":{}}
    for frame in source:
        references=[frame['cancel_latest']['Known']]
        for order in frame['raw_orders']:
            references.extend(order[name] for name in ['accepted','formed','latest_lifecycle','latest_cancel','latest_direct'])
            references.extend(order['trades'])
        for key in ['scoped_cancellation_requests','snapshot_scoped_cancellation_requests']:
            references.extend(query['latest'].get('Known') if isinstance(query['latest'],dict) else None for query in frame[key])
        for reference in references:
            if reference is None:continue
            position=reference['position']
            catalog_refs['contexts'].setdefault(position['context_reference'],len(catalog_refs['contexts']))
            catalog_refs['runs'].setdefault(position['source_watermark']['run_id'],len(catalog_refs['runs']))
            key=position_key(reference)
            if key in catalog_refs['references']:
                assert catalog_refs['references'][key][1]==reference
            else:catalog_refs['references'][key]=(len(catalog_refs['references']),reference)
    identities = {}
    targets = {}
    for frame in source:
        for order in frame['raw_orders']:
            identities.setdefault(order['identity'],len(identities)+1)
            origin = order['origin']
            assert origin is not None and 'GtcLimit' in origin['terms'], '此原夹具只覆盖GTC'
            targets[order['identity']] = origin['terms']['GtcLimit']['size_shares']
            for trade in order['trades']:
                payload=trade['record']['Evidence']['evidence']
                result=payload.get('TradeLifecycleV3',payload.get('TradeLifecycle'))['result']
                result=result.get('result',result)
                identities.setdefault(result['trade_identity'],len(identities)+1)
    catalog=list(targets)
    def directory(frame):
        public = {order['identity'] for order in frame['raw_orders']}
        present=torch.tensor([[key in public for key in catalog]],dtype=torch.bool,device='cuda')
        return FrozenIntentOrders(present,integers([identities[key] for key in catalog]),present.clone(),
            rational([targets[key] for key in catalog]),torch.zeros_like(present),present.clone())
    buy_ids={value['request']:index for index,value in enumerate(source[-1]['buy_source_receipts'])}
    leg_keys=[(receipt['request'],term['item'],term['target']) for receipt in source[-1]['buy_source_receipts'] for term in receipt['terms']]
    leg_saved=[]
    request_ids={value['request']:index+1 for index,value in enumerate(source[-1]['cancel_source_requests'])}
    cancel=Numbers.run_exact_batch(lambda:PublicCancellationState(integers(list(request_ids.values())),
        torch.zeros((1,len(request_ids)),dtype=torch.bool,device='cuda'),completion_columns(None,request_ids,catalog_refs)))
    cancel_seen=set()
    target_rows=[]
    universe=sorted({platform_order_id(key) for key in catalog}|{order for frame in source for query in frame['scoped_cancellation_requests'] for order in query['scope'].get('orders',[])})
    from validate_cancel_source import prepare_cancel_source,receive_cancel_source
    cancellation_references=[value[1] for value in catalog_refs['references'].values()
        if any(name in value[1]['record']['Evidence']['evidence'].get('Control',{}).get('result',{})
            for name in ['Cancellation','CancellationRequestCompleted'])]
    source_columns=prepare_cancel_source(cancellation_references,request_ids,universe,catalog_refs)
    cancellation_positions={position_key(reference) for reference in cancellation_references}
    next_slot=len(cancellation_references)
    for key,(_,body) in list(catalog_refs['references'].items()):
        if key not in cancellation_positions:
            catalog_refs['references'][key]=(next_slot,body)
            next_slot+=1
    directory_sequences=integers([int(name.removeprefix('sim-')) if name.startswith('sim-') else 0 for name in universe])
    directory_present=torch.tensor([[name.startswith('sim-') for name in universe]],dtype=torch.bool,device='cuda')
    target_received=torch.zeros((1,0),dtype=torch.bool,device='cuda')
    device_targets=CancellationTargets(target_received,integers([]),CancellationScopes(target_received,
        torch.empty((1,0),dtype=torch.int64,device='cuda'),torch.zeros((1,0,len(universe)),dtype=torch.bool,device='cuda')),
        torch.empty((1,0),dtype=torch.int64,device='cuda'),torch.empty((1,0),dtype=torch.int64,device='cuda'),torch.zeros(1,dtype=torch.int32,device='cuda'))
    scoped_saved=[]
    maximum_retained=0
    cancel_saved=[]
    seen=set()
    state=None
    binding_state=SubmissionBindings.empty(1,'cuda')
    rank=0
    saved=[]
    objects_saved=[]
    native_time_saved=[]
    for frame in source:
        incoming=[]
        for order in frame['raw_orders']:
            for name in ['accepted','formed','latest_lifecycle','latest_cancel','latest_direct']:
                reference=order[name]
                if reference is not None and position_key(reference) not in seen:
                    seen.add(position_key(reference));incoming.append(reference)
            for reference in order['trades']:
                if position_key(reference) not in seen:
                    seen.add(position_key(reference));incoming.append(reference)
        for reference in incoming:
            evidence=reference['record']['Evidence']['evidence']
            if 'Cancellation' in evidence.get('Control',{}).get('result',{}):target_rows.append(reference)
        incoming.sort(key=lambda r:(r['visibility']['Feedback']['delivery_sequence'],r['position']['record_ordinal']))
        registered={value['request'] for value in frame['cancel_source_requests']}
        receipt_mask=torch.tensor([[name in registered for name in request_ids]],dtype=torch.bool,device='cuda')
        latest=frame['cancel_latest']['Known']
        reference=latest if latest is not None and position_key(latest) not in cancel_seen else None
        if reference is not None:cancel_seen.add(position_key(reference))
        cancellation_incoming=[row for row in incoming if 'Cancellation' in row['record']['Evidence']['evidence'].get('Control',{}).get('result',{})]
        if reference is not None:cancellation_incoming.append(reference)
        cancel=cancel.register(receipt_mask)
        cancel,device_targets,cancel_rows=receive_cancel_source(source_columns,cancellation_incoming,cancel,device_targets,directory_sequences,directory_present)
        from cancel_projection import cancellation_order_events
        normal_incoming=[row for row in incoming if row not in cancellation_incoming]
        def compute():
            events=event_columns(normal_incoming,identities,rank,buy_ids,catalog_refs)
            candidate=PublicRegistryState(events) if state is None else state.receive(events)
            cancel_events=cancellation_order_events(cancel_rows,torch.arange(rank,rank+len(cancellation_incoming),device='cuda')[None,:])
            candidate=candidate.receive(cancel_events)
            submission=SubmissionBindings(events.received & ((events.kinds==BASE_ACCEPTED)|(events.kinds==QUOTE_ACCEPTED)),
                events.request_slots,events.item_ordinals,events.private_sequences,events.receive_rank,events.orders,
                torch.ones_like(events.received),events.fault)
            bindings=binding_state.receive(submission)
            return candidate,candidate.freeze_orders(directory(frame),torch.tensor([frame['logical_at']],device='cuda')),bindings
        state,view,binding_state=Numbers.run_exact_batch(compute)
        rank+=max(1,len(incoming))
        maximum_retained=max(maximum_retained,state.events.received.size(1))
        sources=Numbers.run_exact_batch(lambda:state.freeze_order_sources(directory(frame),torch.tensor([frame['logical_at']],device='cuda')))
        compare_order_sources(frame,sources,catalog,catalog_refs)
        native_times=compare_native_order_times(frame,view,sources,catalog)
        native_time_saved.append((native_times,native_times.formed_time.clone(),native_times.latest_direct_time.clone(),native_times.ages.clone()))
        assert view.fault.cpu().tolist()==[0]
        indexes={key:index for index,key in enumerate(catalog)}
        unfinished=view.unfinished.cpu().tolist()[0]
        assert {key for key,index in indexes.items() if unfinished[index]}=={order['identity'] for order in frame['orders']},frame['stage']
        for order in frame['orders']:
            index=indexes[order['identity']]
            for name in ['matched','active']:
                oracle=order[name]
                known=getattr(view,name+'_known').cpu().tolist()[0][index]
                assert known==('Known' in oracle),(frame['stage'],name,oracle)
                if known:assert amounts(getattr(view,name))[index]==Fraction(oracle['Known']),(frame['stage'],name)
            assert view.cancellable_known.cpu().tolist()[0][index]==('Known' in order['cancellable'])
            if 'Known' in order['cancellable']:assert view.cancellable.cpu().tolist()[0][index]==order['cancellable']['Known']
        commit_population=view.commitment_present.cpu().tolist()[0]
        assert {key for key,index in indexes.items() if commit_population[index]}=={item['order'] for item in frame['commitments'] if item['order'] is not None},(frame['stage'],'完整承诺人口')
        for commitment in frame['commitments']:
            if commitment['order'] is None:continue
            index=indexes[commitment['order']]
            known=view.commitment_known.cpu().tolist()[0][index]
            assert known==('Known' in commitment['remaining'])
            if known:assert amounts(view.commitment)[index]==Fraction(commitment['remaining']['Known'])
        receipts={receipt['request'] for receipt in frame['buy_source_receipts']}
        def leg_freeze():
            registered=torch.tensor([[request in receipts for request,_,_ in leg_keys]],dtype=torch.bool,device='cuda')
            legs=SubmittedLegs(torch.ones_like(registered),registered,
                torch.tensor([[buy_ids[request] for request,_,_ in leg_keys]],device='cuda'),
                torch.tensor([[item for _,item,_ in leg_keys]],device='cuda'),
                rational([target for _,_,target in leg_keys]),torch.ones_like(registered))
            return freeze_leg_commitments(legs,binding_state,directory(frame),view)
        legs_view=Numbers.run_exact_batch(leg_freeze)
        assert legs_view.fault.cpu().tolist()==[0]
        assert binding_state.received.size(1)<=len(leg_keys), '关联目录按请求腿保留，不累计版本/padding'
        # 真实已领取关联不因中途历史未装而被覆盖为Unknown；无关联腿仍未知。
        def incomplete_binding_freeze():
            registered=torch.tensor([[request in receipts for request,_,_ in leg_keys]],dtype=torch.bool,device='cuda')
            submitted=SubmittedLegs(torch.ones_like(registered),registered,
                torch.tensor([[buy_ids[request] for request,_,_ in leg_keys]],device='cuda'),
                torch.tensor([[item for _,item,_ in leg_keys]],device='cuda'),
                rational([target for _,_,target in leg_keys]),torch.ones_like(registered))
            return freeze_leg_commitments(submitted,replace(binding_state,prefix_known=torch.zeros(1,dtype=torch.bool,device='cuda')),
                directory(frame),view)
        incomplete=Numbers.run_exact_batch(incomplete_binding_freeze)
        actual_bindings={(int(request),int(item)) for request,item,received in zip(
            binding_state.request_slots.cpu().tolist()[0],binding_state.item_ordinals.cpu().tolist()[0],
            binding_state.received.cpu().tolist()[0]) if received}
        for index,(request,item,_) in enumerate(leg_keys):
            if (buy_ids[request],item) in actual_bindings:
                assert incomplete.known.cpu().tolist()[0][index]==legs_view.known.cpu().tolist()[0][index]

        included=legs_view.present.cpu().tolist()[0]
        assert {(request,item) for (request,item,_),active_leg in zip(leg_keys,included) if active_leg}=={
            (item['request'],item['item']) for item in frame['commitments']},(frame['stage'],'完整逐腿承诺人口')
        remaining=amounts(legs_view.remaining)
        has_order=legs_view.has_order.cpu().tolist()[0]
        known=legs_view.known.cpu().tolist()[0]
        for oracle in frame['commitments']:
            index=next(index for index,(request,item,_) in enumerate(leg_keys) if (request,item)==(oracle['request'],oracle['item']))
            assert has_order[index]==(oracle['order'] is not None)
            assert known[index]==('Known' in oracle['remaining'])
            if known[index]:assert remaining[index]==Fraction(oracle['remaining']['Known'])
        def objects_freeze():
            formation,objects_directory=cancel_object_columns(frame,catalog,identities,state.events)
            return freeze_cancel_objects(view,formation,objects_directory),objects_directory
        objects,objects_directory=Numbers.run_exact_batch(objects_freeze)
        compare_objects(frame,objects,objects_directory,catalog)
        objects_saved.append((objects,objects.members.clone(),objects.present.clone()))
        leg_saved.append((legs_view,remaining))
        saved.append((view,amounts(view.active),view.unfinished.clone()))
        cancel_view=Numbers.run_exact_batch(lambda:cancel.freeze_global(torch.tensor([frame['logical_at']],device='cuda')))
        assert cancel_view.fault.cpu().tolist()==[0]
        assert values(cancel_view.pending_count)==[frame['cancel_pending']['Known']],frame['stage']
        assert cancel_view.has_latest.cpu().tolist()==[latest is not None]
        assert cancel_view.counts_applicable.cpu().tolist()==[latest is not None]
        if latest is not None:
            compare_response_reference(latest,0,cancel_view.latest_response,catalog_refs)
            assert values(cancel_view.response_counts)==[count['Known'] for count in frame['cancel_response_counts']]
        else:
            assert frame['cancel_response_counts']==['NotApplicable']*3
        cancel_saved.append((cancel_view,values(cancel_view.pending_count),values(cancel_view.response_counts)))
        scoped,frozen_scoped=Numbers.run_exact_batch(lambda:scoped_freeze(frame,cancel,request_ids,target_rows,universe,objects,catalog,view,legs_view,leg_keys,device_targets))
        compare_scoped(frame,scoped,catalog_refs)
        compare_scoped(frame,frozen_scoped,catalog_refs,'snapshot_scoped_cancellation_requests')
        for snapshot in [scoped,frozen_scoped]:
            reference=snapshot.latest_response
            saved_reference={name:(value.clone() if isinstance(value,torch.Tensor) else values(value))
                for name,value in vars(reference).items()}
            scoped_saved.append((snapshot,values(snapshot.response_counts),snapshot.pending_known.clone(),saved_reference))
    for scoped,counts,pending_known,saved_reference in scoped_saved:
        assert values(scoped.response_counts)==counts and torch.equal(scoped.pending_known,pending_known),'旧作用域读取不倒灌'
        for name,value in vars(scoped.latest_response).items():
            expected=saved_reference[name]
            assert torch.equal(value,expected) if isinstance(value,torch.Tensor) else values(value)==expected, '旧完整响应引用不倒灌'
    for objects,members,present in objects_saved:
        assert torch.equal(objects.members,members) and torch.equal(objects.present,present),'旧撤单对象不倒灌'
    for view,active,unfinished in saved:
        assert amounts(view.active)==active and torch.equal(view.unfinished,unfinished),'旧冻结读取不可倒灌'
    for view,remaining in leg_saved:
        assert amounts(view.remaining)==remaining,'旧逐腿冻结不可倒灌'
    for view,pending,counts in cancel_saved:
        assert values(view.pending_count)==pending and values(view.response_counts)==counts,'旧撤单冻结不可倒灌'
    for frozen,formed,direct,ages in native_time_saved:
        assert torch.equal(frozen.formed_time,formed) and torch.equal(frozen.latest_direct_time,direct) and torch.equal(frozen.ages,ages),'旧Native订单时间冻结不可倒灌'
    return len(source),len(seen),maximum_retained


def main():
    started=time.perf_counter()
    counts=[check_family(json.loads(Path(path).read_text())) for path in sys.argv[1:]]
    torch.cuda.synchronize()
    print(json.dumps({'frozen_prefixes':sum(n for n,_,_ in counts),'distinct_original_order_records':sum(n for _,n,_ in counts),
        'receipt_without_order_keeps_original_leg_target':True,'continuous_received_state':True,'maximum_retained_order_records_per_family':[width for _,_,width in counts],'derived_oracle_initialization':False,'old_freeze_unchanged':True,
        'complete_first_delivery_stream':False,'global_cancel_request_completed_state':True,'single_paired_market_cancel_objects':True,'scoped_cancel_request_results':True,'original_action_snapshot_needed_cancel_projection':True,'latest_response_full_source_position_checked':True,'Order_formed_and_latest_direct_original_times_and_full_reference_checked':True,'cancel_noe_mapper_core_and_closed_decoder':True,'cancel_first_delivery_filtered_original_references':True,'scoped_unknown_order_location_preserved':True,'device_sim_identity_tie_order':True,
        'native_order_age_and_cancel_time_fields_from_frozen_original_sources':True,
        'native_unfinished_order_active_cancelable_and_count_scope_fields':True,
        'original_native_encoder_invoked':False,
        'full_registry_or_root':False,'seconds':time.perf_counter()-started,'device_peak_allocated_bytes':torch.cuda.max_memory_allocated()}))


if __name__=='__main__':main()
