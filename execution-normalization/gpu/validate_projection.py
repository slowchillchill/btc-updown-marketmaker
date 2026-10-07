"""实际SIM设备来源批→Core→领取的原NOE位置与正文冷对照。"""
import torch
import json
import sys
from dataclasses import replace
from pathlib import Path
from queues import Numbers,ExactInt,_take
from fill_pool import _take_record
from feedback import FeedbackEmitter
from latency import byte_column
from validate_base_execution import integers,rational,values,units
from fields import encode_fields
from projection import Projection
from consumption import PendingRecords,ReceiveGates
from core import OUT_EVIDENCE,OUT_FACT


def concat_integer(values_):
    width=max(value.words.size(1) for value in values_)
    return ExactInt(torch.cat([value.with_capacity(width).words for value in values_]),
        torch.cat([value.sign for value in values_]),torch.cat([value.fault for value in values_]))


def entity_sequence(identity,kind):
    value=json.loads(identity.split(':',1)[1])[-1]
    return int(value[kind]['source_'+kind.lower()+'_id'].removeprefix('sim-'))


def expected_semantic(record, resolution_index=0, terminal_index=0, token_index=0, token_slots=None):
    zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    body=next(iter(record['record'].values()))
    if 'fact' in body:
        fact=body['fact']
        value=fact['disposition']['AccountTransition']
        transition=value['transition']
        if 'Existing' in transition:
            transition=transition['Existing']
        name,fields=(transition,{}) if isinstance(transition,str) else next(iter(transition.items()))
        if name=='ResolutionRedeemApplied':
            from fields import RecordList
            balance=value['balance_delta']
            existing=balance['existing']
            request=fields['settlement_request_id']
            assert request['policy_version']=='RESOLUTION_REDEEM_ONLY'
            assert request['resolution_identity']==fields['resolution_identity']
            vector=existing['outcome_balance_deltas']
            vector_fields=RecordList((torch.tensor([token_slots[e['token_id']] for e in vector],dtype=torch.int64,device='cuda'),
                rational([e['balance_shares_delta'] for e in vector])),torch.ones((1,len(vector)),dtype=torch.bool,device='cuda'))
            names=['cash_pusd_delta','available_pusd_delta','principal_encumbrance_pusd_delta','immediate_taker_fee_encumbrance_pusd_delta',
                'maker_rebate_receivable_pusd_delta','taker_rebate_receivable_pusd_delta','maker_rebate_locked_remainder_pusd_delta','taker_rebate_locked_remainder_pusd_delta']
            return encode_fields([zero,zero,torch.tensor([value['economic_effective_at_utc_ms']],device='cuda'),
                torch.full_like(zero,resolution_index),zero,*[rational([existing[n]]) for n in names],vector_fields,
                rational([balance['settlement_fee_liability_pusd_delta']]),rational([balance['actual_taker_fee_liability_pusd_delta']])])
        tag={'OrderAcceptedEncumbranceApplied' :0,'CallerCancellationEncumbranceReleased':2,'MarketResolutionEncumbranceReleased':5,'FillFinalityEconomicApplied':6,'TakerRebateAccrualApplied':10,
            'MakerRebateAccrualFinalized':9,
            'MakerRebateDayFinalized':11,'TakerRebateDayFinalized':12,'MakerRebatePaid':13,'TakerRebatePaid':14}[name]
        if tag==9:
            sequence=0
        elif tag>=11:
            sequence=fields['attribution_day_utc_ms']
        else:
            identity=fields['order_identity' if tag in [0,2,5] else 'trade_identity']
            entity=json.loads(identity.split(':',1)[1])[-1]
            sequence=int(next(iter(next(iter(entity.values())).values())).removeprefix('sim-'))
        balance=value['balance_delta']
        settlement_liability=balance.get('settlement_fee_liability_pusd_delta','0')
        taker_liability=balance.get('actual_taker_fee_liability_pusd_delta','0')
        if 'existing' in balance:
            balance=balance['existing']
        names=['cash_pusd_delta','available_pusd_delta','principal_encumbrance_pusd_delta','immediate_taker_fee_encumbrance_pusd_delta',
            'maker_rebate_receivable_pusd_delta','taker_rebate_receivable_pusd_delta','maker_rebate_locked_remainder_pusd_delta','taker_rebate_locked_remainder_pusd_delta']
        outcomes=balance['outcome_balance_deltas']
        semantic=[zero,torch.full_like(zero,-1) if value['condition_id'] is None else zero,torch.tensor([value['economic_effective_at_utc_ms']],device='cuda'),
            torch.full_like(zero,tag),integers([sequence]),torch.full_like(zero,fields.get('path_ordinal',0)),
            *[rational([balance[name]]) for name in names],torch.tensor([bool(outcomes)],device='cuda'),torch.full_like(zero,token_index if outcomes else 0),
            rational([outcomes[0]['balance_shares_delta'] if outcomes else '0']),rational([settlement_liability]),rational([taker_liability])]
        if tag==5:
            semantic.append(torch.full_like(zero,resolution_index))
        return encode_fields(semantic)
    kind,value=next(iter(body['evidence'].items()))
    value=value['result']
    if kind=='TradeLifecycleV3':
        maker_read=value['maker_fee_equivalent_pusd']
        maker_kind=0 if isinstance(maker_read,dict) and 'Known' in maker_read else 1 if maker_read=='Unavailable' else 2
        maker_value=maker_read['Known'] if maker_kind==0 else '0'
        value=value['result']
        state={'MatchedNotBroadcasted':0,'Matched':1,'Retrying':2,'Mined':3,'Confirmed':4,'Failed':5}[value['state']]
        disposition={'PendingUnapplied':0,'PendingApplied':1,'FinalSuccess':2,'FinalFailureUnapplied':3,'FinalFailureCompensated':4}[value['economic_disposition']]
        return encode_fields([zero,zero,torch.full_like(zero,token_index),integers([value['fill_sequence']]),integers([entity_sequence(value['order_identity'],'Order')]),
            torch.full_like(zero,state),torch.full_like(zero,disposition),torch.tensor([value['matched_at_utc_ms']],device='cuda'),
            torch.full_like(zero,{'Taker':0,'MakerInferred':1}[value['liquidity_role']]),rational([value['source_price_pusd_per_share']]),integers([units(value['base_quantity_shares'])]),
            integers([units(value['quote_amount_pusd'])]),integers([units(value['immediate_taker_fee_pusd'])]),
            torch.tensor([value['economic_application_account_transition_seq'] is not None],device='cuda'),
            integers([value['economic_application_account_transition_seq'] or 0]),torch.full_like(zero,maker_kind),rational([maker_value])])
    if kind=='OrderLifecycle':
        if isinstance(value['event'],dict) and 'MarketResolvedCanceled' in value['event']:
            return encode_fields([zero,zero,torch.full_like(zero,token_index),integers([entity_sequence(value['order_identity'],'Order')]),torch.full_like(zero,5),zero,torch.full_like(zero,resolution_index)])
        if value['event']=='CallerCanceled':
            return encode_fields([zero,zero,torch.full_like(zero,token_index),integers([entity_sequence(value['order_identity'],'Order')]),torch.full_like(zero,4),zero])
        return encode_fields([zero,zero,torch.full_like(zero,token_index),integers([entity_sequence(value['order_identity'],'Order')]),zero,
            torch.full_like(zero,{'Open':0,'Matched':1}[value['event']['Accepted']['state']])])
    if kind=='Control':
        if 'Cancellation' in value:
            result=value['Cancellation']
            return encode_fields([zero,integers([entity_sequence(result['order_identity'],'Order')]),
                torch.full_like(zero,['Canceled','NotCanceledNoEffect','EvidenceUnknownNoEffect'].index(result['outcome']))])
        if 'CancellationRequestCompleted' in value:
            result=value['CancellationRequestCompleted']['result']
            return encode_fields([zero,*[integers([result[name]]) for name in ['canceled_count','not_canceled_no_effect_count','evidence_unknown_no_effect_count']]])
        value=value['Submission']['outcome']['Accepted']
        return encode_fields([zero,zero,zero,torch.full_like(zero,token_index),integers([entity_sequence(value['order']['order_identity'],'Order')]),
            torch.full_like(zero,{'Open':0,'Matched':1}[value['order']['state']]),
            integers([units(value['measurement']['Base']['matched_base_quantity_shares'])]),
            integers([units(value['measurement']['Base']['open_base_quantity_shares'])]),
            torch.tensor([value['effective_processing_at_utc_ms']],device='cuda')])
    if kind=='SettlementLifecycleV2':
        if 'Request' in value:
            request=value['Request']
            assert request['index_sets']==['0x1','0x2'] and request['settlement_request_id']['policy_version']=='RESOLUTION_REDEEM_ONLY'
            outcomes={'Registered':0,'Pending':1,'FinalSuccess':2,'FinalFailureNoEffect':3}
            observations=['Registered','SubmissionUnknown','ObservationTimeout','QueryUnknown','QueryAcceptedPending','QueryFinalSuccess',
                'QueryConfirmedNoTransaction','RetryAuthorized','RetryNotAuthorized','TerminalSuccess','TerminalFailureNoEffect']
            return encode_fields([zero,zero,torch.full_like(zero,resolution_index),zero,torch.ones_like(zero),torch.full_like(zero,2),
                torch.full_like(zero,outcomes[request['outcome']]),torch.full_like(zero,observations.index(request['observation']))])
        if 'Resolved' in value:
            return encode_fields([zero,zero,torch.full_like(zero,resolution_index),torch.full_like(zero,terminal_index)])
        if 'MakerAccrualFinalized' in value:
            from fields import RecordList
            value=value['MakerAccrualFinalized']
            items=value['contributions']
            vector=RecordList((integers([entity_sequence(item['trade_identity'],'Trade') for item in items]),
                torch.zeros(len(items),dtype=torch.int64,device='cuda'),torch.zeros(len(items),dtype=torch.int64,device='cuda'),
                torch.tensor([item['attribution_day_start_utc_ms'] for item in items],dtype=torch.int64,device='cuda'),
                rational([item['contribution_pusd'] for item in items])),torch.ones((1,len(items)),dtype=torch.bool,device='cuda'))
            return encode_fields([zero,zero,torch.full_like(zero,{'Calculated':0,'ZeroNoEligibleMakerFill':1,'UnknownIncomplete':2}[value['status']]),vector])
        if 'DayFinalized' in value:
            from fields import IntegerList
            value=value['DayFinalized']
            refs=value['contribution_refs']
            return encode_fields([zero,torch.full_like(zero,{'Maker':0,'Taker':1}[value['kind']]),
                torch.tensor([value['attribution_day_start_utc_ms']],device='cuda'),rational([value['accrued_pusd']]),
                rational([value['payable_pusd']]),rational([value['locked_remainder_pusd']]),
                IntegerList(integers([entity_sequence(ref,'Trade') for ref in refs]),torch.ones((1,len(refs)),dtype=torch.bool,device='cuda'))])
        if 'Payment' in value:
            value=value['Payment']
            kind=torch.full_like(zero,{'Maker':0,'Taker':1}[value['kind']])
            day=torch.tensor([value['attribution_day_start_utc_ms']],device='cuda')
            return encode_fields([zero,kind,day,zero,torch.full_like(zero,12),kind,day,rational([value['paid_pusd']])])
        value=value['Contribution']
        identity=json.loads(value['trade_identity'].split(':',1)[1])[-1]
        sequence=int(identity['Trade']['source_trade_id'].removeprefix('sim-'))
        return encode_fields([zero,zero,integers([sequence]),torch.ones_like(zero),
            torch.tensor([value['attribution_day_start_utc_ms']],device='cuda'),rational([value['contribution_pusd']])])
    raise AssertionError(kind)


def validate(source,sampler,prepared,accepted,accepted_pool,frames,rebate_rate):
    zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    emitter=FeedbackEmitter.open(1)
    # 原开市后控制登记已经独立接纳，此处只承接其batch前沿，不重造登记NOE。
    empty=torch.empty((1,0),dtype=torch.int64,device='cuda')
    emitter,_=Numbers.run_exact_batch(lambda:emitter.emit(sampler,empty.bool(),empty.bool(),empty,empty,empty,
        empty.to(torch.int32),control_admission=torch.tensor([True],device='cuda')))
    state=Projection.open(emitter,zero,zero,zero,zero,torch.tensor([0,0],device='cuda'),integers([1]),zero,zero,zero,zero)
    at=torch.tensor([source['processing_at']],device='cuda')
    identity,offsets=byte_column([b'gpu-multiask-post'])
    sampled=sampler.sample_requests(zero,identity,offsets,torch.ones_like(zero))
    received,fault=sampled.arrival_at(at)
    assert not fault.any().cpu().item()
    state,batch=Numbers.run_exact_batch(lambda:state.acceptance(sampler,prepared,accepted,accepted_pool,at,received))
    batches=[batch]
    for record,frame,price in frames:
        if int(frame.event.ordinal.cpu()[0])==0:
            continue
        event_at=record.matched_at+record.offsets[record.selected_path,frame.event.ordinal]
        state,batch=Numbers.run_exact_batch(lambda:state.finality(sampler,record,frame,price,rebate_rate,event_at))
        batches.append(batch)
    expected={}
    for record in source['noe_records']:
        position=record['position']
        expected[(position['source_watermark']['projection_batch_sequence'],position['record_ordinal'])]=record
    categories={'DirectResponse':0,'OrderLifecycle':1,'TradeLifecycle':2,'SettlementLifecycle':3}
    checked=0
    for batch in batches:
        assert batch.fault.cpu().tolist()==[0]
        mask=batch.emitted.cpu().tolist()[0]
        sequence=values(batch.batch_sequences)
        ordinal=values(batch.record_ordinals)
        private=values(batch.private_sequences)
        for column,active in enumerate(mask):
            if not active:
                continue
            original=expected[(sequence[column],ordinal[column])]
            body=next(iter(original['record'].values()))
            reference=body.get('source_reference') or body['fact']['source_reference']
            tag,metadata=next(iter(reference['record'].items()))
            assert private[column]==metadata['private_feedback_sequence']
            assert batch.source_categories[0,column].cpu().item()==categories[tag]
            assert batch.platform_at[0,column].cpu().item()==metadata.get('platform_event_at_utc_ms',metadata.get('platform_responded_at_utc_ms'))
            assert batch.received_at[0,column].cpu().item()==metadata.get('feedback_received_at_utc_ms',metadata.get('response_received_at_utc_ms'))
            assert batch.normalized.outcomes[0,column].cpu().item()==(OUT_FACT if 'fact' in body else OUT_EVIDENCE)
            row=batch.rows[0,column]
            actual=state.normalizer.bank.semantic[row.reshape(1)]
            value=Numbers.run_exact_batch(lambda:expected_semantic(original))
            width=max(actual.size(1),value.size(1))
            assert torch.equal(torch.nn.functional.pad(actual,(0,width-actual.size(1))),
                torch.nn.functional.pad(value,(0,width-value.size(1))))
            checked+=1
    assert checked==len(expected)==21
    # 真实设备生成的记录与原位置侧表一起进入双门；领取不能重编号批内位置。
    active=torch.cat([batch.emitted for batch in batches],dim=1)
    private=concat_integer([batch.private_sequences for batch in batches])
    received=torch.cat([batch.received_at for batch in batches],dim=1)
    rows=torch.cat([batch.rows for batch in batches],dim=1)
    source_ordinal=ExactInt.from_i64_tensor(state.normalizer.bank.source_order[rows.flatten(),3])
    pending=PendingRecords.from_columns(active,received,private,source_ordinal,rows,integers([0]))
    gates=ReceiveGates.unset(1).set_receive(torch.tensor([10000],device='cuda'),torch.tensor([True],device='cuda'))
    pending,blocked=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not blocked.emitted.any().cpu().item()
    gates=gates.set_public(torch.tensor([10000],device='cuda'),torch.tensor([True],device='cuda'))
    pending,delivered=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert int(delivered.emitted.sum().cpu())==21
    lookup={int(batch.rows[0,column].cpu()):expected[(values(batch.batch_sequences)[column],values(batch.record_ordinals)[column])]
        for batch in batches for column,active in enumerate(batch.emitted.cpu().tolist()[0]) if active}
    actual=[lookup[row]['position'] for row,valid in zip(delivered.record_refs.cpu().tolist()[0],delivered.emitted.cpu().tolist()[0]) if valid]
    assert actual==[record['position'] for record in source['noe_records']]
    # Ledger经济子路径消费上面真正生成的设备delta，无原NOE JSON→delta反造。
    # 全部其它Lifecycle/预留/归属仍由后续完整Ledger协调，不能假装已应用Evidence。
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ledger/gpu'))
    from memory import EconomicColumns,EconomicMemory
    import importlib.util
    spec=importlib.util.spec_from_file_location('gpu_ledger_platform_projection',
        Path(__file__).resolve().parents[2]/'ledger/gpu/platform.py')
    platform_module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=platform_module
    spec.loader.exec_module(platform_module)
    initial=source['opening_available_pusd']
    platform=Numbers.run_exact_batch(lambda:platform_module.PlatformBalances.open(
        rational([initial,initial,'0','0','0','0','0','0']),rational(['0','0']),
        torch.tensor([0,0],device='cuda'),integers([0])))
    columns=Numbers.run_exact_batch(lambda:EconomicColumns.from_projection_batches(batches))
    ledger=EconomicMemory.open(platform,columns)
    ledger=Numbers.run_exact_batch(lambda:ledger.receive(delivered))
    assert int(ledger.received.sum().cpu())==7
    # 工程夹具已绑定单订单/两token；遮住真实seq2关联必须只停受影响前驱。
    association=(columns.sequences.compare(columns.sequences.constant(2)).order!=0).reshape_as(columns.enabled)
    ledger=Numbers.run_exact_batch(lambda:ledger.apply_pending(association))
    assert values(ledger.platform.platform_source_prefix)==[1]
    assert int(ledger.applied.sum().cpu())==1
    ledger=Numbers.run_exact_batch(lambda:ledger.apply_pending(torch.ones_like(columns.enabled)))
    assert values(ledger.platform.platform_source_prefix)==[7]
    assert int((ledger.applied & columns.enabled).sum().cpu())==7 and not ledger.fault.any().cpu().item()
    expected_platform=source['ledger_projection']['platform_confirmed_projection']
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    from fractions import Fraction
    assert [Fraction(n,d) for n,d in zip(values(ledger.platform.balances.numerator),values(ledger.platform.balances.denominator))]==[
        Fraction(expected_platform[name]) for name in names]
    expected_assets={row['token_id']:row['balance_shares'] for row in expected_platform['outcome_balances']}
    assert [Fraction(n,d) for n,d in zip(values(ledger.platform.outcomes.numerator),values(ledger.platform.outcomes.denominator))]==[
        Fraction(expected_assets.get(token,'0')) for token in ['1','2']]
    # 真实共享NOE→Ledger顺序/绑定候选自行生成entry/local seq，原对照只检查结果。
    from admission import AdmissionLifecycle
    from position_cost import PositionCosts
    from validate_order_funds import open_prefix,check as check_order_funds
    from validate_market_economics import open_prefix as open_markets,check as check_markets
    from validate_rebate_read import check as check_rebates
    from validate_accepted_principal import open_prefix as open_principal,check as check_principal,check_growth as check_principal_growth
    from validate_market_account import origin as market_origin,check as check_market_account,check_nonzero_origin
    from success_pool import SuccessPools
    pools=Numbers.run_exact_batch(lambda:SuccessPools.open(torch.tensor([0,0],device='cuda'),
        torch.tensor([0,0],device='cuda'),torch.tensor([0,1],device='cuda'),torch.tensor([1,0],device='cuda'),1))
    from buy_cost import limit_buy_cost
    from projected_markets import ProjectedMarkets
    assert not source['reservation_checkpoints'][0]['projection']['market_projections']
    initial_population=ProjectedMarkets.open(torch.empty(0,dtype=torch.int64,device='cuda'),
        torch.empty((1,0),dtype=torch.bool,device='cuda'),torch.ones(1,dtype=torch.bool,device='cuda'))
    def open_initial(capacity=None):
        capacity=columns.row_refs.size(1) if capacity is None else capacity
        return Numbers.run_exact_batch(lambda:AdmissionLifecycle.open(platform,state.normalizer.bank,columns,
        integers([source['reservation_checkpoints'][0]['projection']['ledger_entry_seq']]),integers([0]),pools,
        position_costs=PositionCosts.empty(1,capacity,'cuda'),
        order_funds=open_prefix(1,capacity,capacity,columns.row_refs.size(1),source['reservation_checkpoints'][0]),
        unresolved_markets=open_markets(pools,source['reservation_checkpoints'][0],source['initial'][0]['market']['condition_id']),
        projected_markets=initial_population,lifecycle_capacity=capacity,
        accepted_principal=open_principal(pools,source['reservation_checkpoints'][0]),
        market_balance_origin=market_origin(source['reservation_checkpoints'][0],source['initial'][0]['market']['condition_id'])))
    admission=open_initial()
    cost=Numbers.run_exact_batch(lambda:limit_buy_cost(integers([500]),rational(['0.3']),rational([source['fee_rate']]),
        source['fee_exponent'],post_only=torch.tensor([False],device='cuda'),minimum_principal_micro=integers([1_000_000])))
    principal=Numbers.run_exact_batch(lambda:type(rational(['0'])).ratio(cost.principal_micro,cost.principal_micro.constant(1_000_000)))
    fee=Numbers.run_exact_batch(lambda:type(principal).ratio(cost.fee_1e5_units,cost.fee_1e5_units.constant(100_000)))
    missing,reserve_pending=Numbers.run_exact_batch(lambda:admission.reserve_action(zero[:,None],zero[:,None],principal,fee,
        torch.ones((1,1),dtype=torch.bool,device='cuda')))
    assert missing.fault.cpu().tolist()==[0] and reserve_pending.required_market_conditions.cpu().tolist()==[0]
    assert values(missing.current_entry_sequence)==values(admission.current_entry_sequence)
    assert missing.reservations.request_slots.cpu().tolist()==admission.reservations.request_slots.cpu().tolist()
    assert reserve_pending.outcomes.cpu().tolist()==[[0]], '物理目录不足没有Reserved决定或资金提交'
    admission=replace(missing,projected_markets=missing.projected_markets.extended(zero))
    admission,reserve_event=Numbers.run_exact_batch(lambda:admission.reserve_action(zero[:,None],zero[:,None],principal,fee,
        torch.ones((1,1),dtype=torch.bool,device='cuda')))
    assert admission.projected_markets.present.cpu().tolist()==[[True]], '原成功本地预留即进入市场投影人口'
    admission=Numbers.run_exact_batch(lambda:admission.submission_started(zero,zero,zero,zero,torch.tensor([True],device='cuda')))
    assert values(admission.current_entry_sequence)==[5] and values(admission.current_local_prefix)==[2]
    append_start=admission
    check_market_account(admission,source['reservation_checkpoints'][2],source['initial'][0]['market']['condition_id'])
    # 消费实际Submission Accepted，尚未消费任何economic；不能把接纳计数拖到经济到达。
    submitted_reference=next(row for row in source['reservation_checkpoints']
        if row['request'] and row['request']['status']=='PlatformConfirmedPendingEconomicFact')
    submission_column=next(i for i in range(columns.row_refs.size(1))
        if admission.lifecycle.emitted[0,i].cpu().item()
        and admission.lifecycle.schemas[0,i].cpu().item()==3 and admission.lifecycle.accepted[0,i].cpu().item())
    submission_only,used=Numbers.run_exact_batch(lambda:append_start.apply_record(
        torch.tensor([submission_column],device='cuda'),torch.tensor([True],device='cuda'),
        torch.tensor([10000],device='cuda'),torch.tensor([True],device='cuda')))
    assert used.cpu().tolist()==[True] and submission_only.reservations.statuses[0,0].cpu().item()==2
    assert values(submission_only.platform.platform_source_prefix)==[0]
    assert not (submission_only.applied&columns.enabled).any().cpu().item()
    assert check_principal(submission_only,submitted_reference)>0
    check_market_account(submission_only,submitted_reference,source['initial'][0]['market']['condition_id'])
    assert next(m for m in submitted_reference['projection']['market_projections'] if m['condition_id']==source['initial'][0]['market']['condition_id'])['open_order_count']>0
    assert submission_only.orders.accepted_principal_counted.sum().cpu().item()==1
    from closed import ClosedAdmission
    cache=ClosedAdmission.empty(columns)
    # 原领取第8条是经济seq3；其前驱seq2真实位于后续第12条。
    prefix=torch.arange(delivered.record_refs.size(1),device='cuda')[None,:] < 8
    first=replace(delivered,emitted=delivered.emitted & prefix)
    later=replace(delivered,emitted=delivered.emitted & ~prefix)
    cache=Numbers.run_exact_batch(lambda:cache.accept(columns,first))
    admission,cache=Numbers.run_exact_batch(lambda:cache.drain(admission,torch.ones_like(columns.enabled),passes=3))
    first_pending=(cache.received & ~cache.consumed).clone()
    assert first_pending.any().cpu().item()
    first_rank=cache.receive_rank.clone()
    first_available=cache.available_at.clone()
    first_delivery=values(cache.delivered_sequences)
    cache=Numbers.run_exact_batch(lambda:cache.accept(columns,later))
    assert torch.equal(cache.receive_rank[first_pending],first_rank[first_pending])
    assert torch.equal(cache.available_at[first_pending],first_available[first_pending])
    assert [v for v,keep in zip(values(cache.delivered_sequences),first_pending.flatten().cpu().tolist()) if keep]==[
        v for v,keep in zip(first_delivery,first_pending.flatten().cpu().tolist()) if keep]
    admission,cache=Numbers.run_exact_batch(lambda:cache.drain(admission,torch.ones_like(columns.enabled),passes=3))
    assert not (cache.received & ~cache.consumed).any().cpu().item() and admission.fault.cpu().tolist()==[0]
    # 同一原21条NOE，同订单三成交，成交物理目录仅一槽：满池不变成业务错误或提交半个记录。
    limited=open_initial(1)
    limited=replace(limited,projected_markets=limited.projected_markets.extended(zero))
    limited,_=Numbers.run_exact_batch(lambda:limited.reserve_action(zero[:,None],zero[:,None],principal,fee,
        torch.ones((1,1),dtype=torch.bool,device='cuda')))
    limited=Numbers.run_exact_batch(lambda:limited.submission_started(zero,zero,zero,zero,
        torch.tensor([True],device='cuda')))
    assert values(limited.current_entry_sequence)==values(append_start.current_entry_sequence)
    limited_start=limited
    small_waiting=Numbers.run_exact_batch(lambda:ClosedAdmission.empty(columns).accept(columns,delivered))
    initial_rank=small_waiting.receive_rank.clone()
    initial_available=small_waiting.available_at.clone()
    limited,small_waiting=Numbers.run_exact_batch(lambda:small_waiting.drain(limited,torch.ones_like(columns.enabled),passes=3))
    assert limited.fault.cpu().tolist()==[0] and small_waiting.fault.cpu().tolist()==[0]
    assert limited.trades.capacity_required.cpu().tolist()==[2], limited.trades.capacity_required.cpu().tolist()
    assert limited.trades.occupied.sum().cpu().item()==1
    assert (small_waiting.received & ~small_waiting.consumed).any().cpu().item()
    assert values(limited.current_entry_sequence)[0]<values(admission.current_entry_sequence)[0]
    expanded=limited.resized_lifecycle(trades=admission.trades.occupied.size(1))
    assert values(expanded.trades.keys)[:1]==values(limited.trades.keys)
    resumed,small_waiting=Numbers.run_exact_batch(lambda:small_waiting.drain(expanded,torch.ones_like(columns.enabled),passes=3))
    assert resumed.fault.cpu().tolist()==[0] and not (small_waiting.received & ~small_waiting.consumed).any().cpu().item()
    assert values(resumed.current_entry_sequence)==values(admission.current_entry_sequence)
    assert values(resumed.platform.platform_source_prefix)==values(admission.platform.platform_source_prefix)
    assert values(resumed.platform.balances.numerator)==values(admission.platform.balances.numerator)
    assert values(resumed.platform.balances.denominator)==values(admission.platform.balances.denominator)
    assert torch.equal(resumed.applied,admission.applied)
    assert torch.equal(small_waiting.receive_rank,initial_rank) and torch.equal(small_waiting.available_at,initial_available)
    assert torch.equal(resumed.trades.occupied,admission.trades.occupied)
    assert values(resumed.trades.keys)==values(admission.trades.keys)
    assert check_order_funds(resumed,source['reservation_checkpoints'][-1])
    assert check_markets(resumed,source['reservation_checkpoints'][-1],source['initial'][0]['market']['condition_id'])
    # 同一真实设备NOE人口分成两页接入。第一页末是完整feedback边界；
    # 只在来源接齐后关receive门，不把分页末尾当新的业务闭时点。
    from validate_append import check_incremental_receive
    from accepted_principal import AcceptedPrincipal
    # 同一真实来源故意缺本金condition物理槽；扫描须退出而不提交counted/金额半状态。
    principal_limited_start=replace(limited_start,accepted_principal=AcceptedPrincipal.open(1,
        torch.empty(0,dtype=torch.int64,device='cuda'),known=True))
    incremental_checked=check_incremental_receive(state.normalizer.bank,batches,delivered,
        append_start,admission,gates,capacity_start=principal_limited_start)
    assert values(admission.current_entry_sequence)==[source['ledger_projection']['ledger_entry_seq']]
    assert values(admission.current_local_prefix)==[source['ledger_projection']['ledger_local_prefix']]
    assert values(admission.platform.platform_source_prefix)==[7]
    assert int(admission.applied.sum().cpu())==21
    converted=next(row for row in source['reservation_checkpoints'] if row['request'] and row['request']['status']=='Converted')
    assert admission.reservations.statuses.cpu().tolist()[0][0]==3
    assert values(admission.reservations.last_sequences)[0]==converted['request']['last_ledger_entry_seq']
    assert int(admission.orders.flags.sum().cpu())==1 and int(admission.trades.flags.sum().cpu())==0
    assert check_order_funds(admission,source['reservation_checkpoints'][-1])
    assert check_markets(admission,source['reservation_checkpoints'][-1],source['initial'][0]['market']['condition_id'])
    assert check_rebates(admission,source['reservation_checkpoints'][-1],source['initial'][0]['market']['condition_id'])
    check_principal(admission,source['reservation_checkpoints'][-1])
    check_market_account(admission,source['reservation_checkpoints'][-1],source['initial'][0]['market']['condition_id'])
    from validate_repair import check as check_repair
    repair_checked=check_repair(source,append_start,admission,bank=state.normalizer.bank,
        pending=pending,waiting=cache,delivered=delivered)
    check_principal_growth(admission,source,delivered)
    # 该同一实际来源已登记第二市场，但成功池只安装原condition0；非空scope没有池成员
    # 不能用其零归约猜结算历史完整。与累计增长相同，cold槽1绑定真实Enrollment canonical身份。
    enrollment=next(row['kind']['MarketEnrollment']['result']['market']['condition_id']
        for source_batch in source['normalization_reference'] if source_batch['name']=='actual_source'
        for row in source_batch['input']['evidence'] if 'MarketEnrollment' in row['kind'])
    assert enrollment!=source['initial'][0]['market']['condition_id']
    assert admission.success_pools.condition_slots.cpu().tolist()==[0,0]
    from input_success import success_inputs
    uninstalled_scope=Numbers.run_exact_batch(lambda:success_inputs(admission.success_pools,
        torch.tensor([[[1],[-1]]],device='cuda'),torch.tensor([[[True],[False]]],device='cuda'),
        torch.ones((1,2),dtype=torch.bool,device='cuda'),torch.tensor([0,1],device='cuda'),
        prefix_known=torch.ones(1,dtype=torch.bool,device='cuda')))
    assert uninstalled_scope.fault.cpu().tolist()==[0]
    assert uninstalled_scope.paired.cpu().tolist()==[[[0.,0.,1.],[0.,1.,1.]]]
    again,remaining=Numbers.run_exact_batch(lambda:admission.apply_closed(delivered,
        torch.ones_like(columns.enabled),maximum_retry_rounds=1))
    assert values(again.current_entry_sequence)==values(admission.current_entry_sequence)
    assert not remaining.any().cpu().item()
    assert admission.has_available.cpu().tolist()==[True] and admission.available_through.cpu().tolist()==[10000]
    # 同一真实SIM证据再次Core准备产生Duplicate；Ledger领取后NotApplicable不写位置/seq。
    duplicate_batches=[]
    for batch in batches:
        _,duplicate=Numbers.run_exact_batch(lambda:state.normalizer.prepare(batch.rows,batch.emitted))
        assert duplicate.outcomes[duplicate.emitted].cpu().tolist()==[3]*int(duplicate.emitted.sum().cpu())
        duplicate_batches.append(replace(batch,normalized=duplicate,economic_enabled=torch.zeros_like(batch.economic_enabled)))
    duplicate_columns=Numbers.run_exact_batch(lambda:EconomicColumns.from_projection_batches(duplicate_batches))
    duplicate_ledger=Numbers.run_exact_batch(lambda:AdmissionLifecycle.open(platform,state.normalizer.bank,duplicate_columns,
        integers([26]),integers([2]),pools))
    duplicate_ledger,remaining=Numbers.run_exact_batch(lambda:duplicate_ledger.apply_closed(delivered,
        torch.ones_like(columns.enabled),maximum_retry_rounds=1))
    assert not remaining.any().cpu().item() and not duplicate_ledger.applied.any().cpu().item()
    assert values(duplicate_ledger.current_entry_sequence)==[26] and duplicate_ledger.fault.cpu().tolist()==[0]
    # 真实原Ledger同一经济先到，没有Submission绑定不能Convert另一笔未发送预留。
    early=Numbers.run_exact_batch(lambda:AdmissionLifecycle.open(platform,state.normalizer.bank,columns,integers([3]),integers([0]),pools,
        position_costs=PositionCosts.empty(1,columns.row_refs.size(1),'cuda'),
        order_funds=open_prefix(1,columns.row_refs.size(1),columns.row_refs.size(1),columns.row_refs.size(1),source['reservation_checkpoints'][0]),
        unresolved_markets=open_markets(pools,source['reservation_checkpoints'][0],source['initial'][0]['market']['condition_id']),
        accepted_principal=open_principal(pools,source['reservation_checkpoints'][0]),
        market_balance_origin=market_origin(source['reservation_checkpoints'][0],source['initial'][0]['market']['condition_id'])))
    early,_=Numbers.run_exact_batch(lambda:early.reserve_action(zero[:,None],zero[:,None],rational(['1.5']),rational(['0']),
        torch.ones((1,1),dtype=torch.bool,device='cuda')))
    early=Numbers.run_exact_batch(lambda:early.submission_started(zero,zero,zero,zero,torch.tensor([True],device='cuda')))
    early,_=Numbers.run_exact_batch(lambda:early.reserve_action(torch.ones_like(zero[:,None]),zero[:,None],rational(['0.25']),rational(['0']),
        torch.ones((1,1),dtype=torch.bool,device='cuda')))
    sequence=values(columns.sequences)
    first_economic=next(i for i in range(columns.row_refs.size(1)) if columns.enabled[0,i].cpu().item() and sequence[i]==1)
    submission=next(i for i in range(columns.row_refs.size(1)) if early.lifecycle.emitted[0,i].cpu().item() and early.lifecycle.schemas[0,i].cpu().item()==3)
    enabled=torch.tensor([True],device='cuda')
    early,used=Numbers.run_exact_batch(lambda:early.apply_record(torch.tensor([first_economic],device='cuda'),enabled,torch.tensor([10000],device='cuda'),enabled))
    reference=source['economic_first_reference']
    check_market_account(early,reference['before'],source['initial'][0]['market']['condition_id'])
    economic_before_submission=early
    assert check_order_funds(early,reference['before'])
    assert check_markets(early,reference['before'],source['initial'][0]['market']['condition_id'])
    early_cumulative=check_principal(early,reference['before'])
    assert used.cpu().tolist()==[True] and early.reservations.statuses[0,:2].cpu().tolist()==[1,0]
    assert values(early.current_entry_sequence)==[reference['before']['projection']['ledger_entry_seq']]
    assert values(early.reservations.last_sequences)[:2]==[reference['before']['request']['last_ledger_entry_seq'],reference['unsent']['last_ledger_entry_seq']]
    early_funds=Numbers.run_exact_batch(lambda:early.funds())
    assert [Fraction(n,d) for n,d in zip(values(early_funds.actual_available.numerator),values(early_funds.actual_available.denominator))]==[Fraction(reference['before']['cash']['actual_available_pusd'])]
    assert [Fraction(n,d) for n,d in zip(values(early_funds.maker_available.numerator),values(early_funds.maker_available.denominator))]==[Fraction(reference['before']['cash']['maker_available_pusd'])]
    early,used=Numbers.run_exact_batch(lambda:early.apply_record(torch.tensor([submission],device='cuda'),enabled,torch.tensor([10000],device='cuda'),enabled))
    assert used.cpu().tolist()==[True] and early.reservations.statuses[0,:2].cpu().tolist()==[3,0]
    assert values(early.current_entry_sequence)==[reference['after']['projection']['ledger_entry_seq']]
    assert check_order_funds(early,reference['after'])
    assert check_markets(early,reference['after'],source['initial'][0]['market']['condition_id'])
    assert check_principal(early,reference['after'])==early_cumulative
    nonzero_origin_checked=check_nonzero_origin(economic_before_submission,early,reference,source['initial'][0]['market']['condition_id'])
    assert values(early.reservations.last_sequences)[:2]==[reference['after']['request']['last_ledger_entry_seq'],reference['unsent']['last_ledger_entry_seq']]
    fill_economic=next(i for i in range(columns.row_refs.size(1)) if columns.enabled[0,i].cpu().item() and sequence[i]==2)
    fill_sequence=values(early.lifecycle.fills)
    pending_trade=next(i for i in range(columns.row_refs.size(1)) if early.lifecycle.emitted[0,i].cpu().item() and
        early.lifecycle.schemas[0,i].cpu().item()==8 and fill_sequence[i]==1 and early.lifecycle.pending_trades[0,i].cpu().item())
    early,used=Numbers.run_exact_batch(lambda:early.apply_record(torch.tensor([fill_economic],device='cuda'),enabled,torch.tensor([10000],device='cuda'),enabled))
    assert used.cpu().tolist()==[True]
    assert int(early.trades.flags.sum().cpu())==reference['economic_before_trade']['projection']['platform_confirmed_projection']['pending_fill_count']
    assert not early.trade_success.has_fact.any().cpu().item()
    assert check_order_funds(early,reference['economic_before_trade'])
    assert check_markets(early,reference['economic_before_trade'],source['initial'][0]['market']['condition_id'])
    check_market_account(early,reference['economic_before_trade'],source['initial'][0]['market']['condition_id'])
    early,used=Numbers.run_exact_batch(lambda:early.apply_record(torch.tensor([pending_trade],device='cuda'),enabled,torch.tensor([10000],device='cuda'),enabled))
    assert used.cpu().tolist()==[True] and early.fault.cpu().tolist()==[0]
    assert int(early.trades.flags.sum().cpu())==reference['pending_after_economic']['projection']['platform_confirmed_projection']['pending_fill_count']==1
    assert values(early.current_entry_sequence)==[reference['pending_after_economic']['projection']['ledger_entry_seq']]
    assert not early.trade_success.success_applied.any().cpu().item()
    assert check_order_funds(early,reference['pending_after_economic'])
    assert check_markets(early,reference['pending_after_economic'],source['initial'][0]['market']['condition_id'])
    check_market_account(early,reference['pending_after_economic'],source['initial'][0]['market']['condition_id'])
    assert next(m for m in reference['pending_after_economic']['projection']['market_projections'] if m['condition_id']==source['initial'][0]['market']['condition_id'])['pending_fill_count']>0
    funds=Numbers.run_exact_batch(lambda:admission.funds())
    assert admission.projected_markets.present.cpu().tolist()==[[True]], 'Converted和归零不移除市场人口'
    expected_cash=source['reservation_checkpoints'][-1]['cash']
    for actual,name in [(funds.actual_available,'actual_available_pusd'),(funds.maker_available,'maker_available_pusd'),
                        (funds.actual_reserved,'actual_reserved_pusd'),(funds.maker_reserved,'maker_reserved_pusd')]:
        assert [Fraction(n,d) for n,d in zip(values(actual.numerator),values(actual.denominator))]==[Fraction(expected_cash[name])]
    assert funds.fault.cpu().tolist()==[0]
    before_cash=Numbers.run_exact_batch(lambda:admission.maker_cash)
    repeated,_=Numbers.run_exact_batch(lambda:admission.apply_closed(delivered,torch.ones_like(columns.enabled),maximum_retry_rounds=1))
    assert torch.equal(repeated.maker_cash.numerator.words,before_cash.numerator.words)
    # 有界双根资金张量工程验证，仅验证root轴隔离；不创建正式训练根。
    # 该候选不消费NOE，复用只读列与其余冻结字段，仅装配资金/预留所属状态。
    from reservation import ReservationBook
    dual_platform=Numbers.run_exact_batch(lambda:platform_module.PlatformBalances.open(
        rational([initial,initial,'0','0','0','0','0','0']*2),rational(['0']*4),
        torch.tensor([0,0,1,1],device='cuda'),integers([0,0])))
    def funds_candidate(fault):
        return replace(admission,platform=dual_platform,maker_cash=rational([initial,initial]),
            reservations=ReservationBook.empty(2,capacity=1),current_entry_sequence=integers([3,3]),
            current_local_prefix=integers([0,0]),fault=fault,projected_markets=None)
    requests=torch.tensor([[0],[0]],device='cuda')
    pair_enabled=torch.ones((2,1),dtype=torch.bool,device='cuda')
    dual=Numbers.run_exact_batch(lambda:funds_candidate(torch.zeros(2,dtype=torch.int32,device='cuda')))
    dual,event=Numbers.run_exact_batch(lambda:dual.reserve_action(requests,requests,rational(['1','1']),rational(['0','0']),pair_enabled))
    assert event.outcomes.cpu().tolist()==[[1],[1]] and values(dual.current_entry_sequence)==[4,4]
    broken=Numbers.run_exact_batch(lambda:funds_candidate(torch.tensor([1,0],dtype=torch.int32,device='cuda')))
    broken,event=Numbers.run_exact_batch(lambda:broken.reserve_action(requests,requests,rational(['1','1']),rational(['0','0']),pair_enabled))
    assert values(broken.current_entry_sequence)==[3,4]
    assert broken.reservations.request_slots.cpu().tolist()==[[-1],[0]] and broken.fault.cpu().tolist()==[1,0]
    market=next(row for row in source['reservation_checkpoints'][-1]['markets'] if row['condition_id']==source['initial'][0]['market']['condition_id'])
    assert [Fraction(n,d) for n,d in zip(values(admission.success_pools.success_quantity.numerator),values(admission.success_pools.success_quantity.denominator))]==[Fraction(value) for value in market['success_shares']]
    assert [Fraction(n,d) for n,d in zip(values(admission.success_pools.success_cost.numerator),values(admission.success_pools.success_cost.denominator))]==[Fraction(int(value['numerator']),int(value['denominator'])) for value in market['success_principal_cost_pusd']]
    assert int(admission.trade_success.success_applied.sum().cpu())==3
    original_days=source['reservation_checkpoints'][-1]['rebate_days']
    expected_rebates={entity_sequence(value['trade_identity'],'Trade'):(0 if day['kind']=='Maker' else 1,
        day['attribution_day_start_utc_ms'],Fraction(value['contribution_pusd']))
        for day in original_days for value in day['contributions']}
    stored=admission.rebate_contributions
    original_keys=values(stored.trades)
    original_amounts=[Fraction(n,d) for n,d in zip(values(stored.amounts.numerator),values(stored.amounts.denominator))]
    actual_rebates={key:(kind,day,amount) for key,kind,day,amount,present in zip(original_keys,
        stored.kinds.flatten().cpu().tolist(),stored.days.flatten().cpu().tolist(),original_amounts,
        stored.occupied.flatten().cpu().tolist()) if present}
    assert actual_rebates==expected_rebates and len(actual_rebates)==3
    # 原贡献身份再次出现在新Ledger位置时也不重复应计；本索引不改现金/应收。
    first=next(i for i,present in enumerate(stored.occupied.flatten().cpu().tolist()) if present)
    duplicate_rebate,required,changed=Numbers.run_exact_batch(lambda:stored.add(integers([original_keys[first]]),
        stored.kinds[:,first],stored.days[:,first],stored.conditions[:,first],
        type(stored.amounts).from_values([original_amounts[first].numerator],[original_amounts[first].denominator]),
        torch.tensor([True],device='cuda')))
    assert changed.cpu().tolist()==[False] and int(duplicate_rebate.occupied.sum().cpu())==3
    first_reference=source['contribution_first_reference']
    first_fact=first_reference['record']['Evidence']['evidence']['SettlementLifecycleV2']['result']['Contribution']
    first_trade=entity_sequence(first_fact['trade_identity'],'Trade')
    early_contribution=Numbers.run_exact_batch(lambda:AdmissionLifecycle.open(platform,state.normalizer.bank,columns,
        integers([3]),integers([0]),pools))
    fill_columns=values(early_contribution.lifecycle.fills)
    contribution_at=next(i for i in range(columns.row_refs.size(1)) if early_contribution.lifecycle.emitted[0,i].cpu().item()
        and early_contribution.lifecycle.schemas[0,i].cpu().item()==9 and fill_columns[i]==first_trade)
    early_contribution,used=Numbers.run_exact_batch(lambda:early_contribution.apply_record(torch.tensor([contribution_at],device='cuda'),
        torch.tensor([True],device='cuda'),torch.tensor([10000],device='cuda'),torch.tensor([True],device='cuda')))
    assert used.cpu().tolist()==[True] and not early_contribution.trade_success.has_fact.any().cpu().item()
    assert values(early_contribution.current_entry_sequence)==[first_reference['checkpoint']['projection']['ledger_entry_seq']]
    assert int(early_contribution.rebate_contributions.occupied.sum().cpu())==1
    assert values(early_contribution.platform.platform_source_prefix)==[0]
    # 同一真实已领取块跨人口扩池：第二条贡献容量不足时不提交位置/条目，扩池后重试。
    capacity_candidate=Numbers.run_exact_batch(lambda:AdmissionLifecycle.open(platform,state.normalizer.bank,columns,
        integers([3]),integers([0]),pools))
    from rebate_contribution import RebateContributions
    capacity_candidate=replace(capacity_candidate,rebate_contributions=Numbers.run_exact_batch(
        lambda:RebateContributions.empty(1,1,zero.device)))
    contribution_rows=[i for i in range(columns.row_refs.size(1)) if capacity_candidate.lifecycle.emitted[0,i].cpu().item()
        and capacity_candidate.lifecycle.schemas[0,i].cpu().item()==9]
    for ordinal,local in enumerate(contribution_rows[:2]):
        capacity_candidate,used=Numbers.run_exact_batch(lambda:capacity_candidate.apply_record(torch.tensor([local],device='cuda'),
            torch.tensor([True],device='cuda'),torch.tensor([10000],device='cuda'),torch.tensor([True],device='cuda')))
        assert used.cpu().tolist()==[ordinal==0]
    assert values(capacity_candidate.current_entry_sequence)==[4] and int(capacity_candidate.applied.sum().cpu())==1
    assert capacity_candidate.rebate_contributions.capacity_required.cpu().tolist()==[2]
    assert capacity_candidate.fault.cpu().tolist()==[0]
    original_key=values(capacity_candidate.rebate_contributions.trades)[0]
    original_amount=Fraction(values(capacity_candidate.rebate_contributions.amounts.numerator)[0],
        values(capacity_candidate.rebate_contributions.amounts.denominator)[0])
    capacity_candidate=Numbers.run_exact_batch(lambda:capacity_candidate.resized_contributions(2),initial_word_capacity=8)
    assert capacity_candidate.rebate_contributions.trades.words.size(1)==8
    assert values(capacity_candidate.rebate_contributions.trades)==[original_key,0]
    assert [Fraction(n,d) for n,d in zip(values(capacity_candidate.rebate_contributions.amounts.numerator),
        values(capacity_candidate.rebate_contributions.amounts.denominator))]==[original_amount,Fraction(0)]
    capacity_candidate,used=Numbers.run_exact_batch(lambda:capacity_candidate.apply_record(torch.tensor([contribution_rows[1]],device='cuda'),
        torch.tensor([True],device='cuda'),torch.tensor([10000],device='cuda'),torch.tensor([True],device='cuda')))
    assert used.cpu().tolist()==[True] and int(capacity_candidate.rebate_contributions.occupied.sum().cpu())==2
    assert values(capacity_candidate.current_entry_sequence)==[5] and int(capacity_candidate.applied.sum().cpu())==2
    executed=Numbers.run_exact_batch(lambda:admission.trade_success.executed(zero[:,None],torch.ones_like(zero[:,None],dtype=torch.bool)))
    assert executed.known.cpu().tolist()==[[True,True]] and executed.fault.cpu().tolist()==[0]
    expected_executed=source['reservation_checkpoints'][-1]['markets'][0]['executed']
    sys.path[:0] = [str(Path(__file__).resolve().parents[2]/'model-train/native-inputs/gpu'),
                   str(Path(__file__).resolve().parents[2]/'data-pipeline/gpu')]
    from input_executed import executed_inputs,write_executed_fields
    native_executed = Numbers.run_exact_batch(lambda:executed_inputs(admission.trade_success,
        torch.tensor([[[0],[-1]]],device='cuda'),torch.tensor([[[True],[False]]],device='cuda'),
        torch.ones((1,2),dtype=torch.bool,device='cuda'),prefix_known=torch.ones(1,dtype=torch.bool,device='cuda')))
    native_slow = write_executed_fields(torch.zeros((1,433),device='cuda'),native_executed)
    import math
    expected_native = [element for row in expected_executed for key in ['shares','principal_pusd','actual_fee_pusd']
        for element in [math.copysign(math.log1p(abs(float(Fraction(row[key])))),float(Fraction(row[key]))),1.,1.]]
    torch.testing.assert_close(native_slow[0,176:194].cpu(),torch.tensor(expected_native),rtol=1e-6,atol=1e-7)
    assert native_slow[0,194:212].cpu().tolist()==[0.,1.,1.]*6
    assert native_executed.fault.cpu().tolist()==[0]
    for name,key in [('quantity','shares'),('principal','principal_pusd'),('actual_fee','actual_fee_pusd')]:
        value=getattr(executed,name)
        actual=[Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
        assert actual==[Fraction(row[key]) for row in expected_executed],(name,actual,expected_executed)
    cost_read=Numbers.run_exact_batch(admission.position_costs.read)
    assert cost_read.known.cpu().tolist()==[True] and cost_read.fault.cpu().tolist()==[0]
    for name,key in [('cost','position_cost_basis_pusd'),('fee','fees_pusd')]:
        value=getattr(cost_read,name)
        assert [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]==[Fraction(expected_platform[key])]
    asset_cost=Numbers.run_exact_batch(lambda:admission.position_costs.asset_cost(admission.platform.asset_roots))
    assert asset_cost.known.cpu().tolist()==[True,True] and asset_cost.fault.cpu().tolist()==[0,0]
    expected_costs={row['token_id']:row['position_cost_basis_pusd'] for row in expected_platform['outcome_balances']}
    assert [Fraction(n,d) for n,d in zip(values(asset_cost.cost.numerator),values(asset_cost.cost.denominator))]==[
        Fraction(expected_costs.get(token,'0')) for token in ['1','2']]
    estimate=Numbers.run_exact_batch(lambda:admission.trade_success.estimated_rebate(admission.trades,
        admission.rebate_contributions,zero[:,None],torch.ones_like(zero[:,None],dtype=torch.bool)))
    assert estimate.known.cpu().tolist()==[[True,True]] and estimate.fault.cpu().tolist()==[0]
    original_estimate=[row['estimated_pusd']['Known'] for row in market['rebates']]
    assert [Fraction(n,d) for n,d in zip(values(estimate.amount.numerator),values(estimate.amount.denominator))]==[
        Fraction(int(row['numerator']),int(row['denominator'])) for row in original_estimate]
    exact_contributions=[Fraction(n,d) for n,d in zip(values(admission.trade_success.unpaired_contribution.numerator),values(admission.trade_success.unpaired_contribution.denominator))]
    expected_contributions={int(row['fact']['fill_sequence']):Fraction(int(row['unpaired_contribution_shares']['Known']['numerator']),
        int(row['unpaired_contribution_shares']['Known']['denominator'])) for row in source['reservation_checkpoints'][-1]['trades'] if row['success_applied']}
    for slot,(sequence,present) in enumerate(zip(values(admission.trades.keys),admission.trades.occupied.flatten().cpu().tolist())):
        if present:
            assert exact_contributions[slot]==expected_contributions[sequence]
    assert [Fraction(n,d) for n,d in zip(values(admission.success_pools.quantity.numerator),values(admission.success_pools.quantity.denominator))]==[Fraction(value) for value in market['orphan_shares']]
    assert [Fraction(n,d) for n,d in zip(values(admission.success_pools.paired_quantity.numerator),values(admission.success_pools.paired_quantity.denominator))]==[Fraction(market['paired_shares'])]*2
    before=ledger.platform
    again=Numbers.run_exact_batch(lambda:ledger.receive(delivered).apply_pending(torch.ones_like(columns.enabled)))
    assert values(again.platform.platform_source_prefix)==[7]
    assert torch.equal(again.platform.balances.numerator.words,before.balances.numerator.words)
    assert ledger.available_at[ledger.received].cpu().tolist()==[10000]*7
    from validate_two_root_projection import validate_two_root_projection
    two_root_report=validate_two_root_projection(source)
    return {'nonzero_market_origin_all_in_and_counts':nonzero_origin_checked,'repair_fields':repair_checked,'two_root_projection':two_root_report,'device_generated_NOE_records':checked,'device_batches':len(batches),'original_positions_and_receive_order_checked':True,
        'bank_physical_rows':state.normalizer.bank.kinds.numel(),'masked_or_unemitted_rows':state.normalizer.bank.kinds.numel()-checked,
        'actual_device_Ledger_economic_position_predecessor_checked':True,
        'Ledger_admission_device_generated_entry_local_and_binding_checked':True,
        'actual_reserved_market_population_persists_after_conversion_and_directory_retry':True,
        'device_closed_cache_waiting_then_new_records_retry_checked':True,'actual_trade_directory_capacity_exit_then_growth_preserves_waiting_prefix_checked':True,
        'incremental_feedback_append_preserves_account_waiting_and_delivery_checked':True,
        'incremental_feedback_pages':incremental_checked,
        'duplicate_NOE_NotApplicable_without_Ledger_commit_checked':True,'unbound_economic_before_Submission_unsent_reservation_checked':True,'actual_existing_economic_MakerCash_and_four_funds_checked':True,'two_root_one_item_funds_fault_isolation_checked':True,'actual_trade_economic_success_pool_checked':True,'fill_economic_before_pending_lifecycle_checked':True,'actual_original_six_rebate_read_fields_checked':True,'actual_per_Ledger_record_unresolved_Pi_and_maximum_loss_checked':True,'actual_economic_before_Trade_OrderFunds_Unknown_and_binding_checked':True,'actual_unpaired_trade_contribution_checked':True,'actual_taker_contribution_index_checked':True,'actual_trade_role_and_net_fee_executed_read_checked':True,'Native_actual_executed_quantity_principal_net_fee_176_to_211_checked':True,'actual_applied_position_cost_and_fee_read_checked':True,'actual_estimated_rebate_read_checked':True,'full_clock_Ledger_reservation_delivered':False}
