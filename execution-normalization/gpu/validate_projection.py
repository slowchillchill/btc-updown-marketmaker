"""实际SIM设备来源批→Core→领取的原NOE位置与正文冷对照。"""
import torch
import json
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


def expected_semantic(record):
    zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    body=next(iter(record['record'].values()))
    if 'fact' in body:
        fact=body['fact']
        value=fact['disposition']['AccountTransition']
        transition=value['transition']
        if 'Existing' in transition:
            transition=transition['Existing']
        name,fields=next(iter(transition.items()))
        tag={'OrderAcceptedEncumbranceApplied':0,'FillFinalityEconomicApplied':6,'TakerRebateAccrualApplied':10}[name]
        identity=fields['order_identity' if tag==0 else 'trade_identity']
        entity=json.loads(identity.split(':',1)[1])[-1]
        sequence=int(next(iter(next(iter(entity.values())).values())).removeprefix('sim-'))
        balance=value['balance_delta']
        if 'existing' in balance:
            balance=balance['existing']
        names=['cash_pusd_delta','available_pusd_delta','principal_encumbrance_pusd_delta','immediate_taker_fee_encumbrance_pusd_delta',
            'maker_rebate_receivable_pusd_delta','taker_rebate_receivable_pusd_delta','maker_rebate_locked_remainder_pusd_delta','taker_rebate_locked_remainder_pusd_delta']
        outcomes=balance['outcome_balance_deltas']
        return encode_fields([zero,zero,torch.tensor([value['economic_effective_at_utc_ms']],device='cuda'),
            torch.full_like(zero,tag),integers([sequence]),torch.full_like(zero,fields.get('path_ordinal',0)),
            *[rational([balance[name]]) for name in names],torch.tensor([bool(outcomes)],device='cuda'),zero,
            rational([outcomes[0]['balance_shares_delta'] if outcomes else '0']),rational(['0']),rational(['0'])])
    kind,value=next(iter(body['evidence'].items()))
    value=value['result']
    if kind=='TradeLifecycleV3':
        value=value['result']
        state={'MatchedNotBroadcasted':0,'Matched':1,'Retrying':2,'Mined':3,'Confirmed':4,'Failed':5}[value['state']]
        disposition={'PendingUnapplied':0,'PendingApplied':1,'FinalSuccess':2,'FinalFailureUnapplied':3,'FinalFailureCompensated':4}[value['economic_disposition']]
        return encode_fields([zero,zero,zero,integers([value['fill_sequence']]),integers([entity_sequence(value['order_identity'],'Order')]),
            torch.full_like(zero,state),torch.full_like(zero,disposition),torch.tensor([value['matched_at_utc_ms']],device='cuda'),
            torch.full_like(zero,{'Taker':0,'Maker':1}[value['liquidity_role']]),rational([value['source_price_pusd_per_share']]),integers([units(value['base_quantity_shares'])]),
            integers([units(value['quote_amount_pusd'])]),integers([units(value['immediate_taker_fee_pusd'])]),
            torch.tensor([value['economic_application_account_transition_seq'] is not None],device='cuda'),
            integers([value['economic_application_account_transition_seq'] or 0]),torch.full_like(zero,2),rational(['0'])])
    if kind=='OrderLifecycle':
        return encode_fields([zero,zero,zero,integers([entity_sequence(value['order_identity'],'Order')]),zero,
            torch.full_like(zero,{'Open':0,'Matched':1}[value['event']['Accepted']['state']])])
    if kind=='Control':
        value=value['Submission']['outcome']['Accepted']
        return encode_fields([zero,zero,zero,zero,integers([entity_sequence(value['order']['order_identity'],'Order')]),
            torch.full_like(zero,{'Open':0,'Matched':1}[value['order']['state']]),
            integers([units(value['measurement']['Base']['matched_base_quantity_shares'])]),
            integers([units(value['measurement']['Base']['open_base_quantity_shares'])]),
            torch.tensor([value['effective_processing_at_utc_ms']],device='cuda')])
    if kind=='SettlementLifecycleV2':
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
    return {'device_generated_NOE_records':checked,'device_batches':len(batches),'original_positions_and_receive_order_checked':True,
        'bank_physical_rows':state.normalizer.bank.kinds.numel(),'masked_or_unemitted_rows':state.normalizer.bank.kinds.numel()-checked,
        'full_clock_Ledger_reservation_delivered':False}
