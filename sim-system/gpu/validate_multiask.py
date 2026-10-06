"""原外层合法多ask请求的逐fill及同时间义务生产接线验证。"""
import json
import sys
import time
import torch
from queues import Numbers,PriceQueueBatch
from account_state import PlatformAccountState
from base_execution import prepare_base_order
from base_acceptance import run_base_acceptance_batch
from fill_finality import FillRecord
from fill_pool import FillPool
from validate_base_execution import integers,rational,units,values,assert_checkpoint
from latency import EmpiricalDelaySampler, byte_column
from pathlib import Path
from feedback import FeedbackEmitter
import importlib.util
import sys as modules


def main():
    source = json.load(sys.stdin)
    started = time.perf_counter()
    asks = source['initial'][0]['payload']['event']['asks']
    def prepare():
        queue = PriceQueueBatch.empty(rational([ask['price_pusd_per_share'] for ask in asks]),capacity=2)
        queue,_ = queue.append_external(integers([units(ask['quantity_shares']) for ask in asks]))
        event = source['addition']['payload']['event']
        before = integers([units(event['quantity_before_shares']),units(asks[1]['quantity_shares'])])
        after = integers([units(event['quantity_after_shares']),units(asks[1]['quantity_shares'])])
        queue,_ = queue.apply_level_transition(before,after)
        account = PlatformAccountState.open(integers([10_000_000]),torch.tensor([0,0],device='cuda'))
        prepared = prepare_base_order(queue,account,torch.arange(len(asks),device='cuda')[None,:],
            integers([5_000_000]),integers([1_500_000]),rational(['0.3']),rational([source['fee_rate']]),source['fee_exponent'],
            torch.tensor([0],device='cuda'),torch.tensor([True],device='cuda'))
        return queue,account,prepared
    queue,account,prepared = Numbers.run_exact_batch(prepare)
    at = torch.tensor([source['processing_at']],device='cuda')
    inactive = torch.tensor([False],device='cuda')
    template = FillRecord.create(prepared.asset_plan.matched_base,prepared.asset_plan.used_quote,prepared.fee.total_micro,
        torch.tensor([0],device='cuda'),at,inactive,source['finality_path'])
    pool = FillPool.empty(template,capacity=1)
    rebate = Numbers.run_exact_batch(lambda:rational([source['taker_rebate_rate']]))
    queue,account,pool,accepted = run_base_acceptance_batch(prepared,queue,account,pool,at,
        torch.zeros((1,prepared.maximum_fills),dtype=torch.int64,device='cuda'),rebate,
        torch.zeros(queue.rows,dtype=torch.int64,device='cuda'))
    from fill_pool import _take_record
    from queues import _take
    from asset_buy import take_ratio
    gpu_trade_frames=[]
    gpu_economic_frames=[(accepted.submission_delta,0,integers([1]),torch.zeros(1,dtype=torch.int64,device='cuda'),at)]
    # 同价多段对应同来源价，各实际fill保留原plan人口。
    source_prices=[]
    for column,frame in enumerate(accepted.fills):
        record=_take_record(pool.records,frame.slot)
        price=take_ratio(prepared.taker_prices,torch.tensor([column],device='cuda'))
        if frame.event.emitted.cpu().item():
            source_prices.append(price)
            gpu_trade_frames.append((record,frame,price))
    price_by_slot=type(source_prices[0])(type(source_prices[0].numerator)(
        torch.cat([price.numerator.words for price in source_prices]),
        torch.cat([price.numerator.sign for price in source_prices]),
        torch.cat([price.numerator.fault for price in source_prices])),type(source_prices[0].denominator)(
        torch.cat([price.denominator.words for price in source_prices]),
        torch.cat([price.denominator.sign for price in source_prices]),
        torch.cat([price.denominator.fault for price in source_prices])))
    assert accepted.submission_delta.emitted.cpu().tolist() == [True]
    assert pool.capacity == 4 and values(pool.next_sequence) == [4]
    actual = [values(frame.fill_sequence)[0] for frame in accepted.fills if frame.event.emitted.cpu().item()]
    assert actual == [1,2,3]
    assert values(pool.records.base) == [1_000_001,1_000_000,1_000_000,0]
    assert values(pool.records.quote) == [200_000,200_000,250_000,0]
    assert values(pool.records.fee) == values(prepared.fee.per_fill_micro)
    original_quantity = prepared.quantity
    order = []
    for index,checkpoint in enumerate(source['checkpoints']):
        if index:
            scheduled,slot,pending = pool.next_obligation()
            assert pending.cpu().tolist() == [True] and scheduled.cpu().tolist() == [checkpoint['at']]
            pool,account,frame = Numbers.run_exact_batch(lambda:pool.advance_one(account,
                torch.tensor([checkpoint['at']],device='cuda'),rebate))
            assert frame.event.emitted.cpu().tolist() == [True]
            assert values(frame.fill_sequence) == [checkpoint['fill_sequence']]
            order.append(checkpoint['fill_sequence'])
            gpu_trade_frames.append((_take_record(pool.records,frame.slot),frame,take_ratio(price_by_slot,frame.slot)))
        assert_checkpoint(checkpoint['state'],original_quantity,account)
        for record in checkpoint['state']['fill_records']:
            coordinate = record['sequence']-1
            assert pool.records.ordinal[coordinate].cpu().item() == record['ordinal']
            assert values(pool.records.economic_application_sequence)[coordinate] == (record['economic_application_sequence'] or 0)
    assert order == [1,2,3,1,2,3] and not pool.next_obligation()[-1].any().cpu().item()
    for record,frame,_ in gpu_trade_frames:
        event=frame.event
        event_at=record.matched_at+record.offsets[record.selected_path,event.ordinal]
        for delta,tag in [(event.economic_delta,6),(event.failure_delta,7),(event.rebate_delta,10)]:
            if delta.emitted.cpu().item():
                gpu_economic_frames.append((delta,tag,frame.fill_sequence,event.ordinal,event_at))
    # 原完整BusinessProjectionBatch→MemoryNOE是冷来源参考，运行热链不调用CPU端口。
    profile_sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    profile_path=Path(__file__).resolve().parents[1]/'config/latency-profiles'/profile_sha/'profile.json'
    sampler=EmpiricalDelaySampler.build(profile_path.read_bytes(),profile_sha,
        [source['session_seed'].encode('ascii')])
    lifecycle=[]
    direct=[]
    emitter=FeedbackEmitter.open(1)
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'execution-normalization/gpu'))
    from source_order import map_source_ordinals
    source_ordinal_checks=0
    actual_inputs={case['input']['source_watermark']['projection_batch_sequence']:case['input']
        for case in source['normalization_reference'] if case['name']=='actual_source'}
    for batch in source['feedback_batches']:
        drafts=[]
        for category_number,category in enumerate(['orders','trades','settlements'],start=1):
            for row in batch[category]:
                feedback=row.get('feedback',row)
                metadata=feedback['metadata']
                lifecycle.append(metadata)
                delta=feedback.get('account_delta') if category=='trades' else next(iter(feedback['event'].values())).get('account_delta')
                drafts.append((False,metadata,category_number,1+int(delta is not None)))
        direct.extend(row['metadata'] for row in batch['direct'])
        drafts.extend((True,row['metadata'],0,1) for row in batch['direct'])
        drafts.sort(key=lambda row:row[1]['private_feedback_sequence'])
        active=torch.ones((1,len(drafts)),dtype=torch.bool,device='cuda')
        is_direct=torch.tensor([[row[0] for row in drafts]],dtype=torch.bool,device='cuda')
        platform=torch.tensor([[row[1]['platform_responded_at_utc_ms'] if row[0] else row[1]['platform_event_at_utc_ms']
                                for row in drafts]],dtype=torch.int64,device='cuda')
        received=torch.tensor([[row[1]['response_received_at_utc_ms'] if row[0] else 0 for row in drafts]],dtype=torch.int64,device='cuda')
        emitter,emission=Numbers.run_exact_batch(lambda:emitter.emit(sampler,active,is_direct,platform,platform,
            received,torch.zeros_like(platform,dtype=torch.int32),
            control_admission=torch.tensor([batch['has_control_admission']],device='cuda')))
        assert emission.emitted.cpu().tolist()==[[True]*len(drafts)]
        assert values(emission.private_sequences)==[row[1]['private_feedback_sequence'] for row in drafts]
        assert values(emission.batch_sequences)==[batch['batch_sequence']]*len(drafts)
        assert emission.received_at.cpu().tolist()==[[row[1]['response_received_at_utc_ms'] if row[0] else
            row[1]['feedback_received_at_utc_ms'] for row in drafts]]
        mapped=map_source_ordinals(active,
            torch.tensor([[row[2] for row in drafts]],dtype=torch.int64,device='cuda'),
            torch.tensor([[row[3] for row in drafts]],dtype=torch.int64,device='cuda'),
            torch.zeros_like(platform,dtype=torch.int32))
        by_private={}
        for evidence in actual_inputs[batch['batch_sequence']]['evidence']:
            record=evidence['source_record']
            if isinstance(record,dict):
                record=next(iter(record.values()))
                if 'private_feedback_sequence' in record:
                    by_private.setdefault(record['private_feedback_sequence'],[]).append(evidence['source_ordinal'])
        assert mapped.first_ordinals.cpu().tolist()==[[min(by_private[row[1]['private_feedback_sequence']]) for row in drafts]]
        assert mapped.record_counts.cpu().tolist()==[[len(by_private[row[1]['private_feedback_sequence']]) for row in drafts]]
        assert not mapped.fault.any().cpu().item()
        source_ordinal_checks+=len(drafts)
    sequence=integers([row['private_feedback_sequence'] for row in lifecycle])
    roots=torch.zeros(len(lifecycle),dtype=torch.int64,device='cuda')
    sampled=sampler.sample_feedback(roots,sequence)
    arrived,fault=sampled.arrival_at(torch.tensor([row['platform_event_at_utc_ms'] for row in lifecycle],device='cuda'))
    assert not fault.any().cpu().item()
    assert arrived.cpu().tolist()==[row['feedback_received_at_utc_ms'] for row in lifecycle]
    identities,offsets=byte_column([row['global_request_id'].encode('utf-8') for row in direct])
    roots=torch.zeros(len(direct),dtype=torch.int64,device='cuda')
    sampled=sampler.sample_requests(roots,identities,offsets,torch.ones_like(roots))
    arrived,fault=sampled.arrival_at(torch.tensor([row['platform_responded_at_utc_ms'] for row in direct],device='cuda'))
    assert not fault.any().cpu().item()
    assert arrived.cpu().tolist()==[row['response_received_at_utc_ms'] for row in direct]
    assert source['initial_noe_records']>0 and source['noe_records']
    path=Path(__file__).resolve().parents[2]/'execution-normalization/gpu/validate_core.py'
    spec=importlib.util.spec_from_file_location('gpu_noe_core_validation',path)
    normalization=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(normalization)
    normalization_report=normalization.validate(source['normalization_reference'])
    trade_report=normalization.validate_trades(source['noe_records'],gpu_trade_frames)
    economic_report=normalization.validate_economic(source['noe_records'],gpu_economic_frames)
    path=Path(__file__).resolve().parents[2]/'execution-normalization/gpu/consumption.py'
    spec=importlib.util.spec_from_file_location('gpu_noe_consumption',path)
    consumption=importlib.util.module_from_spec(spec)
    modules.modules[spec.name]=consumption
    spec.loader.exec_module(consumption)
    # 冷对照输入是原NOE已接纳记录，device消费不重新编号record position。
    records=list(reversed(source['noe_records']))
    def source_coordinate(row):
        body=next(iter(row['record'].values()))
        reference=body.get('source_reference') or body['fact']['source_reference']
        metadata=next(iter(reference['record'].values()))
        return (metadata.get('feedback_received_at_utc_ms',metadata.get('response_received_at_utc_ms')),
                metadata['private_feedback_sequence'],body['source_order']['ordinal'])
    columns=[source_coordinate(row) for row in records]
    active=torch.ones((1,len(records)),dtype=torch.bool,device='cuda')
    pending=Numbers.run_exact_batch(lambda:consumption.PendingRecords.from_columns(active,
        torch.tensor([[row[0] for row in columns]],device='cuda'),integers([row[1] for row in columns]),
        integers([row[2] for row in columns]),torch.arange(len(records)-1,-1,-1,device='cuda')[None,:],integers([0])))
    gates=consumption.ReceiveGates.unset(1)
    pending,closed=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not closed.emitted.any().cpu().item()
    gates=gates.set_receive(torch.tensor([10_000],device='cuda'),torch.tensor([True],device='cuda'))
    pending,closed=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not closed.emitted.any().cpu().item()
    gates=gates.set_public(torch.tensor([1_200],device='cuda'),torch.tensor([True],device='cuda'))
    pending,closed=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not closed.emitted.any().cpu().item()
    gates=gates.set_public(torch.tensor([10_000],device='cuda'),torch.tensor([True],device='cuda'))
    pending,closed=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert closed.emitted.cpu().tolist()==[[True]*len(records)]
    assert closed.record_refs.cpu().tolist()==[list(range(len(records)))]
    assert values(closed.delivered_sequences)==[row['visibility']['Feedback']['delivery_sequence'] for row in source['noe_records']]
    pending,repeated=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not repeated.emitted.any().cpu().item()
    # 同一发射候选u64溢出不能耗序号或发出半批，已成功旧批不回滚。
    boundary=FeedbackEmitter(integers([(1<<64)-2]),integers([2]),torch.zeros(1,dtype=torch.int32,device='cuda'))
    active=torch.ones((1,2),dtype=torch.bool,device='cuda')
    direct_mask=torch.zeros_like(active)
    at=torch.full((1,2),1200,dtype=torch.int64,device='cuda')
    candidate,event=Numbers.run_exact_batch(lambda:boundary.emit(sampler,active,direct_mask,at,at,at,
        torch.zeros_like(at,dtype=torch.int32)))
    assert not event.emitted.any().cpu().item() and event.root_fault.cpu().tolist()==[1]
    assert values(candidate.next_private_sequence)==[(1<<64)-2] and values(candidate.next_batch_sequence)==[2]
    platform_checked=False
    if source['ledger_projection'] is not None:
        path=Path(__file__).resolve().parents[2]/'ledger/gpu/platform.py'
        spec=importlib.util.spec_from_file_location('gpu_ledger_platform',path)
        platform=importlib.util.module_from_spec(spec)
        modules.modules[spec.name]=platform
        spec.loader.exec_module(platform)
        initial=source['opening_available_pusd']
        state=Numbers.run_exact_batch(lambda:platform.PlatformBalances.open(
            rational([initial,initial,'0','0','0','0','0','0']),rational(['0','0']),
            torch.tensor([0,0],device='cuda'),integers([0])))
        economic=[]
        field_names=['cash_pusd_delta','available_pusd_delta','principal_encumbrance_pusd_delta',
            'immediate_taker_fee_encumbrance_pusd_delta','maker_rebate_receivable_pusd_delta',
            'taker_rebate_receivable_pusd_delta','maker_rebate_locked_remainder_pusd_delta',
            'taker_rebate_locked_remainder_pusd_delta']
        for record in source['noe_records']:
            body=next(iter(record['record'].values()))
            if 'fact' not in body or 'AccountTransition' not in body['fact']['economic_identity']:
                continue
            fact=body['fact']
            delta=fact['disposition']['AccountTransition']['balance_delta']
            outcomes={row['token_id']:row['balance_shares_delta'] for row in delta['outcome_balance_deltas']}
            economic.append(Numbers.run_exact_batch(lambda:(
                integers([fact['economic_identity']['AccountTransition']['account_transition_seq']]),
                rational([delta[field] for field in field_names]),
                rational([outcomes.get('1','0'),outcomes.get('2','0')]))))
        # 保留实际领取史1,3,4,2,7,5,6；设备缺前驱候选等待而非重排史。
        assert [values(row[0])[0] for row in economic]==[1,3,4,2,7,5,6]
        platform_applied=torch.zeros(len(economic),dtype=torch.bool,device='cuda')
        for _ in range(len(economic)):
            for index,(sequence,balance_delta,outcome_delta) in enumerate(economic):
                state,event=Numbers.run_exact_batch(lambda:state.apply_transition(sequence,balance_delta,outcome_delta,
                    torch.tensor([True],device='cuda'),platform_applied[index:index+1],torch.tensor([True],device='cuda')))
                platform_applied[index]=platform_applied[index] | event.applied[0]
        assert platform_applied.cpu().tolist()==[True]*len(economic) and values(state.platform_source_prefix)==[7]
        expected=source['ledger_projection']['platform_confirmed_projection']
        names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
            'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
        from fractions import Fraction
        actual=[Fraction(n,d) for n,d in zip(values(state.balances.numerator),values(state.balances.denominator))]
        assert actual==[Fraction(expected[name]) for name in names]
        expected_outcomes={row['token_id']:row['balance_shares'] for row in expected['outcome_balances']}
        actual=[Fraction(n,d) for n,d in zip(values(state.outcomes.numerator),values(state.outcomes.denominator))]
        assert actual==[Fraction(expected_outcomes.get(token,'0')) for token in ['1','2']]
        platform_checked=True
    import os
    artifact=os.environ.get('B_GPU_ORDER_ARTIFACT_DIR')
    if artifact:
        Path(artifact,'original-noe-reference.json').write_text(json.dumps({
            'feedback_batches':source['feedback_batches'],'noe_records':source['noe_records'],
            'normalization_reference':source['normalization_reference'],
            'original_ledger_projection':source['ledger_projection']},ensure_ascii=False))
    print(json.dumps({'original_external_5share_request':True,'taker_fills':3,'checkpoints':len(source['checkpoints']),
        'same_time_consumption_fill_sequences':order,'fill_pool_capacity':pool.capacity,
        'elapsed_seconds_with_reference_reads':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'original_NOE_records':len(source['noe_records']),'actual_lifecycle_device_delay_checks':len(lifecycle),
        'actual_device_private_batch_numbering_checked':True,
        'actual_device_source_ordinal_checks':source_ordinal_checks,
        'actual_NOE_device_delivery_order_checked':True,
        'original_Ledger_reference_checked':source['ledger_projection'] is not None,
        'Ledger_platform_balance_and_predecessor_candidate_checked':platform_checked,
        'normalization_core_candidate':normalization_report,
        'actual_SIM_device_trade_mapping':trade_report,
        'actual_SIM_device_economic_mapping':economic_report,
        'original_feedback_dual_visibility_gate_checked':True,
        'global_clock_NOE_Ledger_delivered':False}))


if __name__ == '__main__':
    main()
