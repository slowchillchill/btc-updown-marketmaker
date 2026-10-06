"""一次真实MINED前缀冷初始化，此后设备CONFIRMED→NOE→Ledger日链连续推进。

不是活动根恢复接口；原before只在开头安装一次，后续每个实际记录均使用设备状态。
"""
from dataclasses import replace
from pathlib import Path
from fractions import Fraction
import importlib.util
import json
import sys
import time
import torch
from queues import Numbers,ExactRational
from account_state import PlatformAccountState
from fill_finality import FillRecord
from fill_pool import FillPool,_take_record
from taker_days import TakerDays
from latency import EmpiricalDelaySampler
from feedback import FeedbackEmitter
from validate_base_execution import integers,rational,values,units


def amounts(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def main():
    source=json.load(sys.stdin)
    start=time.perf_counter()
    zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    enabled=torch.ones(1,dtype=torch.bool,device='cuda')
    before=source['before']
    projection=before['projection']
    platform=projection['platform_confirmed_projection']
    prior_trade=before['trades'][0]
    fact=prior_trade['fact']
    matched=torch.tensor([fact['matched_at_utc_ms']],device='cuda')
    base=integers([units(fact['base_quantity_shares'])])
    quote=integers([units(fact['quote_amount_pusd'])])
    fee=integers([units(fact['immediate_taker_fee_pusd'])])
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    def prepare():
        account=PlatformAccountState.open(integers([units(platform['cash_pusd'])]),torch.tensor([0,0],device='cuda'))
        account=replace(account,available=integers([units(platform['available_pusd'])]),
            principal=integers([units(platform['principal_encumbrance_pusd'])]),fee=integers([units(platform['immediate_taker_fee_encumbrance_pusd'])]),
            rebates=rational([platform[name] for name in names[4:]]),outcomes=integers([units(fact['base_quantity_shares']),0]),
            next_sequence=integers([projection['platform_source_prefix']+1]))
        record=FillRecord.create(base,quote,fee,zero,matched,enabled,source['finality_path'])
        record=replace(record,ordinal=torch.ones_like(zero),economic_applied=enabled,
            economic_application_sequence=integers([fact['economic_application_account_transition_seq']]))
        pool=FillPool.empty(record,capacity=1)
        days=TakerDays.empty(1,'cuda',fills=1,days=1)
        pool,days,_,_=pool.enqueue_with_days(record,days)
        days,_=days.advance_clock(torch.tensor([87_000_000],device='cuda'),enabled)
        return account,pool,days
    account,pool,days=Numbers.run_exact_batch(prepare)
    at=torch.tensor([source['confirmed_at']],device='cuda')
    rate=Numbers.run_exact_batch(lambda:rational([source['taker_rebate_rate']]))
    pool,account,days,frame=Numbers.run_exact_batch(lambda:pool.advance_with_days(account,days,at,rate))
    days,account,final=Numbers.run_exact_batch(lambda:days.finalize_one(account,enabled))
    days,account,payment=Numbers.run_exact_batch(lambda:days.pay_one(account,enabled))
    assert frame.event.terminal.cpu().tolist()==[True] and final.emitted.cpu().tolist()==[True] and payment.emitted.cpu().tolist()==[True]
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'execution-normalization/gpu'))
    from projection import Projection
    from validate_projection import expected_semantic
    from consumption import PendingRecords,ReceiveGates
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    profile=Path(__file__).resolve().parents[1]/'config/latency-profiles'/sha/'profile.json'
    sampler=EmpiricalDelaySampler.build(profile.read_bytes(),sha,[source['session_seed'].encode('ascii')])
    emitter=replace(FeedbackEmitter.open(1),next_private_sequence=integers([source['next_private']]),
        next_batch_sequence=integers([source['next_batch']]))
    noe=Projection.open(emitter,zero,zero,zero,zero,torch.tensor([0,0],device='cuda'),integers([1]),zero,zero,zero,zero)
    record=_take_record(pool.records,frame.slot)
    noe,batch=Numbers.run_exact_batch(lambda:noe.finality(sampler,record,frame,rational([fact['source_price_pusd_per_share']]),
        rate,at,finalized=(final,),payments=(payment,),kind=torch.ones_like(zero)))
    assert batch.fault.cpu().tolist()==[0]
    expected={(r['position']['source_watermark']['projection_batch_sequence'],r['position']['record_ordinal']):r for r in source['records']}
    by_row={}
    for column,active in enumerate(batch.emitted.cpu().tolist()[0]):
        if not active:
            continue
        key=values(batch.batch_sequences)[column],values(batch.record_ordinals)[column]
        original=expected[key]
        body=next(iter(original['record'].values()))
        reference=body.get('source_reference') or body['fact']['source_reference']
        metadata=next(iter(reference['record'].values()))
        assert values(batch.private_sequences)[column]==metadata['private_feedback_sequence']
        assert batch.received_at[0,column].cpu().item()==metadata['feedback_received_at_utc_ms']
        row=int(batch.rows[0,column].cpu())
        assert int(noe.normalizer.bank.source_order[row,3].cpu())==body['source_order']['ordinal']
        semantic=Numbers.run_exact_batch(lambda:expected_semantic(original))
        actual=noe.normalizer.bank.semantic[row:row+1]
        width=max(actual.size(1),semantic.size(1))
        assert torch.equal(torch.nn.functional.pad(actual,(0,width-actual.size(1))),torch.nn.functional.pad(semantic,(0,width-semantic.size(1))))
        by_row[row]=original
    assert len(by_row)==7
    pending=PendingRecords.from_columns(batch.emitted,batch.received_at,batch.private_sequences,
        Numbers.ExactInt.from_i64_tensor(noe.normalizer.bank.source_order[batch.rows.flatten(),3]),batch.rows,integers([0]))
    closed=torch.tensor([source['closed_at']],device='cuda')
    gates=ReceiveGates.unset(1).set_receive(closed,enabled)
    pending,blocked=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not blocked.emitted.any().cpu().item()
    gates=gates.set_public(closed,enabled)
    pending,delivered=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    order=[by_row[row] for row,active in zip(delivered.record_refs.cpu().tolist()[0],delivered.emitted.cpu().tolist()[0]) if active]
    assert [r['position'] for r in order]==[r['position'] for r in source['records']]
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ledger/gpu'))
    from memory import EconomicColumns
    from admission import AdmissionLifecycle
    from closed import ClosedAdmission
    from success_pool import SuccessPools
    from rebate_day import RebateDays
    spec=importlib.util.spec_from_file_location('gpu_platform_fok_day',Path(__file__).resolve().parents[2]/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    def prepare_ledger():
        balances=module.PlatformBalances.open(rational([platform[name] for name in names]),
            rational([fact['base_quantity_shares'],'0']),torch.tensor([0,0],device='cuda'),integers([projection['platform_source_prefix']]))
        columns=EconomicColumns.from_projection_batches([batch])
        pools=SuccessPools.open(torch.tensor([0,0],device='cuda'),torch.tensor([0,0],device='cuda'),
            torch.tensor([0,1],device='cuda'),torch.tensor([1,0],device='cuda'),1)
        rebatedays=RebateDays.empty(torch.tensor([[0]],device='cuda'),1)
        admission=AdmissionLifecycle.open(balances,noe.normalizer.bank,columns,
            integers([projection['ledger_entry_seq']]),integers([projection['ledger_local_prefix']]),pools,rebate_days=rebatedays)
        admission=replace(admission,maker_cash=rational([str(Fraction(before['cash']['maker_available_pusd'])+Fraction(before['cash']['maker_reserved_pusd']))]))
        trades,_=admission.trades.set(integers([fact['fill_sequence']]),zero,enabled,admission.current_entry_sequence,enabled)
        slot,_,_=trades.lookup(integers([fact['fill_sequence']]))
        success=admission.trade_success.update(slot,zero,zero,rational([fact['base_quantity_shares']]),
            rational([fact['quote_amount_pusd']]),torch.ones_like(zero),enabled,enabled,enabled)
        return replace(admission,trades=trades,trade_success=success)
    admission=Numbers.run_exact_batch(prepare_ledger)
    cache=ClosedAdmission.empty(admission.columns)
    cache=Numbers.run_exact_batch(lambda:cache.accept(admission.columns,delivered))
    admission,cache=Numbers.run_exact_batch(lambda:cache.drain(admission,torch.ones_like(cache.received)))
    assert cache.fault.cpu().tolist()==[0]
    assert not (cache.received & ~cache.consumed).any().cpu().item()
    # 参考仅核对已发生的结果；它不提供设备应用次序或重试控制。
    actual=source['attempts'][-1]['after']['projection']
    assert values(admission.current_entry_sequence)==[actual['ledger_entry_seq']]
    assert values(admission.platform.platform_source_prefix)==[actual['platform_source_prefix']]
    assert amounts(admission.platform.balances)==[Fraction(actual['platform_confirmed_projection'][name]) for name in names]
    original_sequences={json.dumps(a['position'],sort_keys=True):a['after']['projection']['ledger_entry_seq']
        for a in source['attempts'] if not a['deferred']}
    applied_sequences=values(admission.applied_sequences)
    for column,active in enumerate(batch.emitted.cpu().tolist()[0]):
        if active:
            original=by_row[int(batch.rows[0,column].cpu())]
            assert applied_sequences[column]==original_sequences[json.dumps(original['position'],sort_keys=True)]
    first_rank=cache.receive_rank.clone()
    first_available=cache.available_at.clone()
    first_delivery=values(cache.delivered_sequences)
    repeated=Numbers.run_exact_batch(lambda:cache.accept(admission.columns,replace(delivered,available_at=delivered.available_at+1)))
    repeated_admission,repeated=Numbers.run_exact_batch(lambda:repeated.drain(admission,torch.ones_like(cache.received),passes=1))
    assert torch.equal(repeated.receive_rank,first_rank) and torch.equal(repeated.available_at,first_available)
    assert values(repeated.delivered_sequences)==first_delivery
    assert values(repeated_admission.current_entry_sequence)==values(admission.current_entry_sequence)
    assert admission.rebate_days.payment_observed.cpu().tolist()==[[True]]
    assert amounts(admission.rebate_days.condition_payable)==[Fraction('1.4')]
    assert amounts(admission.rebate_days.condition_locked)==[Fraction('0.0000016')]
    assert amounts(admission.success_pools.success_quantity)==[Fraction('1000.001'),0]
    print(json.dumps({'original_outer_FOK_source':True,'same_actual_V4_batch_and_payloads_checked':7,
        'original_received_order_and_dual_gates_checked':True,'device_closed_record_consumer_checked':True,
        'reference_attempt_order_drives_device':False,'repeat_keeps_first_visibility_and_delivery':True,
        'cold_prefix_initializations':1,'day_and_payment_money_seq_and_positions_checked':True,
        'elapsed_seconds_with_reference_reads':time.perf_counter()-start,'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'full_root_recovery_or_Maker_day_pipeline_delivered':False}))


if __name__=='__main__':
    main()
