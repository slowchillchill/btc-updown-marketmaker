"""一次真实GTC接单前缀冷初始化，此后设备hidden Maker→终态→NOE→Ledger日链连续推进。

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
from maker_days import MakerDays
from rebate_coordinator import RebateCoordinator
from queues import PriceQueueBatch
from order_quantity import BaseOrderQuantity
from base_execution import reduce_order_maker
from latency import EmpiricalDelaySampler
from feedback import FeedbackEmitter
from validate_base_execution import integers,rational,values,units


def amounts(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def main():
    source=json.load(sys.stdin)
    start=time.perf_counter()
    phase_start=start
    phases={}
    exact_calls=0
    exact_attempts=0
    phase_calls=0
    phase_attempts=0

    def run(compute):
        # 只计纯候选调用/物理重试，不读取业务条件或改变生产资源协调。
        nonlocal exact_calls,exact_attempts
        exact_calls+=1
        def attempt():
            nonlocal exact_attempts
            exact_attempts+=1
            return compute()
        return Numbers.run_exact_batch(attempt)

    def finish_phase(name):
        nonlocal phase_start,phase_calls,phase_attempts
        # 定向验证的阶段边界同步；不向生产热循环添加此同步。
        torch.cuda.synchronize()
        now=time.perf_counter()
        phases[name]={
            'seconds':now-phase_start,
            'exact_batch_calls':exact_calls-phase_calls,
            'pure_compute_attempts':exact_attempts-phase_attempts,
            'resource_retries':exact_attempts-phase_attempts-(exact_calls-phase_calls),
            'device_allocated_bytes':torch.cuda.memory_allocated(),
            'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        }
        phase_start=now
        phase_calls=exact_calls
        phase_attempts=exact_attempts
        print(json.dumps({'stage':name,**phases[name]}),file=sys.stderr,flush=True)
    zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    enabled=torch.ones(1,dtype=torch.bool,device='cuda')
    before=source['before']
    projection=before['projection']
    platform=projection['platform_confirmed_projection']
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    matched=torch.tensor([source['maker_at']],device='cuda')
    rate=run(lambda:rational([source['maker_rate']]))
    fee_rate=run(lambda:rational([source['fee_rate']]))
    price=run(lambda:rational(['0.4']))
    def prepare():
        account=PlatformAccountState.open(integers([units(platform['cash_pusd'])]),torch.tensor([0,0],device='cuda'))
        account=replace(account,available=integers([units(platform['available_pusd'])]),
            principal=integers([units(platform['principal_encumbrance_pusd'])]),fee=integers([units(platform['immediate_taker_fee_encumbrance_pusd'])]),
            rebates=rational([platform[name] for name in names[4:]]),next_sequence=integers([projection['platform_source_prefix']+1]))
        # 接单前缀仅初始化一次；后续真实hidden减量通过原队列计量产生Maker fill。
        queue=PriceQueueBatch.empty(price,capacity=2)
        queue,_=queue.append_account(torch.tensor([0],device='cuda'),integers([305_000_000]))
        quantity=BaseOrderQuantity.from_taker_plan(integers([305_000_000]),integers([122_000_000]),integers([0]),integers([0]))
        queue,_=queue.apply_level_transition(integers([0]),integers([300_000_000]))
        queue,effects=queue.apply_level_transition(integers([300_000_000]),integers([0]))
        quantity,fill=reduce_order_maker(quantity,effects,zero)
        record=FillRecord.create(fill.base,fill.quote,integers([0]),zero,matched,fill.emitted,source['finality_path'])
        pool=FillPool.empty(record,capacity=1)
        days=MakerDays.empty(zero[:,None],torch.tensor([[source['market_end']]],device='cuda'),enabled[:,None],fills=1,days=1)
        pool,days,_,_=pool.enqueue_with_maker(record,days,zero,price,fee_rate,source['fee_exponent'])
        return account,pool,days
    account,pool,days=run(prepare)
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
    finish_phase('cold_SIM_and_NOE_assembly')
    batches=[]
    for step in source['finality_path']['steps']:
        at=matched+step['offset_ms']
        pool,account,days,frame,market=run(lambda:pool.advance_with_maker(account,days,at,rate))
        record=_take_record(pool.records,frame.slot)
        noe,batch=run(lambda:noe.finality(sampler,record,frame,price,rate,at,
            maker_results=(market,),liquidity_role=torch.ones_like(zero),maker_fee_equivalent=days.fee_equivalent))
        batches.append(batch)
    calendar=RebateCoordinator(days,TakerDays.empty(1,'cuda',fills=1,days=1))
    for at_value in [source['market_end'],source['day_at']]:
        at=torch.tensor([at_value],device='cuda')
        calendar,account,frame=run(lambda:calendar.advance_clock(account,at,enabled))
        assert frame.consumed.cpu().tolist()==[True]
        noe,batch=run(lambda:noe.rebate_clock(sampler,frame,at))
        batches.append(batch)
    assert account.fault.cpu().tolist()==[0]
    finish_phase('device_SIM_and_NOE_producer')
    expected={(r['position']['source_watermark']['projection_batch_sequence'],r['position']['record_ordinal']):r for r in source['records']}
    by_row={}
    for batch in batches:
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
            semantic=run(lambda:expected_semantic(original))
            actual=noe.normalizer.bank.semantic[row:row+1]
            width=max(actual.size(1),semantic.size(1))
            assert torch.equal(torch.nn.functional.pad(actual,(0,width-actual.size(1))),torch.nn.functional.pad(semantic,(0,width-semantic.size(1))))
            by_row[row]=original
    assert len(by_row)==len(expected)==9
    finish_phase('original_NOE_payload_comparison')
    from transport import compact_committed_batches
    from validate_transport import check_transport_layout
    layout_checked=run(lambda:check_transport_layout(batches))
    batches,transport_population=run(lambda:compact_committed_batches(batches))
    active=torch.cat([batch.emitted for batch in batches],dim=1)
    received=torch.cat([batch.received_at for batch in batches],dim=1)
    rows=torch.cat([batch.rows for batch in batches],dim=1)
    from validate_projection import concat_integer
    private=concat_integer([batch.private_sequences for batch in batches])
    pending=PendingRecords.from_columns(active,received,private,
        Numbers.ExactInt.from_i64_tensor(noe.normalizer.bank.source_order[rows.flatten(),3]),rows,integers([0]))
    closed=torch.tensor([source['closed_at']],device='cuda')
    gates=ReceiveGates.unset(1).set_receive(closed,enabled)
    pending,blocked=run(lambda:pending.take_closed(gates))
    assert not blocked.emitted.any().cpu().item()
    gates=gates.set_public(closed,enabled)
    pending,delivered=run(lambda:pending.take_closed(gates))
    order=[by_row[row] for row,active in zip(delivered.record_refs.cpu().tolist()[0],delivered.emitted.cpu().tolist()[0]) if active]
    assert [r['position'] for r in order]==[r['position'] for r in source['records']]
    # Registry在首次公开领取后、Ledger消费前接纳实际设备NOE。
    sys.path[:0]=[str(Path(__file__).resolve().parents[2]/'shared-run/gpu'),
        str(Path(__file__).resolve().parents[2]/'ledger/gpu')]
    from registry_projection import order_events_from_closed
    from registry import PublicRegistryState,FrozenIntentOrders,TRADE,compact_order_events
    receive_rank=torch.arange(delivered.record_refs.size(1),device='cuda')[None,:]
    registry_events=run(lambda:order_events_from_closed(noe.normalizer.bank,batches,delivered,receive_rank))
    registry_state=run(lambda:PublicRegistryState(compact_order_events(registry_events)))
    trade_population=registry_state.events.received & (registry_state.events.kinds==TRADE)
    assert int(trade_population.sum().cpu())==1
    trade_values=amounts(registry_state.events.trade_quantity)
    for slot,active_trade in enumerate(trade_population.cpu().tolist()[0]):
        if active_trade:assert trade_values[slot]==Fraction('300')
    # 本MINED冷前缀没有提交接受引用/原意图绑定：读面保持Unknown，不猜回历史。
    public_orders=FrozenIntentOrders(enabled[:,None],noe.order_sequence,torch.zeros_like(enabled[:,None]),
        run(lambda:rational(['0'])),torch.zeros_like(enabled[:,None]),torch.zeros_like(enabled[:,None]))
    registry_view=run(lambda:registry_state.freeze_orders(public_orders,closed))
    assert registry_view.fault.cpu().tolist()==[0]
    assert registry_view.matched_known.cpu().tolist()==[[False]]
    assert registry_view.commitment_known.cpu().tolist()==[[False]]
    saved_registry=amounts(registry_view.matched)
    # 一个实际Trade载荷被改坏，随后被更高private正常来源替换：故障不得丢失。
    corrupted=replace(registry_events,roles=torch.where(registry_events.kinds==TRADE,
        torch.full_like(registry_events.roles,9),registry_events.roles))
    corrupted_state=run(lambda:PublicRegistryState(compact_order_events(corrupted)))
    newer=replace(registry_events,private_sequences=registry_events.private_sequences+registry_events.private_sequences.constant(100),
        receive_rank=registry_events.receive_rank+registry_events.received.size(1))
    corrupted_state=run(lambda:corrupted_state.receive(newer))
    assert corrupted_state.events.fault.cpu().tolist()==[1]
    registry_state=run(lambda:registry_state.receive(newer))
    assert amounts(registry_view.matched)==saved_registry
    finish_phase('device_receive_and_original_order_comparison')
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ledger/gpu'))
    from memory import EconomicColumns
    from admission import AdmissionLifecycle
    from position_cost import PositionCosts
    from closed import ClosedAdmission
    from success_pool import SuccessPools
    from rebate_day import RebateDays
    spec=importlib.util.spec_from_file_location('gpu_platform_fok_day',Path(__file__).resolve().parents[2]/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    def prepare_ledger():
        balances=module.PlatformBalances.open(rational([platform[name] for name in names]),
            rational(['0','0']),torch.tensor([0,0],device='cuda'),integers([projection['platform_source_prefix']]))
        columns=EconomicColumns.from_projection_batches(batches)
        pools=SuccessPools.open(torch.tensor([0,0],device='cuda'),torch.tensor([0,0],device='cuda'),
            torch.tensor([0,1],device='cuda'),torch.tensor([1,0],device='cuda'),1)
        rebatedays=RebateDays.empty(torch.tensor([[0]],device='cuda'),1)
        admission=AdmissionLifecycle.open(balances,noe.normalizer.bank,columns,
            integers([projection['ledger_entry_seq']]),integers([projection['ledger_local_prefix']]),pools,rebate_days=rebatedays,
            position_costs=PositionCosts.empty(1,columns.row_refs.size(1),'cuda'))
        admission=replace(admission,maker_cash=rational([str(Fraction(before['cash']['maker_available_pusd'])+Fraction(before['cash']['maker_reserved_pusd']))]))
        return admission
    admission=run(prepare_ledger)
    cache=ClosedAdmission.empty(admission.columns)
    cache=run(lambda:cache.accept(admission.columns,delivered))
    finish_phase('cold_Ledger_assembly_and_accept')
    drain_attempt_start=exact_attempts
    admission,cache=run(lambda:cache.drain(admission,torch.ones_like(cache.received)))
    drain_attempts=exact_attempts-drain_attempt_start
    finish_phase('first_device_Ledger_drain')
    assert cache.fault.cpu().tolist()==[0]
    assert not (cache.received & ~cache.consumed).any().cpu().item()
    # 参考仅核对已发生的结果；它不提供设备应用次序或重试控制。
    actual=source['attempts'][-1]['after']['projection']
    assert values(admission.current_entry_sequence)==[actual['ledger_entry_seq']]
    assert values(admission.platform.platform_source_prefix)==[actual['platform_source_prefix']]
    assert amounts(admission.platform.balances)==[Fraction(actual['platform_confirmed_projection'][name]) for name in names]
    cost_read=run(admission.position_costs.read)
    assert cost_read.known.cpu().tolist()==[True] and cost_read.fault.cpu().tolist()==[0]
    assert amounts(cost_read.cost)==[Fraction(actual['platform_confirmed_projection']['position_cost_basis_pusd'])]
    assert amounts(cost_read.fee)==[Fraction(actual['platform_confirmed_projection']['fees_pusd'])]
    cost_assets=run(lambda:admission.position_costs.asset_cost(admission.platform.asset_roots))
    assert cost_assets.known.cpu().tolist()==[True,True] and cost_assets.fault.cpu().tolist()==[0,0]
    expected_costs={row['token_id']:row['position_cost_basis_pusd'] for row in actual['platform_confirmed_projection']['outcome_balances']}
    assert amounts(cost_assets.cost)==[Fraction(expected_costs.get(token,'0')) for token in ['1','2']]
    original_sequences={json.dumps(a['position'],sort_keys=True):a['after']['projection']['ledger_entry_seq']
        for a in source['attempts'] if not a['deferred']}
    applied_sequences=values(admission.applied_sequences)
    offset=0
    for batch in batches:
        for column,active in enumerate(batch.emitted.cpu().tolist()[0]):
            if active:
                original=by_row[int(batch.rows[0,column].cpu())]
                assert applied_sequences[offset+column]==original_sequences[json.dumps(original['position'],sort_keys=True)]
        offset+=batch.emitted.size(1)
    finish_phase('original_Ledger_position_and_balance_comparison')
    first_rank=cache.receive_rank.clone()
    first_available=cache.available_at.clone()
    first_delivery=values(cache.delivered_sequences)
    repeated=run(lambda:cache.accept(admission.columns,replace(delivered,available_at=delivered.available_at+1)))
    repeated_admission,repeated=run(lambda:repeated.drain(admission,torch.ones_like(cache.received),passes=1))
    assert torch.equal(repeated.receive_rank,first_rank) and torch.equal(repeated.available_at,first_available)
    assert values(repeated.delivered_sequences)==first_delivery
    assert values(repeated_admission.current_entry_sequence)==values(admission.current_entry_sequence)
    finish_phase('repeat_accept_and_drain_comparison')
    executed=run(lambda:admission.trade_success.executed(zero[:,None],torch.ones_like(zero[:,None],dtype=torch.bool)))
    assert executed.known.cpu().tolist()==[[True,True]] and executed.fault.cpu().tolist()==[0]
    expected_executed=source['after']['markets'][0]['executed']
    for name,key in [('quantity','shares'),('principal','principal_pusd'),('actual_fee','actual_fee_pusd')]:
        value=getattr(executed,name)
        actual=[Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
        assert actual==[Fraction(row[key]) for row in expected_executed],(name,actual,expected_executed)
    estimate=run(lambda:admission.trade_success.estimated_rebate(admission.trades,
        admission.rebate_contributions,zero[:,None],torch.ones_like(zero[:,None],dtype=torch.bool)))
    assert estimate.known.cpu().tolist()==[[True,True]] and estimate.fault.cpu().tolist()==[0]
    original_estimate=[row['estimated_pusd']['Known'] for row in source['after']['markets'][0]['rebates']]
    assert [Fraction(n,d) for n,d in zip(values(estimate.amount.numerator),values(estimate.amount.denominator))]==[
        Fraction(int(row['numerator']),int(row['denominator'])) for row in original_estimate]
    assert admission.rebate_days.payment_observed.cpu().tolist()==[[True]]
    assert amounts(admission.rebate_days.condition_payable)==[Fraction('1.008')]
    assert amounts(admission.rebate_days.condition_locked)==[Fraction('0')]
    assert amounts(admission.success_pools.success_quantity)==[Fraction('300'),0]
    finish_phase('account_public_reads_and_original_comparison')
    print(json.dumps({'stages':phases,'transport_two_root_layout_and_empty_padding_checked':layout_checked,
        'original_transport_width':transport_population.original_width,'compacted_transport_width':transport_population.allocated_width,
        'physical_record_width':cache.received.size(1),'received_record_population':int(cache.received.sum().cpu()),
        'first_drain_apply_record_calls':drain_attempts*cache.received.size(1)**2,
        'first_drain_pure_compute_attempts':drain_attempts,
        'timings_are_validation_stage_synchronizations_not_training_throughput':True,
        'original_outer_non_post_only_GTC_hidden_Maker_source':True,'same_actual_V4_batch_and_payloads_checked':9,
        'original_received_order_and_dual_gates_checked':True,'device_closed_record_consumer_checked':True,'actual_device_NOE_Registry_Maker_trade_checked':True,
        'Registry_missing_cold_submission_remains_Unknown':True,'replaced_malformed_Trade_keeps_fault':True,
        'reference_attempt_order_drives_device':False,'repeat_keeps_first_visibility_and_delivery':True,
        'cold_prefix_initializations':1,'actual_trade_role_and_net_fee_executed_read_checked':True,'actual_applied_position_cost_and_fee_read_checked':True,'actual_estimated_rebate_read_checked':True,'day_and_payment_money_seq_and_positions_checked':True,
        'elapsed_seconds_with_reference_reads':time.perf_counter()-start,'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'complete_global_SIM_clock_or_activity_recovery_delivered':False}))


if __name__=='__main__':
    main()
