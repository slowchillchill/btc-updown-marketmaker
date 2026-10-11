"""原空Book/GTC/Maker/Terminal材料进入真实连续宿主的经济尾段。

独立CPU检查点只核金额与业务结果；生产宿主选择事件、领取和结算请求。
"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import importlib.util,json,os,sys,time,shutil
import torch
from queues import Numbers
from validate_base_execution import integers,rational,values,units
from continuous_actor import ContinuousActorRun
from continuous_execution import ContinuousExecution
from ledger_drain import PublicLedgerReceiver
from settlement import SettlementCoordinator,PublicSettlementRun
from scheduling import ScheduledTime
from send_clock import BoundedSendClock
from public_source import PublicGroupSource
from public_device_page import PreparedPublicPage,build_public_device_page
from memory import EconomicColumns
from admission import AdmissionLifecycle
from order_funds import OrderFunds
from closed import ClosedAdmission
from success_pool import SuccessPools
from position_cost import PositionCosts
from market_economics import UnresolvedMarkets
from rebate_day import RebateDays
from resolution import ResolutionSources,MarketResolutions
from registry import PublicRegistryEvents,PublicRegistryState,FrozenIntentOrders
from public_ledger import PublicLedgerState
from consumption import PendingRecords
from commitments import SubmittedLegs
from settlement_redemption import RedemptionSources
from latency import byte_column
from model_train.continuous_actor import ActorModelQueue


def verify(source,runtime,projection,accepted_batch,block,sampler,receipt,kind,asset,fee,rebate,started):
    root=Path(__file__).resolve().parents[2]
    artifact=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])
    for name in ('public','hidden'):shutil.copytree(source['source_binding'][name],artifact/('original-'+name))
    T=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    run=Numbers.run_exact_batch;yes=T([True],torch.bool);zero=T([0])
    before=source['ledger_before']['projection'];opening=before['platform_confirmed_projection']
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    spec=importlib.util.spec_from_file_location('continuous_tail_platform',root/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    def assemble():
        balances=module.PlatformBalances.open(rational([opening[key] for key in names]),rational(['0','0']),T([0,0]),integers([before['platform_source_prefix']]))
        pools=SuccessPools.open(T([0,0]),T([0,0]),T([0,1]),T([1,0]),1)
        markets=UnresolvedMarkets.open(pools,zero)
        resolutions=MarketResolutions.open(pools,markets,ResolutionSources.bind(block.terminal_columns,[a['token_id'] for a in block.assets]))
        columns=EconomicColumns.from_projection_batches([accepted_batch])
        ledger=AdmissionLifecycle.open(balances,projection.normalizer.bank,columns,integers([before['ledger_entry_seq']]),
            integers([before['ledger_local_prefix']]),pools,position_costs=PositionCosts.empty(1,1,'cuda'),
            order_funds=OrderFunds.empty(1,columns.row_refs.size(1),columns.row_refs.size(1),columns.row_refs.size(1),'cuda'),
            rebate_days=RebateDays.empty(T([[0]]),2),
            unresolved_markets=markets,resolutions=resolutions)
        ordinal=integers(projection.normalizer.bank.source_order[accepted_batch.rows.flatten(),3].cpu().tolist())
        pending=PendingRecords.from_columns(accepted_batch.emitted,accepted_batch.received_at,accepted_batch.private_sequences,ordinal,accepted_batch.rows,integers([0]))
        return PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),ClosedAdmission.empty(columns),ledger)
    public=run(assemble)
    intents=run(lambda:FrozenIntentOrders(yes[:,None],integers([1]),yes[:,None],rational([kind['size_shares']]),T([[False]],torch.bool),yes[:,None]))
    item=source['post_reference']['request']['request']['items'][0]
    submitted=run(lambda:SubmittedLegs(yes[:,None],receipt.registered[:,None],T([[0]]),T([[item['batch_item_ordinal']]]),
        rational([kind['size_shares']]),yes[:,None],T([[0]]),T([[asset]]),yes[:,None]))
    page=artifact/'tail-public-page'
    build_public_device_page(os.environ['B_GPU_COLUMNS_BINARY'],source['source_binding']['public'],page)
    prepared=PreparedPublicPage(page,source['source_binding']['public'])
    public_source=PublicGroupSource(prepared,1,page_groups=2)
    auto_mode=source.get('auto_mode',False)
    if not auto_mode:
        runtime=runtime.install_settlement_registration(T([[0]]),T([[1]]),T([[50001 if source['with_query'] else 20001]]))
        if source['with_query']:runtime=runtime.install_settlement_queries(T([[30001]]),T([[40001]]),T([[2]]))
    redemption=RedemptionSources.bind(block.terminal_columns,[a['token_id'] for a in block.assets],[0,0],[0,1],1)
    if auto_mode:runtime=runtime.install_auto_redemption(redemption)
    identities,offsets=byte_column([text.encode() for text in block.terminal_columns.canonical_ids])
    backend=ContinuousExecution(None,None,None,sampler,None,T([0,1]),T([source['market_end']]*2),rebate,
        rational(['0.2']),T([0,0]),T([0,1]),lambda *args:T([[0]*runtime.hidden.queues.capacity]),None,
        condition_by_market=T([[0]]),market_end_by_market=T([[source['market_end']]]),
        redemption_sources=redemption,settlement_policy=T([[0]]),resolution_identity_bytes=identities,
        resolution_identity_offsets=offsets)
    # 原正式费率上下文，hidden Maker消费者沿原TakeRatio读取，不让测试复制算法。
    from types import SimpleNamespace
    backend.rule=SimpleNamespace(values=rational(['0','0','0.07']),context={'fee_exponent':1})
    trace=[]
    def progress(stage,**data):
        trace.append(dict(stage=stage,elapsed_seconds=time.perf_counter()-started,**data))
        with (artifact/'continuous-tail-progress.jsonl').open('a') as out:out.write(json.dumps(trace[-1])+'\n')
        if stage in ('sim_phase','continuous_event'):
            print(json.dumps(trace[-1]),file=sys.stderr,flush=True)
    clock=BoundedSendClock(runtime,projection,public,public_source,backend.handlers(),ScheduledTime.absent(zero),lambda p:p.normalizer.bank)
    received=[]
    receiver=PublicLedgerReceiver(word_capacity=16)
    def receive(state,delivery,enabled):
        at=int(clock.gates.public_visible_at.cpu()[0])
        ready=delivery.closed.emitted.cpu().tolist()[0]
        for slot,on in enumerate(ready):
            if on:received.append(dict(row=int(delivery.closed.record_refs[0,slot].cpu()),available_at=at))
        state,complete=receiver(state,delivery,enabled)
        assert complete.cpu().tolist()==[True],('原waiting未完成',receiver.captured.required if receiver.captured else None)
        return state,complete
    def freeze_account(state,at):return state.freeze_account(intents,at,submitted_legs=submitted)
    settle=None if auto_mode else PublicSettlementRun(SettlementCoordinator.open(public.ledger.resolutions),freeze_account,backend.register_settlement)
    def forbidden(*args):raise AssertionError('交易截止后不得新增模型观察/调用Actor')
    cutoff=T([source['post_reference']['processing_at']])
    models=ActorModelQueue(torch.zeros((2,1,64),device='cuda'),T([[0,0,0]]),cutoff,forbidden,forbidden,forbidden)
    shared=ContinuousActorRun(clock,cutoff,models,forbidden,receive,trading_until=cutoff,settlement=settle)
    result=clock.advance_before(cutoff,yes,progress=progress)
    assert result.complete.cpu().tolist()==[True]
    # 真实Terminal、晚确认、请求式成功/查询均由同一生产宿主推进。
    if source['late_maker']:
        result=shared.advance_before(T([89_100_001]),yes,progress=progress)
        assert result.complete.cpu().tolist()==[True] and result.fault.cpu().tolist()==[0]
        assert torch.equal(clock.runtime.calendars.maker.mature,clock.runtime.calendars.maker.occupied)
        assert not clock.runtime.calendars.maker.finalized.any().cpu().item()
        if not auto_mode:assert values(clock.runtime.settlement_registrations.sends)==[0]
    if source.get('terminal_conflict',False):
        # 原TypedRaw第二Terminal经DPT输出MARKET Fresh→Invalid，不伪造控制行。
        boundaries=[]
        for bucket in source['buckets']:
            for reference in bucket['references']:
                fact=reference['payload'].get('Inline',{}).get('MarketFact',{})
                boundary=fact.get('payload',{}).get('QualityBoundary')
                if boundary and boundary['scope']['kind']=='MARKET':
                    boundaries.append((bucket['bucket']['bucket_time_utc_ms'],reference,boundary))
        assert len(boundaries)==1
        boundary_at,reference,boundary=boundaries[0]
        assert boundary_at==31500 and boundary['current_quality']=='invalid'
        assert boundary['reason_code']=='POLYMARKET_TERMINAL_DUPLICATE_CONFLICT'
        result=shared.advance_before(T([source['confirmed_at']]),yes,progress=progress)
        assert result.complete.cpu().tolist()==[True] and result.fault.cpu().tolist()==[0]
        before_runtime=clock.runtime;before_projection=clock.projection;before_public=clock.public
        cash=values(before_runtime.account.cash);outcomes=values(before_runtime.account.outcomes)
        expected_outcomes=[0]*len(block.assets);expected_outcomes[asset]=units('300')
        assert outcomes==expected_outcomes and before_runtime.resolution_obligations.pending.any().cpu().item()
        assert before_runtime.market_controls.quality.cpu().tolist()==[[5]]
        if not os.environ.get('B_GPU_CONFLICT_OLD_CANDIDATE'):
            assert before_runtime.resolution_obligations.invalidated_by_quality.cpu().tolist()==[[True]]
            assert before_runtime.resolution_obligations.fault.cpu().tolist()==[0]
            assert before_runtime.account.fault.cpu().tolist()==[0] and before_runtime.calendars.maker.fault.cpu().tolist()==[0]
            assert redemption.gather(before_runtime.resolution_obligations.reference_indices[:,0])[-1].cpu().tolist()==[0]
        result=shared.advance_before(T([source['confirmed_at']+1]),yes,progress=progress)
        report=dict(conflict_source=reference,at=source['confirmed_at'],fault=result.fault.cpu().tolist(),
            before_cash=cash,after_cash=values(clock.runtime.account.cash),before_outcomes=outcomes,
            after_outcomes=values(clock.runtime.account.outcomes),auto_completed=clock.runtime.resolution_obligations.auto_redeemed.cpu().tolist())
        (artifact/'terminal-conflict-result.json').write_text(json.dumps(report,indent=2)+'\n')
        if os.environ.get('B_GPU_CONFLICT_OLD_CANDIDATE'):
            assert result.complete.cpu().tolist()==[True] and result.fault.cpu().tolist()==[0]
            assert values(clock.runtime.account.cash)==[cash[0]+units('300')]
            assert values(clock.runtime.account.outcomes)==[0,0]
            assert clock.runtime.resolution_obligations.auto_redeemed.cpu().tolist()==[[True]]
            print(json.dumps(dict(old_candidate_conflict_wrong_redemption=True,**report)))
        else:
            assert result.complete.cpu().tolist()==[False] and result.fault.cpu().tolist()==[1]
            assert result.stage_result.stage_result.fault.cpu().tolist()==[0], '原finality/Maker候选本身合法，失败来自失效义务'
            assert clock.runtime is before_runtime and clock.projection is before_projection and clock.public is before_public
            assert values(clock.runtime.account.cash)==cash and values(clock.runtime.account.outcomes)==outcomes
            assert clock.runtime.resolution_obligations.auto_redeemed.cpu().tolist()==[[False]]
            assert clock.runtime.resolution_obligations.pending.any().cpu().item()
            print(json.dumps(dict(conflict_rejected_common_transaction_preserved=True,**report)))
        return
    result=shared.advance_before(T([source['confirmed_at']+200000]),yes,progress=progress)
    assert result.complete.cpu().tolist()==[True] and result.fault.cpu().tolist()==[0]
    assert not models.tasks and torch.equal(shared.next_observation,cutoff)
    assert clock.runtime.resolution_obligations.pending.sum().cpu().item()==0
    assert values(clock.runtime.account.outcomes)==[0,0]
    if not auto_mode:assert values(clock.runtime.settlement_registrations.sends)==[1]
    else:
        assert clock.runtime.settlement_registrations is None
        assert clock.runtime.resolution_obligations.auto_redeemed.cpu().tolist()==[[True]]
    if source['with_query']:
        assert values(clock.runtime.settlement_registrations.query_state.queries)==[1]
        assert values(clock.runtime.settlement_registrations.query_state.consumed_queries)==[1]
    expected=(source['finality_reference'][-1] if auto_mode else source['settlement_success_reference'])['ledger']['projection']
    balances=clock.public.ledger.platform.balances
    amounts=lambda v:[Fraction(n,d) for n,d in zip(values(v.numerator),values(v.denominator))]
    assert amounts(balances)==[Fraction(expected['platform_confirmed_projection'][key]) for key in names]
    assert values(clock.public.ledger.current_entry_sequence)==[expected['ledger_entry_seq']]
    assert values(clock.public.ledger.platform.platform_source_prefix)==[expected['platform_source_prefix']]
    if not auto_mode:assert clock.public.ledger.resolutions.requests.outcomes.cpu().tolist()==[2]
    else:
        assert not clock.public.ledger.resolutions.requests.present.any().cpu().item()
        assert clock.public.ledger.resolutions.auto_redeemed.cpu().tolist()==[True]
    if not source['late_maker']:
        old_cash=values(clock.runtime.account.cash);old_entry=values(clock.public.ledger.current_entry_sequence)
        held=clock.public
        # 日成熟的原下一真实时刻，前一毫秒不得支付；不是给空档造100ms观察。
        day_at,_,present=clock.runtime.calendars.next_clock()
        assert present.cpu().tolist()==[True] and day_at.cpu().tolist()==[89_100_000]
        result=shared.advance_before(day_at,yes,progress=progress)
        assert result.complete.cpu().tolist()==[True] and values(clock.runtime.account.cash)==old_cash
        assert values(clock.public.ledger.current_entry_sequence)==old_entry
        result=shared.advance_before(day_at+10000,yes,progress=progress)
        assert result.complete.cpu().tolist()==[True] and result.fault.cpu().tolist()==[0]
        assert not models.tasks and torch.equal(shared.next_observation,cutoff)
        contribution=Fraction(expected['platform_confirmed_projection']['maker_rebate_receivable_pusd'])
        assert contribution==Fraction('1.008')
        paid=source['rebate_reference']['projection']
        assert amounts(clock.public.ledger.platform.balances)==[Fraction(paid['platform_confirmed_projection'][key]) for key in names]
        assert values(clock.public.ledger.current_entry_sequence)==[paid['ledger_entry_seq']]
        assert values(clock.public.ledger.platform.platform_source_prefix)==[paid['platform_source_prefix']]
        assert values(clock.runtime.account.cash)==[old_cash[0]+units(contribution)]
        assert amounts(clock.public.ledger.platform.balances)[0]==amounts(held.ledger.platform.balances)[0]+contribution
        assert amounts(clock.public.ledger.platform.balances)[4]==0
        assert amounts(clock.public.ledger.maker_cash)==[amounts(held.ledger.maker_cash)[0]+contribution]
    else:
        paid=source['rebate_reference']['projection']
        assert amounts(clock.public.ledger.platform.balances)==[Fraction(paid['platform_confirmed_projection'][key]) for key in names]
        assert amounts(clock.public.ledger.platform.balances)[4]==0
        assert values(clock.public.ledger.current_entry_sequence)==[paid['ledger_entry_seq']]
        assert values(clock.public.ledger.platform.platform_source_prefix)==[paid['platform_source_prefix']]
        assert torch.equal(clock.runtime.calendars.maker.finalized,clock.runtime.calendars.maker.occupied)
        assert torch.equal(clock.runtime.calendars.maker.paid,clock.runtime.calendars.maker.occupied)
    bank=clock.projection.normalizer.bank
    # 首次可见时刻保真实反馈received_at；不统一推迟至200000或尾段终点。
    all_received={row['row']:row['available_at'] for row in received}
    for batch in clock.public.ledger.columns.projection_batches:
        for column,on in enumerate(batch.emitted.cpu().tolist()[0]):
            if on:
                row=int(batch.rows[0,column].cpu())
                assert all_received[row]==int(batch.received_at[0,column].cpu())
    assert not clock.public.pending_noe.active.any().cpu().item()
    assert not (clock.public.waiting.received&~clock.public.waiting.consumed).any().cpu().item()
    (artifact/'continuous-tail-trace.json').write_text(json.dumps(trace,indent=2)+'\n')
    print(json.dumps(dict(production_continuous_tail=True,auto_mode=auto_mode,with_query=source['with_query'],
        terminal_pending_and_real_settlement_checked=True,next_day_maker_boundary_checked=True,
        actual_receive_times=received,observations=0,elapsed_seconds=time.perf_counter()-started)))


def verify_rebate_days(source,runtime,projection,sampler,taker_rate,maker_rate,account,started):
    """原GTC的Taker/Maker日结果进入同一宿主；日目录从冷开户保留一槽。"""
    from admission import AdmissionLifecycle
    from position_cost import PositionCosts
    def amounts(ratio):return [Fraction(n,d) for n,d in zip(values(ratio.numerator),values(ratio.denominator))]
    from types import SimpleNamespace
    artifact=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])
    public,cold=account
    run=Numbers.run_exact_batch
    T=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    yes=T([True],torch.bool);zero=T([0])
    assert public.ledger.rebate_days.occupied.shape==(1,1)
    assert not public.ledger.rebate_days.occupied.any().cpu().item()
    # 原真实后继Economic的组件代表：前驱未消费与物理不足分别返回。
    columns=public.ledger.columns;width=columns.row_refs.size(1)
    initial=run(lambda:AdmissionLifecycle.open(cold.platform,projection.normalizer.bank,columns,
        cold.current_entry_sequence,cold.current_local_prefix,cold.success_pools,
        position_costs=PositionCosts.empty(1,width,'cuda'),order_funds=OrderFunds.empty(1,width,width,width,'cuda'),
        rebate_days=cold.rebate_days,unresolved_markets=cold.unresolved_markets,
        market_balance_origin=cold.market_balance_origin,accepted_principal=cold.accepted_principal))
    sequences=T(values(columns.sequences)).reshape_as(columns.enabled)
    schemas=initial.lifecycle.schemas
    later=public.waiting.received&(sequences==values(cold.platform.platform_source_prefix)[0]+2)&((schemas==11)|(schemas==18)|(schemas==21)|(schemas==24))
    assert later.any().cpu().item()
    probe=replace(public,ledger=initial,waiting=replace(public.waiting,received=later,consumed=torch.zeros_like(later)))
    receiver=PublicLedgerReceiver(word_capacity=16)
    probe,complete=receiver(probe,SimpleNamespace(fault=torch.zeros_like(zero,dtype=torch.int32)),yes)
    assert complete.cpu().tolist()==[True] and receiver.captured.resources_fit.cpu().item()
    assert (probe.waiting.received&~probe.waiting.consumed).any().cpu().item()
    assert amounts(probe.ledger.platform.balances)==amounts(cold.platform.balances)
    page=artifact/'rebate-public-page'
    build_public_device_page(os.environ['B_GPU_COLUMNS_BINARY'],source['tail_public'],page)
    prepared=PreparedPublicPage(page,source['tail_public'])
    public_source=PublicGroupSource(prepared,1,page_groups=2)
    backend=ContinuousExecution(None,None,None,sampler,None,T([0,1]),T([source['market_end']]*2),
        taker_rate,maker_rate,T([0,0]),T([0,1]),None,None)
    clock=BoundedSendClock(runtime,projection,public,public_source,backend.handlers(),ScheduledTime.absent(zero),lambda p:p.normalizer.bank)
    trace=[]
    def progress(stage,**data):
        row=dict(stage=stage,elapsed_seconds=time.perf_counter()-started,**data);trace.append(row)
        with (artifact/'rebate-tail-progress.jsonl').open('a') as out:out.write(json.dumps(row)+'\n')
        if stage in ('continuous_event','sim_phase'):print(json.dumps(row),file=sys.stderr,flush=True)
    def forbidden(*args):raise AssertionError('日结尾段不得构造模型观察/动作')
    models=ActorModelQueue(torch.zeros((2,1,64),device='cuda'),T([[0,0,0]]),zero,forbidden,forbidden,forbidden)
    shared=ContinuousActorRun(clock,zero,models,forbidden,receiver,trading_until=zero)
    horizon=T([source['rebate_tail_reference'][-1]['at']+10000])
    first=shared.advance_before(horizon,yes,progress=progress)
    captured=receiver.captured
    assert first.blocked.cpu().tolist()==[True] and first.fault.cpu().tolist()==[0]
    assert captured is not None and not captured.resources_fit.cpu().item()
    # 资源不足不提交候选；需求来自captured，活动目录仍为原一槽。
    assert clock.public.ledger.rebate_days.capacity_required.cpu().tolist()==[1]
    assert captured.required['days'].cpu().tolist()==[2] and shared.economic_resume.cpu().tolist()==[True]
    assert clock.public.ledger.rebate_days.kinds.cpu().tolist()==[[1]]
    assert not clock.next_event(yes).present.any().cpu().item(), '第二Day领取后应无外部待选事件'
    at=clock.gates.public_visible_at.clone();rank=clock.public.waiting.receive_rank.clone()
    available=clock.public.waiting.available_at.clone();consumed=clock.public.waiting.consumed.clone()
    entry=values(clock.public.ledger.current_entry_sequence)
    clock.public=replace(clock.public,ledger=clock.public.ledger.resized_rebate_days(int(captured.required['days'].max().cpu())))
    assert torch.equal(clock.public.waiting.receive_rank,rank) and torch.equal(clock.public.waiting.available_at,available)
    assert torch.equal(clock.public.waiting.consumed,consumed) and values(clock.public.ledger.current_entry_sequence)==entry
    resumed=[]
    def receive(state,delivery,enabled):
        if not resumed:
            assert torch.equal(clock.gates.public_visible_at,at)
            assert torch.equal(state.waiting.receive_rank,rank) and torch.equal(state.waiting.available_at,available)
            assert torch.equal(state.waiting.consumed,consumed)
            resumed.append(True)
        return receiver(state,delivery,enabled)
    shared.receive=receive
    second=shared.advance_before(horizon,yes,progress=progress)
    assert second.complete.cpu().tolist()==[True] and second.fault.cpu().tolist()==[0]
    assert resumed and not shared.economic_resume.any().cpu().item()
    assert receiver.captured.resources_fit.cpu().item()
    assert clock.public.ledger.rebate_days.kinds.cpu().tolist()==[[1,0]]
    assert not (clock.public.waiting.received&~clock.public.waiting.consumed).any().cpu().item()
    expected=source['rebate_tail_reference'][-1]['ledger']['projection']
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    assert amounts(clock.public.ledger.platform.balances)==[Fraction(expected['platform_confirmed_projection'][name]) for name in names]
    assert values(clock.public.ledger.current_entry_sequence)==[expected['ledger_entry_seq']]
    assert values(clock.public.ledger.current_local_prefix)==[expected['ledger_local_prefix']]
    assert values(clock.public.ledger.platform.platform_source_prefix)==[expected['platform_source_prefix']]
    seen=[]
    from validate_projection import expected_semantic
    for reference in source['rebate_tail_reference']:
        for original in reference['records']:
            body=next(iter(original['record'].values()));source_ref=body.get('source_reference') or body['fact']['source_reference']
            category,metadata=next(iter(source_ref['record'].items()))
            assert category=='SettlementLifecycle'
            private=metadata['private_feedback_sequence'];ordinal=original['position']['record_ordinal']
            batch=original['position']['source_watermark']['projection_batch_sequence']
            # 银行完整源位置由原batch/private/ordinal查找，随后独立CPU正文核对。
            for projected in clock.public.ledger.columns.projection_batches:
                for i,on in enumerate(projected.emitted.cpu().tolist()[0]):
                    if on and values(projected.batch_sequences)[i]==batch and values(projected.record_ordinals)[i]==ordinal:
                        assert values(projected.private_sequences)[i]==private
                        row=int(projected.rows[0,i].cpu());seen.append((batch,ordinal))
                        semantic=run(lambda:expected_semantic(original))
                        actual=clock.projection.normalizer.bank.semantic[row:row+1]
                        width=max(actual.size(1),semantic.size(1))
                        assert torch.equal(torch.nn.functional.pad(actual,(0,width-actual.size(1))),torch.nn.functional.pad(semantic,(0,width-semantic.size(1))))
                        assert int(projected.received_at[0,i].cpu())==metadata['feedback_received_at_utc_ms']
    assert len(seen)==sum(len(r['records']) for r in source['rebate_tail_reference']) and len(set(seen))==len(seen)
    saved_entry=values(clock.public.ledger.current_entry_sequence);saved=amounts(clock.public.ledger.platform.balances)
    repeat=shared.advance_before(horizon+1,yes,progress=progress)
    assert repeat.complete.cpu().tolist()==[True] and repeat.fault.cpu().tolist()==[0]
    assert values(clock.public.ledger.current_entry_sequence)==saved_entry and amounts(clock.public.ledger.platform.balances)==saved
    assert not models.tasks and shared.next_observation.cpu().tolist()==[0]
    print(json.dumps(dict(real_taker_then_maker_day_capacity_retry=True,required_days=2,
        actual_resume_at=at.cpu().tolist(),original_cpu_day_NOE_checked=seen,
        no_external_event_at_resource_exit=True,legitimate_predecessor_wait_component=True,
        no_new_observations=True,elapsed_seconds=time.perf_counter()-started)),flush=True)
