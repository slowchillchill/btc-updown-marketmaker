"""原紧凑TWAP/空Book→无订单Terminal/Resolved→真实后继Lifecycle对照。"""
from dataclasses import replace
from pathlib import Path
import json
import os
import sys
import time
import torch
from account_state import PlatformAccountState
from base_orders import BaseOrders
from base_runtime import BaseSimRuntime
from base_runtime_storage import BaseRuntimeStorage, CapturedHiddenBuckets, TerminalFeedback
from fill_finality import FillRecord
from fill_pool import FillPool
from hidden_frontier import HiddenBucketColumns, HiddenBucketFrontier
from hidden_inputs import HiddenPmBlock
from maker_days import MakerDays
from mutation_requests import MutationRequests
from order_quantity import BaseOrderQuantity
from queues import Numbers
from rebate_coordinator import RebateCoordinator
from taker_days import TakerDays
from validate_base_execution import integers, rational, values
from validate_base_orders import compare, progress


def main():
    source = json.load(sys.stdin)
    started = time.perf_counter()
    tensor = lambda x, dtype=torch.int64: torch.tensor(x, dtype=dtype, device='cuda')
    block = HiddenPmBlock(source['buckets'], source['assets'], [], source_binding=source['source_binding'])
    page_mode=os.environ.get('B_GPU_HIDDEN_DEVICE_PAGES')=='1'
    prepared=None; page_report={}
    if page_mode:
        import subprocess
        import numpy as np
        import hidden_inputs
        from hidden_device_page import build_hidden_device_columns,PreparedHiddenMaterial,_json_bytes
        destination=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])/'hidden-device-columns'
        before_build=time.perf_counter()
        binding=build_hidden_device_columns(block,destination,source['source_binding'],progress=progress)
        build_seconds=time.perf_counter()-before_build
        (destination.parent/'hidden-columns-binding.json').write_bytes(_json_bytes(binding.__dict__))
        original=block; timings=[]
        saved=(subprocess.run,hidden_inputs.decimal_columns,Numbers.ExactRational.ratio,torch.argsort,torch.bincount)
        def forbidden(*args,**kwargs): raise AssertionError('hidden页加载不得再解析事件/decimal/GCD/排序或调用verify CLI')
        try:
            subprocess.run=hidden_inputs.decimal_columns=forbidden
            Numbers.ExactRational.ratio=staticmethod(forbidden)
            torch.argsort=torch.bincount=forbidden
            for _ in range(2):
                before_load=time.perf_counter()
                prepared=PreparedHiddenMaterial(destination,source['source_binding'],binding=binding)
                first=HiddenPmBlock.from_prepared(prepared.page(0,2))
                second=HiddenPmBlock.from_prepared(prepared.page(2,3))
                assert prepared.banks['buckets'].decoded_records==0
                assert first.terminal_columns is second.terminal_columns
                assert first.decimals is second.decimals
                timings.append(time.perf_counter()-before_load)
        finally:
            subprocess.run,hidden_inputs.decimal_columns=saved[:2]
            Numbers.ExactRational.ratio=staticmethod(saved[2])
            torch.argsort,torch.bincount=saved[3:]
        for name in ('event_indices','step_tags','snapshot_levels','snapshot_offsets','reference_rows'):
            assert np.array_equal(prepared.arrays[name],getattr(original,name).cpu().numpy()),name
        compare(prepared.decimals,original.decimals,'loaded_exact_hidden_dictionary')
        assert prepared.banks['buckets']==source['buckets']
        assert prepared.banks['assets']==source['assets']
        # 调用方已保存绑定不会跟随待加载页自述SHA变化。
        manifest_path=destination/'manifest.json'; manifest_bytes=manifest_path.read_bytes()
        changed=json.loads(manifest_bytes); changed['market_count']+=1
        manifest_path.write_bytes(_json_bytes(changed))
        try:
            try: PreparedHiddenMaterial(destination,source['source_binding'],binding=binding)
            except ValueError: pass
            else: raise AssertionError('改自述manifest不能替换独立publisher绑定')
        finally: manifest_path.write_bytes(manifest_bytes)
        block=first
        page_report={'hidden_columns_build_seconds':build_seconds,'hidden_columns_load_seconds':timings,
            'hidden_columns_bytes':sum(p.stat().st_size for p in destination.iterdir()),
            'hidden_second_load_no_event_JSON_decimal_GCD_sort_or_verify':True,
            'hidden_caller_independent_binding_rejects_changed_manifest':True,
            'hidden_full_original_bucket_and_exact_columns_checked':True,
            'hidden_real_complete_bucket_pages':[[0,2],[2,3]]}
    ends = tensor([source['market_end']]*len(block.assets))
    columns = HiddenBucketColumns.from_block(block)
    def open_runtime():
        zero = integers([0])
        quantity = BaseOrderQuantity.from_taker_plan(integers([1]), integers([1]), zero, zero)
        template = FillRecord.create(integers([1]), integers([1]), zero, tensor([0]), tensor([0]),
            tensor([False], torch.bool), {'steps':[{'state':'MATCHED','offset_ms':0},
                {'state':'MINED','offset_ms':10},{'state':'CONFIRMED','offset_ms':20}],
                'economic_application_ordinal':1})
        return BaseSimRuntime(block.empty_state(segment_capacity=1), MutationRequests.empty(zero),
            BaseOrders.empty(quantity), PlatformAccountState.open(integers([10_000_000]), tensor([0]*len(block.assets))),
            FillPool.empty(template), RebateCoordinator(MakerDays.empty(tensor([[0]]), tensor([[source['market_end']]]),
                tensor([[True]], torch.bool)), TakerDays.empty(1, 'cuda')),
            HiddenBucketFrontier.open(columns, 1))
    initial = Numbers.run_exact_batch(open_runtime).install_hidden_market_controls().install_hidden_resolutions()
    # 原同会话反馈计数只在此工程入口冷安装一次；实际Resolved正文来自GPU源/执行候选。
    root=Path(__file__).resolve().parents[2]
    sys.path.insert(0,str(root/'execution-normalization/gpu'))
    from projection import Projection
    from latency import EmpiricalDelaySampler
    from feedback import FeedbackEmitter
    from fields import decode_fields
    from consumption import PendingRecords,ReceiveGates
    from core import OUT_EVIDENCE
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    sampler=EmpiricalDelaySampler.build((root/'sim-system/config/latency-profiles'/sha/'profile.json').read_bytes(),
        sha,[source['session_seed'].encode('ascii')])
    emitter=replace(FeedbackEmitter.open(1),next_private_sequence=integers([source['next_private']]),
        next_batch_sequence=integers([source['next_batch']]))
    zero=tensor([0])
    projection=Projection.open(emitter,zero,zero,zero,zero,initial.account.asset_roots,integers([0]),
        zero,zero,zero,zero)
    condition,end=tensor([[0]]),tensor([[source['market_end']]])
    from core import EvidenceBank,Normalizer
    projection=replace(projection,normalizer=Normalizer.empty(EvidenceBank.empty('cuda'),1))
    original_projection=projection
    sys.path.insert(0,str(root/'ledger/gpu'))
    sys.path.insert(0,str(root/'shared-run/gpu'))
    from memory import EconomicColumns
    from admission_lifecycle import LifecycleColumns
    # 一次开户/初始市场前缀冷安装；之后由同一公开领取器和Ledger设备事务消费。
    import importlib.util
    from admission import AdmissionLifecycle
    from closed import ClosedAdmission
    from success_pool import SuccessPools
    from position_cost import PositionCosts
    from market_economics import UnresolvedMarkets
    from resolution import ResolutionSources,MarketResolutions
    from registry import PublicRegistryEvents,PublicRegistryState
    from public_ledger import PublicLedgerState
    spec=importlib.util.spec_from_file_location('gpu_terminal_platform',root/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    before_ledger=source['ledger_before']['projection']
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd',
        'maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    original_balances=before_ledger['platform_confirmed_projection']
    def assemble(*,known=True,columns=None):
        balances=module.PlatformBalances.open(rational([original_balances[key] for key in names]),
            rational(['0','0']),tensor([0,0]),integers([before_ledger['platform_source_prefix']]))
        pools=SuccessPools.open(tensor([0,0]),tensor([0,0]),tensor([0,1]),tensor([1,0]),1)
        markets=UnresolvedMarkets.open(pools,tensor([0]),known=known)
        sources=ResolutionSources.bind(block.terminal_columns,[asset['token_id'] for asset in block.assets])
        resolutions=MarketResolutions.open(pools,markets,sources)
        if columns is None:
            columns=EconomicColumns.empty(1,'cuda')
        admission=AdmissionLifecycle.open(balances,projection.normalizer.bank,columns,
            integers([before_ledger['ledger_entry_seq']]),integers([before_ledger['ledger_local_prefix']]),pools,
            position_costs=PositionCosts.empty(1,1,'cuda'),unresolved_markets=markets,resolutions=resolutions)
        empty=columns.row_refs
        pending=PendingRecords.from_columns(torch.zeros_like(empty,dtype=torch.bool),empty.clone(),
            integers([]),integers([]),empty,integers([0]))
        return PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),
            ClosedAdmission.empty(columns),admission)
    public=Numbers.run_exact_batch(assemble)
    storage = BaseRuntimeStorage(initial, initial)
    frozen = storage.snapshot()
    horizon, enabled = tensor([21000]), tensor([False], torch.bool)
    fee, rebate = Numbers.run_exact_batch(lambda:(rational(['0.07']), rational(['0.08'])))
    def capture(current):
        return CapturedHiddenBuckets(current, horizon, enabled, tensor([0]*len(block.assets)), tensor([[0]]),
            fee, 1, rebate, ends, word_capacity=4, feedback_capacity=1,
            stage_progress=lambda stage:progress('reference_'+stage))
    loop=capture(storage)
    loop.replay()
    torch.cuda.synchronize()
    compare(storage.runtime, frozen, 'first_disabled_reference_entry')
    assert loop.resources_fit.cpu().item() and loop.required_words.cpu().item() == 0
    assert loop.reference_iterations.cpu().tolist() == [0] and loop.page.count.cpu().item() == 0
    enabled.fill_(True)
    loop.replay()
    torch.cuda.synchronize()
    runtime = storage.runtime
    assert runtime.hidden_frontier.next_reference.cpu().tolist() == [2]
    assert runtime.hidden_frontier.closed_at.cpu().tolist() == [21000]
    assert values(runtime.hidden_frontier.index_position) == [block.buckets[0]['bucket']['index_end']]
    assert loop.reference_iterations.cpu().tolist() == [2] and loop.iterations.cpu().tolist() == [0]
    assert loop.closed.cpu().tolist() == [True] and loop.page.count.cpu().item() == 0
    for name in ('hidden', 'requests', 'orders', 'account', 'fills', 'calendars', 'market_controls'):
        compare(getattr(runtime, name), getattr(frozen, name), 'reference_business_unchanged_'+name)
    pure_reference_freeze = storage.snapshot()
    progress('two_actual_TWAP_references_closed', actual_references=2)
    # 同一固定图/输入地址进入下一完整Book桶，不从reference价格填盘口。
    horizon.fill_(22000)
    loop.replay()
    torch.cuda.synchronize()
    assert storage.runtime.hidden.initialized.cpu().tolist() == [True]*len(block.assets)
    assert storage.runtime.hidden.queues.rows == 0
    assert storage.runtime.hidden_frontier.closed_at.cpu().tolist() == [22000]
    assert loop.reference_iterations.cpu().tolist() == [0] and loop.page.count.cpu().item() == 0
    compare(pure_reference_freeze.hidden, frozen.hidden, 'old_reference_freeze_not_backfilled')
    if page_mode:
        old_storage=storage; old_loop=loop; old_columns=columns
        old_public_at_page_end=public
        assert not columns.end_of_input
        assert storage.runtime.hidden_frontier.scheduled()[1].cpu().tolist()==[False]
        block=second; columns=HiddenBucketColumns.from_block(block)
        frontier=storage.runtime.hidden_frontier.handoff(columns)
        assert values(frontier.index_position)==values(storage.runtime.hidden_frontier.index_position)
        assert frontier.closed_at.cpu().tolist()==[22000] and frontier.closed_bucket.cpu().tolist()==[1]
        continued=replace(storage.runtime,hidden_frontier=frontier)
        storage=BaseRuntimeStorage(continued,continued)
        loop=capture(storage)
        assert public is old_public_at_page_end
        for name in ('hidden','requests','orders','account','fills','calendars','market_controls','source_resolutions'):
            compare(getattr(storage.runtime,name),getattr(old_storage.runtime,name),'page_handoff_keeps_'+name)
        assert old_storage.runtime.hidden_frontier.columns is old_columns
        compare(pure_reference_freeze.hidden,frozen.hidden,'old_page_freeze_stays_immutable')
        page_report.update(hidden_same_runtime_and_public_ledger_across_pages=True,
            hidden_page_end_is_not_EOF=True,hidden_complete_u64_frontier_and_old_source_handle_preserved=True,
            hidden_stable_Terminal_bank_preserved_across_pages=True)
    # 原已登记end义务实际消费后，再进入真实terminal来源桶；不伪造MarketFact已应用。
    horizon.fill_(31000)
    choice = storage.runtime.choose(None, None, horizon, enabled)
    assert choice.enabled(3).cpu().tolist() == [True]
    ended = Numbers.run_exact_batch(lambda:storage.runtime.rebate_clock(choice, ends))
    assert ended[1].consumed.cpu().tolist() == [True]
    storage.commit(ended[0], torch.ones((), dtype=torch.bool, device='cuda'))
    before = storage.snapshot()
    loop.replay()
    torch.cuda.synchronize()
    frontier = storage.runtime.hidden_frontier
    assert block.reference_tags[frontier.next_reference].cpu().tolist() == [9]
    assert frontier.next_reference.cpu().tolist() == [0 if page_mode else source['terminal_reference_index']]
    assert frontier.closed_at.cpu().tolist() == [22000] and frontier.applying.cpu().tolist() == [True]
    assert loop.closed.cpu().tolist() == [False]
    assert loop.reference_iterations.cpu().tolist() == [(0 if page_mode else source['terminal_reference_index'])-before.hidden_frontier.next_reference.cpu().item()]
    assert frontier.fault.cpu().tolist() == [0] and loop.page.count.cpu().item() == 0
    terminal_before=storage.snapshot()
    choice=storage.runtime.choose(None,None,horizon,enabled)
    exhausted=replace(projection,emitter=replace(projection.emitter,
        next_private_sequence=integers([(1<<64)-1])))
    failed,unchanged,failed_batch,failed_effect=Numbers.run_exact_batch(lambda:
        storage.runtime.apply_hidden_terminal(choice,storage.runtime.hidden_frontier.next_reference,
            condition,end,exhausted,sampler,tensor([0,1])))
    assert failed_effect.fault.cpu().tolist()==[1]
    assert not failed_effect.resolved.any().cpu().item() and not failed_batch.emitted.any().cpu().item()
    assert not failed.source_resolutions.present.any().cpu().item()
    assert failed.resolution_obligations.reference_indices.cpu().tolist()==[[-1]]
    assert failed.hidden_frontier.next_reference.cpu().tolist()==terminal_before.hidden_frontier.next_reference.cpu().tolist()
    assert values(unchanged.emitter.next_private_sequence)==[(1<<64)-1]
    candidate,projected,batch,effect=Numbers.run_exact_batch(lambda:storage.runtime.apply_hidden_terminal(
        choice,storage.runtime.hidden_frontier.next_reference,condition,end,projection,sampler,tensor([0,1])))
    assert effect.fault.cpu().tolist()==[0] and effect.resolved.cpu().tolist()==[True]
    # 通用producer的空订单物理padding不属于原NOE人口。
    from transport import select_batch_columns
    assert batch.emitted.sum(1).cpu().tolist()==[1]
    assert all(not event.effect.closed.any().cpu().item() for event in effect.orders.events)
    emitted_columns=batch.emitted.to(torch.int64).argmax(1,keepdim=True)
    batch=select_batch_columns(batch,emitted_columns)
    denied=storage.commit(candidate,tensor(False,torch.bool),TerminalFeedback(effect,batch))
    compare(storage.runtime,terminal_before,'Terminal_resource_exit_preserves_whole_prefix')
    assert not denied.effect.resolved.any().cpu().item() and not denied.batch.emitted.any().cpu().item()
    compare(projection,original_projection,'Terminal_resource_exit_keeps_original_emitter')
    committed=storage.commit(candidate,tensor(True,torch.bool),TerminalFeedback(effect,batch))
    projection=projected
    batch=committed.batch
    runtime=storage.runtime
    assert runtime.source_resolutions.present.cpu().tolist()==[[True]]
    assert runtime.resolution_obligations.reference_indices.cpu().tolist()==[[source['terminal_reference_index']]]
    assert not runtime.resolution_obligations.pending.any().cpu().item()
    assert runtime.hidden_frontier.next_reference.cpu().tolist()==[(0 if page_mode else source['terminal_reference_index'])+1]
    compare(runtime.account,before.account,'Resolved_is_not_cash_or_automatic_redeem')
    reference=source['qualified_sources'][0]
    ref_index=runtime.source_resolutions.reference_indices.cpu().item()
    column=block.terminal_columns.fields[ref_index].cpu().tolist()
    assert block.terminal_columns.canonical_ids[column[3]]==reference['event_id']
    assert block.terminal_columns.terminals[column[2]]==reference['terminal']
    assert runtime.source_resolutions.source_times.cpu().item()==reference['source_at']
    from fractions import Fraction
    payouts=block._values(block.terminal_columns.payout_indices[ref_index])
    assert [Fraction(n,d) for n,d in zip(values(payouts.numerator),values(payouts.denominator))]==[
        Fraction(value) for value in reference['parsed_payouts']]
    assert batch.emitted.cpu().tolist()==[[True]] and batch.normalized.outcomes.cpu().tolist()==[[OUT_EVIDENCE]]
    assert not batch.economic_enabled.any().cpu().item()
    record=source['resolved_records'][0]
    evidence=record['record']['Evidence']
    original_result=evidence['evidence']['SettlementLifecycleV2']['result']['Resolved']
    identity=json.loads(original_result['resolution_identity'].split(':',1)[1])[-1]['Resolution']['source_resolution_id']
    assert identity==reference['event_id'] and original_result['payout_vector']==reference['terminal']
    actual_row=batch.rows.flatten()
    assert projection.normalizer.bank.payload_schemas[actual_row].cpu().tolist()==[17]
    sys.path.insert(0,str(root/'ledger/gpu'))
    sys.path.insert(0,str(root/'shared-run/gpu'))
    from cancel_projection import cancellation_rows_from_closed
    from memory import EconomicColumns
    from admission_lifecycle import LifecycleColumns
    resolved_columns=Numbers.run_exact_batch(lambda:LifecycleColumns.from_projection(
        projection.normalizer.bank,EconomicColumns.from_projection_batches([batch])))
    assert resolved_columns.fault.cpu().tolist()==[[0]]
    decoded,error=Numbers.run_exact_batch(lambda:decode_fields(projection.normalizer.bank.semantic[actual_row],
        ['scalar']*4,batch.emitted.flatten()))
    assert error.cpu().tolist()==[0] and [value.cpu().item() for value in decoded]==[0,0,column[3],column[2]]
    metadata=evidence['source_reference']['record']['SettlementLifecycle']
    assert values(batch.private_sequences)==[metadata['private_feedback_sequence']]
    assert values(batch.batch_sequences)==[record['position']['source_watermark']['projection_batch_sequence']]
    assert values(batch.record_ordinals)==[record['position']['record_ordinal']]
    assert batch.platform_at.cpu().tolist()==[[metadata['platform_event_at_utc_ms']]]
    assert batch.received_at.cpu().tolist()==[[metadata['feedback_received_at_utc_ms']]]
    assert batch.source_categories.cpu().tolist()==[[3]]
    ordinal=integers([int(projection.normalizer.bank.source_order[actual_row,3].cpu().item())])
    assert values(ordinal)==[evidence['source_order']['ordinal']]
    old_public=public
    public=Numbers.run_exact_batch(lambda:public.append_batch(projection.normalizer.bank,batch))
    gates=ReceiveGates.unset(1).set_receive(tensor([source['resolved_close_at']]),enabled)
    public,blocked=Numbers.run_exact_batch(lambda:public.receive(projection.normalizer.bank,gates,
        torch.ones_like(public.waiting.received)))
    assert not blocked.closed.emitted.any().cpu().item()
    assert values(public.ledger.current_entry_sequence)==[before_ledger['ledger_entry_seq']]
    gates=gates.set_public(tensor([source['resolved_close_at']]),enabled)
    public,delivery=Numbers.run_exact_batch(lambda:public.receive(projection.normalizer.bank,gates,
        torch.ones_like(public.waiting.received)))
    assert delivery.closed.emitted.cpu().tolist()==[[True]] and delivery.fault.cpu().tolist()==[0]
    assert not delivery.cancellation_targets.received.any().cpu().item()
    assert not delivery.cancellation_completions.received.any().cpu().item()
    admission=public.ledger
    after_ledger=source['ledger_after']['projection']
    assert values(admission.current_entry_sequence)==[after_ledger['ledger_entry_seq']]
    assert values(admission.current_entry_sequence)==[before_ledger['ledger_entry_seq']+1]
    assert values(admission.current_local_prefix)==[after_ledger['ledger_local_prefix']]
    assert values(admission.platform.platform_source_prefix)==[after_ledger['platform_source_prefix']]
    assert admission.available_through.cpu().tolist()==[source['resolved_close_at']]
    assert public.waiting.available_at.cpu().tolist()==[[source['resolved_close_at']]]
    assert public.waiting.consumed.cpu().tolist()==[[True]] and admission.applied.cpu().tolist()==[[True]]
    assert values(admission.platform.balances.numerator)==values(old_public.ledger.platform.balances.numerator)
    assert values(admission.platform.balances.denominator)==values(old_public.ledger.platform.balances.denominator)
    resolved=Numbers.run_exact_batch(lambda:admission.resolutions.read(admission.success_pools))
    original=source['ledger_resolutions'][0]
    assert original['request'] is None and original['category']=='Directional'
    assert resolved.present.cpu().tolist()==[True] and resolved.maker_category.cpu().tolist()==[False]
    assert resolved.identities.cpu().tolist()==[column[3]] and resolved.terminals.cpu().tolist()==[column[2]]
    for amount,key in [(resolved.paired_receivable,'paired_receivable'),(resolved.winner_orphan_receivable,'orphan_receivable')]:
        assert [Fraction(n,d) for n,d in zip(values(amount.numerator),values(amount.denominator))]==[Fraction(original[key])]
    # 此已判定阶段使用同一每条提交更新的Pi/peak，未兑付应收不加现金。
    market_read=Numbers.run_exact_batch(lambda:admission.unresolved_markets.read(admission.success_pools,
        admission.trade_success,admission.rebate_days,resolutions=admission.resolutions))
    original_market=source['ledger_after']['markets'][0]
    from validate_market_economics import exact
    for amount,key in [(market_read.guarantee,'guaranteed_position_value_pusd'),
        (market_read.orphan_cost,'unresolved_orphan_principal_cost_pusd'),(market_read.profit,'confirmed_net_pnl_pusd'),
        (market_read.maximum_loss,'maximum_confirmed_net_loss_pusd')]:
        assert [Fraction(n,d) for n,d in zip(values(amount.numerator),values(amount.denominator))]==[exact(original_market[key])]
    assert not old_public.ledger.resolutions.present.any().cpu().item()
    held_ledger=public
    public,repeated=Numbers.run_exact_batch(lambda:public.receive(projection.normalizer.bank,gates,
        torch.ones_like(public.waiting.received)))
    assert not repeated.closed.emitted.any().cpu().item()
    compare(public.ledger,held_ledger.ledger,'repeat_Resolved_no_entry_or_cash_change')
    assert source['ledger_before']['cash']==source['ledger_after']['cash']
    # 相同完整position显式重传也不增加条目，不刷新首次available。
    reused,waiting=Numbers.run_exact_batch(lambda:admission.apply_closed(delivery.closed,
        torch.ones_like(admission.applied)))
    assert not waiting.any().cpu().item()
    compare(reused,admission,'same_Resolved_position_already_applied')
    missing_binding=replace(old_public.ledger,resolutions=None)
    rejected,_=Numbers.run_exact_batch(lambda:missing_binding.apply_record(tensor([0]),enabled,
        tensor([source['resolved_close_at']]),enabled))
    assert rejected.fault.cpu().tolist()==[1] and not rejected.applied.any().cpu().item()
    assert values(rejected.current_entry_sequence)==[before_ledger['ledger_entry_seq']]
    # 实际可达冷前缀未安装经济历史：原Resolved身份/payout不能补出成本/份额完整性。
    unknown=Numbers.run_exact_batch(lambda:assemble(known=False))
    unknown=Numbers.run_exact_batch(lambda:unknown.append_batch(projection.normalizer.bank,batch))
    assert unknown.ledger.resolutions.known.cpu().tolist()==[False]
    unknown,unknown_delivery=Numbers.run_exact_batch(lambda:unknown.receive(projection.normalizer.bank,gates,
        torch.ones_like(unknown.waiting.received)))
    assert unknown_delivery.fault.cpu().tolist()==[0] and unknown.ledger.resolutions.present.cpu().tolist()==[True]
    unknown_read=Numbers.run_exact_batch(lambda:unknown.ledger.resolutions.read(unknown.ledger.success_pools))
    unknown_market=Numbers.run_exact_batch(lambda:unknown.ledger.unresolved_markets.read(unknown.ledger.success_pools,
        unknown.ledger.trade_success,unknown.ledger.rebate_days,resolutions=unknown.ledger.resolutions))
    assert unknown_read.known.cpu().tolist()==[False] and unknown_market.known.cpu().tolist()==[False]
    assert unknown.ledger.unresolved_markets.known.cpu().tolist()==[False]
    assert values(unknown.ledger.current_entry_sequence)==[after_ledger['ledger_entry_seq']]
    # 传输vector顺序改变不能改变token归属；仅此冷绑定组件代表，非新源事件。
    reversed_columns=replace(block.terminal_columns,terminals=tuple(
        dict(terminal,payout_vector=list(reversed(terminal['payout_vector'])))
        for terminal in block.terminal_columns.terminals))
    rebound=Numbers.run_exact_batch(lambda:ResolutionSources.bind(reversed_columns,
        [asset['token_id'] for asset in block.assets]))
    compare(rebound.payouts,admission.resolutions.sources.payouts,'payouts_match_token_not_vector_position')
    # 同一固定图接后继真实Lifecycle；此前从未提前安装它。
    loop.replay()
    torch.cuda.synchronize()
    assert storage.runtime.hidden_frontier.closed_at.cpu().tolist()==[31000]
    assert loop.reference_iterations.cpu().tolist()==[1] and loop.closed.cpu().tolist()==[True]
    for original in source['original_controls']:
        controls=storage.runtime.market_controls
        mapped=controls.mapped_lifecycle.cpu().item()
        assert (None if mapped<0 else block.lifecycle_values[mapped])==original['mapped']
        assert controls.quality.cpu().item()=={'fresh':0,'stale':1,'missing':2,'unknown_not_observed':3,'confirmed_gap':4,'invalid':5}[original['quality']]
    held=storage.snapshot()
    loop.replay()
    torch.cuda.synchronize()
    compare(storage.runtime,held,'closed_Terminal_bucket_replay_no_duplicate')
    compare(pure_reference_freeze.source_resolutions,frozen.source_resolutions,'old_freeze_has_no_future_resolution')
    progress('actual_Terminal_Resolved_then_Lifecycle_closed',actual_resolved=1,actual_controls=len(source['original_controls']))
    report={'actual_typedraw_TWAP_references':2, 'actual_bucket_times':[21000,22000,31000],
        'pure_reference_bucket_closed_without_SIM_business_effects':True,
        'missing_source_time_did_not_fabricate_bucket':True,
        'same_graph_reference_then_actual_empty_Books_checked':True,
        'actual_Terminal_Resolved_then_Lifecycle_consumed_and_complete_bucket_closed':True,
        'original_SIM_control_reducer_receives_actual_post_Terminal_Lifecycle_checked':True,
        'canonical_market_facts_exactly_restored_against_original_materialization':source['canonical_market_facts_checked'],
        'actual_terminal_precedes_lifecycle_and_is_not_skipped':True,
        'actual_control_facts_consumed_after_terminal':len(source['original_controls']),
        'actual_first_Resolved_canonical_identity_full_payout_source_and_NOE_metadata_checked':True,
        'Terminal_resource_exit_withholds_state_and_Resolved_feedback_checked':True,
        'Terminal_private_sequence_overflow_reverts_candidate_and_withholds_Resolved_checked':True,
        'Resolved_schema_is_distinct_from_CancelCompleted_at_actual_consumer_checked':True,
        'Resolved_actual_V4_unique_memory_reference_to_Ledger_entry_and_cash_checked':True,
        'device_PublicLedgerState_receives_Resolved_once_and_preserves_first_visibility_and_position_checked':True,
        'Resolved_payout_token_binding_and_missing_source_binding_explicit_fault_checked':True,
        'zero_balance_Resolved_read_is_not_nonzero_receivable_or_settlement_barrier_coverage':True,
        'actual_Resolved_does_not_repair_unknown_cold_economic_history_checked':True,
        'no_orders_or_pending_fills_in_original_Terminal_population':True,
        'cold_projection_prefix_initialization':True,
        'Resolved_Ledger_consumer_and_request_settlement_complete':False,
        'first_disabled_reference_entry_and_old_freeze_checked':True,
        'market_fact_handler_complete':False, 'full_unified_host_clock_complete':False,
        'elapsed_seconds':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),**page_report}
    if page_mode:
        report['hidden_ended_material_once_build_only']=True
        report['hidden_streaming_publication_catalog_and_nonempty_order_handoff_complete']=False
        (Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])/'hidden-pages-report.json').write_text(json.dumps(report,indent=2))
    print(json.dumps(report))


if __name__ == '__main__':
    main()
