"""原紧凑TWAP/空Book→无订单Terminal/Resolved→真实后继Lifecycle对照。"""
from dataclasses import replace
from pathlib import Path
import json
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
    storage = BaseRuntimeStorage(initial, initial)
    frozen = storage.snapshot()
    horizon, enabled = tensor([21000]), tensor([False], torch.bool)
    fee, rebate = Numbers.run_exact_batch(lambda:(rational(['0.07']), rational(['0.08'])))
    loop = CapturedHiddenBuckets(storage, horizon, enabled, tensor([0]*len(block.assets)), tensor([[0]]),
        fee, 1, rebate, ends, word_capacity=4, feedback_capacity=1,
        stage_progress=lambda stage:progress('reference_'+stage))
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
    assert frontier.next_reference.cpu().tolist() == [source['terminal_reference_index']]
    assert frontier.closed_at.cpu().tolist() == [22000] and frontier.applying.cpu().tolist() == [True]
    assert loop.closed.cpu().tolist() == [False]
    assert loop.reference_iterations.cpu().tolist() == [source['terminal_reference_index']-before.hidden_frontier.next_reference.cpu().item()]
    assert frontier.fault.cpu().tolist() == [0] and loop.page.count.cpu().item() == 0
    terminal_before=storage.snapshot()
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
    original_projection=projection
    choice=storage.runtime.choose(None,None,horizon,enabled)
    exhausted=replace(projection,emitter=replace(projection.emitter,
        next_private_sequence=integers([(1<<64)-1])))
    failed,unchanged,failed_batch,failed_effect=Numbers.run_exact_batch(lambda:
        storage.runtime.apply_hidden_terminal_without_orders(choice,storage.runtime.hidden_frontier.next_reference,
            condition,end,exhausted,sampler))
    assert failed_effect.fault.cpu().tolist()==[1]
    assert not failed_effect.resolved.any().cpu().item() and not failed_batch.emitted.any().cpu().item()
    assert not failed.source_resolutions.present.any().cpu().item()
    assert failed.resolution_obligations.reference_indices.cpu().tolist()==[[-1]]
    assert failed.hidden_frontier.next_reference.cpu().tolist()==terminal_before.hidden_frontier.next_reference.cpu().tolist()
    assert values(unchanged.emitter.next_private_sequence)==[(1<<64)-1]
    candidate,projected,batch,effect=Numbers.run_exact_batch(lambda:storage.runtime.apply_hidden_terminal_without_orders(
        choice,storage.runtime.hidden_frontier.next_reference,condition,end,projection,sampler))
    assert effect.fault.cpu().tolist()==[0] and effect.resolved.cpu().tolist()==[True]
    assert all(not event.effect.closed.any().cpu().item() for event in effect.orders.events)
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
    assert runtime.hidden_frontier.next_reference.cpu().tolist()==[source['terminal_reference_index']+1]
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
    sys.path.insert(0,str(root/'shared-run/gpu'))
    from cancel_projection import cancellation_rows_from_closed
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
    pending=PendingRecords.from_columns(batch.emitted,batch.received_at,batch.private_sequences,ordinal,batch.rows,integers([0]))
    gates=ReceiveGates.unset(1).set_receive(tensor([source['resolved_close_at']]),enabled)
    pending,blocked=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not blocked.emitted.any().cpu().item()
    gates=gates.set_public(tensor([source['resolved_close_at']]),enabled)
    pending,closed=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert closed.emitted.cpu().tolist()==[[True]]
    targets,completions=Numbers.run_exact_batch(lambda:
        cancellation_rows_from_closed(projection.normalizer.bank,[batch],closed))
    assert targets.fault.cpu().tolist()==[0] and completions.fault.cpu().tolist()==[0]
    assert not targets.emitted.any().cpu().item() and not completions.emitted.any().cpu().item()
    pending,repeated=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not repeated.emitted.any().cpu().item()
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
    print(json.dumps({'actual_typedraw_TWAP_references':2, 'actual_bucket_times':[21000,22000,31000],
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
        'no_orders_or_pending_fills_in_original_Terminal_population':True,
        'cold_projection_prefix_initialization':True,
        'Resolved_Ledger_consumer_and_request_settlement_complete':False,
        'first_disabled_reference_entry_and_old_freeze_checked':True,
        'market_fact_handler_complete':False, 'full_unified_host_clock_complete':False,
        'elapsed_seconds':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated()}))


if __name__ == '__main__':
    main()
