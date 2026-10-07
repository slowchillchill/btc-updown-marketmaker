"""原typedraw空Book→外层GTC→Terminal，同设备SIM/NOE/公开Ledger连续对照。

源页/原请求/可信领取截点为工程输入；不使用原订单后态生成设备状态。
"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import importlib.util
import json
import os
import sys
import time
import torch
from queues import Numbers, _take
from validate_base_execution import integers, rational, values, units
from validate_base_orders import compare, progress
from account_state import PlatformAccountState
from base_orders import BaseOrders
from base_runtime import BaseSimRuntime
from base_runtime_storage import BaseRuntimeStorage, CapturedHiddenBuckets, TerminalFeedback
from fill_finality import FillRecord
from fill_pool import FillPool
from hidden_frontier import HiddenBucketColumns, HiddenBucketFrontier
from hidden_inputs import HiddenPmBlock
from maker_days import MakerDays
from taker_days import TakerDays
from rebate_coordinator import RebateCoordinator
from mutation_requests import MutationRequests
from pending_base_execution import FrozenBaseTerms
from order_quantity import BaseOrderQuantity
from latency import EmpiricalDelaySampler, byte_column


def main():
    source=json.load(sys.stdin)
    if os.environ.get("B_GPU_ORDER_ARTIFACT_DIR"):
        (Path(os.environ["B_GPU_ORDER_ARTIFACT_DIR"])/"original-terminal-order-reference.json").write_text(json.dumps(source))
    started=time.perf_counter()
    root=Path(__file__).resolve().parents[2]
    tensor=lambda x,dtype=torch.int64:torch.tensor(x,dtype=dtype,device='cuda')
    enabled,zero=tensor([True],torch.bool),tensor([0])
    with_pending=source.get('with_pending',False)
    post=source['post_reference']
    request=post['request']['request']
    item=request['items'][0]
    kind=item['item']['order']['kind']['GtcLimit']
    token=item['item']['order']['token_id']
    price=kind['limit_price_pusd_per_share']
    # 运行价位来自原合法意图，源Book不需要有该档，也不虚构外部盘口。
    block=HiddenPmBlock(source['buckets'],source['assets'],[(token,'BID',price)],source_binding=source['source_binding'])
    asset=next(i for i,value in enumerate(block.assets) if value['token_id']==token)
    before=source['ledger_before']['projection']
    opening=before['platform_confirmed_projection']
    def build():
        blank=integers([0])
        quantity=BaseOrderQuantity.from_taker_plan(integers([1]),integers([1]),blank,blank)
        fill=FillRecord.create(integers([1]),integers([1]),blank,zero,zero,tensor([False],torch.bool),
            source['engineering_finality_path'] if with_pending else {'steps':[{'state':'MATCHED','offset_ms':0},{'state':'MINED','offset_ms':10},{'state':'CONFIRMED','offset_ms':20}],
             'economic_application_ordinal':1})
        return BaseSimRuntime(block.empty_state(segment_capacity=2 if with_pending else 1),MutationRequests.empty(blank,capacity=1,action_capacity=1),BaseOrders.empty(quantity),
            PlatformAccountState.open(integers([units(opening['cash_pusd'])]),tensor([0,0])),FillPool.empty(fill),
            RebateCoordinator(MakerDays.empty(tensor([[0]]),tensor([[source['market_end']]]),enabled[:,None]),
                TakerDays.empty(1,'cuda')),HiddenBucketFrontier.open(HiddenBucketColumns.from_block(block),1))
    runtime=Numbers.run_exact_batch(build).install_hidden_market_controls().install_hidden_resolutions()
    fee,rebate=Numbers.run_exact_batch(lambda:(rational(['0.07']),rational(['0.08'])))
    end=tensor([source['market_end']]*2)
    storage=BaseRuntimeStorage(runtime,runtime)
    horizon=tensor([22000])
    loop=CapturedHiddenBuckets(storage,horizon,enabled,tensor([0,0]),tensor([[0]*runtime.hidden.queues.capacity]),fee,1,rebate,end,
        word_capacity=4,feedback_capacity=1,stage_progress=lambda stage:progress('resting_'+stage))
    loop.replay();torch.cuda.synchronize()
    runtime=storage.runtime
    assert runtime.hidden_frontier.closed_at.cpu().tolist()==[22000]
    assert runtime.hidden.initialized.cpu().tolist()==[True,True]
    assert not runtime.hidden.queues.kind.any().cpu().item()
    assert values(runtime.hidden.external_shares.numerator)==[0]
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    sampler=EmpiricalDelaySampler.build((root/'sim-system/config/latency-profiles'/sha/'profile.json').read_bytes(),sha,[source['session_seed'].encode('ascii')])
    identity,offsets=byte_column([request['context']['global_request_id'].encode()])
    requests,receipt=runtime.requests.run_registration(sampler,identity,offsets,zero,tensor([1]),integers([post['input_sequence']]),
        tensor([post['sent_at']]),tensor([[0]]),tensor([[item['batch_item_ordinal']]]),enabled[:,None],
        tensor([[False]],torch.bool),zero,enabled,tensor([0],torch.int32))
    assert receipt.arrived_at.cpu().tolist()==[post['processing_at']]
    runtime=replace(runtime,requests=requests)
    terms=Numbers.run_exact_batch(lambda:FrozenBaseTerms(zero,integers([units(kind['size_shares'])]),
        integers([units(Fraction(kind['size_shares'])*Fraction(price))]),rational([price]),tensor([asset]),zero,tensor([0],torch.int32),zero))
    horizon.fill_(post['processing_at'])
    choice=runtime.choose(None,None,horizon,enabled)
    assert choice.enabled(5).cpu().tolist()==[True]
    runtime,effect=Numbers.run_exact_batch(lambda:runtime.accept_base(choice,terms,tensor([[-1]]),fee,1,tensor([[0]*runtime.hidden.queues.capacity]),rebate,zero,zero))
    assert effect.order.emitted.cpu().tolist()==[True] and effect.fault.cpu().tolist()==[0]
    assert values(runtime.orders.read(effect.order.slots).quantity.open_base_units)==[5_000_000]
    assert (runtime.hidden.queues.kind==2).sum().cpu().item()==1 and not runtime.fills.occupied.any().cpu().item()
    progress('actual_empty_book_GTC_accepted')
    sys.path.insert(0,str(root/'execution-normalization/gpu'))
    from projection import Projection
    from feedback import FeedbackEmitter
    from validate_projection import expected_semantic
    from consumption import PendingRecords,ReceiveGates
    emitter=replace(FeedbackEmitter.open(1),next_private_sequence=integers([source['next_private']]),next_batch_sequence=integers([source['next_batch']]))
    projection=Projection.open(emitter,zero,zero,zero,zero,tensor([0,0]),integers([0]),zero,zero,zero,zero)
    projection,accepted_batch=Numbers.run_exact_batch(lambda:projection.pending_base_acceptance(sampler,runtime.orders,runtime.fills,effect,tensor([0,1])))
    assert accepted_batch.emitted.sum().cpu().item()==3
    maker_batches=[]
    if with_pending:
        horizon.fill_(27000)
        maker_storage=BaseRuntimeStorage(runtime,runtime)
        maker_loop=CapturedHiddenBuckets(maker_storage,horizon,enabled,tensor([0,0]),
            tensor([[0]*runtime.hidden.queues.capacity]),fee,1,rebate,end,word_capacity=4,feedback_capacity=1,
            stage_progress=lambda stage:progress('actual_pending_Maker_'+stage))
        maker_loop.replay();torch.cuda.synchronize();runtime=maker_storage.runtime
        assert maker_loop.page.count.cpu().item()==1 and runtime.fills.occupied.sum().cpu().item()==1
        frame=maker_loop.page.read(0)
        for transition in frame.effects.fills:
            projection,batch=Numbers.run_exact_batch(lambda:projection.fill_from_origin(sampler,runtime.orders,
                runtime.fills,transition,tensor([0,1]),runtime.calendars,rebate,frame.at))
            maker_batches.append(batch)
        assert sum(int(batch.emitted.sum().cpu()) for batch in maker_batches)==1
        assert values(runtime.orders.read(effect.order.slots).quantity.matched_base_units)==[3_000_000]
        assert values(runtime.orders.read(effect.order.slots).quantity.open_base_units)==[2_000_000]
    accepted_runtime=runtime
    horizon.fill_(31000)
    choice=runtime.choose(None,None,horizon,enabled)
    assert choice.enabled(3).cpu().tolist()==[True]
    runtime,clock=Numbers.run_exact_batch(lambda:runtime.rebate_clock(choice,end))
    assert clock.consumed.cpu().tolist()==[True]
    assert not runtime.hidden.queues.kind.any().cpu().item()
    assert values(runtime.account.principal)==values(accepted_runtime.account.principal)
    # 同一固定源图的下一真实桶停于Terminal，不预消费后继Lifecycle。
    terminal_storage=BaseRuntimeStorage(runtime,runtime)
    terminal_loop=CapturedHiddenBuckets(terminal_storage,horizon,enabled,tensor([0,0]),tensor([[0]*runtime.hidden.queues.capacity]),fee,1,rebate,end,
        word_capacity=4,feedback_capacity=1,stage_progress=lambda stage:progress('terminal_'+stage))
    terminal_loop.replay();torch.cuda.synchronize()
    runtime=terminal_storage.runtime
    choice=runtime.choose(None,None,horizon,enabled)
    old_runtime=runtime
    candidate,projected,terminal_batch,terminal=Numbers.run_exact_batch(lambda:runtime.apply_hidden_terminal(choice,
        runtime.hidden_frontier.next_reference,tensor([[0]]),tensor([[source['market_end']]]),projection,sampler,tensor([0,1])))
    assert terminal.fault.cpu().tolist()==[0] and terminal.orders.events[0].effect.closed.cpu().tolist()==[True]
    assert terminal.orders.events[0].effect.delta.emitted.cpu().tolist()==[True]
    denied=terminal_storage.commit(candidate,tensor(False,torch.bool),TerminalFeedback(terminal,terminal_batch))
    compare(terminal_storage.runtime,old_runtime,'denied_Terminal_all_owned_prefix')
    assert not denied.batch.emitted.any().cpu().item()
    committed=terminal_storage.commit(candidate,tensor(True,torch.bool),TerminalFeedback(terminal,terminal_batch))
    projection=projected;terminal_batch=committed.batch;runtime=terminal_storage.runtime
    assert values(runtime.orders.read(effect.order.slots).quantity.market_resolved_canceled_base_units)==[2_000_000 if with_pending else 5_000_000]
    assert values(runtime.orders.read(effect.order.slots).quantity.open_base_units)==[0]
    assert values(runtime.account.principal)==[1_200_000 if with_pending else 0]
    assert values(runtime.account.available)==[units(opening['cash_pusd'])-(1_200_000 if with_pending else 0)]
    assert terminal_batch.emitted.sum().cpu().item()==3
    assert runtime.resolution_obligations.pending.sum().cpu().item()==int(with_pending)
    if with_pending:
        assert values(runtime.resolution_obligations.sequences)[0]==1
        assert runtime.calendars.maker.market_result.cpu().tolist()==[[-1]]
    sys.path.insert(0,str(root/'ledger/gpu'));sys.path.insert(0,str(root/'shared-run/gpu'))
    from fields import decode_fields
    from memory import EconomicColumns
    from admission import AdmissionLifecycle
    from order_funds import OrderFunds
    from closed import ClosedAdmission
    from success_pool import SuccessPools
    from position_cost import PositionCosts
    from market_economics import UnresolvedMarkets
    from rebate_day import RebateDays
    from resolution import ResolutionSources,MarketResolutions
    from registry import PublicRegistryEvents,PublicRegistryState
    from public_ledger import PublicLedgerState
    spec=importlib.util.spec_from_file_location('terminal_order_platform',root/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd','maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    assert source["ledger_before"]["order_funds"]==[] and source["ledger_before"]["trades"]==[]
    def assemble():
        balances=module.PlatformBalances.open(rational([opening[key] for key in names]),rational(['0','0']),tensor([0,0]),integers([before['platform_source_prefix']]))
        pools=SuccessPools.open(tensor([0,0]),tensor([0,0]),tensor([0,1]),tensor([1,0]),1)
        markets=UnresolvedMarkets.open(pools,zero)
        resolutions=MarketResolutions.open(pools,markets,ResolutionSources.bind(block.terminal_columns,[a['token_id'] for a in block.assets]))
        columns=EconomicColumns.from_projection_batches([accepted_batch])
        ledger=AdmissionLifecycle.open(balances,projection.normalizer.bank,columns,integers([before['ledger_entry_seq']]),
            integers([before['ledger_local_prefix']]),pools,position_costs=PositionCosts.empty(1,1,'cuda'),
            order_funds=OrderFunds.empty(1,columns.row_refs.size(1),columns.row_refs.size(1),columns.row_refs.size(1),'cuda'),
            rebate_days=RebateDays.empty(tensor([[0]]),2) if with_pending else None,
            unresolved_markets=markets,resolutions=resolutions)
        ordinal=integers(projection.normalizer.bank.source_order[accepted_batch.rows.flatten(),3].cpu().tolist())
        pending=PendingRecords.from_columns(accepted_batch.emitted,accepted_batch.received_at,accepted_batch.private_sequences,ordinal,accepted_batch.rows,integers([0]))
        return PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),ClosedAdmission.empty(columns),ledger)
    public=Numbers.run_exact_batch(assemble)
    gates=ReceiveGates.unset(1).set_receive(tensor([29999]),enabled).set_public(tensor([29999]),enabled)
    public,first=Numbers.run_exact_batch(lambda:public.receive(projection.normalizer.bank,gates,torch.ones_like(public.waiting.received)))
    assert first.fault.cpu().tolist()==[0] and first.closed.emitted.sum().cpu().item()==3
    assert values(public.ledger.current_entry_sequence)==[source['ledger_accepted']['projection']['ledger_entry_seq']]
    old_public=public
    for incoming in [*maker_batches,terminal_batch]:
        public=Numbers.run_exact_batch(lambda:public.append_batch(projection.normalizer.bank,incoming))
    gates=gates.set_receive(tensor([41000]),enabled).set_public(tensor([41000]),enabled)
    public,last=Numbers.run_exact_batch(lambda:public.receive(projection.normalizer.bank,gates,torch.ones_like(public.waiting.received)))
    assert last.fault.cpu().tolist()==[0] and last.closed.emitted.sum().cpu().item()==(4 if with_pending else 3)
    assert not public.waiting.received.logical_and(~public.waiting.consumed).any().cpu().item()
    after=source['ledger_after']['projection']
    assert values(public.ledger.current_entry_sequence)==[after['ledger_entry_seq']]
    assert values(public.ledger.platform.platform_source_prefix)==[after['platform_source_prefix']]
    assert values(public.ledger.current_local_prefix)==[after['ledger_local_prefix']]
    actual_balances=public.ledger.platform.balances
    assert [Fraction(n,d) for n,d in zip(values(actual_balances.numerator),values(actual_balances.denominator))]==[Fraction(after['platform_confirmed_projection'][key]) for key in names]
    occupied=public.ledger.orders.occupied.flatten()
    assert public.ledger.orders.flags.flatten()[occupied].cpu().tolist()==[False]
    assert public.ledger.orders.conditions.flatten()[occupied].cpu().tolist()==[0]
    assert [v for v,p in zip(values(public.ledger.orders.last_sequences),occupied.cpu().tolist()) if p]==[source['ledger_after']['order_lifecycle'][0]['last_ledger_entry_seq']]
    from validate_order_funds import check as check_order_funds
    assert check_order_funds(public.ledger,source['ledger_after'])
    assert check_order_funds(old_public.ledger,source['ledger_accepted'])
    from registry import FrozenIntentOrders
    intents=Numbers.run_exact_batch(lambda:FrozenIntentOrders(enabled[:,None],integers([1]),enabled[:,None],rational([kind['size_shares']]),
        tensor([[False]],torch.bool),enabled[:,None]))
    public_orders=Numbers.run_exact_batch(lambda:public.registry.freeze_orders(intents,tensor([41000])))
    assert public_orders.active_known.cpu().tolist()==[[True]] and public_orders.cancellable.cpu().tolist()==[[False]]
    assert public_orders.commitment_present.cpu().tolist()==[[with_pending]]
    assert not old_public.ledger.resolutions.present.any().cpu().item() and old_public.ledger.orders.flags.any().cpu().item()
    # 原正文/位置/序号逐记录对照；原数据只作oracle，不驱动设备消费。
    for batch,records in [(accepted_batch,source['accepted_records']),*[(b,source['resolved_records']) for b in maker_batches],(terminal_batch,source['resolved_records'])]:
        for column,present in enumerate(batch.emitted.cpu().tolist()[0]):
            if not present:continue
            ordinal=values(batch.record_ordinals)[column]
            original=next(r for r in records if r['position']['record_ordinal']==ordinal and r['position']['source_watermark']['projection_batch_sequence']==values(batch.batch_sequences)[column])
            row=batch.rows[0,column]
            expected=Numbers.run_exact_batch(lambda:expected_semantic(original,token_index=asset))
            semantic=projection.normalizer.bank.semantic[row]
            assert torch.equal(semantic[:expected.size(1)],expected[0]) and not semantic[expected.size(1):].any().cpu().item()
            assert values(batch.batch_sequences)[column]==original['position']['source_watermark']['projection_batch_sequence']
            body=next(iter(original['record'].values()))
            private=body['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
            assert values(batch.private_sequences)[column]==private
    held=public
    public,repeat=Numbers.run_exact_batch(lambda:public.receive(projection.normalizer.bank,gates,torch.ones_like(public.waiting.received)))
    assert not repeat.closed.emitted.any().cpu().item();compare(public.ledger,held.ledger,'repeat_whole_actual_Terminal')
    # 同一Runtime的后继Lifecycle不再生成第二个Resolved，原reference索引共同完成。
    resumed_storage=BaseRuntimeStorage(runtime,runtime)
    resumed=CapturedHiddenBuckets(resumed_storage,horizon,enabled,tensor([0,0]),tensor([[0]*runtime.hidden.queues.capacity]),fee,1,rebate,end,word_capacity=4,feedback_capacity=1)
    resumed.replay();torch.cuda.synchronize()
    assert resumed_storage.runtime.hidden_frontier.closed_at.cpu().tolist()==[31000]
    runtime=resumed_storage.runtime
    if with_pending:
        # 实际已登记fill0仍pending；物理增长后未登记fill1终态不影响旧义务。
        obligations=runtime.resolution_obligations
        from fill_pool import FillPoolTransition, _take_record
        small_pool=replace(runtime.fills,records=_take_record(runtime.fills.records,zero),
            occupied=runtime.fills.occupied[:,:1],sequences=_take(runtime.fills.sequences,zero),
            origins=runtime.fills.origins.take(zero))
        grown_pool=small_pool.resized(2)
        assert grown_pool.capacity==2 and values(grown_pool.sequences)[0]==values(small_pool.sequences)[0]
        probe_event=replace(transition.event,emitted=enabled,terminal=enabled,fault=tensor([0],torch.int32))
        old_one=replace(obligations,pending=obligations.pending[:,:,:1],sequences=_take(obligations.sequences,zero))
        later=FillPoolTransition(integers([2]),tensor([1]),probe_event)
        unchanged=Numbers.run_exact_batch(lambda:old_one.complete(later))
        compare(unchanged,old_one,'new_unregistered_terminal_does_not_alias_old_pending_slot')
        grown=old_one.resized(grown_pool.capacity)
        assert grown.pending.cpu().tolist()==[[[True,False]]]
        assert values(grown.sequences)[0]==values(obligations.sequences)[0]
        assert values(grown.sequences)[1:]==[0]*(grown_pool.capacity-1)
        assert grown.reference_indices.cpu().tolist()==obligations.reference_indices.cpu().tolist()
        maker_rate=Numbers.run_exact_batch(lambda:rational(['0.2']))
        for original in source['finality_reference']:
            horizon.fill_(original['at'])
            choice=runtime.choose(None,None,horizon,enabled)
            assert choice.enabled(2).cpu().tolist()==[True] and choice.at.cpu().tolist()==[original['at']]
            runtime,transition,market=runtime.finality(choice,maker_rate,rebate,end)
            confirmed=original['at']==33000
            assert runtime.resolution_obligations.pending.sum().cpu().item()==int(not confirmed)
            assert (runtime.calendars.maker.market_result.cpu().item()>=0)==confirmed
            assert transition.event.emitted.cpu().tolist()==[True]
            projection,batch=Numbers.run_exact_batch(lambda:projection.fill_from_origin(sampler,runtime.orders,
                runtime.fills,transition,tensor([0,1]),runtime.calendars,rebate,choice.at,maker_results=(market,)))
            assert batch.emitted.sum().cpu().item()==len(original['records'])
            public=Numbers.run_exact_batch(lambda:public.append_batch(projection.normalizer.bank,batch))
            gates=gates.set_receive(tensor([50000]),enabled).set_public(tensor([50000]),enabled)
            public,received=Numbers.run_exact_batch(lambda:public.receive(projection.normalizer.bank,gates,torch.ones_like(public.waiting.received)))
            assert received.fault.cpu().tolist()==[0]
            expected=original['ledger']['projection']
            assert values(public.ledger.current_entry_sequence)==[expected['ledger_entry_seq']]
            assert values(public.ledger.platform.platform_source_prefix)==[expected['platform_source_prefix']]
            balances=public.ledger.platform.balances
            assert [Fraction(n,d) for n,d in zip(values(balances.numerator),values(balances.denominator))]==[Fraction(expected['platform_confirmed_projection'][key]) for key in names]
            result=Numbers.run_exact_batch(lambda:public.ledger.resolutions.read(public.ledger.success_pools))
            assert [Fraction(n,d) for n,d in zip(values(result.winner_orphan_receivable.numerator),values(result.winner_orphan_receivable.denominator))]==[Fraction(original['resolutions'][0]['orphan_receivable'])]
            assert public.ledger.rebate_days.maker_accrual_finalized.cpu().tolist()==[[confirmed]]
    print(json.dumps({'actual_empty_source_Books_to_outer_GTC_new_runtime_bid_checked':True,
        'actual_Terminal_order_close_tag5_release_and_Resolved_same_public_Ledger_checked':True,
        'original_end_clears_FIFO_without_releasing_GTC_then_Terminal_releases_once_checked':True,
        'new_schema18_19_complete_resolution_identity_and_original_NOE_positions_checked':True,
        'old_public_prefix_and_resource_denied_Terminal_preserved_checked':True,
        'actual_records':6+int(with_pending),'pending_actual_fill_population':int(with_pending),
        'actual_Maker_pending_Terminal_then_MINED_CONFIRMED_and_market_result_checked':with_pending,'full_root_and_requested_settlement':False,
        'elapsed_seconds_with_reference_reads':time.perf_counter()-started,'device_peak_allocated_bytes':torch.cuda.max_memory_allocated()}))


if __name__=='__main__':main()
