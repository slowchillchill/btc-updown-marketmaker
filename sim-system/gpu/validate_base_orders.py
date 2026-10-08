"""真实原GTC请求→设备接纳目录→原hidden变化/撤单连续计量对照。

工程源仍提供行情步骤和密封路径；未交全根调度、SDK动态准入或完整响应正文。
"""
from dataclasses import fields,is_dataclass,replace
import json
import os
from pathlib import Path
import sys
import time
import torch
from queues import Numbers, ExactInt, run_queue_batch
from latency import EmpiricalDelaySampler, byte_column
from mutation_requests import MutationRequests
from base_orders import BaseOrders
from pending_base_execution import FrozenBaseTerms, run_pending_base_acceptance
from base_execution import cancel_base_order
from fill_finality import FillRecord
from fill_pool import FillPool, run_fill_pool_batch
from hidden_base_execution import run_hidden_base_transition
from hidden_market import HiddenAssetState,run_hidden_asset_batch
from maker_days import MakerDays
from taker_days import TakerDays
from rebate_coordinator import RebateCoordinator
from scheduling import ScheduledTime,next_sim_drive
from base_runtime import BaseSimRuntime
from validate_base_execution import setup, integers, rational, values, units, assert_checkpoint


_progress_started=time.perf_counter()
_progress_last={}


def progress(stage, **population):
    """当前有限入口的阶段证据；不回读业务条件或记录逐微算子日志。"""
    directory=os.environ.get('B_GPU_ORDER_ARTIFACT_DIR')
    interval=float(os.environ.get('B_GPU_PROGRESS_INTERVAL_SECONDS','10'))
    elapsed=time.perf_counter()-_progress_started
    if directory and interval>0 and (stage not in _progress_last or elapsed-_progress_last[stage]>=interval):
        _progress_last[stage]=elapsed
        with (Path(directory)/'runtime-progress.jsonl').open('a') as target:
            target.write(json.dumps({'stage':stage,'elapsed_seconds':elapsed,
                                     **population})+'\n')


def main(source=None):
    source = json.load(sys.stdin) if source is None else source
    started = time.perf_counter()
    tensor = lambda values, dtype=torch.int64: torch.tensor(values, dtype=dtype, device='cuda')
    active, zero_fault = tensor([True], torch.bool), tensor([0], torch.int32)
    ordinal = source['post_terms']['ordinal']
    original_kind = source['post_terms']['order']['kind']['GtcLimit']
    base = units(original_kind['size_shares'])
    from fractions import Fraction
    quote = Fraction(original_kind['size_shares'])*Fraction(original_kind['limit_price_pusd_per_share'])*1_000_000
    assert quote.denominator == 1
    root = Path(__file__).resolve().parents[2]
    sha = source['proxy_latency_sha256']
    sampler = EmpiricalDelaySampler.build((root/'sim-system/config/latency-profiles'/sha/'profile.json').read_bytes(),
                                         sha, [source['session_seed'].encode('ascii')])
    sys.path.insert(0,str(root/'execution-normalization/gpu'))
    from projection import Projection
    from feedback import FeedbackEmitter
    from validate_projection import expected_semantic
    from consumption import PendingRecords,ReceiveGates
    # 原根开户/静态目录一次安装；订单、请求、角色、原价以后从设备目录取。
    zero=tensor([0])
    noe=Projection.open(FeedbackEmitter.open(1),zero,zero,zero,zero,tensor([0,0]),
        integers([0]),tensor([-1]),tensor([-1]),zero,zero)
    asset_tokens=tensor([0,1])
    batches=[]
    identity_bytes, identity_offsets = byte_column([source['post_request_key'].encode()])
    requests = MutationRequests.empty(integers([0]), capacity=1, action_capacity=1)
    requests, receipt = requests.run_registration(sampler, identity_bytes, identity_offsets,
        tensor([0]), tensor([1]), integers([source['post_input_sequence']]), tensor([source['post_sent_at']]),
        tensor([[0]]), tensor([[ordinal]]), tensor([[True]], torch.bool), tensor([[False]], torch.bool),
        tensor([0]), active, zero_fault)
    assert receipt.arrived_at.cpu().tolist() == [source['processing_at']]
    ask = source['initial'][0]['payload']['event']['asks'][0]
    queue, account, prepared = Numbers.run_exact_batch(lambda: setup(ask, source))
    # 完整原双资产初始簿及本链后来出现的bid价位一次绑定；不从接单余量反推hidden前量。
    prices=['0.2','0.3','0.4','0.4','0.6']
    level_assets=tensor([0,0,0,1,1])
    row_roots=tensor([0]*len(prices))
    hidden_state=Numbers.run_exact_batch(lambda:HiddenAssetState.empty(level_assets,rational(prices),
        tensor([0,0]),1,segment_capacity=2))
    material=source.get('hidden_material')
    for asset,event in enumerate(source['initial'] if material is None else []):
        book=event['payload']['event']
        quantities=['0']*len(prices)
        for name,side_rows in [('bids',[1,2] if asset==0 else [3]),('asks',[0] if asset==0 else [4])]:
            for level in book[name]:
                index=next(index for index in side_rows if prices[index]==level['price_pusd_per_share'])
                quantities[index]=level['quantity_shares']
        shares,tick=Numbers.run_exact_batch(lambda:(rational(quantities),
            rational([book['initial_tick_size_pusd_per_share']]*2)))
        hidden_state,_=run_hidden_asset_batch(hidden_state,lambda state:state.initial_snapshot(
            tensor([asset==0,asset==1],torch.bool),shares,tick,tensor([0,0])))
    queue=hidden_state.queues
    orders = BaseOrders.empty(prepared.quantity, capacity=1)
    initial_time = requests.next_pending().effective_at
    template = FillRecord.create(integers([1]), integers([1]), integers([0]), tensor([0]), initial_time,
        tensor([False], torch.bool), source['finality_path'])
    fills = FillPool.empty(template, capacity=1)
    terms = FrozenBaseTerms(tensor([0]), integers([base]), integers([quote.numerator]),
        rational([original_kind['limit_price_pusd_per_share']]), tensor([0]), tensor([1]), zero_fault,tensor([0]))
    fee_rate, rebate = Numbers.run_exact_batch(lambda: (rational([source['fee_rate']]), rational([source['taker_rebate_rate']])))
    maker_rate = Numbers.run_exact_batch(lambda:rational([source['maker_rebate_rate']]))
    calendars = RebateCoordinator(MakerDays.empty(tensor([[0]]),tensor([[source['market_end']]]),
        tensor([[True]],torch.bool),fills=1,days=1),TakerDays.empty(1,'cuda',fills=1,days=1))
    frontier=None
    block=None
    prepared_hidden=None
    hidden_page_report={}
    if material is not None:
        from hidden_inputs import HiddenPmBlock
        from hidden_frontier import HiddenBucketColumns,HiddenBucketFrontier
        slots=[('1','ASK','0.2'),('1','BID','0.3'),('1','BID','0.4'),('2','BID','0.4'),('2','ASK','0.6')]
        block=HiddenPmBlock(material['assets'],slots,source_binding=material['source_binding'])
        assert all(not value['references'] for value in block.buckets), '该有限源尚未接市场reference处理器'
        assert len(block.buckets)==2, '初始双Book一桶，七条Maker消息同一完整桶'
        if os.environ.get('B_GPU_HIDDEN_ORDER_PAGES')=='1':
            from hidden_device_page import build_hidden_device_columns,PreparedHiddenMaterial
            destination=Path(os.environ['B_GPU_ORDER_ARTIFACT_DIR'])/'hidden-order-columns'
            build_hidden_device_columns(block,destination,material['source_binding'],progress=progress)
            prepared_hidden=PreparedHiddenMaterial(destination,material['source_binding'])
            compare(prepared_hidden.decimals,block.decimals,'order_pages_shared_exact_source')
            # CLI结构字段与独立Rust Value键序不同；仍比较每个完整桶及原数组顺序。
            actual_buckets=[prepared_hidden.banks['buckets'][index] for index in range(len(prepared_hidden.banks['buckets']))]
            assert actual_buckets==material['buckets'], '原完整桶身份、引用、步骤及闭桶字段'
            block=HiddenPmBlock.from_prepared(prepared_hidden.page(0,1))
            hidden_page_report={'nonempty_hidden_pages':[[0,1],[1,2]],
                'nonempty_hidden_columns_bytes':sum(p.stat().st_size for p in destination.iterdir()),
                'nonempty_once_build_uses_same_original_DPT_material':True}
        hidden_state=Numbers.run_exact_batch(lambda:block.empty_state(segment_capacity=2))
        frontier=Numbers.run_exact_batch(lambda:HiddenBucketFrontier.open(HiddenBucketColumns.from_block(block),1))
    runtime=BaseSimRuntime(hidden_state,requests,orders,account,fills,calendars,frontier)
    hidden_source=ScheduledTime(tensor([source['maker_at']]),active,zero_fault)
    hidden_eof=tensor([False],torch.bool)
    horizon=tensor([source['market_end']])
    choose=lambda:runtime.choose(hidden_source,hidden_eof,horizon,active)
    compact_index=0
    if block is not None:
        initial_runtime = runtime
        choice=choose()
        assert choice.enabled(4).cpu().tolist()==[True] and choice.at.cpu().tolist()==[1100]
        runtime=runtime.advance_role_time(choice,tensor([source['market_end']]*2)).begin_hidden_bucket(choice)
        for compact_index in range(len(block.buckets[0]['steps'])):
            runtime,effect=runtime.apply_hidden_step(choose(),compact_index,tensor([0,0]),
                tensor([[0]*runtime.hidden.queues.capacity]),fee_rate,source['fee_exponent'],rebate)
            assert effect is None or not hasattr(effect,'fills') or not effect.fills
        compact_index+=1
        runtime,closed=runtime.finish_hidden_bucket()
        assert closed.cpu().tolist()==[True] and runtime.hidden.initialized.cpu().tolist()==[True,True]
        assert values(runtime.hidden_frontier.index_position)==[block.buckets[0]['bucket']['index_end']]
        from base_runtime_storage import BaseRuntimeStorage,CapturedHiddenBuckets
        initial_storage = BaseRuntimeStorage(initial_runtime, runtime)
        initial_loop = CapturedHiddenBuckets(initial_storage,tensor([1100]),active,tensor([0,0]),
            tensor([[0]*runtime.hidden.queues.capacity]),fee_rate,source['fee_exponent'],rebate,
            tensor([source['market_end']]*2),word_capacity=4,feedback_capacity=1,
            stage_progress=lambda stage:progress('initial_page_'+stage))
        initial_loop.replay()
        torch.cuda.synchronize()
        assert initial_loop.resources_fit.cpu().item() and initial_loop.page.count.cpu().item()==0
        assert initial_loop.iterations.cpu().tolist()==[len(block.buckets[0]['steps'])]
        compare(initial_storage.runtime,runtime,'device_selected_initial_complete_snapshots')
        progress('device_selected_initial_snapshots_checked',actual_steps=len(block.buckets[0]['steps']))
        # 同一原双Book漏一个实际ASK绑定：新ragged图不得部分初始化失败根。
        missing_block = HiddenPmBlock(material['assets'],slots[:-1],
                                      source_binding=material['source_binding'])
        missing_start = replace(initial_runtime,
            hidden=Numbers.run_exact_batch(lambda:missing_block.empty_state(segment_capacity=2)),
            hidden_frontier=Numbers.run_exact_batch(lambda:HiddenBucketFrontier.open(
                HiddenBucketColumns.from_block(missing_block),1)))
        check_initial_gather(missing_start,source,tensor,fee_rate,rebate,'missing_snapshot',True)
        # 实际typedraw双空Book材料、零运行档位；不是给非空源补padding造空。
        empty_material = source['empty_hidden_material']
        empty_block = HiddenPmBlock(empty_material['assets'],[],
                                    source_binding=empty_material['source_binding'])
        empty_start = replace(initial_runtime,
            hidden=Numbers.run_exact_batch(lambda:empty_block.empty_state(segment_capacity=2)),
            hidden_frontier=Numbers.run_exact_batch(lambda:HiddenBucketFrontier.open(
                HiddenBucketColumns.from_block(empty_block),1)))
        check_initial_gather(empty_start,source,tensor,fee_rate,rebate,'empty_snapshot',False)
        queue=runtime.hidden.queues
    if prepared_hidden is not None:
        # 原NextGlobalEventAt许可本请求的严格更早处理，不许可未安装下一桶。
        same_time=replace(runtime,requests=replace(runtime.requests,
            effective_at=torch.full_like(runtime.requests.effective_at,source['maker_at'])))
        waiting_choice=same_time.choose(None,None,tensor([source['maker_at']]),active)
        assert not waiting_choice.present.cpu().item()
        assert not same_time.begin_hidden_bucket(waiting_choice).hidden_frontier.applying.cpu().item()
        hidden_page_report["uninstalled_next_bucket_equal_time_waits_and_strictly_earlier_request_runs"]=True
    choice=choose()
    assert choice.enabled(5).cpu().tolist()==[True] and torch.equal(choice.at,requests.next_pending().effective_at)
    # 源边界未知时不把公开grid当关闭证明；同ms完整hidden仍先于真实pending。
    unavailable=next_sim_drive(requests,fills,calendars,
        replace(hidden_source,present=tensor([False],torch.bool)),hidden_eof,horizon,active)
    assert not unavailable.present.any().cpu().item()
    collision=next_sim_drive(requests,fills,calendars,
        replace(hidden_source,at=requests.next_pending().effective_at),hidden_eof,horizon,active)
    assert collision.enabled(4).cpu().tolist()==[True] and not collision.enabled(5).any().cpu().item()
    held=run_pending_base_acceptance(requests,orders,queue,account,fills,terms,tensor([[0]]),
        fee_rate,source['fee_exponent'],torch.zeros((1,queue.capacity),dtype=torch.int64,device='cuda'),
        rebate,row_roots,tensor([0]),calendars=calendars,enabled=tensor([False],torch.bool))
    assert held[-1].fault.cpu().tolist()==[0] and torch.equal(held[0].done,requests.done)
    assert values(held[3].cash)==values(account.cash) and values(held[1].next_sequence)==[1]
    runtime=runtime.advance_role_time(choice,tensor([source['market_end']]*2))
    runtime,accepted=runtime.accept_base(choice,terms,tensor([[0]]),fee_rate,source['fee_exponent'],
        torch.zeros((1,queue.capacity),dtype=torch.int64,device='cuda'),rebate,row_roots,tensor([0]))
    requests,orders,queue,account,fills,calendars=(runtime.requests,runtime.orders,runtime.hidden.queues,
        runtime.account,runtime.fills,runtime.calendars)
    inactive_role=runtime.advance_role_time(replace(choose(),at=horizon,phase=tensor([2])),
        tensor([source['market_end']]*2))
    assert not inactive_role.hidden.matching_closed.any().cpu().item()
    assert torch.equal(inactive_role.hidden.queues.kind,queue.kind)
    noe,batch=Numbers.run_exact_batch(lambda:noe.pending_base_acceptance(sampler,orders,fills,accepted,asset_tokens))
    batches.append(batch)
    assert accepted.fault.cpu().tolist() == [0] and accepted.order.emitted.cpu().tolist() == [True]
    assert values(accepted.order.sequence) == [1] and accepted.response.emitted.cpu().tolist() == [True]
    assert not requests.next_pending().exists.cpu().item()
    slot = accepted.order.slots
    read = orders.read(slot)
    assert read.request_keys.cpu().tolist() == [0] and read.item_ordinals.cpu().tolist() == [ordinal]
    assert read.item_refs.cpu().tolist() == [0] and read.accepted_at.cpu().tolist() == [source['processing_at']]
    assert_checkpoint(source['checkpoints'][0]['state'], read.quantity, account)
    # 原queue A段保存本根稳定slot。扩目录后不改变已有段中的slot或原完整u64身份。
    assert values(orders.read(slot).sequence) == [1]
    assert fills.origins.roles[0,0].cpu().item() == 0
    assert values(fills.origins.order_sequences) == [1]
    assert values(fills.origins.prices.numerator) == [1] and values(fills.origins.prices.denominator) == [5]
    assert queue.order_ids[1][queue.kind[1] == 2].cpu().tolist() == [int(slot.cpu()[0])]
    if prepared_hidden is not None:
        from base_runtime_storage import BaseRuntimeStorage
        held_page=BaseRuntimeStorage(runtime,runtime).snapshot()
        second_block=HiddenPmBlock.from_prepared(prepared_hidden.page(1,2))
        continued=replace(runtime,hidden_frontier=runtime.hidden_frontier.handoff(
            HiddenBucketColumns.from_block(second_block)))
        for name in ('hidden','requests','orders','account','fills','calendars'):
            compare(getattr(continued,name),getattr(held_page,name),'nonempty_handoff_'+name)
        assert continued.hidden_frontier.columns.source.terminal_columns is block.terminal_columns
        assert continued.hidden_frontier.closed_at.cpu().tolist()==[1100]
        assert continued.hidden_frontier.closed_bucket.cpu().tolist()==[0]
        assert values(continued.orders.read(slot).sequence)==[1]
        assert continued.fills.occupied.cpu().tolist()==[[True]]
        record=continued.fills.records
        assert record.states[record.selected_path,record.ordinal].cpu().tolist()==[1]  # 原MATCHED，尚待终局。
        assert values(continued.fills.sequences)==[1]
        assert held_page.hidden_frontier.columns.source is block
        assert not held_page.hidden_frontier.columns.end_of_input
        runtime=continued
        block=second_block
        hidden_page_report.update(actual_open_GTC_FIFO_request_and_pending_Taker_fill_kept_across_pages=True,
            nonempty_page_first_closure_is_not_EOF=True,nonempty_old_runtime_freeze_preserved=True,
            same_source_directory_Terminal_bank_and_full_u64_fill_identity_kept=True)
    maker_record = None
    cancel_batch=None
    observed_pending_zero_quote = False
    checked_bad_hidden_predecessor=False
    checked_captured_maker=False
    finality_page=None
    if block is not None:
        runtime,finality_page=check_captured_finalities(runtime,source,tensor,maker_rate,rebate,slot)
        orders,fills,account,calendars=runtime.orders,runtime.fills,runtime.account,runtime.calendars
    taker_index=0
    hidden_page=None
    for checkpoint in source['checkpoints'][1:]:
        stage = checkpoint['stage']
        if stage in ('taker_finality', 'maker_finality'):
            if stage=='taker_finality' and finality_page is not None:
                feedback=finality_page.read(taker_index)
                taker_index+=1
                transition,market,at=feedback.transition,feedback.market,feedback.at
                assert at.cpu().tolist()==[checkpoint['at']]
                bound=noe.bind_base_order(runtime.orders.read(transition.origin.order_slots),asset_tokens,
                    transition.event.emitted)
                noe,batch=Numbers.run_exact_batch(lambda:bound.finality(sampler,feedback.record,transition,
                    transition.origin.prices,rebate,at,maker_results=(market,),
                    liquidity_role=transition.origin.roles))
                batches.append(batch)
                # 页中两次实际效果已逐项对照；单槽退出另核MINED账户，完整循环核CONFIRMED账户。
                continue
            choice=choose()
            at=choice.at
            assert choice.enabled(2).cpu().tolist()==[True] and at.cpu().tolist()==[checkpoint['at']]
            runtime,transition,market=runtime.finality(choice,maker_rate,rebate,tensor([source['market_end']]*2))
            orders,fills,account,calendars=runtime.orders,runtime.fills,runtime.account,runtime.calendars
            noe,batch=Numbers.run_exact_batch(lambda:noe.fill_from_origin(sampler,orders,fills,transition,
                asset_tokens,calendars,rebate,at,maker_results=(market,)))
            batches.append(batch)
            assert transition.origin.roles.cpu().tolist() == [int(stage=='maker_finality')]
        elif stage == 'hidden_step':
            if block is not None:
                if hidden_page is None:
                    runtime,hidden_page=check_captured_hidden_bucket(runtime,source,tensor,fee_rate,
                        source['fee_exponent'],rebate,slot)
                    hidden_state,orders,queue,account,fills,calendars=(runtime.hidden,runtime.orders,
                        runtime.hidden.queues,runtime.account,runtime.fills,runtime.calendars)
                    hidden_source=replace(hidden_source,present=tensor([False],torch.bool))
                    hidden_eof=active
                    observed_pending_zero_quote=True  # 原reference中间前缀已核零quote pending。
                    checked_captured_maker=True
                    for number in range(int(hidden_page.count.cpu().item())):
                        feedback=hidden_page.read(number)
                        for maker_transition in feedback.effects.fills:
                            noe,batch=Numbers.run_exact_batch(lambda:noe.fill_from_origin(sampler,orders,fills,
                                maker_transition,asset_tokens,calendars,rebate,feedback.at))
                            batches.append(batch)
                            if maker_transition.event.emitted.cpu().item():
                                assert maker_record is None
                                maker_record=maker_transition
                                assert values(maker_record.fill_sequence)==[2]
                                assert maker_record.origin.roles.cpu().tolist()==[1]
                continue
            choice=choose()
            assert choice.enabled(4).cpu().tolist()==[True] and choice.at.cpu().tolist()==[source['maker_at']]
            event = source['events'][checkpoint['ordinal']]['payload']['event']
            bid = event['side'] == 'BID'
            before = Numbers.run_exact_batch(lambda:rational([event['quantity_before_shares']]))
            after = Numbers.run_exact_batch(lambda:rational([event['quantity_after_shares']]))
            source_price = Numbers.run_exact_batch(lambda:rational([event['price_pusd_per_share']]))
            hidden_state=runtime.hidden
            if not checked_bad_hidden_predecessor:
                wrong_before=Numbers.run_exact_batch(lambda:before+rational(['0.000001']))
                bad=run_hidden_base_transition(hidden_state,orders,account,fills,calendars,
                    tensor([1 if bid else 0]),wrong_before,after,source_price,tensor([0]),
                    tensor([source['maker_at']]),active,active,tensor([[0]*queue.capacity]),row_roots,
                    fee_rate,source['fee_exponent'],rebate)
                assert bad[-1].fault.cpu().tolist()==[1]
                assert torch.equal(bad[0].queues.kind,queue.kind)
                assert values(bad[0].external_shares.numerator)==values(hidden_state.external_shares.numerator)
                assert values(bad[1].read(slot).quantity.open_base_units)==values(orders.read(slot).quantity.open_base_units)
                assert values(bad[2].cash)==values(account.cash) and values(bad[3].next_sequence)==values(fills.next_sequence)
                assert not any(frame.event.emitted.any().cpu().item() for frame in bad[-1].fills)
                checked_bad_hidden_predecessor=True
            runtime=runtime.advance_role_time(choice,tensor([source['market_end']]*2))
            runtime,hidden=runtime.hidden_base(choice,tensor([1 if bid else 0]),before,after,source_price,
                tensor([0]),active,tensor([[0]*queue.capacity]),row_roots,fee_rate,source['fee_exponent'],rebate)
            hidden_state,orders,queue,account,fills,calendars=(runtime.hidden,runtime.orders,runtime.hidden.queues,
                runtime.account,runtime.fills,runtime.calendars)
            if checkpoint['ordinal']==len(source['events'])-1:
                hidden_source=replace(hidden_source,present=tensor([False],torch.bool))
                hidden_eof=active
            assert hidden.fault.cpu().tolist() == [0]
            read = orders.read(slot)
            if values(read.quantity.pending_maker_depletion_base_units)[0] > 0:
                observed_pending_zero_quote = True
            for maker_transition in hidden.fills:
                noe,batch=Numbers.run_exact_batch(lambda:noe.fill_from_origin(sampler,orders,fills,maker_transition,
                    asset_tokens,calendars,rebate,tensor([source['maker_at']])))
                batches.append(batch)
                if maker_transition.event.emitted.cpu().item():
                    assert maker_record is None
                    maker_record = maker_transition
                    assert values(maker_transition.fill_sequence) == [2]
                    assert maker_transition.origin.roles.cpu().tolist() == [1]
        elif stage == 'canceled':
            if source.get('live_query') is not None:
                from validate_sim_order_query import advance_live
                runtime,noe,query_batch,live_query_resources=advance_live(source,runtime,noe,sampler,asset_tokens)
                batches.append(query_batch)
                requests=runtime.requests
            read = orders.read(slot)
            ids,offsets=byte_column([source['post_request_key'].encode(),source['cancel_request_key'].encode()])
            requests,cancel_receipt=requests.run_registration(sampler,ids,offsets,tensor([1]),tensor([1]),
                integers([source['cancel_input_sequence']]),tensor([source['cancel_sent_at']]),
                tensor([[1]]),tensor([[0]]),tensor([[True]],torch.bool),tensor([[False]],torch.bool),
                tensor([0]),active,zero_fault)
            assert cancel_receipt.arrived_at.cpu().tolist()==[source['cancel_arrived_at']]
            from pending_cancel_execution import BaseCancelTarget
            target=BaseCancelTarget(tensor([1]),read.sequence,zero_fault)
            runtime=replace(runtime,requests=requests)
            choice=choose()
            assert choice.enabled(5).cpu().tolist()==[True] and choice.at.cpu().tolist()==[source['cancel_arrived_at']]
            runtime=runtime.advance_role_time(choice,tensor([source['market_end']]*2))
            runtime,canceled=runtime.cancel_base(choice,target,row_roots,tensor([1]))
            requests,orders,queue,account=(runtime.requests,runtime.orders,runtime.hidden.queues,runtime.account)
            assert canceled.delta.emitted.cpu().tolist()==[True] and canceled.response.emitted.cpu().tolist()==[True]
            from projection import CancelTargetFeedback
            from fields import RecordList
            refs=RecordList((zero,zero),active[:,None])
            bound=noe.bind_base_order(canceled.order,asset_tokens,canceled.delta.emitted)
            bound=replace(bound,request_slots=canceled.response.request_key,item_ordinals=canceled.pending.ordinal)
            feedback=CancelTargetFeedback(canceled.order_sequence,integers([0]),refs,canceled.outcome,
                canceled.processed,canceled.delta,canceled.fault)
            noe,cancel_batch=Numbers.run_exact_batch(lambda:bound.cancellation(sampler,(feedback,),
                canceled.pending.effective_at,canceled.response.received_at,canceled.response.method,canceled.response.emitted))
        assert_checkpoint(checkpoint['state'], orders.read(slot).quantity, account)
        assert orders.read(slot).last_updated_at.cpu().tolist()==[checkpoint['state']['last_updated_at']],stage
    assert observed_pending_zero_quote and maker_record is not None
    assert values(fills.next_sequence) == [3] and values(orders.next_sequence) == [2]
    assert fills.origins.roles.cpu().tolist() == [[0,1]]
    assert values(fills.origins.order_sequences) == [1,1]
    assert values(fills.origins.prices.numerator) == [1,3] and values(fills.origins.prices.denominator) == [5,10]
    assert calendars.maker.market_pending.cpu().tolist() == [[0]]
    assert calendars.maker.terminal_consumed.cpu().tolist() == [[True]]
    assert not calendars.taker.pending.any().cpu().item()
    expected={(record['position']['source_watermark']['projection_batch_sequence'],record['position']['record_ordinal']):record
        for record in source['prior_records']}
    by_row={}
    from consumption import PendingRecords,ReceiveGates
    pending=None
    for batch in batches:
        assert batch.fault.cpu().tolist()==[0]
        for column,wanted in enumerate(batch.emitted.cpu().tolist()[0]):
            if not wanted:continue
            key=values(batch.batch_sequences)[column],values(batch.record_ordinals)[column]
            original=expected[key]
            body=next(iter(original['record'].values()))
            source_ref=body.get('source_reference') or body['fact']['source_reference']
            category,metadata=next(iter(source_ref['record'].items()))
            assert values(batch.private_sequences)[column]==metadata['private_feedback_sequence']
            received_name='response_received_at_utc_ms' if category=='DirectResponse' else 'feedback_received_at_utc_ms'
            assert int(batch.received_at[0,column].cpu())==metadata[received_name]
            row=int(batch.rows[0,column].cpu())
            if body.get('evidence',{}).get('Control',{}).get('result',{}).get('OrderQuery') is not None:
                from validate_sim_order_query import semantic_from_reference
                semantic=Numbers.run_exact_batch(lambda:semantic_from_reference(original))
            else:
                semantic=Numbers.run_exact_batch(lambda:expected_semantic(original))
            actual=noe.normalizer.bank.semantic[row:row+1]
            width=max(actual.size(1),semantic.size(1))
            assert torch.equal(torch.nn.functional.pad(actual,(0,width-actual.size(1))),
                torch.nn.functional.pad(semantic,(0,width-semantic.size(1)))),(key,'实际动态来源正文')
            assert int(noe.normalizer.bank.source_order[row,3].cpu())==body['source_order']['ordinal']
            by_row[row]=original
        if pending is None:
            from queues import _take
            pending=PendingRecords.from_columns(batch.emitted,batch.received_at,batch.private_sequences,
                Numbers.ExactInt.from_i64_tensor(noe.normalizer.bank.source_order[batch.rows.flatten(),3]),batch.rows,integers([0]))
        else:
            pending=Numbers.run_exact_batch(lambda:pending.append_batch(noe.normalizer.bank,batch))
    assert len(by_row)==len(expected)
    closed=tensor([max(int(record['visibility']['Feedback']['available_at_utc_ms']) for record in source['prior_records'])])
    gates=ReceiveGates.unset(1).set_receive(closed,active).set_public(closed,active)
    pending,delivery=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    delivered=[by_row[row]['position'] for row,present in zip(delivery.record_refs.cpu().tolist()[0],delivery.emitted.cpu().tolist()[0]) if present]
    assert delivered==[record['position'] for record in source['prior_records']]
    _,repeat=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert not repeat.emitted.any().cpu().item()
    from validate_base_noe_ledger import check as check_public_account
    account_report=check_public_account(source,noe,batches,orders.read(slot),receipt,
        Numbers.run_exact_batch(lambda:rational([original_kind['size_shares']])),cancel_batch,
        return_state=source.get('order_queries') is not None)
    if source.get('order_queries') is not None:
        from validate_sim_order_query import check
        query_report=check(source,runtime,noe,sampler,asset_tokens,account_report,live_query_resources)
        print(json.dumps(query_report),flush=True)
        return
    # 已完成请求无pending：调度padding零terms/故障不得污染既存状态。
    padding = FrozenBaseTerms(tensor([-1]), integers([0]), integers([0]), rational(['0']),
                              tensor([-1]), tensor([-1]), tensor([1], torch.int32),tensor([-1]))
    old_requests, old_orders, old_queue, old_account, old_fills = requests, orders, queue, account, fills
    requests, orders, queue, account, fills, idle = run_pending_base_acceptance(requests, orders, queue,
        account, fills, padding, tensor([[-1]]), fee_rate, source['fee_exponent'],
        torch.zeros((1,queue.capacity), dtype=torch.int64, device='cuda'), rebate, row_roots, tensor([-1]),
        calendars=calendars)
    calendars = idle.calendars
    assert idle.fault.cpu().tolist() == [0]
    assert not idle.acceptance.submission_delta.emitted.cpu().item() and not idle.response.emitted.cpu().item()
    for actual, previous in ((account.cash, old_account.cash), (account.next_sequence, old_account.next_sequence),
                             (orders.next_sequence, old_orders.next_sequence), (fills.next_sequence, old_fills.next_sequence)):
        assert values(actual) == values(previous)
    assert torch.equal(requests.done, old_requests.done) and torch.equal(queue.kind, old_queue.kind)
    # 受控新工程请求复用同原SDK terms：只有订单目录满，队列已有8槽余量。
    # 不改变原13个checkpoint或把此新增请求称原会话自然输入。
    identity_bytes, identity_offsets = byte_column([source['post_request_key'].encode(),source['cancel_request_key'].encode(), b'gpu-next-engineering-post'])
    requests, _ = requests.run_registration(sampler, identity_bytes, identity_offsets, tensor([2]), tensor([1]),
        integers([13]), tensor([source['cancel_arrived_at']+1]), tensor([[1]]), tensor([[ordinal]]),
        tensor([[True]], torch.bool), tensor([[False]], torch.bool), tensor([0]), active, zero_fault)
    assert orders.capacity == 1 and orders.occupied.sum().cpu().item() == 1
    queue = queue.resized(8)
    old_quantity = orders.read(slot).quantity
    next_terms = replace(terms, item_refs=tensor([1]))
    requests, orders, queue, account, fills, next_effect = run_pending_base_acceptance(requests, orders, queue,
        account, fills, next_terms, tensor([[0]]), fee_rate, source['fee_exponent'],
        torch.zeros((1,8), dtype=torch.int64, device='cuda'), rebate, row_roots, tensor([1]),calendars=calendars)
    assert next_effect.fault.cpu().tolist() == [0] and next_effect.order.emitted.cpu().tolist() == [True]
    assert orders.capacity == 2 and queue.capacity == 8
    assert values(orders.next_sequence) == [3] and values(fills.next_sequence) == [3]
    from dataclasses import fields
    for field in fields(old_quantity):
        if isinstance(getattr(old_quantity,field.name),torch.Tensor):
            assert torch.equal(getattr(orders.read(slot).quantity,field.name),getattr(old_quantity,field.name))
        else:
            assert values(getattr(orders.read(slot).quantity, field.name)) == values(getattr(old_quantity, field.name))
    assert values(orders.read(next_effect.order.slots).sequence) == [2]
    orders = orders.resized(4)
    assert values(orders.read(slot).sequence) == [1]
    assert queue.order_ids[1][queue.kind[1] == 2].cpu().tolist() == [1]
    # 前述真实新增工程GTC仍开放；纯Maker end义务必须清段而不释放订单/占款。
    ending=BaseSimRuntime(replace(runtime.hidden,queues=queue),requests,orders,account,fills,calendars)
    end_choice=ending.choose(hidden_source,hidden_eof,horizon,active)
    assert end_choice.enabled(3).cpu().tolist()==[True] and end_choice.at.cpu().tolist()==[source['market_end']]
    open_before=orders.read(next_effect.order.slots).quantity.open_base_units
    principal_before=account.principal
    ending,end_frame=ending.rebate_clock(end_choice,tensor([source['market_end']]*2))
    assert end_frame.consumed.cpu().tolist()==[True]
    assert ending.hidden.matching_closed.cpu().tolist()==[True,True]
    assert ending.hidden.book_closed.cpu().tolist()==[True,True]
    assert not ending.hidden.queues.kind.any().cpu().item()
    assert values(ending.orders.read(next_effect.order.slots).quantity.open_base_units)==values(open_before)
    assert values(ending.account.principal)==values(principal_before)
    torch.cuda.synchronize()
    print(json.dumps({'device_request_registration_to_real_GTC_acceptance_checked': True,
        'directory_routes_actual_hidden_maker_depletion_and_cancel_checked': True,
        'zero_quote_pending_keeps_original_fill_population_checked': True,
        'order_u64_identity_and_queue_slot_survive_growth_checked': True,
        'completed_request_inactive_zero_terms_no_effect_or_fault_checked': True,
        'controlled_order_only_growth_keeps_eight_queue_segments_checked': True,
        'actual_order_role_and_source_price_bound_to_each_fill_checked':True,
        'same_fill_pool_finality_uses_actual_role_and_calendar_predecessors_checked':True,
        'single_original_hidden_transition_queue_order_fill_calendar_common_commit_checked':True,
        'original_full_initial_books_and_exact_hidden_predecessor_joined_once_checked':True,
        'bad_hidden_predecessor_rolls_back_book_queue_order_account_fill_checked':checked_bad_hidden_predecessor,
        'continuous_directory_bound_order_taker_maker_NOE_payload_and_positions_checked':True,
        'dynamic_NOE_records':len(by_row),
        'device_phase_gate_keeps_future_pending_without_submission_checked':True,
        'next_SIM_drive_from_actual_obligation_sources_controls_phase_masks_checked':True,
        'controlled_same_time_hidden_before_pending_and_unknown_source_pause_checked':True,
        'single_SIM_runtime_owns_hidden_queue_account_requests_orders_and_obligations_checked':True,
        'pure_market_end_obligation_closes_matching_but_preserves_open_order_encumbrance_checked':True,
        'verified_typedraw_initial_and_complete_seven_message_maker_bucket_checked':block is not None,
        'complete_maker_bucket_frontier_keeps_original_index_until_all_steps_commit_checked':block is not None,
        'compact_hidden_source_identities_are_new_DPT_identities_not_old_constructed_V3':block is not None,
        'actual_positive_maker_joint_candidate_CUDA_graph_replay_to_same_NOE_checked':checked_captured_maker,
        'actual_positive_maker_fixed_address_commit_then_consumed_step_replay_checked':checked_captured_maker,
        'actual_positive_maker_pool_shortage_keeps_owned_state_before_cold_growth_checked':checked_captured_maker,
        'controlled_global_resource_gate_blocks_successful_Maker_candidate_and_feedback_checked':checked_captured_maker,
        'actual_Maker_native_CUDA_WHILE_uses_device_cursor_and_skips_consumed_step_checked':checked_captured_maker,
        'native_CUDA_WHILE_pool_shortage_and_initial_zero_iteration_checked':checked_captured_maker,
        'actual_Taker_MINED_CONFIRMED_two_native_iterations_keep_both_feedbacks_checked':finality_page is not None,
        'actual_finality_feedback_page_full_preserves_MINED_prefix_and_retries_checked':finality_page is not None,
        'actual_complete_Maker_bucket_native_source_cursor_and_feedback_page_checked':hidden_page is not None,
        'actual_fixed_type_device_step_and_snapshot_gather_checked':material is not None,
        'actual_fixed_type_missing_ASK_snapshot_preserves_root_and_cursor_checked':material is not None,
        'actual_fixed_type_typedraw_both_empty_Books_zero_levels_checked':material is not None,
        'runtime_hidden_reference_handler_covered':False,
        'public_ledger':account_report,
        'original_SIM_checkpoints': len(source['checkpoints']),
        'elapsed_seconds_with_reference_reads': time.perf_counter()-started,
        'device_peak_allocated_bytes': torch.cuda.max_memory_allocated(), **hidden_page_report,
        'global_clock_SDK_admission_and_full_response_body_delivered': False}))


def compare(a,b,path="candidate"):
    if isinstance(a,torch.Tensor):
        assert torch.equal(a,b),(path,a.cpu().tolist(),b.cpu().tolist())
    elif isinstance(a,ExactInt):
        assert values(a)==values(b) and torch.equal(a.fault,b.fault),(path,values(a),values(b),a.fault.cpu().tolist(),b.fault.cpu().tolist())
    elif is_dataclass(a):
        for field in fields(a):
            # 真实源引用为同一个冻结对象；不把Python对象绑定当设备业务状态。
            if field.name not in ('source','columns'):
                compare(getattr(a,field.name),getattr(b,field.name),path+"."+field.name)
    elif isinstance(a,(tuple,list)):
        assert len(a)==len(b)
        for number,(left,right) in enumerate(zip(a,b)):compare(left,right,path+"["+str(number)+"]")
    elif a is None or isinstance(a,(str,int,bool)):
        assert a==b

def check_captured_finalities(runtime,source,tensor,maker_rate,taker_rate,slot):
    """真实MINED/CONFIRMED：图内由设备选择两次，反馈页保留两个原人口。

    原逐步纯候选仅用于同源差分；实际执行不以Rust checkpoint决定每次body。
    一槽页用同源MINED前缀验证物理退出，再以新页继续未消费义务。
    """
    from base_runtime_storage import BaseRuntimeStorage,CapturedFinalitySteps,FinalityFeedback
    from fill_pool import _take_record
    progress('capture_actual_Taker_finalities')
    horizon=tensor([source['market_end']])
    enabled=tensor([True],torch.bool)
    end=tensor([source['market_end']]*2)
    reference=runtime
    frames=[]
    prefixes=[]
    checkpoints=[value for value in source['checkpoints'] if value['stage']=='taker_finality']
    assert len(checkpoints)==2
    for checkpoint in checkpoints:
        choice=reference.choose(None,None,horizon,enabled)
        assert choice.enabled(2).cpu().tolist()==[True] and choice.at.cpu().tolist()==[checkpoint['at']]
        reference,transition,market=reference.finality(choice,maker_rate,taker_rate,end)
        assert reference.orders.read(slot).last_updated_at.cpu().tolist()==[checkpoint['state']['last_updated_at']]
        index=transition.slot.clamp(0,reference.fills.capacity-1)
        frames.append(FinalityFeedback(choice.at,transition,market,_take_record(reference.fills.records,index)))
        prefixes.append(reference)
    storage=BaseRuntimeStorage(runtime,reference)
    loop=CapturedFinalitySteps(storage,horizon,enabled,maker_rate,taker_rate,end,
        word_capacity=4,feedback_capacity=2)
    loop.replay()
    torch.cuda.synchronize()
    assert loop.resources_fit.cpu().item() and loop.page.count.cpu().item()==2
    compare(storage.runtime,reference,'two_finality_iterations')
    for index,frame in enumerate(frames):
        compare(loop.page.read(index),frame,'finality_page_'+str(index))
    assert_checkpoint(checkpoints[-1]['state'],storage.runtime.orders.read(slot).quantity,storage.runtime.account)
    # 新资源页仅一槽：第一笔真实提交保留，第二笔只报页需求，不能发布候选效果。
    short_storage=BaseRuntimeStorage(runtime,reference)
    short=CapturedFinalitySteps(short_storage,horizon,enabled,maker_rate,taker_rate,end,
        word_capacity=4,feedback_capacity=1)
    short.replay()
    torch.cuda.synchronize()
    assert not short.resources_fit.cpu().item() and short.required_feedback.cpu().item()==2
    assert short.page.count.cpu().item()==1 and not short.output.transition.event.emitted.cpu().item()
    compare(short_storage.runtime,prefixes[0],'page_full_preserves_MINED')
    compare(short.page.read(0),frames[0],'first_feedback_preserved')
    assert_checkpoint(checkpoints[0]['state'],short_storage.runtime.orders.read(slot).quantity,short_storage.runtime.account)
    # 冷消费/换页后从同一已提交MINED状态继续；不重发第一笔、不回滚先前成功。
    retry=CapturedFinalitySteps(short_storage,horizon,enabled,maker_rate,taker_rate,end,
        word_capacity=4,feedback_capacity=2)
    retry.replay()
    torch.cuda.synchronize()
    assert retry.resources_fit.cpu().item() and retry.page.count.cpu().item()==1
    compare(short_storage.runtime,reference,'new_page_continues_CONFIRMED')
    compare(retry.page.read(0),frames[1],'retry_feedback')
    frozen=storage.snapshot()
    loop.replay()
    torch.cuda.synchronize()
    compare(storage.runtime,frozen,'finished_finality_no_reapply')
    assert loop.page.count.cpu().item()==2 and loop.required_feedback.cpu().item()==0
    for index,frame in enumerate(frames):
        compare(loop.page.read(index),frame,'immutable_feedback_'+str(index))
    progress('actual_Taker_finality_pages_checked',actual_iterations=2)
    return frozen,loop.page


def check_initial_gather(original,source,tensor,fee_rate,rebate,label,expect_fault):
    from base_runtime_storage import BaseRuntimeStorage,CapturedHiddenBuckets
    block=original.hidden_frontier.columns.source
    horizon=tensor([1100])
    enabled=tensor([True],torch.bool)
    ends=tensor([source['market_end']]*2)
    reference=original
    choice=reference.choose(None,None,horizon,enabled)
    reference=reference.advance_role_time(choice,ends).begin_hidden_bucket(choice)
    for index in range(len(block.buckets[0]['steps'])):
        choice=reference.choose(None,None,horizon,enabled)
        reference,_=reference.apply_hidden_step(choice,index,tensor([0,0]),
            tensor([[0]*reference.hidden.queues.capacity]),fee_rate,source['fee_exponent'],rebate)
    reference,closed=reference.finish_hidden_bucket()
    assert reference.hidden.fault.any().cpu().item()==expect_fault
    assert closed.cpu().tolist()==[not expect_fault]
    storage=BaseRuntimeStorage(original,reference)
    loop=CapturedHiddenBuckets(storage,horizon,enabled,tensor([0,0]),
        tensor([[0]*original.hidden.queues.capacity]),fee_rate,source['fee_exponent'],rebate,ends,
        word_capacity=4,feedback_capacity=1,stage_progress=lambda stage:progress(label+'_'+stage))
    loop.replay()
    torch.cuda.synchronize()
    assert loop.resources_fit.cpu().item() and loop.page.count.cpu().item()==0
    assert loop.closed.cpu().tolist()==[not expect_fault]
    compare(storage.runtime,reference,label+'_selected_ragged_complete_state')
    if not expect_fault:
        assert storage.runtime.hidden.initialized.cpu().tolist()==[True,True]
        assert storage.runtime.hidden.queues.rows==0
        assert loop.iterations.cpu().tolist()==[len(block.buckets[0]['steps'])]
    progress(label+'_ragged_checked')


def check_captured_hidden_bucket(runtime,source,tensor,fee_rate,fee_exponent,rebate,slot):
    """原完整七消息桶；实际设备游标消费原冷类型序列，不以checkpoint择步。"""
    from base_runtime_storage import BaseRuntimeStorage,CapturedHiddenBuckets,HiddenStepFeedback
    block=runtime.hidden_frontier.columns.source
    progress('original_Maker_bucket_reference',source_steps=len(block.operations))
    horizon=tensor([source['market_end']])
    enabled=tensor([True],torch.bool)
    end=tensor([source['market_end']]*2)
    reference=runtime
    choice=reference.choose(None,None,horizon,enabled)
    assert choice.enabled(4).cpu().tolist()==[True] and choice.at.cpu().tolist()==[source['maker_at']]
    reference=reference.advance_role_time(choice,end).begin_hidden_bucket(choice)
    first=0 if getattr(block,'prepared_page',None) is not None else len(block.buckets[0]['steps'])
    frames=[]
    before_positive=None
    needs={}
    checkpoint_index=0
    checkpoints=[value for value in source['checkpoints'] if value['stage']=='hidden_step']
    saw_pending=False
    for index in range(first,len(block.operations)):
        choice=reference.choose(None,None,horizon,enabled)
        if block.operations[index]['tag']=='LevelChange' and checkpoint_index==len(checkpoints)-1:
            before_positive=reference
            reference,effect=check_captured_hidden_step(reference,index,tensor,fee_rate,fee_exponent,rebate)
        else:
            reference,effect=reference.apply_hidden_step(choice,index,tensor([0,0]),
                tensor([[0]*reference.hidden.queues.capacity]),fee_rate,fee_exponent,rebate)
        if block.operations[index]['tag']=='LevelChange':
            for name in ('required_segments','required_records','required_maker_fills','required_maker_days'):
                value=getattr(effect,name)
                needs[name]=value if name not in needs else torch.maximum(needs[name],value)
            checkpoint=checkpoints[checkpoint_index]
            event=source['events'][checkpoint['ordinal']]['payload']['event']
            transition=block.operations[index]['fields']['transition']
            for name in ('quantity_before_shares','quantity_after_shares','price_pusd_per_share'):
                assert transition[name]==event[name]
            assert_checkpoint(checkpoint['state'],reference.orders.read(slot).quantity,reference.account)
            checkpoint_index+=1
            saw_pending |= values(reference.orders.read(slot).quantity.pending_maker_depletion_base_units)[0]>0
            if any(frame.event.emitted.cpu().item() for frame in effect.fills):
                frames.append(HiddenStepFeedback(choice.at,tensor([index]),effect))
        if index<len(block.operations)-1:
            _,closed=reference.finish_hidden_bucket()
            assert not closed.cpu().item()
    reference,closed=reference.finish_hidden_bucket()
    assert closed.cpu().tolist()==[True] and saw_pending and len(frames)==1
    # 仅安装参考所需物理容量，完整源/量/顺序仍交真实候选处理。
    runtime=replace(runtime,hidden=replace(runtime.hidden,queues=runtime.hidden.queues.resized(
        reference.hidden.queues.capacity)),fills=runtime.fills.resized(reference.fills.capacity),
        calendars=replace(runtime.calendars,maker=runtime.calendars.maker.resized(
            fills=reference.calendars.maker.fill_occupied.size(1),days=reference.calendars.maker.occupied.size(1))))
    storage=BaseRuntimeStorage(runtime,reference)
    progress('capture_complete_hidden_page',physical_source_steps=len(block.operations))
    loop=CapturedHiddenBuckets(storage,horizon,enabled,tensor([0,0]),
        tensor([[0]*runtime.hidden.queues.capacity]),fee_rate,fee_exponent,rebate,end,
        word_capacity=4,feedback_capacity=1,stage_progress=lambda stage:progress('hidden_page_'+stage))
    loop.replay()
    progress('native_complete_bucket_launched')
    torch.cuda.synchronize()
    assert loop.resources_fit.cpu().item() and loop.closed.cpu().tolist()==[True]
    assert loop.page.count.cpu().item()==1
    assert loop.iterations.cpu().tolist()==[len(block.operations)-first]
    compare(storage.runtime,reference,'complete_original_Maker_bucket')
    compare(loop.page.read(0),frames[0],'whole_bucket_actual_positive_quote')
    for name,expected in needs.items():
        assert torch.equal(getattr(loop,name),expected),(name,getattr(loop,name).cpu().tolist(),expected.cpu().tolist())
    assert values(storage.runtime.hidden_frontier.index_position)==[block.buckets[-1]['bucket']['index_end']]
    assert storage.runtime.hidden_frontier.scheduled()[1].cpu().tolist()==[True]
    assert_checkpoint(checkpoints[-1]['state'],storage.runtime.orders.read(slot).quantity,storage.runtime.account)
    frozen=storage.snapshot()
    loop.replay()
    torch.cuda.synchronize()
    compare(storage.runtime,frozen,'closed_bucket_no_reapply')
    assert loop.resources_fit.cpu().item() and loop.required_words.cpu().item()==0
    assert loop.page.count.cpu().item()==1 and not loop.closed.cpu().item()
    for name in needs:
        assert not getattr(loop,name).any().cpu().item(),name
    compare(loop.page.read(0),frames[0],'whole_bucket_old_feedback')
    # 原fill池1不足以创建fill2：新整页对象必须给出具体池需求，并保已成功步骤。
    # runtime在冷容量准备前的原填满池人口由before_positive保留；目录仍为1槽。
    assert before_positive.fills.capacity==1
    blocked_runtime=replace(runtime,fills=before_positive.fills)
    blocked_storage=BaseRuntimeStorage(blocked_runtime,blocked_runtime)
    progress('capture_hidden_page_actual_fill_shortage')
    blocked=CapturedHiddenBuckets(blocked_storage,horizon,enabled,tensor([0,0]),
        tensor([[0]*runtime.hidden.queues.capacity]),fee_rate,fee_exponent,rebate,end,
        word_capacity=4,feedback_capacity=1,stage_progress=lambda stage:progress('shortage_page_'+stage))
    blocked.replay()
    torch.cuda.synchronize()
    assert not blocked.resources_fit.cpu().item() and blocked.required_records.cpu().tolist()==[2]
    assert blocked.page.count.cpu().item()==0 and not blocked.closed.cpu().item()
    assert blocked.iterations.cpu().tolist()==[len(block.operations)-first-1]
    compare(blocked_storage.runtime,before_positive,'full_bucket_shortage_preserves_accepted_prefix')
    # 按设备需求只增长fill目录，其余状态来自该对象已提交前缀。
    expanded=replace(blocked_storage.snapshot(),fills=blocked_storage.runtime.fills.resized(
        int(blocked.required_records.max().cpu().item())))
    retry_storage=BaseRuntimeStorage(expanded,reference)
    progress('capture_hidden_page_after_required_growth')
    retry=CapturedHiddenBuckets(retry_storage,horizon,enabled,tensor([0,0]),
        tensor([[0]*runtime.hidden.queues.capacity]),fee_rate,fee_exponent,rebate,end,
        word_capacity=4,feedback_capacity=1,stage_progress=lambda stage:progress('retry_page_'+stage))
    retry.replay()
    torch.cuda.synchronize()
    assert retry.resources_fit.cpu().item() and retry.closed.cpu().item() and retry.page.count.cpu().item()==1
    assert retry.iterations.cpu().tolist()==[1]
    compare(retry_storage.runtime,reference,'required_growth_completes_same_bucket')
    compare(retry.page.read(0),frames[0],'growth_retry_actual_feedback')
    progress('hidden_page_pool_demand_and_retry_checked',required_fill_records=2)
    progress('complete_original_Maker_bucket_checked',original_messages=len(source['events']))
    return frozen,loop.page


def check_captured_hidden_step(runtime,index,tensor,fee_rate,fee_exponent,rebate):
    """同一真实正Maker源步：物理预热/扩容在冷边界，候选和需求留设备。

    只证明当前固定shape源类型的capture/replay及活动地址提交。不据此声明
    WHILE、页追加或GPU业务分派已完成；后续NOE实际消费这次graph候选。
    """
    from dataclasses import fields,is_dataclass
    # 原容量重试生成参考候选，但不安装它；只取其物理容量增长原状态。
    horizon=tensor([runtime.hidden_frontier.columns.source.operations[index]['market']['window']['end_at_utc_ms']])
    enabled=tensor([True],torch.bool)
    conditions=tensor([0,0])
    def compute(value):
        choice=value.choose(None,None,horizon,enabled)
        paths=torch.zeros((value.orders.roots,value.hidden.queues.capacity),dtype=torch.int64,device=horizon.device)
        return value.apply_hidden_step(choice,index,conditions,paths,fee_rate,fee_exponent,rebate)
    original_runtime=runtime
    expected,_=compute(runtime)
    runtime=replace(runtime,
        hidden=replace(runtime.hidden,queues=runtime.hidden.queues.resized(expected.hidden.queues.capacity)),
        fills=runtime.fills.resized(expected.fills.capacity),
        calendars=replace(runtime.calendars,maker=runtime.calendars.maker.resized(
            fills=expected.calendars.maker.fill_occupied.size(1),days=expected.calendars.maker.occupied.size(1))))
    expected,effect=compute(runtime)
    compute_candidate=lambda:Numbers.device_exact_batch(lambda:compute(runtime),word_capacity=4,
        device=runtime.account.cash.words.device)
    warm=compute_candidate()
    assert warm.fits.cpu().item() and warm.value[1].fault.cpu().tolist()==[0]
    torch.cuda.synchronize()
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        captured=compute_candidate()
    graph.replay()
    torch.cuda.synchronize()
    assert captured.fits.cpu().item(), '仅资源退出边界可读此需求，不提交不足容量候选'
    actual,actual_effect=captured.value
    compare(warm.value[0],expected,"warm")
    compare(actual,expected,"captured")
    compare(actual_effect,effect,"effects")
    assert any(frame.event.emitted.cpu().item() for frame in actual_effect.fills)
    # 再次replay同一未提交原输入，纯候选应一致，不消耗订单/fill号或RNG。
    graph.replay()
    torch.cuda.synchronize()
    compare(actual,expected)
    from base_runtime_storage import BaseRuntimeStorage,CapturedHiddenBaseStep
    # 同一真实正Maker在尚未扩fill池的原状态上仅生成容量请求；全字段不提交。
    blocked=Numbers.device_exact_batch(lambda:compute(original_runtime),word_capacity=4,
        device=runtime.account.cash.words.device)
    blocked_storage=BaseRuntimeStorage(original_runtime,blocked.value[0])
    blocked_paths=torch.zeros((blocked_storage.runtime.orders.roots,blocked_storage.runtime.hidden.queues.capacity),dtype=torch.int64,device=horizon.device)
    blocked_loop=CapturedHiddenBaseStep(blocked_storage,index,horizon,enabled,conditions,blocked_paths,
        fee_rate,fee_exponent,rebate,word_capacity=4)
    blocked_loop.replay()
    torch.cuda.synchronize()
    assert not blocked_loop.resources_fit.cpu().item() and blocked_loop.output.required_records.cpu().tolist()==[2]
    compare(blocked_storage.runtime,original_runtime,"pool_shortage_keeps_input")
    # 受控全物理批门：本根的实际候选已成功，另处资源不足仍不允许它发布。
    suppressed_storage=BaseRuntimeStorage(runtime,expected)
    suppressed_graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(suppressed_graph):
        successful=Numbers.device_exact_batch(lambda:compute(suppressed_storage.runtime),word_capacity=4,
            device=runtime.account.cash.words.device)
        suppressed=suppressed_storage.commit(successful.value[0],torch.zeros_like(successful.fits),successful.value[1])
    suppressed_graph.replay()
    torch.cuda.synchronize()
    assert any(frame.event.emitted.cpu().item() for frame in successful.value[1].fills)
    compare(suppressed_storage.runtime,runtime,"global_resource_gate_keeps_input")
    for frame in suppressed.fills:
        assert not frame.event.emitted.cpu().item() and not frame.event.terminal.cpu().item()
        for name in ('economic_delta','failure_delta','rebate_delta'):
            assert not getattr(frame.event,name).emitted.cpu().item()
    storage=BaseRuntimeStorage(runtime,expected)
    old=storage.snapshot()
    paths=torch.zeros((storage.runtime.orders.roots,storage.runtime.hidden.queues.capacity),dtype=torch.int64,device=horizon.device)
    loop=CapturedHiddenBaseStep(storage,index,horizon,enabled,conditions,paths,
        fee_rate,fee_exponent,rebate,word_capacity=4)
    loop.replay()
    torch.cuda.synchronize()
    assert loop.resources_fit.cpu().item()
    compare(storage.runtime,expected,"committed")
    compare(loop.output,effect,"committed_effects")
    frozen=storage.snapshot()
    frozen_effects=storage.snapshot_output(loop.output)
    # 已提交游标继续作为同一图的输入；同原step已消费，不再生成fill/消耗序号。
    loop.replay()
    torch.cuda.synchronize()
    compare(storage.runtime,frozen,"consumed_step")
    assert not any(frame.event.emitted.cpu().item() for frame in loop.output.fills)
    compare(old,runtime,"old_snapshot")
    compare(frozen,expected,"frozen_after_commit")
    compare(frozen_effects,effect,"frozen_committed_effects")
    # 构造时该真实步骤已经消费，首次launch即零迭代，不能读捕获池未写入输出。
    idle_storage=BaseRuntimeStorage(frozen,frozen)
    idle_loop=CapturedHiddenBaseStep(idle_storage,index,horizon,enabled,conditions,paths,
        fee_rate,fee_exponent,rebate,word_capacity=4)
    idle_loop.replay()
    torch.cuda.synchronize()
    compare(idle_storage.runtime,frozen,"first_launch_zero_iteration")
    assert idle_loop.candidate.required_words.cpu().item()==0
    assert idle_loop.candidate.fits.cpu().item() and idle_loop.resources_fit.cpu().item()
    assert idle_loop.output.fault.cpu().tolist()==[0]
    assert idle_loop.output.required_records.cpu().tolist()==[0]
    assert not any(frame.event.emitted.cpu().item() for frame in idle_loop.output.fills)
    return frozen,frozen_effects


if __name__ == '__main__':
    main()
