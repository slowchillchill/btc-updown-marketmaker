"""原DPT恢复/原SIM行情与FIFO逐步骤的独立短链对照。所有计算在AutoDL。"""
import json
from pathlib import Path
import sys
import time
import torch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from hidden_inputs import HiddenPmBlock, QUALITY
from queues import Numbers
from queues import ExactInt, run_queue_batch
from validate_queues import describe, values


def shares(value):
    numerator, denominator = values(value.numerator), values(value.denominator)
    from fractions import Fraction
    return [Fraction(n, d) for n, d in zip(numerator, denominator)]


def main():
    original = json.load(sys.stdin)
    started = time.perf_counter()
    block = HiddenPmBlock(original['assets'], original['slots'], source_binding=original['source_binding'])
    state = Numbers.run_exact_batch(lambda: block.empty_state(segment_capacity=1))
    from fractions import Fraction
    for index, expected in enumerate(original['expected']):
        event = block.operations[index]
        # 本工程单市场短链按实际已接纳桶时刻推进；生产宿主必须给出其实际处理时刻。
        # 冷构造HiddenPmBlock本身没有调用该时钟，也不能把预取当作apply。
        state = state.advance_matching_time(torch.full_like(state.quality, event['source_time']),
                                            torch.full_like(state.quality, event['market']['window']['end_at_utc_ms']))
        state, effects = block.apply(state, index)
        if index == original['insertion_index']:
            from dataclasses import replace
            account_level = (block.level_assets == block.operations[index]['asset']) & (block.level_sides == 0)
            amount = ExactInt.from_i64_tensor(account_level.to(torch.int64) * 3_000_000)
            ids = torch.where(account_level, 7, -1)
            queue, _ = run_queue_batch(state.queues, lambda value: value.append_account(ids, amount))
            state = replace(state, queues=queue)
        actual_maker = []
        if effects is not None:
            amounts = values(effects.maker_base_units)
            orders = effects.order_ids.cpu().flatten().tolist()
            for order, amount in zip(orders, amounts):
                if amount > 0:
                    assert order == 7
                    actual_maker.append({'order': 'account-order', 'base': str(amount)})
        assert actual_maker == expected['maker'], (index, actual_maker, expected['maker'])
        assert not state.fault.any().cpu().item(), (index, block.operations[index]['tag'], state.fault.cpu().tolist())
        assert state.initialized.cpu().tolist() == [asset['initialized'] for asset in expected['assets']]
        assert state.quality.cpu().tolist() == [QUALITY[asset['quality']] for asset in expected['assets']]
        tick = shares(state.tick)
        for number, asset in enumerate(expected['assets']):
            assert bool(state.tick_present[number].cpu()) == (asset['tick'] is not None)
            if asset['tick'] is not None:
                assert tick[number] == Fraction(asset['tick'])
        actual_shares = shares(state.external_shares)
        assert actual_shares == [Fraction(value) for value in expected['shares']], {
            'index':index,'tag':event['tag'],'time':event['source_time'],'end':event['market']['window']['end_at_utc_ms'],
            'closed':state.matching_closed.cpu().tolist(),'actual':[str(value) for value in actual_shares],
            'expected':expected['shares']}
        assert describe(state.queues) == expected['queues'], index
    # 细于1e-6源数量未量化掉；只有原SIM队列计量才转micro units。
    precise = [Fraction(value) for point in original['expected'] for value in point['shares']]
    assert Fraction('20.0000008') in precise
    empty_asset = next(entry['asset'] for entry in block.operations
                       if entry['tag'] == 'InitialSnapshot' and not entry['levels'])
    empty_slots = [number for number,(token,_,_) in enumerate(original['slots'])
                   if token == original['assets'][empty_asset]['token_id']]
    assert any(point['assets'][empty_asset]['initialized'] and all(Fraction(point['shares'][slot]) == 0 for slot in empty_slots)
               for point in original['expected'])
    assert all(not bucket['references'] for bucket in original['buckets']), '本短链不替代市场参考/终态处理器'
    assert state.matching_closed.cpu().tolist() == [True] * len(original['assets'])
    assert any(point['maker'] == [{'order': 'account-order', 'base': '2000000'}] for point in original['expected'])
    assert all(not any(isinstance(value, torch.Tensor) for value in entry.values()) for entry in block.operations)
    assert block.event_indices.shape == (len(block.operations), 14)
    check_runtime_buckets(block, original)
    # 调用方旧裸桶的修改不再进入生产入口；实际来源载荷校验由严格reader承担。
    assert block.buckets == original['buckets']
    torch.cuda.synchronize()
    print(json.dumps({'checks': ['compact_independent_hidden_source', 'original_sim_exact_asset_predecessor',
        'uninitialized_distinct_from_initialized_empty', 'fine_source_quantity_preserved',
        'original_fifo_all_segments', 'epoch_required_resync_and_recovery', 'end_closes_matching_before_level',
        'maker_depletion_returned_to_consumer','payload_bound_to_actual_material','shared_event_indices_without_N_times_L'],
        'buckets': len(block.buckets), 'steps': len(block.operations), 'elapsed_seconds': time.perf_counter()-started,
        'device_peak_allocated_bytes': torch.cuda.max_memory_allocated(),
        'verified_full_bucket_drives_SIM_runtime_and_only_complete_bucket_closes_frontier_checked': True,
        'prefetch_does_not_close_runtime_and_last_real_closure_controls_EOF_checked': True,
        'original_snapshot_missing_one_bound_level_rolls_back_assets_queue_and_frontier_checked': True,
        'runtime_bucket_maker_order_or_reference_handler_covered': False,
        'full_request_noe_ledger_clock_delivered': False}))


def check_runtime_buckets(block, original):
    """相同已验收原桶驱动持续SIM；无订单，逐资产/前量对照原独立归约。

    上面的原A3/FIFO effects检查保持；这里不把未登记的测试A段冒充Base订单。
    固定源步骤发射和工程上界由冷夹具提供，不声称全根设备循环已交付。
    """
    from account_state import PlatformAccountState
    from order_quantity import BaseOrderQuantity
    from base_orders import BaseOrders
    from fill_finality import FillRecord
    from fill_pool import FillPool
    from mutation_requests import MutationRequests
    from taker_days import TakerDays
    from maker_days import MakerDays
    from rebate_coordinator import RebateCoordinator
    from base_runtime import BaseSimRuntime
    from hidden_frontier import HiddenBucketColumns, HiddenBucketFrontier
    from validate_base_execution import integers, rational
    tensor=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device='cuda')
    one=tensor([True],torch.bool)
    end=block.operations[0]['market']['window']['end_at_utc_ms']
    end_assets=tensor([end]*len(block.assets))
    columns=HiddenBucketColumns.from_block(block)
    def open_runtime(source_block=block):
        zero=integers([0])
        quantity=BaseOrderQuantity.from_taker_plan(integers([1]),integers([1]),zero,zero)
        template=FillRecord.create(integers([1]),integers([1]),zero,tensor([0]),tensor([0]),
            tensor([False],torch.bool),{'steps':[{'state':'MATCHED','offset_ms':0},
                {'state':'MINED','offset_ms':10},{'state':'CONFIRMED','offset_ms':20}],
                'economic_application_ordinal':1})
        return BaseSimRuntime(source_block.empty_state(segment_capacity=1),MutationRequests.empty(zero),
            BaseOrders.empty(quantity),PlatformAccountState.open(integers([10_000_000]),tensor([0]*len(block.assets))),
            FillPool.empty(template),RebateCoordinator(
                MakerDays.empty(tensor([[0]]),tensor([[end]]),tensor([[True]],torch.bool)),TakerDays.empty(1,'cuda')),
            HiddenBucketFrontier.open(columns if source_block is block else HiddenBucketColumns.from_block(source_block),1))
    runtime=Numbers.run_exact_batch(open_runtime)
    assert runtime.hidden_frontier.has_closed.cpu().tolist()==[False]
    assert values(runtime.hidden_frontier.index_position)==[0]
    step_index=0
    for bucket_index,bucket in enumerate(block.buckets):
        frontier=runtime.hidden_frontier
        source,eof=frontier.scheduled()
        choice=runtime.choose(source,eof,tensor([bucket['bucket']['bucket_time_utc_ms']]),one)
        # 本源原市场end钟在最后同刻hidden桶前真实消费；非Rebate阶段调用不消费。
        runtime,_=runtime.rebate_clock(choice,end_assets)
        source,eof=runtime.hidden_frontier.scheduled()
        choice=runtime.choose(source,eof,tensor([bucket['bucket']['bucket_time_utc_ms']]),one)
        assert choice.enabled(4).cpu().tolist()==[True]
        runtime=runtime.advance_role_time(choice,end_assets).begin_hidden_bucket(choice)
        probe,closed=runtime.finish_hidden_bucket()
        assert closed.cpu().tolist()==[False]
        assert torch.equal(probe.hidden_frontier.next_bucket,runtime.hidden_frontier.next_bucket)
        for _ in bucket['steps']:
            source,eof=runtime.hidden_frontier.scheduled()
            choice=runtime.choose(source,eof,tensor([bucket['bucket']['bucket_time_utc_ms']]),one)
            paths=torch.zeros((1,runtime.hidden.queues.capacity),dtype=torch.int64,device='cuda')
            runtime,_=runtime.apply_hidden_step(choice,step_index,tensor([0]*len(block.assets)),paths,
                rational(['0.07']),1,rational(['0.08']))
            expected=original['expected'][step_index]
            assert not runtime.hidden.fault.any().cpu().item(),(step_index,runtime.hidden.fault)
            assert runtime.hidden.initialized.cpu().tolist()==[v['initialized'] for v in expected['assets']]
            assert runtime.hidden.quality.cpu().tolist()==[QUALITY[v['quality']] for v in expected['assets']]
            from fractions import Fraction
            assert shares(runtime.hidden.external_shares)==[Fraction(v) for v in expected['shares']]
            assert runtime.hidden_frontier.next_bucket.cpu().tolist()==[bucket_index]
            assert values(runtime.hidden_frontier.index_position)==[bucket['bucket']['index_start']]
            step_index+=1
        runtime,closed=runtime.finish_hidden_bucket()
        assert closed.cpu().tolist()==[True]
        assert runtime.hidden_frontier.closed_at.cpu().tolist()==[bucket['bucket']['bucket_time_utc_ms']]
        assert values(runtime.hidden_frontier.index_position)==[bucket['bucket']['index_end']]
        source,eof=runtime.hidden_frontier.scheduled()
        assert eof.cpu().tolist()==[bucket_index+1==len(block.buckets)]
    assert step_index==len(block.operations)
    assert runtime.hidden.matching_closed.cpu().tolist()==[True]*len(block.assets)
    # 原真实快照有bid/ask；漏掉其中一个物理价位绑定，不得部分安装快照。
    snapshot_index=next(i for i,v in enumerate(block.operations) if v['tag']=='InitialSnapshot' and len(v['levels'])>1)
    snapshot=block.operations[snapshot_index]
    asset_token=block.assets[snapshot['asset']]['token_id']
    missing_slot=next(slot for slot in block.slots if slot[0]==asset_token and slot[1]=='ASK')
    damaged=HiddenPmBlock(original['assets'],[v for v in block.slots if v != missing_slot],
                          source_binding=original['source_binding'])
    failed=Numbers.run_exact_batch(lambda:open_runtime(damaged))
    source,eof=failed.hidden_frontier.scheduled()
    horizon=source.at
    choice=failed.choose(source,eof,horizon,one)
    failed=failed.begin_hidden_bucket(choice)
    for index in range(snapshot_index):
        source,eof=failed.hidden_frontier.scheduled()
        choice=failed.choose(source,eof,horizon,one)
        paths=torch.zeros((1,failed.hidden.queues.capacity),dtype=torch.int64,device='cuda')
        failed,_=failed.apply_hidden_step(choice,index,tensor([0]*len(block.assets)),paths,
            rational(['0.07']),1,rational(['0.08']))
    prior=failed
    source,eof=failed.hidden_frontier.scheduled()
    choice=failed.choose(source,eof,horizon,one)
    paths=torch.zeros((1,failed.hidden.queues.capacity),dtype=torch.int64,device='cuda')
    failed,_=failed.apply_hidden_step(choice,snapshot_index,tensor([0]*len(block.assets)),paths,
        rational(['0.07']),1,rational(['0.08']))
    assert failed.hidden.fault.any().cpu().item()
    assert torch.equal(failed.hidden.initialized,prior.hidden.initialized)
    assert torch.equal(failed.hidden.quality,prior.hidden.quality)
    assert torch.equal(failed.hidden.tick_present,prior.hidden.tick_present)
    assert torch.equal(failed.hidden.matching_closed,prior.hidden.matching_closed)
    assert torch.equal(failed.hidden.book_closed,prior.hidden.book_closed)
    assert shares(failed.hidden.tick)==shares(prior.hidden.tick)
    assert shares(failed.hidden.external_shares)==shares(prior.hidden.external_shares)
    old_queue=prior.hidden.queues.resized(failed.hidden.queues.capacity)
    assert torch.equal(failed.hidden.queues.kind,old_queue.kind)
    assert torch.equal(failed.hidden.queues.order_ids,old_queue.order_ids)
    for name in ('total_base','total_quote','consumed_base','account_remaining','debt'):
        assert values(getattr(failed.hidden.queues,name))==values(getattr(old_queue,name))
    assert torch.equal(failed.hidden_frontier.next_step,prior.hidden_frontier.next_step)
    failed,closed=failed.finish_hidden_bucket()
    assert closed.cpu().tolist()==[False] and values(failed.hidden_frontier.index_position)==[0]


if __name__ == '__main__':
    main()
