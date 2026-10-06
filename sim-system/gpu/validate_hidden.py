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
    block = HiddenPmBlock(original['buckets'], original['assets'], original['slots'], source_binding=original['source_binding'])
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
    import copy
    damaged = copy.deepcopy(original['buckets'])
    initial = next(step for bucket in damaged for _, step in bucket['steps'] if 'InitialSnapshot' in step['change'] and step['change']['InitialSnapshot']['bids'])
    initial['change']['InitialSnapshot']['bids'][0]['quantity_shares'] = '999'
    try:
        HiddenPmBlock(damaged, original['assets'], original['slots'], source_binding=original['source_binding'])
    except ValueError as error:
        assert '不一致' in str(error)
    else:
        raise AssertionError('保留ID改变载荷不得进入设备状态')
    torch.cuda.synchronize()
    print(json.dumps({'checks': ['compact_independent_hidden_source', 'original_sim_exact_asset_predecessor',
        'uninitialized_distinct_from_initialized_empty', 'fine_source_quantity_preserved',
        'original_fifo_all_segments', 'epoch_required_resync_and_recovery', 'end_closes_matching_before_level',
        'maker_depletion_returned_to_consumer','payload_bound_to_actual_material','shared_event_indices_without_N_times_L'],
        'buckets': len(block.buckets), 'steps': len(block.operations), 'elapsed_seconds': time.perf_counter()-started,
        'device_peak_allocated_bytes': torch.cuda.max_memory_allocated(),
        'full_request_noe_ledger_clock_delivered': False}))


if __name__ == '__main__':
    main()
