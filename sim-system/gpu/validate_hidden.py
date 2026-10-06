"""原DPT恢复/原SIM行情与FIFO逐步骤的独立短链对照。所有计算在AutoDL。"""
import json
from pathlib import Path
import sys
import time
import torch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from hidden_inputs import HiddenPmBlock, QUALITY
from queues import Numbers
from validate_queues import describe, values


def shares(value):
    numerator, denominator = values(value.numerator), values(value.denominator)
    from fractions import Fraction
    return [Fraction(n, d) for n, d in zip(numerator, denominator)]


def main():
    original = json.load(sys.stdin)
    started = time.perf_counter()
    block = HiddenPmBlock(original['buckets'], original['assets'], original['slots'])
    state = Numbers.run_exact_batch(lambda: block.empty_state(segment_capacity=1))
    from fractions import Fraction
    for index, expected in enumerate(original['expected']):
        event = block.operations[index]
        # 本工程单市场短链按实际已接纳桶时刻推进；生产宿主必须给出其实际处理时刻。
        # 冷构造HiddenPmBlock本身没有调用该时钟，也不能把预取当作apply。
        state = state.advance_matching_time(torch.full_like(state.quality, event['source_time']),
                                            torch.full_like(state.quality, event['market']['window']['end_at_utc_ms']))
        state = block.apply(state, index)
        assert not state.fault.any().cpu().item(), (index, block.operations[index]['tag'], state.fault.cpu().tolist())
        assert state.initialized.cpu().tolist() == [asset['initialized'] for asset in expected['assets']]
        assert state.quality.cpu().tolist() == [QUALITY[asset['quality']] for asset in expected['assets']]
        tick = shares(state.tick)
        for number, asset in enumerate(expected['assets']):
            assert bool(state.tick_present[number].cpu()) == (asset['tick'] is not None)
            if asset['tick'] is not None:
                assert tick[number] == Fraction(asset['tick'])
        assert shares(state.external_shares) == [Fraction(value) for value in expected['shares']], index
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
    torch.cuda.synchronize()
    print(json.dumps({'checks': ['compact_independent_hidden_source', 'original_sim_exact_asset_predecessor',
        'uninitialized_distinct_from_initialized_empty', 'fine_source_quantity_preserved',
        'original_fifo_all_segments', 'epoch_required_resync_and_recovery', 'end_closes_matching_before_level'],
        'buckets': len(block.buckets), 'steps': len(block.operations), 'elapsed_seconds': time.perf_counter()-started,
        'device_peak_allocated_bytes': torch.cuda.max_memory_allocated(),
        'full_request_noe_ledger_clock_delivered': False}))


if __name__ == '__main__':
    main()
