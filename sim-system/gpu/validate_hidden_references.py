"""原紧凑TWAP桶的设备引用消费；MarketFact仍等待所属真实市场处理器。"""
import json
import sys
import time
import torch
from account_state import PlatformAccountState
from base_orders import BaseOrders
from base_runtime import BaseSimRuntime
from base_runtime_storage import BaseRuntimeStorage, CapturedHiddenBuckets
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
    initial = Numbers.run_exact_batch(open_runtime)
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
    for name in ('hidden', 'requests', 'orders', 'account', 'fills', 'calendars'):
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
    assert block.reference_tags[frontier.next_reference].cpu().tolist() == [2]
    assert torch.equal(frontier.next_reference, before.hidden_frontier.next_reference)
    assert frontier.closed_at.cpu().tolist() == [22000] and frontier.applying.cpu().tolist() == [True]
    assert loop.closed.cpu().tolist() == [False] and loop.reference_iterations.cpu().tolist() == [0]
    assert frontier.fault.cpu().tolist() == [0] and loop.page.count.cpu().item() == 0
    held = storage.snapshot()
    loop.replay()
    torch.cuda.synchronize()
    compare(storage.runtime, held, 'unknown_market_handler_waits_without_consuming')
    progress('actual_terminal_MarketFact_keeps_reference_cursor')
    print(json.dumps({'actual_typedraw_TWAP_references':2, 'actual_bucket_times':[21000,22000,31000],
        'pure_reference_bucket_closed_without_SIM_business_effects':True,
        'missing_source_time_did_not_fabricate_bucket':True,
        'same_graph_reference_then_actual_empty_Books_checked':True,
        'actual_MarketFact_waiting_preserves_cursor_and_closed_frontier':True,
        'first_disabled_reference_entry_and_old_freeze_checked':True,
        'market_fact_handler_complete':False, 'full_unified_host_clock_complete':False,
        'elapsed_seconds':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated()}))


if __name__ == '__main__':
    main()
