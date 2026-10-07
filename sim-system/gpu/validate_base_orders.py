"""真实原GTC请求→设备接纳目录→原hidden变化/撤单连续计量对照。

工程源仍提供行情步骤和密封路径；未交全根调度、SDK动态准入或完整响应正文。
"""
from dataclasses import replace
import json
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
from validate_base_execution import setup, integers, rational, values, units, assert_checkpoint


def main():
    source = json.load(sys.stdin)
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
    identity_bytes, identity_offsets = byte_column([source['post_request_key'].encode()])
    requests = MutationRequests.empty(integers([0]), capacity=1, action_capacity=1)
    requests, receipt = requests.run_registration(sampler, identity_bytes, identity_offsets,
        tensor([0]), tensor([1]), integers([source['post_input_sequence']]), tensor([source['post_sent_at']]),
        tensor([[0]]), tensor([[ordinal]]), tensor([[True]], torch.bool), tensor([[False]], torch.bool),
        tensor([0]), active, zero_fault)
    assert receipt.arrived_at.cpu().tolist() == [source['processing_at']]
    ask = source['initial'][0]['payload']['event']['asks'][0]
    queue, account, prepared = Numbers.run_exact_batch(lambda: setup(ask, source))
    orders = BaseOrders.empty(prepared.quantity, capacity=1)
    initial_time = requests.next_pending().effective_at
    template = FillRecord.create(integers([1]), integers([1]), integers([0]), tensor([0]), initial_time,
        tensor([False], torch.bool), source['finality_path'])
    fills = FillPool.empty(template, capacity=1)
    terms = FrozenBaseTerms(tensor([0]), integers([base]), integers([quote.numerator]),
        rational([original_kind['limit_price_pusd_per_share']]), tensor([0]), tensor([1]), zero_fault)
    fee_rate, rebate = Numbers.run_exact_batch(lambda: (rational([source['fee_rate']]), rational([source['taker_rebate_rate']])))
    requests, orders, queue, account, fills, accepted = run_pending_base_acceptance(requests, orders, queue,
        account, fills, terms, tensor([[0]]), fee_rate, source['fee_exponent'],
        torch.zeros((1,queue.capacity), dtype=torch.int64, device='cuda'), rebate, tensor([0, 0]), tensor([0]))
    assert accepted.fault.cpu().tolist() == [0] and accepted.order.emitted.cpu().tolist() == [True]
    assert values(accepted.order.sequence) == [1] and accepted.response.emitted.cpu().tolist() == [True]
    assert not requests.next_pending().exists.cpu().item()
    slot = accepted.order.slots
    read = orders.read(slot)
    assert read.request_keys.cpu().tolist() == [0] and read.item_ordinals.cpu().tolist() == [ordinal]
    assert read.item_refs.cpu().tolist() == [0] and read.accepted_at.cpu().tolist() == [source['processing_at']]
    assert_checkpoint(source['checkpoints'][0]['state'], read.quantity, account)
    # 原queue A段保存本根稳定slot。扩目录后不改变已有段中的slot或原完整u64身份。
    orders = orders.resized(4)
    assert values(orders.read(slot).sequence) == [1]
    assert queue.order_ids[1][queue.kind[1] == 2].cpu().tolist() == [int(slot.cpu()[0])]
    maker_record = None
    observed_pending_zero_quote = False
    for checkpoint in source['checkpoints'][1:]:
        stage = checkpoint['stage']
        if stage in ('taker_finality', 'maker_finality'):
            at = tensor([checkpoint['at']])
            fills, account, _ = Numbers.run_exact_batch(lambda: fills.advance_one(account, at, rebate))
        elif stage == 'hidden_step':
            event = source['events'][checkpoint['ordinal']]['payload']['event']
            bid = event['side'] == 'BID'
            before = integers([0, units(event['quantity_before_shares'])] if bid else [units(event['quantity_before_shares']), 0])
            after = integers([0, units(event['quantity_after_shares'])] if bid else [units(event['quantity_after_shares']), 0])
            queue, effect = run_queue_batch(queue, lambda state: state.apply_level_transition(before, after))
            orders, makers = Numbers.run_exact_batch(lambda: orders.reduce_maker(effect, tensor([0, 0])))
            assert makers.fault.cpu().tolist() == [0]
            read = orders.read(slot)
            index = slot  # 单根；运行协调器保留root×slot无损映射。
            from queues import _take
            emitted = makers.emitted[:, slot[0]].reshape(1)
            if emitted.cpu().item():
                assert maker_record is None
                maker_record = fills.new_record(_take(makers.base, index), _take(makers.quote, index), integers([0]),
                    read.assets, tensor([source['maker_at']]), emitted, tensor([0]))
                fills, _ = run_fill_pool_batch(fills, lambda candidate: candidate.enqueue(maker_record))
                fills, account, _ = Numbers.run_exact_batch(lambda: fills.advance_one(account, maker_record.matched_at, rebate))
            elif values(read.quantity.pending_maker_depletion_base_units)[0] > 0:
                observed_pending_zero_quote = True
        elif stage == 'canceled':
            read = orders.read(slot)
            queue, quantity, account, _, _, _, delta = Numbers.run_exact_batch(lambda: cancel_base_order(queue,
                read.quantity, account, tensor([-1, int(slot.cpu()[0])]), tensor([0, 0]),
                tensor([False], torch.bool), active))
            orders = Numbers.run_exact_batch(lambda: orders.update(read, quantity, delta.emitted))
            assert delta.emitted.cpu().tolist() == [True]
        assert_checkpoint(checkpoint['state'], orders.read(slot).quantity, account)
    assert observed_pending_zero_quote and maker_record is not None
    assert values(fills.next_sequence) == [3] and values(orders.next_sequence) == [2]
    # 已完成请求无pending：调度padding零terms/故障不得污染既存状态。
    padding = FrozenBaseTerms(tensor([-1]), integers([0]), integers([0]), rational(['0']),
                              tensor([-1]), tensor([-1]), tensor([1], torch.int32))
    old_requests, old_orders, old_queue, old_account, old_fills = requests, orders, queue, account, fills
    requests, orders, queue, account, fills, idle = run_pending_base_acceptance(requests, orders, queue,
        account, fills, padding, tensor([[-1]]), fee_rate, source['fee_exponent'],
        torch.zeros((1,queue.capacity), dtype=torch.int64, device='cuda'), rebate, tensor([0, 0]), tensor([-1]))
    assert idle.fault.cpu().tolist() == [0]
    assert not idle.acceptance.submission_delta.emitted.cpu().item() and not idle.response.emitted.cpu().item()
    for actual, previous in ((account.cash, old_account.cash), (account.next_sequence, old_account.next_sequence),
                             (orders.next_sequence, old_orders.next_sequence), (fills.next_sequence, old_fills.next_sequence)):
        assert values(actual) == values(previous)
    assert torch.equal(requests.done, old_requests.done) and torch.equal(queue.kind, old_queue.kind)
    torch.cuda.synchronize()
    print(json.dumps({'device_request_registration_to_real_GTC_acceptance_checked': True,
        'directory_routes_actual_hidden_maker_depletion_and_cancel_checked': True,
        'zero_quote_pending_keeps_original_fill_population_checked': True,
        'order_u64_identity_and_queue_slot_survive_growth_checked': True,
        'completed_request_inactive_zero_terms_no_effect_or_fault_checked': True,
        'original_SIM_checkpoints': len(source['checkpoints']),
        'elapsed_seconds_with_reference_reads': time.perf_counter()-started,
        'device_peak_allocated_bytes': torch.cuda.max_memory_allocated(),
        'global_clock_SDK_admission_and_full_response_body_delivered': False}))


if __name__ == '__main__':
    main()
