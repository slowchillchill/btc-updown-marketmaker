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
from hidden_base_execution import run_base_level_transition
from maker_days import MakerDays
from taker_days import TakerDays
from rebate_coordinator import RebateCoordinator
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
        rational([original_kind['limit_price_pusd_per_share']]), tensor([0]), tensor([1]), zero_fault,tensor([0]))
    fee_rate, rebate = Numbers.run_exact_batch(lambda: (rational([source['fee_rate']]), rational([source['taker_rebate_rate']])))
    maker_rate = Numbers.run_exact_batch(lambda:rational([source['maker_rebate_rate']]))
    calendars = RebateCoordinator(MakerDays.empty(tensor([[0]]),tensor([[source['market_end']]]),
        tensor([[True]],torch.bool),fills=1,days=1),TakerDays.empty(1,'cuda',fills=1,days=1))
    requests, orders, queue, account, fills, accepted = run_pending_base_acceptance(requests, orders, queue,
        account, fills, terms, tensor([[0]]), fee_rate, source['fee_exponent'],
        torch.zeros((1,queue.capacity), dtype=torch.int64, device='cuda'), rebate, tensor([0, 0]), tensor([0]),
        calendars=calendars)
    calendars = accepted.calendars
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
    maker_record = None
    observed_pending_zero_quote = False
    for checkpoint in source['checkpoints'][1:]:
        stage = checkpoint['stage']
        if stage in ('taker_finality', 'maker_finality'):
            at = tensor([checkpoint['at']])
            fills, account, maker, taker, transition, _ = Numbers.run_exact_batch(lambda:
                fills.advance_mixed(account,calendars.maker,calendars.taker,at,maker_rate,rebate))
            calendars = RebateCoordinator(maker,taker)
            assert transition.origin.roles.cpu().tolist() == [int(stage=='maker_finality')]
        elif stage == 'hidden_step':
            event = source['events'][checkpoint['ordinal']]['payload']['event']
            bid = event['side'] == 'BID'
            before = integers([units(event['quantity_before_shares'])])
            after = integers([units(event['quantity_after_shares'])])
            source_price = Numbers.run_exact_batch(lambda:rational([event['price_pusd_per_share']]))
            queue,orders,account,fills,calendars,hidden = run_base_level_transition(queue,orders,account,fills,
                calendars,tensor([1 if bid else 0]),before,after,source_price,tensor([0]),
                tensor([source['maker_at']]),active,tensor([[0]*queue.capacity]),tensor([0,0]),tensor([0,0]),
                fee_rate,source['fee_exponent'],rebate)
            assert hidden.fault.cpu().tolist() == [0]
            read = orders.read(slot)
            if values(read.quantity.pending_maker_depletion_base_units)[0] > 0:
                observed_pending_zero_quote = True
            for maker_transition in hidden.fills:
                if maker_transition.event.emitted.cpu().item():
                    assert maker_record is None
                    maker_record = maker_transition
                    assert values(maker_transition.fill_sequence) == [2]
                    assert maker_transition.origin.roles.cpu().tolist() == [1]
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
    assert fills.origins.roles.cpu().tolist() == [[0,1]]
    assert values(fills.origins.order_sequences) == [1,1]
    assert values(fills.origins.prices.numerator) == [1,3] and values(fills.origins.prices.denominator) == [5,10]
    assert calendars.maker.market_pending.cpu().tolist() == [[0]]
    assert calendars.maker.terminal_consumed.cpu().tolist() == [[True]]
    assert not calendars.taker.pending.any().cpu().item()
    # 已完成请求无pending：调度padding零terms/故障不得污染既存状态。
    padding = FrozenBaseTerms(tensor([-1]), integers([0]), integers([0]), rational(['0']),
                              tensor([-1]), tensor([-1]), tensor([1], torch.int32),tensor([-1]))
    old_requests, old_orders, old_queue, old_account, old_fills = requests, orders, queue, account, fills
    requests, orders, queue, account, fills, idle = run_pending_base_acceptance(requests, orders, queue,
        account, fills, padding, tensor([[-1]]), fee_rate, source['fee_exponent'],
        torch.zeros((1,queue.capacity), dtype=torch.int64, device='cuda'), rebate, tensor([0, 0]), tensor([-1]),
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
    identity_bytes, identity_offsets = byte_column([source['post_request_key'].encode(), b'gpu-next-engineering-post'])
    requests, _ = requests.run_registration(sampler, identity_bytes, identity_offsets, tensor([1]), tensor([1]),
        integers([13]), tensor([source['cancel_arrived_at']+1]), tensor([[1]]), tensor([[ordinal]]),
        tensor([[True]], torch.bool), tensor([[False]], torch.bool), tensor([0]), active, zero_fault)
    assert orders.capacity == 1 and orders.occupied.sum().cpu().item() == 1
    queue = queue.resized(8)
    old_quantity = orders.read(slot).quantity
    next_terms = replace(terms, item_refs=tensor([1]))
    requests, orders, queue, account, fills, next_effect = run_pending_base_acceptance(requests, orders, queue,
        account, fills, next_terms, tensor([[0]]), fee_rate, source['fee_exponent'],
        torch.zeros((1,8), dtype=torch.int64, device='cuda'), rebate, tensor([0, 0]), tensor([1]),calendars=calendars)
    assert next_effect.fault.cpu().tolist() == [0] and next_effect.order.emitted.cpu().tolist() == [True]
    assert orders.capacity == 2 and queue.capacity == 8
    assert values(orders.next_sequence) == [3] and values(fills.next_sequence) == [3]
    from dataclasses import fields
    for field in fields(old_quantity):
        if field.name != 'fault':
            assert values(getattr(orders.read(slot).quantity, field.name)) == values(getattr(old_quantity, field.name))
    assert values(orders.read(next_effect.order.slots).sequence) == [2]
    orders = orders.resized(4)
    assert values(orders.read(slot).sequence) == [1]
    assert queue.order_ids[1][queue.kind[1] == 2].cpu().tolist() == [1]
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
        'original_SIM_checkpoints': len(source['checkpoints']),
        'elapsed_seconds_with_reference_reads': time.perf_counter()-started,
        'device_peak_allocated_bytes': torch.cuda.max_memory_allocated(),
        'global_clock_SDK_admission_and_full_response_body_delivered': False}))


if __name__ == '__main__':
    main()
