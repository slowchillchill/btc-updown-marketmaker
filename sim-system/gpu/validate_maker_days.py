"""原非post-only GTC hidden队列正Maker→终态市场→日结/付款的有限设备对照。

原matching生产路径供给源结果；不冒称外层Actor、NOE或全局宿主已经闭合。
"""
from fractions import Fraction
import json
import sys
import time
import torch
from queues import Numbers, PriceQueueBatch
from account_state import PlatformAccountState
from order_quantity import BaseOrderQuantity
from base_execution import reduce_order_maker
from fill_finality import FillRecord
from fill_pool import FillPool
from maker_days import MakerDays
from validate_base_execution import integers, rational, values, units


def amounts(value):
    return [Fraction(n, d) for n, d in zip(values(value.numerator), values(value.denominator))]


def check_account(account, expected):
    assert account.fault.cpu().tolist() == [0]
    for name in ['cash', 'available', 'principal', 'fee']:
        assert values(getattr(account, name)) == [units(expected[name])], (name, values(getattr(account, name)), expected[name])
    assert values(account.next_sequence) == [expected['next_sequence']]
    balances = dict(expected['outcomes'])
    assert values(account.outcomes) == [units(balances.get(token, '0')) for token in ['1', '2']]
    assert amounts(account.rebates) == [Fraction(v) for v in expected['rebates']]


def main():
    source = json.load(sys.stdin)
    started = time.perf_counter()
    zero = torch.zeros(1, dtype=torch.int64, device='cuda')
    enabled = torch.ones(1, dtype=torch.bool, device='cuda')
    def prepare():
        account = PlatformAccountState.open(integers([639_500_520]), torch.tensor([0, 0], device='cuda'))
        account, delta, _ = account.accept_order(integers([122_000_000]), integers([0]), integers([122_000_000]), enabled)
        queue = PriceQueueBatch.empty(rational(['0.4']), capacity=2)
        queue, _ = queue.append_account(torch.tensor([7], device='cuda'), integers([305_000_000]))
        quantity = BaseOrderQuantity.from_taker_plan(integers([305_000_000]), integers([122_000_000]), integers([0]), integers([0]))
        return account, queue, quantity
    account, queue, quantity = Numbers.run_exact_batch(prepare)
    check_account(account, source['before'])
    queue, _ = Numbers.run_exact_batch(lambda: queue.apply_level_transition(integers([0]), integers([300_000_000])))
    queue, effects = Numbers.run_exact_batch(lambda: queue.apply_level_transition(integers([300_000_000]), integers([0])))
    quantity, maker_fill = Numbers.run_exact_batch(lambda: reduce_order_maker(quantity, effects, torch.tensor([7], device='cuda')))
    assert values(maker_fill.base) == [300_000_000] and values(maker_fill.quote) == [120_000_000]
    def register():
        record = FillRecord.create(maker_fill.base, maker_fill.quote, integers([0]), zero,
            torch.tensor([1310], device='cuda'), maker_fill.emitted, source['finality_path'])
        pool = FillPool.empty(record, capacity=1)
        days = MakerDays.empty(zero[:, None], torch.tensor([[2000]], device='cuda'), enabled[:, None], fills=1, days=1)
        pool, days, effect, demand = pool.enqueue_with_maker(record, days, zero, rational(['0.4']))
        assert effect.emitted.cpu().tolist() == [True] and demand.committed.cpu().tolist() == [True]
        return pool, days
    pool, days = Numbers.run_exact_batch(register)
    rate = Numbers.run_exact_batch(lambda: rational([source['fee_rate']]))
    maker_rate = Numbers.run_exact_batch(lambda: rational([source['maker_rate']]))
    results = []
    for checkpoint in source['checkpoints']:
        at = torch.tensor([checkpoint['at']], device='cuda')
        if checkpoint['stage'] == 'end_pending':
            days, slot, ended = days.advance_end(at, enabled)
            days, account, result = Numbers.run_exact_batch(lambda: days.finalize_market(account, slot, ended))
            assert not result.emitted.any().cpu().item()
            assert days.pending.cpu().tolist() == [[1]]
        else:
            pool, account, days, frame, result = Numbers.run_exact_batch(lambda: pool.advance_with_maker(
                account, days, at, rate, source['fee_exponent'], maker_rate))
            assert frame.event.emitted.cpu().tolist() == [True]
            if result.emitted.any().cpu().item():
                results.append(result)
        check_account(account, checkpoint['state'])
    assert len(results) == 1 and results[0].status.cpu().tolist() == [0]
    assert values(results[0].fill_sequences) == [1] and results[0].member_mask.cpu().tolist() == [[True]]
    assert amounts(results[0].contributions) == [Fraction('1.008')]
    assert amounts(days.fee_equivalent) == [Fraction(source['maker_fee_equivalent'])]
    assert days.pending.cpu().tolist() == [[0]] and days.market_pending.cpu().tolist() == [[0]]
    days, advanced = days.advance_clock(torch.tensor([89_100_000], device='cuda'), enabled)
    assert advanced.cpu().tolist() == [True]
    days, account, final = Numbers.run_exact_batch(lambda: days.finalize_one(account, enabled))
    days, account, paid = Numbers.run_exact_batch(lambda: days.pay_one(account, enabled))
    assert amounts(final.accrued) == [Fraction('1.008')]
    assert amounts(final.payable) == [Fraction('1.008')] and amounts(final.locked) == [0]
    assert values(final.contribution_refs) == [1] and final.reference_mask.cpu().tolist() == [[True]]
    assert not final.account_delta.emitted.any().cpu().item()
    assert paid.account_delta.emitted.cpu().tolist() == [True]
    check_account(account, source['after'])
    repeated, same, again = Numbers.run_exact_batch(lambda: days.pay_one(account, enabled))
    assert not again.emitted.any().cpu().item() and values(same.next_sequence) == values(account.next_sequence)
    # 增长复用原身份/来源价与逐fill金额，不把完整值缩到固定机器整数。
    grown = Numbers.run_exact_batch(lambda: days.resized(fills=2, days=2), initial_word_capacity=8)
    assert values(grown.fill_sequences) == [1, 0]
    assert amounts(grown.fill_prices) == [Fraction('0.4'), 0]
    assert amounts(grown.contributions) == [Fraction('1.008'), 0]
    print(json.dumps({'original_non_post_only_GTC_hidden_Maker_source': True,
        'queue_depletion_base_quote_checked': True, 'market_end_waits_for_terminal': True,
        'positive_fill_H5_and_market_vector_checked': True, 'Maker_day_payment_exact_money_checked': True,
        'capacity_growth_preserves_identity_price_contribution': True,
        'elapsed_seconds_with_reference_reads': time.perf_counter() - started,
        'device_peak_allocated_bytes': torch.cuda.max_memory_allocated(),
        'global_clock_NOE_Ledger_Maker_pipeline_delivered': False}))


if __name__ == '__main__':
    main()
