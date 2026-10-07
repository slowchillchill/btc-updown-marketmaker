"""原公开Ledger逐订单金额与设备归属的冷对照；不决定业务消费次序。"""
from dataclasses import replace
from fractions import Fraction
import torch
from queues import Numbers
from order_funds import OrderFunds
from validate_base_execution import integers, rational, values
from validate_projection import entity_sequence


def open_prefix(roots, capacity, trade_capacity, records, checkpoint, *, device='cuda'):
    if roots != 1:
        raise ValueError('此有限原前缀只有一个账户')
    original = checkpoint['order_funds']
    result = OrderFunds.empty(roots, capacity, trade_capacity, records, device, known=True)
    if len(original) > capacity:
        raise ValueError('先为完整原资金目录分配物理容量')
    if not original:
        return result
    known = [isinstance(row['principal'], dict) and 'Known' in row['principal'] and
        isinstance(row['fee'], dict) and 'Known' in row['fee'] for row in original]
    # 缺原未关联引用的中途读面不是活动状态快照；不能补造未知金额。
    if not all(known):
        raise ValueError('未知资金读面缺少原未关联引用，不能作为活动状态安装')
    population = len(original)
    mask = torch.arange(capacity, device=device)[None, :] < population
    keys = integers([entity_sequence(row['identity'], 'Order') for row in original] + [0]*(capacity-population))
    principal = rational([row['principal']['Known'] for row in original] + ['0']*(capacity-population))
    fee = rational([row['fee']['Known'] for row in original] + ['0']*(capacity-population))
    last = integers([row['last_ledger_entry_seq'] or 0 for row in original] + [0]*(capacity-population))
    return replace(result, keys=keys, occupied=mask, conditions=torch.where(mask, 0, -1),
        principal=principal, fee=fee, has_origin=mask.clone(), last_sequence=last,
        has_last_sequence=torch.tensor([[row['last_ledger_entry_seq'] is not None for row in original] +
            [False]*(capacity-population)], dtype=torch.bool, device=device))


def check(admission, checkpoint):
    state = admission.order_funds
    read = Numbers.run_exact_batch(lambda: state.read(admission.lifecycle))
    assert read.fault.cpu().tolist() == [0]
    population = state.occupied.cpu().tolist()[0]
    ids = values(state.keys)
    known = read.known.cpu().tolist()[0]
    principal = [Fraction(n,d) for n,d in zip(values(read.principal.numerator),values(read.principal.denominator))]
    fee = [Fraction(n,d) for n,d in zip(values(read.fee.numerator),values(read.fee.denominator))]
    last = values(read.last_sequence)
    has_last = read.has_last_sequence.cpu().tolist()[0]
    expected = {entity_sequence(row['identity'], 'Order'): row for row in checkpoint['order_funds']}
    assert set(expected) == {identity for identity, present in zip(ids, population) if present}
    for slot, present in enumerate(population):
        if not present:
            continue
        row = expected[ids[slot]]
        expected_known = isinstance(row['principal'], dict) and 'Known' in row['principal']
        assert known[slot] == expected_known
        if expected_known:
            assert principal[slot] == Fraction(row['principal']['Known'])
            assert fee[slot] == Fraction(row['fee']['Known'])
        assert (last[slot] if has_last[slot] else None) == row['last_ledger_entry_seq']
    return True
