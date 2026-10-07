"""原共享公开账户冻结到Native资金/订单/请求字段。

provided标明本装配实际写入的槽位；未提供的经济/结算字段不能以零冒充完整slow433。
完整根还须接同一冻结前缀的持仓、承诺、经济、估值和结算读面。
"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from input_orders import order_inputs, write_order_scope_fields


@dataclass(frozen=True)
class FrozenAccountInputs:
    slow: torch.Tensor
    provided: torch.Tensor
    order_fields: object
    cutoff: torch.Tensor
    fault: torch.Tensor


def frozen_account_inputs(account, scope_members, scope_known, scope_applicable, *, scope_conditions=None, scope_present=None):
    """消费一次真实冻结值；当前/旧scope由根权威选择器传入，不按订单猜scope。"""
    numbers = _numbers()
    roots = account.cutoff.numel()
    device = account.cutoff.device
    orders = order_inputs(account.orders, account.order_sources, account.cutoff, scope_members, scope_known)
    slow = torch.zeros((roots, 433), dtype=torch.float32, device=device)
    provided = torch.zeros_like(slow, dtype=torch.bool)
    slow = write_order_scope_fields(slow, orders, scope_applicable)
    provided[:, 382:412] = True
    fault = account.fault | orders.fault
    if scope_conditions is not None:
        from input_reservations import reservation_inputs, write_reservation_fields
        reservations = reservation_inputs(account.ledger.reservations, scope_conditions, scope_present, scope_applicable)
        slow = write_reservation_fields(slow, reservations)
        provided[:, :54] = True
        fault |= reservations.fault
    elif scope_present is not None:
        raise ValueError('scope存在位不能脱离原condition人口')
    for start, value in [(110, account.funds.actual_available), (113, account.funds.actual_reserved),
                         (116, account.funds.maker_available), (119, account.funds.maker_reserved)]:
        projected, error = value.project(operation=1)
        slow[:, start:start+3] = torch.stack((projected, torch.ones_like(projected), torch.ones_like(projected)), 1)
        provided[:, start:start+3] = True
        fault |= error
    count = account.pending_noe_count
    projected, error = count.project(count.constant(1), operation=1)
    slow[:, 412:415] = torch.stack((projected, torch.ones_like(projected), torch.ones_like(projected)), 1)
    provided[:, 412:415] = True
    fault |= error
    requests = account.cancellations
    if requests is not None:
        pending = requests.pending_count
        value, error = pending.project(pending.constant(1), operation=1)
        slow[:, 415:418] = torch.stack((value, torch.ones_like(value), torch.ones_like(value)), 1)
        fault |= error | requests.fault
        counts = requests.response_counts
        value, error = counts.project(counts.constant(1), operation=1)
        enabled = requests.counts_applicable[:, None].expand(roots, 3)
        rows = torch.stack((torch.where(enabled, value.reshape(roots, 3), 0.0),
                            enabled.float(), enabled.float()), -1)
        slow[:, 418:427] = rows.reshape(roots, 9)
        fault |= numbers.combine_fault_rows(torch.where(enabled, error.reshape(roots, 3), 0))
        present = requests.has_latest
        for offset, left, right in [(427, account.cutoff, requests.responded_at),
                                    (428, account.cutoff, requests.received_at),
                                    (429, requests.received_at, requests.responded_at)]:
            delta = numbers.ExactInt.from_i64_tensor(left) - numbers.ExactInt.from_i64_tensor(right)
            value, error = delta.project(delta.constant(1000), operation=1)
            slow[:, offset] = torch.where(present, value, 0.0)
            fault |= torch.where(present, error, 0)
        slow[:, 430:433] = present[:, None].expand(roots, 3).float()
        # 原Known(None)请求最新响应为NA六项，而订单模板无来源仍有适用位。
        provided[:, 415:433] = True
    return FrozenAccountInputs(slow, provided, orders, account.cutoff.clone(), fault)
