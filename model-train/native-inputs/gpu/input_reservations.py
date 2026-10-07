"""同一Ledger冻结前缀的原current/old预留九项；不合并三个待定状态。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows
from reservation import BEFORE_SEND, SUBMITTED, PLATFORM_PENDING


@dataclass(frozen=True)
class ReservationInputs:
    values: torch.Tensor  # [roots,2,9,3]，本金/费用/人口分别保留。
    fault: torch.Tensor


def reservation_inputs(book, conditions, present, applicable):
    """作用域来自原根当前/旧市场选择，空旧集合为Known零，当前不存在为NA。"""
    roots, width = book.request_slots.shape
    device = book.request_slots.device
    if (conditions.ndim != 3 or conditions.shape[:2] != (roots, 2)
            or conditions.dtype != torch.int64 or conditions.device != device
            or present.shape != conditions.shape or present.dtype != torch.bool
            or present.device != device or applicable.shape != (roots, 2)
            or applicable.dtype != torch.bool or applicable.device != device):
        raise TypeError('预留字段须同设备current/old原condition人口及适用位')
    numbers = _numbers()
    member = ((book.conditions[:, None, :, None] == conditions[:, :, None, :])
              & present[:, :, None, :]).any(3)
    member &= (book.request_slots >= 0)[:, None, :]
    positions = (torch.arange(roots, device=device)[:, None, None]*width
                 + torch.arange(width, device=device)[None, None, :]).expand(roots, 2, width).flatten()
    principal, fee = take_ratio(book.principal, positions), take_ratio(book.fee, positions)
    zero = numbers.ExactRational(principal.numerator.constant(0), principal.denominator.constant(1))
    fields = []
    fault = book.fault.clone()
    for state in (BEFORE_SEND, SUBMITTED, PLATFORM_PENDING):
        selected = member & (book.statuses[:, None, :] == state)
        count = numbers.ExactInt.from_i64_tensor(selected.to(torch.int64).sum(2).flatten())
        values = (sum_rows(select(principal, selected.flatten(), zero), roots*2, width),
                  sum_rows(select(fee, selected.flatten(), zero), roots*2, width),
                  numbers.ExactRational(count, count.constant(1)))
        for value in values:
            projected, error = value.project(operation=1)
            fields.append(torch.stack((torch.where(applicable, projected.reshape(roots, 2), 0.0),
                                       applicable.float(), applicable.float()), -1))
            fault |= numbers.combine_fault_rows(torch.where(applicable, error.reshape(roots, 2), 0))
    return ReservationInputs(torch.stack(fields, 2), fault)


def write_reservation_fields(slow, inputs):
    if slow.shape != (inputs.values.size(0), 433) or slow.dtype != torch.float32 or slow.device != inputs.values.device:
        raise TypeError('预留字段写入同设备float32 slow433')
    result = slow.clone()
    result[:, :54] = inputs.values.reshape(slow.size(0), 54)
    return result
