"""原SIM Quote目标跨ask价位的纯设备规划；不承担请求接纳、费用或账户提交。"""
from dataclasses import dataclass
import torch
from queues import Numbers, ExactInt, ExactRational, QuoteTakerPlan, _take


def take_ratio(value, indices):
    return ExactRational(_take(value.numerator, indices), _take(value.denominator, indices))


@dataclass(frozen=True)
class AssetQuotePlan:
    queue_plan: QuoteTakerPlan
    matched_base: object  # ExactInt[assets]
    used_quote: object
    complete: torch.Tensor
    fault: torch.Tensor
    row_owners: torch.Tensor  # int64[queue rows]，冷资产/ask索引；-1不是本次ask。


def plan_asset_quote(queue, ask_rows, budget, maximum_price):
    """ask_rows为原精确价位升序的冷共享索引[assets,maximum_levels]，尾部-1。

    所有业务分支与预算衔接留在设备。None/部分段停止整个资产计划；FOK宿主仅在
    complete且接单条件成立时应用。未来空档位不会产生流动性或模型可见性。
    """
    assets, width = ask_rows.shape
    if ask_rows.dtype != torch.int64 or ask_rows.device != queue.kind.device:
        raise TypeError('ask坐标须为同设备int64矩阵')
    if budget.sign.shape != (assets,) or maximum_price.numerator.sign.shape != (assets,):
        raise ValueError('资产预算及最高价形状不一致')
    if queue.rows < 1 or assets < 1:
        raise ValueError('资产/价位物理人口须非空；无ask用尾部-1表示')
    present = ask_rows >= 0
    valid = present & (ask_rows < queue.rows)
    safe = ask_rows.clamp(0, queue.rows-1)
    counts = torch.zeros(queue.rows, dtype=torch.int64, device=ask_rows.device).scatter_add(
        0, safe.flatten(), valid.to(torch.int64).flatten())
    owner = torch.full((queue.rows,), -1, dtype=torch.int64, device=ask_rows.device)
    # padding不写入owner，避免覆盖合法row0；固定列循环只使用冷形状。
    for column in range(width):
        row = safe[:, column]
        encoded = torch.zeros_like(owner).scatter_add(0, row, (torch.arange(assets, device=row.device)+1)*valid[:, column])
        owner = torch.where(encoded > 0, encoded-1, owner)
    fault = budget.fault | maximum_price.fault | (budget.sign < 0).to(torch.int32)
    fault = fault | ((present & ~valid).any(1) | ((ask_rows < -1).any(1))).to(torch.int32)
    fault = fault | ((counts[safe] > 1) & valid).any(1).to(torch.int32)
    if width > 1:
        relation = take_ratio(queue.price, safe[:, :-1].flatten()).compare(take_ratio(queue.price, safe[:, 1:].flatten()))
        adjacent = valid[:, :-1] & valid[:, 1:]
        bad_order = (relation.order.reshape(assets, -1) >= 0) & adjacent
        bad_tail = ~present[:, :-1] & present[:, 1:]
        fault = fault | (bad_order.any(1) | bad_tail.any(1)).to(torch.int32)
        fault = fault | Numbers.combine_fault_rows(relation.fault.reshape(assets, -1)*adjacent.to(torch.int32))
    root_zero = budget.constant(0)
    matched = used = root_zero
    stopped = torch.zeros(assets, dtype=torch.bool, device=ask_rows.device)
    flat_zero = queue.total_base.constant(0)
    base = quote = flat_zero
    consumed = queue.consumed_base
    row_fault = queue.fault
    safe_owner = owner.clamp(0, assets-1)
    for column in range(width):
        row = safe[:, column]
        price = take_ratio(queue.price, row)
        permitted = price.compare(maximum_price)
        fault = fault | permitted.fault * valid[:, column].to(torch.int32)
        selected = valid[:, column] & ~stopped & (permitted.order <= 0) & (fault == 0)
        remaining = budget-used
        root_budget = remaining.select(selected, root_zero)
        row_budget = _take(root_budget, safe_owner)
        row_selected = torch.zeros(queue.rows, dtype=torch.int64, device=row.device).scatter_add(0,row,selected.to(torch.int64)) > 0
        row_budget = row_budget.select(row_selected, row_budget.constant(0))
        candidate = queue.plan_quote_budget(row_budget)
        fault = fault | candidate.fault[row] * selected.to(torch.int32)
        addition_base = _take(candidate.matched_base, row).select(selected, root_zero)
        addition_quote = _take(candidate.used_quote, row).select(selected, root_zero)
        matched = matched+addition_base
        used = used+addition_quote
        fault = fault | matched.fault | used.fault
        stopped = stopped | (valid[:, column] & (permitted.order > 0)) | (selected & candidate.stopped[row])
        flat_selected = row_selected.repeat_interleave(queue.capacity)
        base = candidate.delta_base.select(flat_selected, base)
        quote = candidate.delta_quote.select(flat_selected, quote)
        consumed = candidate.consumed_after.select(flat_selected, consumed)
        row_fault = row_fault | candidate.fault * row_selected.to(torch.int32)
    complete = used.compare(budget)
    fault = fault | complete.fault
    row_matched = queue.debt.constant(0)
    row_used = queue.debt.constant(0)
    for column in range(queue.capacity):
        row_matched = row_matched+queue._at(base, column)
        row_used = row_used+queue._at(quote, column)
    combined = QuoteTakerPlan(queue, base, quote, consumed, row_matched, row_used,
                             torch.zeros(queue.rows, dtype=torch.bool, device=ask_rows.device), row_fault)
    return AssetQuotePlan(combined, matched, used, (complete.order == 0) & (fault == 0), fault, owner)


def apply_asset_quote(queue, plan, accepted):
    if accepted.dtype != torch.bool or accepted.shape != plan.complete.shape or accepted.device != queue.kind.device:
        raise TypeError('资产接纳掩码须为同设备bool[assets]')
    safe_owner = plan.row_owners.clamp(0, accepted.numel()-1)
    enabled = (plan.row_owners >= 0) & accepted[safe_owner] & plan.complete[safe_owner] & (plan.fault[safe_owner] == 0)
    return queue.apply_quote_plan(plan.queue_plan, enabled)
