"""已提交NOE记录的紧凑共享传输布局。

设备稳定压紧真实人口；宿主只在物理分配边界读一个最大人口标量，不决定
事实、领取、前驱或业务状态。原bank行、batch/record/private身份均保持。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt, ExactRational, Numbers, _take


@dataclass(frozen=True)
class TransportPopulation:
    original_width: int
    allocated_width: int
    records_per_root: torch.Tensor


def compact_committed_batches(batches):
    """压紧已完成规范化的来源批；不重新规范化，不合并来源业务batch身份。"""
    if not batches:
        raise ValueError('共享传输需要已提交来源批')
    roots = batches[0].rows.size(0)
    widths = [batch.rows.size(1) for batch in batches]
    device = batches[0].rows.device
    for batch in batches:
        if batch.rows.ndim != 2 or batch.rows.size(0) != roots or batch.rows.device != device:
            raise ValueError('共享传输必须使用同根同设备来源批')
    original_width = sum(widths)
    emitted = torch.cat([batch.emitted for batch in batches], dim=1)
    population = emitted.to(torch.int64).sum(1)
    if original_width == 0:
        return tuple(batches), TransportPopulation(0, 0, population)
    # 仅一个物理人口/分配标量。至少一列保留合法空批的格式，不补造事实。
    allocated = max(1, int(population.max().cpu()))
    order = (~emitted).to(torch.int64).argsort(dim=1, stable=True)[:, :allocated]
    root = torch.arange(roots, device=device)[:, None]
    index = (root*original_width + order).flatten()

    def scalar(values):
        return torch.cat(values, dim=1).gather(1, order)

    def integer(values, fields=1):
        word_width = max(value.words.size(1) for value in values)
        joined = ExactInt(torch.cat([value.with_capacity(word_width).words for value in values]),
            torch.cat([value.sign for value in values]), torch.cat([value.fault for value in values]))
        offsets = []
        offset = 0
        for width in widths:
            positions = (root*width + torch.arange(width, device=device)[None, :]).flatten()
            offsets.append((positions[:, None]*fields + torch.arange(fields, device=device)[None, :]+offset).reshape(roots, width*fields))
            offset += roots*width*fields
        joined = _take(joined, torch.cat(offsets, dim=1).flatten())
        positions = (index[:, None]*fields + torch.arange(fields, device=device)[None, :]).flatten()
        return _take(joined, positions)

    def ratio(values, fields=1):
        return ExactRational(integer([value.numerator for value in values], fields),
            integer([value.denominator for value in values], fields))

    def fault(values):
        return Numbers.combine_fault_rows(torch.stack(values, dim=1))

    first = batches[0]
    normalized = replace(first.normalized,
        rows=scalar([batch.normalized.rows for batch in batches]),
        outcomes=scalar([batch.normalized.outcomes for batch in batches]),
        primary_fact_rows=scalar([batch.normalized.primary_fact_rows for batch in batches]),
        emitted=scalar([batch.normalized.emitted for batch in batches]),
        fault=fault([batch.normalized.fault for batch in batches]))
    tensors = ['received_at', 'rows', 'emitted', 'source_categories', 'platform_at',
        'request_slots', 'item_ordinals', 'context_slots', 'run_slots', 'economic_assets', 'economic_enabled']
    updates = {name: scalar([getattr(batch, name) for batch in batches]) for name in tensors}
    for name in ['private_sequences', 'batch_sequences', 'record_ordinals', 'economic_sequences']:
        updates[name] = integer([getattr(batch, name) for batch in batches])
    updates['economic_balances'] = ratio([batch.economic_balances for batch in batches], 8)
    updates['economic_outcomes'] = ratio([batch.economic_outcomes for batch in batches])
    merged = replace(first, normalized=normalized, fault=fault([batch.fault for batch in batches]), **updates)
    return (merged,), TransportPopulation(original_width, allocated, population)
