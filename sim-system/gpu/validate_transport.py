"""实际已提交批的跨根物理布局检查；不创建额外训练根或业务请求。"""
from dataclasses import fields, is_dataclass, replace
import torch
from queues import ExactInt, ExactRational
from transport import compact_committed_batches


def check_transport_layout(batches):
    def duplicate(value):
        if value is None:
            return None
        if isinstance(value, torch.Tensor):
            return value.repeat((2,)+(1,)*(value.ndim-1))
        if is_dataclass(value):
            return replace(value, **{field.name: duplicate(getattr(value, field.name)) for field in fields(value)})
        raise TypeError('原批布局包含未覆盖字段')

    doubled = []
    for batch in batches:
        if batch.rows.size(0) != 1:
            raise ValueError('该原样本检查只复制一个实际根的物理列')
        value = duplicate(batch)
        emitted = value.emitted.clone()
        emitted[1] = False  # 空根仅验证padding，不解释为第二个账户运行。
        value = replace(value, emitted=emitted,
            normalized=replace(value.normalized, emitted=emitted))
        doubled.append(value)
    (actual,), work = compact_committed_batches(doubled)
    order = (~torch.cat([value.emitted for value in doubled], dim=1)).to(torch.int64).argsort(dim=1, stable=True)[:, :work.allocated_width]

    def tensor(expected, got):
        expected = torch.cat(expected, dim=1).gather(1, order)
        assert torch.equal(expected, got)

    def integer(expected, got, field_count=1):
        width = max([got.words.size(1)]+[value.words.size(1) for value in expected])
        words = torch.cat([value.with_capacity(width).words.reshape(2, batch.rows.size(1), field_count, width)
            for value, batch in zip(expected, doubled)], dim=1)
        indices = order[:, :, None, None].expand(2, work.allocated_width, field_count, width)
        assert torch.equal(words.gather(1, indices).flatten(0, 2), got.with_capacity(width).words)
        for name in ['sign', 'fault']:
            values = torch.cat([getattr(value, name).reshape(2, batch.rows.size(1), field_count)
                for value, batch in zip(expected, doubled)], dim=1)
            assert torch.equal(values.gather(1, order[:, :, None].expand(2, work.allocated_width, field_count)).flatten(), getattr(got, name))

    for field in fields(actual):
        name = field.name
        got = getattr(actual, name)
        expected = [getattr(value, name) for value in doubled]
        if name == 'normalized':
            for member in ['rows', 'outcomes', 'primary_fact_rows', 'emitted']:
                tensor([getattr(value, member) for value in expected], getattr(got, member))
        elif name == 'cancellation_method':
            if got is None:
                assert all(value is None for value in expected)
            else:
                tensor([torch.full_like(batch.rows,-1) if value is None else value
                    for batch,value in zip(doubled,expected)],got)
        elif name == 'fault':
            assert torch.equal(got, torch.zeros_like(got))  # 原源批本身已提交无错误。
        elif isinstance(got, torch.Tensor):
            tensor(expected, got)
        elif isinstance(got, ExactInt):
            integer(expected, got)
        elif isinstance(got, ExactRational):
            count = 8 if name == 'economic_balances' else 1
            integer([value.numerator for value in expected], got.numerator, count)
            integer([value.denominator for value in expected], got.denominator, count)
        else:
            raise TypeError('压紧布局包含未覆盖字段')
    assert not actual.emitted[1].any().cpu().item()
    return True
