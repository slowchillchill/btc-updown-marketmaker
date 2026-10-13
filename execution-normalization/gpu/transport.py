"""已提交NOE记录的紧凑共享传输布局。

设备稳定压紧真实人口；物理出口读取单批标量或整阶段尺寸向量，不决定
事实、领取、前驱或业务状态。原bank行、batch/record/private身份均保持。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt, ExactRational, Numbers, _take


def append_root_fields(left, right, roots, left_width, right_width, *, fields=1):
    """按[root,record,field]追加原字段；只依赖物理形状，不读取业务值。"""
    if isinstance(left, ExactRational):
        return ExactRational(
            append_root_fields(left.numerator,right.numerator,roots,left_width,right_width,fields=fields),
            append_root_fields(left.denominator,right.denominator,roots,left_width,right_width,fields=fields))
    if isinstance(left, ExactInt):
        word_width=max(left.words.size(1),right.words.size(1))
        def joined(name):
            values=[getattr(value.with_capacity(word_width),name) for value in [left,right]]
            suffix=(word_width,) if name=='words' else ()
            return torch.cat([value.reshape((roots,width,fields)+suffix)
                for value,width in zip(values,[left_width,right_width])],dim=1).reshape((-1,)+suffix)
        return ExactInt(joined('words'),joined('sign'),joined('fault'))
    if isinstance(left,torch.Tensor):
        return torch.cat([left.reshape(roots,left_width,fields),
            right.reshape(roots,right_width,fields)],dim=1).flatten()
    raise TypeError('共享记录字段须为原精确数值或张量')


def select_root_fields(value, order, width, *, fields=1):
    """本次固定容量的[root,record,field]选择；不读取设备人口。"""
    roots=order.size(0)
    rows=(torch.arange(roots,device=order.device)[:,None]*width+order).flatten()
    positions=(rows[:,None]*fields+torch.arange(fields,device=order.device)).flatten()
    if isinstance(value,ExactRational):
        return ExactRational(_take(value.numerator,positions),_take(value.denominator,positions))
    if isinstance(value,ExactInt):return _take(value,positions)
    return value.reshape(-1)[positions]


def concatenate_root_fields(values,roots,widths,*,fields=1):
    result=values[0];width=widths[0]
    for incoming,extra in zip(values[1:],widths[1:],strict=True):
        result=append_root_fields(result,incoming,roots,width,extra,fields=fields);width+=extra
    return result


@dataclass(frozen=True)
class StageBatchLayout:
    batches: tuple
    widths: torch.Tensor
    offsets: torch.Tensor
    order: torch.Tensor


def stage_batch_layout(batches):
    """各自然批稳定选列并写入设备前缀；物理上界只取原批张量宽。"""
    roots=batches[0].rows.size(0);device=batches[0].rows.device
    selected=tuple(select_batch_columns(batch,(~batch.emitted).to(torch.int64).argsort(dim=1,stable=True))
        for batch in batches)
    widths=torch.stack([batch.emitted.to(torch.int64).sum(1).max() for batch in batches])
    offsets=widths.cumsum(0)-widths
    live=torch.cat([torch.arange(batch.rows.size(1),device=device)<width
        for batch,width in zip(batches,widths,strict=True)])
    # 各批实际列写入offset[j]；未使用物理列仅留在本次临时尾区。
    targets=torch.cat([offset+torch.arange(batch.rows.size(1),device=device)
        for batch,offset in zip(batches,offsets,strict=True)])
    targets=torch.where(live,targets,widths.sum()+(~live).to(torch.int64).cumsum(0)-1)
    order=torch.empty_like(targets).scatter(0,targets,torch.arange(targets.numel(),device=device))
    order=order[None,:].expand(roots,-1)
    return StageBatchLayout(selected,widths,offsets,order)


def take_record_list(value, rows):
    from fields import RecordList,take_field
    width=value.present.size(1)
    positions=(rows[:,None]*width+torch.arange(width,device=rows.device)).flatten()
    return RecordList(tuple(take_field(member,positions) for member in value.fields),value.present[rows])


def append_record_lists(left,right,roots,left_width,right_width):
    """原变长成员人口按根/记录追加，padding不进入内容等值或经济量。"""
    from fields import RecordList
    width=max(left.present.size(1),right.present.size(1))
    def pad(value,rows,old,*,denominator=False):
        if isinstance(value,ExactRational):
            return ExactRational(pad(value.numerator,rows,old),pad(value.denominator,rows,old,denominator=True))
        if isinstance(value,torch.Tensor):
            return torch.nn.functional.pad(value.reshape(rows,old),(0,width-old)).flatten()
        if old==0:
            return ExactInt.from_i64_tensor(torch.full((rows*width,),int(denominator),dtype=torch.int64,device=left.present.device))
        column=torch.arange(width,device=left.present.device)
        positions=(torch.arange(rows,device=column.device)[:,None]*old+column.clamp_max(old-1)).flatten()
        result=_take(value,positions)
        return result.select((column<old).expand(rows,width).flatten(),result.constant(int(denominator)))
    members=tuple(append_root_fields(pad(a,roots*left_width,left.present.size(1)),
        pad(b,roots*right_width,right.present.size(1)),roots,left_width,right_width,fields=width)
        for a,b in zip(left.fields,right.fields,strict=True))
    present=append_root_fields(torch.nn.functional.pad(left.present,(0,width-left.present.size(1))),
        torch.nn.functional.pad(right.present,(0,width-right.present.size(1))),roots,left_width,right_width,fields=width)
    return RecordList(members,present.reshape(-1,width))


def batch_outcome_list(batch):
    """旧单资产经济列的无损视图，新增多资产批沿同一成员格式传输。"""
    from fields import RecordList
    if batch.economic_outcome_list is not None:
        return batch.economic_outcome_list
    present=batch.economic_enabled.flatten() & (batch.economic_outcomes.numerator.sign!=0)
    return RecordList((batch.economic_assets.flatten(),batch.economic_outcomes),present[:,None])


def select_batch_columns(batch, order):
    """原批的不可变列视图；保留原record/private/batch身份及来源人口。"""
    from dataclasses import fields as members
    from fields import RecordList
    roots,width=batch.rows.shape
    root=torch.arange(roots,device=order.device)[:,None]
    coordinates=(root*width+order).flatten()
    def integer(value,count=1):
        return _take(value,(coordinates[:,None]*count+torch.arange(count,device=order.device)).flatten())
    updates={}
    for member in members(batch):
        value=getattr(batch,member.name)
        if value is None or member.name=='fault':
            updates[member.name]=value
        elif isinstance(value,RecordList):
            updates[member.name]=take_record_list(value,coordinates)
        elif member.name=='normalized':
            updates[member.name]=replace(value,**{name:getattr(value,name).gather(1,order)
                for name in ['rows','outcomes','primary_fact_rows','emitted']})
        elif isinstance(value,torch.Tensor):
            updates[member.name]=value.gather(1,order)
        elif isinstance(value,ExactRational):
            count=8 if member.name=='economic_balances' else 1
            updates[member.name]=ExactRational(integer(value.numerator,count),integer(value.denominator,count))
        else:
            updates[member.name]=integer(value)
    return replace(batch,**updates)


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
    # 旧普通批没有取消method列；混合批以非取消-1占位，保持原每记录坐标。
    methods=[batch.cancellation_method for batch in batches]
    updates['cancellation_method'] = None if all(value is None for value in methods) else scalar([
        torch.full_like(batch.rows,-1) if value is None else value
        for batch,value in zip(batches,methods)])
    updates['economic_balances'] = ratio([batch.economic_balances for batch in batches], 8)
    updates['economic_outcomes'] = ratio([batch.economic_outcomes for batch in batches])
    vectors=batch_outcome_list(first)
    joined_width=widths[0]
    for batch in batches[1:]:
        vectors=append_record_lists(vectors,batch_outcome_list(batch),roots,joined_width,batch.rows.size(1))
        joined_width+=batch.rows.size(1)
    updates['economic_outcome_list']=take_record_list(vectors,index)
    merged = replace(first, normalized=normalized, fault=fault([batch.fault for batch in batches]), **updates)
    return (merged,), TransportPopulation(original_width, allocated, population)
