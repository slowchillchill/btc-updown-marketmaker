"""原冻结提交逐腿承诺；receipt先于公开订单关联时仍保留原目标。

资金预留由Ledger负责。本类只读取公开Submission关联和原订单剩余承诺，
不能按余额或订单目录缺项推断未发送/无责任。
"""
from dataclasses import dataclass
import torch
from queues import ExactInt,ExactRational,Numbers,_take
from asset_buy import take_ratio
from success_pool import select
from registry import choose_latest,source_priority,identity_matches


@dataclass(frozen=True)
class SubmittedLegs:
    present: torch.Tensor  # [roots,legs]，仅原SubmitSingle/SubmitPaired合法腿。
    receipt_known: torch.Tensor
    request_slots: torch.Tensor  # 无损原请求字典坐标，不是请求业务序号。
    item_ordinals: torch.Tensor
    target: object
    target_known: torch.Tensor
    condition_slots: torch.Tensor | None = None
    outcome_indices: torch.Tensor | None = None
    association_known: torch.Tensor | None = None
    terms: object | None = None  # 原冻结ModelIntent共享逐腿正文；receipt不建立订单origin。


@dataclass(frozen=True)
class SubmissionBindings:
    received: torch.Tensor  # 原首次领取的Submission Evidence；不包含Pending/Conflict。
    request_slots: torch.Tensor
    item_ordinals: torch.Tensor
    private_sequences: object
    receive_rank: torch.Tensor
    orders: object
    accepted: torch.Tensor
    fault: torch.Tensor
    prefix_known: torch.Tensor | None = None  # 明确安装的完整公开关联前缀；批输入不声明历史。

    @classmethod
    def empty(cls, roots, device, *, prefix_known=True):
        rows = torch.empty((roots,0),dtype=torch.int64,device=device)
        integer = ExactInt.from_i64_tensor(rows.flatten())
        return cls(rows.bool(),rows,rows,integer,rows,integer,rows.bool(),
            torch.zeros(roots,dtype=torch.int32,device=device),
            torch.full((roots,),prefix_known,dtype=torch.bool,device=device))

    def receive(self, incoming):
        """只保留各原请求腿的最新公开outcome；物理压紧不改变业务选择。"""
        from dataclasses import fields
        from registry_projection import concatenate_integer
        roots = self.received.size(0)
        if incoming.received.size(0) != roots:
            raise ValueError('Submission关联须绑定相同根人口')
        fault = self.fault | incoming.fault
        values = {}
        for field in fields(self):
            if field.name == 'prefix_known':
                values[field.name] = self.prefix_known
            elif field.name == 'fault':
                values[field.name] = fault
            elif isinstance(getattr(self,field.name),torch.Tensor):
                values[field.name] = torch.cat((getattr(self,field.name),getattr(incoming,field.name)),1)
            else:
                values[field.name] = concatenate_integer([getattr(self,field.name),getattr(incoming,field.name)],roots)
        combined = SubmissionBindings(**values)
        width = combined.received.size(1)
        if width == 0:
            return combined
        device = combined.received.device
        invalid = ((combined.request_slots<0)|(combined.item_ordinals<0)|(combined.item_ordinals>65535))
        fault |= Numbers.combine_fault_rows(torch.where(combined.received,invalid.to(torch.int32),0))
        for value in [combined.private_sequences,combined.orders]:
            upper = value.compare(value.constant((1<<64)-1))
            invalid = value.fault | upper.fault | ((value.sign<0)|(upper.order>0)).to(torch.int32)
            fault |= Numbers.combine_fault_rows(torch.where(combined.received,invalid.reshape(roots,width),0))
        order = source_priority(combined.private_sequences,combined.receive_rank)
        for key in [combined.item_ordinals,combined.request_slots]:
            order = order.gather(1,key.gather(1,order).argsort(dim=1,stable=True))
        order = order.gather(1,(~combined.received).gather(1,order).to(torch.int32).argsort(dim=1,stable=True))
        same = torch.zeros_like(combined.received)
        if width>1:
            same[:,1:] = ((combined.request_slots.gather(1,order)[:,1:]==combined.request_slots.gather(1,order)[:,:-1])
                & (combined.item_ordinals.gather(1,order)[:,1:]==combined.item_ordinals.gather(1,order)[:,:-1]))
        keep = combined.received.gather(1,order) & ~same
        # 主机仅读取物理最大保留人口以分配，不读取逐腿业务outcome。
        capacity = int(keep.to(torch.int64).sum(1).max().cpu())
        retained_at = (~keep).to(torch.int32).argsort(dim=1,stable=True)[:,:capacity]
        selected = order.gather(1,retained_at)
        coordinates = (torch.arange(roots,device=device)[:,None]*width+selected).flatten()
        retained = {}
        for field in fields(self):
            value = getattr(combined,field.name)
            if field.name == 'prefix_known':
                retained[field.name] = self.prefix_known
            elif field.name == 'fault':
                retained[field.name] = fault
            elif field.name == 'received':
                retained[field.name] = keep.gather(1,retained_at)
            elif isinstance(value,torch.Tensor):
                retained[field.name] = value.gather(1,selected)
            else:
                retained[field.name] = _take(value,coordinates)
        return SubmissionBindings(**retained)


@dataclass(frozen=True)
class LegCommitmentsRead:
    present: torch.Tensor
    has_order: torch.Tensor
    orders: object
    remaining: object
    known: torch.Tensor
    fault: torch.Tensor
    condition_slots: torch.Tensor | None = None
    outcome_indices: torch.Tensor | None = None
    association_known: torch.Tensor | None = None


def freeze_leg_commitments(legs,bindings,order_directory,order_read):
    roots,count=legs.present.shape
    width=bindings.received.size(1)
    device=legs.present.device
    if any(value.shape!=(roots,count) or value.device!=device for value in
            [legs.receipt_known,legs.request_slots,legs.item_ordinals,legs.target_known]):
        raise ValueError('原提交腿目录需要同根人口与设备')
    registered=legs.present & legs.receipt_known
    fault=bindings.fault | order_read.fault
    invalid=(legs.request_slots<0)|(legs.item_ordinals<0)|(legs.item_ordinals>65535)
    fault=fault | Numbers.combine_fault_rows(torch.where(registered,invalid.to(torch.int32),0))
    # 原没有公开outcome时，完整receipt仍使原target进入承诺人口。
    remaining=legs.target
    known=legs.target_known
    has_order=torch.zeros_like(registered)
    has_binding=torch.zeros_like(registered)
    order=legs.target.numerator.constant(0)
    if width:
        member=registered[:,:,None] & bindings.received[:,None,:] & (
            legs.request_slots[:,:,None]==bindings.request_slots[:,None,:]) & (
            legs.item_ordinals[:,:,None]==bindings.item_ordinals[:,None,:])
        at,has_binding=choose_latest(member,source_priority(bindings.private_sequences,bindings.receive_rank))
        coordinates=(torch.arange(roots,device=device)[:,None]*width+at).flatten()
        order=_take(bindings.orders,coordinates)
        has_order=has_binding & bindings.accepted.gather(1,at)
        rejected=has_binding & ~bindings.accepted.gather(1,at)
        order_count=order_directory.present.size(1)
        from_order=ExactRational(order.constant(0),order.constant(1))
        from_order_known=torch.zeros_like(known)
        if order_count:
            same,error=identity_matches(order,order_directory.identities,roots,count,order_count)
            matching=same & order_directory.present[:,None,:] & has_order[:,:,None]
            found=matching.any(2)
            order_at=matching.to(torch.int32).argmax(2)
            order_coordinates=(torch.arange(roots,device=device)[:,None]*order_count+order_at).flatten()
            from_order=take_ratio(order_read.commitment,order_coordinates)
            from_order_known=found & order_read.commitment_known.gather(1,order_at)
            fault=fault | Numbers.combine_fault_rows(torch.where(matching,error,0).reshape(roots,-1))
        remaining=select(from_order,has_order.flatten(),remaining)
        known=torch.where(has_order,from_order_known,known)
        zero=ExactRational(remaining.numerator.constant(0),remaining.denominator.constant(1))
        remaining=select(zero,rejected.flatten(),remaining)
        known=known | rejected
    # 中途冷状态未装历史关联时，当前批不证明其它旧公开outcome不存在。
    prefix_known = bindings.prefix_known
    if prefix_known is None:
        prefix_known = torch.zeros(roots,dtype=torch.bool,device=device)
    if prefix_known.shape!=(roots,) or prefix_known.dtype!=torch.bool or prefix_known.device!=device:
        raise TypeError('Submission完整前缀known需同设备bool[roots]')
    known = known & (prefix_known[:,None] | has_binding)
    fault=fault | Numbers.combine_fault_rows(torch.where(registered,remaining.fault.reshape(roots,count),0))
    population=registered & ~(known & (remaining.numerator.sign==0).reshape(roots,count))
    return LegCommitmentsRead(population,has_order,order,remaining,known,fault,
        legs.condition_slots,legs.outcome_indices,legs.association_known)
