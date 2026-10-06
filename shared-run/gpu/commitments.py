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


@dataclass(frozen=True)
class LegCommitmentsRead:
    present: torch.Tensor
    has_order: torch.Tensor
    orders: object
    remaining: object
    known: torch.Tensor
    fault: torch.Tensor


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
    fault=fault | Numbers.combine_fault_rows(torch.where(registered,remaining.fault.reshape(roots,count),0))
    population=registered & ~(known & (remaining.numerator.sign==0).reshape(roots,count))
    return LegCommitmentsRead(population,has_order,order,remaining,known,fault)
