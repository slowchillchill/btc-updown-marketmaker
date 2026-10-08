"""原取消逐目标正文的拥有值；NOE三类计数不能替代原因和Canceled计量。

原result_refs只索引当前请求正文。此前pending结果保留至末项封口，不保存账户
镜像，不用业务失败自动重试。完整响应排序沿原target_ordinal。
"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt,_take,_columns


@dataclass(frozen=True)
class CancelTargetResult:
    order_sequence: object
    target_ordinal: object
    input_item_refs: object
    processed_at: torch.Tensor
    outcome: torch.Tensor
    reason: torch.Tensor  # 原NotFoundOrNotOwned/NoCancellableRemainder/TemporaryFailureNoEffect=0/1/2。
    canceled_base_units: object
    released_quote_units: object
    account_sequence: object
    account_sequence_known: torch.Tensor
    present: torch.Tensor
    fault: torch.Tensor

    def control_feedback(self):
        from projection import CancelTargetFeedback
        return CancelTargetFeedback(self.order_sequence,self.target_ordinal,self.input_item_refs,
            self.outcome,self.present,None,self.fault)


@dataclass(frozen=True)
class CancelResultGroup:
    request_keys: torch.Tensor
    result_refs: torch.Tensor
    targets: tuple
    present: torch.Tensor


@dataclass(frozen=True)
class CancelResults:
    capacity: int
    groups: tuple = ()

    def resized(self,capacity):
        return replace(self,capacity=max(self.capacity,capacity))

    def store(self,pending,result_refs,targets,enabled):
        required=result_refs+1
        fault=(enabled & ((result_refs<0)|(required>self.capacity))).to(torch.int32)*2
        valid=enabled & (fault==0)
        group=CancelResultGroup(pending.request_key.clone(),result_refs.clone(),tuple(targets),valid.clone())
        return replace(self,groups=(*self.groups,group)),required,fault

    def targets_for_response(self,response):
        from fields import RecordList
        targets=[]
        for group in self.groups:
            selected=group.present & (group.request_keys==response.request_key)
            selected &= ((response.result_refs==group.result_refs[:,None]) & response.present).any(1)
            targets.extend(replace(value,present=value.present & selected) for value in group.targets)
        if not targets:
            return ()
        roots=response.request_key.numel(); width=len(targets)
        device=response.request_key.device
        present=torch.stack([value.present for value in targets],1)
        ordinal=_columns([value.target_ordinal for value in targets])
        order=torch.arange(width,device=device)[None,:].expand(roots,width)
        words=ordinal.words.reshape(roots,width,-1).to(torch.int64)&0xffffffff
        for limb in range(words.size(2)):
            order=order.gather(1,words[:,:,limb].gather(1,order).argsort(dim=1,stable=True))
        order=order.gather(1,(~present.gather(1,order)).to(torch.int32).argsort(dim=1,stable=True))
        count=max(value.input_item_refs.present.size(1) for value in targets)
        reference_masks=torch.stack([torch.nn.functional.pad(value.input_item_refs.present,
            (0,count-value.input_item_refs.present.size(1))) for value in targets],1).reshape(roots*width,count)
        reference_fields=[]
        for field in range(2):
            reference_fields.append(torch.stack([torch.nn.functional.pad(value.input_item_refs.fields[field].reshape(roots,-1),
                (0,count-value.input_item_refs.present.size(1))) for value in targets],1).flatten())
        reference=RecordList(tuple(reference_fields),reference_masks)
        result=[]
        for column in range(width):
            coordinates=torch.arange(roots,device=device)*width+order[:,column]
            ref_at=(coordinates[:,None]*count+torch.arange(count,device=device)).flatten()
            item_refs=RecordList(tuple(value[ref_at] for value in reference.fields),reference.present[coordinates])
            def integer(name):return _take(_columns([getattr(value,name) for value in targets]),coordinates)
            def scalar(name):return torch.stack([getattr(value,name) for value in targets],1).flatten()[coordinates]
            result.append(CancelTargetResult(integer('order_sequence'),integer('target_ordinal'),item_refs,
                scalar('processed_at'),scalar('outcome'),scalar('reason'),integer('canceled_base_units'),
                integer('released_quote_units'),integer('account_sequence'),scalar('account_sequence_known'),
                scalar('present'),scalar('fault')))
        return tuple(result)

    def coordinated(self,original,committed):
        additions=tuple(replace(group,present=group.present & committed) for group in self.groups[len(original.groups):])
        # 与原公开人口装箱相同，只读取物理保留人口；失败不积累空正文槽。
        additions=tuple(group for group in additions if int(group.present.to(torch.int64).sum().cpu()))
        return replace(self,groups=(*original.groups,*additions))
