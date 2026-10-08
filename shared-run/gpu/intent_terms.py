"""原冻结ModelIntent逐腿条款到公开Accepted订单关联；不从Query或SIM状态补条款。"""
from dataclasses import dataclass
import torch
from queues import ExactRational,Numbers
from asset_buy import take_ratio
from registry import FrozenIntentOrders,identity_matches


@dataclass(frozen=True)
class SubmittedBuyTerms:
    """与SubmittedLegs相同腿轴的原不可变正文；首次同身份冻结保原值。

    variant=0GTC/1GTD/2FOK/3FAK。price是原绝对limit/worst价格，
    spend只对FOK/FAK适用，expiry_seconds只对GTD适用。
    此目录须来自实际原ModelIntent，receipt或NOE不能反向生成正文。
    """
    variants: torch.Tensor
    post_only: torch.Tensor
    price: object
    spend: object
    expiry_seconds: torch.Tensor


def freeze_intent_orders(legs,bindings,identities,present):
    """实际Submission Accepted绑定原订单→原冻结腿；receipt单独不建立origin。

    identities/present是同cutoff已公开订单目录，不能提前公开预分配未来槽。
    数量基准和条款共享原legs目录；输出leg_slots保关联，条款不再复制一份。
    """
    roots,count=present.shape;leg_count=legs.present.shape[1]
    device=present.device
    if (present.dtype!=torch.bool or identities.sign.numel()!=present.numel()
            or identities.words.device!=device or legs.present.shape[0]!=roots
            or bindings.received.shape[0]!=roots):
        raise TypeError('原意图关联需要同根公开订单/腿/Submission人口')
    if legs.terms is None:
        raise ValueError('订单条款关联需要安装实际原冻结ModelIntent目录')
    terms=legs.terms
    if any(value.shape!=legs.present.shape or value.device!=device for value in
           (terms.variants,terms.post_only,terms.expiry_seconds)):
        raise TypeError('原意图条款须与提交腿目录同设备同轴')
    if (terms.variants.dtype!=torch.int64 or terms.post_only.dtype!=torch.bool
            or terms.expiry_seconds.dtype!=torch.int64 or any(
                value.numerator.sign.numel()!=legs.present.numel() or value.numerator.words.device!=device
                for value in (terms.price,terms.spend))):
        raise TypeError('原意图条款须保原精确金额、bool和int64类型')
    zero=identities.constant(0);one=identities.constant(1)
    target=ExactRational(zero,one)
    known=torch.zeros_like(present);quoted=known.clone();receipt=known.clone()
    slots=torch.zeros((roots,count),dtype=torch.int64,device=device)
    fault=bindings.fault
    width=bindings.received.shape[1]
    if width and leg_count:
        same,error=identity_matches(identities,bindings.orders,roots,count,width)
        accepted=same&present[:,:,None]&bindings.received[:,None,:]&bindings.accepted[:,None,:]
        matches=((bindings.request_slots[:,:,None]==legs.request_slots[:,None,:])&
            (bindings.item_ordinals[:,:,None]==legs.item_ordinals[:,None,:])&legs.present[:,None,:])
        candidates=(accepted[:,:,:,None]&matches[:,None,:,:]).any(2)
        known=candidates.any(2)
        slots=candidates.to(torch.int32).argmax(2)
        index=(torch.arange(roots,device=device)[:,None]*leg_count+slots).flatten()
        target=take_ratio(legs.target,index)
        variants=terms.variants.gather(1,slots)
        quoted=(variants==2)|(variants==3)
        receipt=legs.receipt_known.gather(1,slots)&known
        fault=fault|Numbers.combine_fault_rows(torch.where(accepted,error,0).reshape(roots,-1))
        # 安装完整原正文却缺其精确target是输入缺口，不能编码为Known零。
        fault=fault|(known&~legs.target_known.gather(1,slots)).any(1).to(torch.int32)
    return FrozenIntentOrders(present,identities,known,target,quoted,receipt,terms,slots),fault
