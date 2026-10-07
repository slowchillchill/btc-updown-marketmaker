"""原V2 Cost/ExternalCash来源映射，精确金额及修正身份保持公共语义。"""
import torch
from queues import ExactRational,Numbers
from fields import encode_fields,RecordList
from lifecycle import LifecycleEvidence,input_fault,source_order
from economic import EconomicEvidence


def map_cost(account,condition,resolution,policy,identity,recognized,paid,has_correction,
             correction,private,ordinal,active):
    """实际Cost累计值；request四身份及correction None均显式传递。"""
    fault=input_fault([private],[account,condition,resolution,policy,identity,correction],ordinal)
    if (active.shape!=account.shape or has_correction.shape!=account.shape
            or active.dtype!=torch.bool or has_correction.dtype!=torch.bool
            or active.device!=account.device or has_correction.device!=account.device):
        raise TypeError('Cost实际人口和修正存在位须同设备bool[root]')
    fault|=((account<0)|(condition<0)|(resolution<0)|(policy<0)|(identity<0)|
        (has_correction&(correction<0))).to(torch.int32)
    fault|=recognized.fault|paid.fault
    logical=encode_fields([account,torch.full_like(account,9),private])
    semantic=encode_fields([account,condition,resolution,policy,identity,recognized,paid,
        has_correction,torch.where(has_correction,correction,0)])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active&(fault==0),fault,22)


def map_external_cash(account,identity,amount,attribution,has_correction,correction,
                      private,ordinal,active):
    """原signed_delta与标签完整传递；标签不替Ledger决定Maker/Other分类。"""
    fault=input_fault([private],[account,identity,attribution,correction],ordinal)|amount.fault
    if (active.shape!=account.shape or has_correction.shape!=account.shape
            or active.dtype!=torch.bool or has_correction.dtype!=torch.bool
            or active.device!=account.device or has_correction.device!=account.device):
        raise TypeError('ExternalCash实际人口和修正存在位须同设备bool[root]')
    fault|=((account<0)|(identity<0)|(attribution<0)|(attribution>2)|
        (has_correction&(correction<0))).to(torch.int32)
    logical=encode_fields([account,torch.full_like(account,9),private])
    semantic=encode_fields([account,identity,amount,attribution,has_correction,
        torch.where(has_correction,correction,0)])
    return LifecycleEvidence(logical,semantic,source_order(private,ordinal),active&(fault==0),fault,23)


def map_settlement_cash_economic(account,condition,at,kind,resolution,policy,identity,
        sequence,balances,outcomes,transported_outcomes,settlement_liability,taker_liability,
        private,ordinal,active):
    """0=CostRecognized、1=CostPaid、2=ExternalCashApplied；不冒充Existing。

    balances是原V2八项精确delta。真实空outcome列表保留空人口；token与运行资产
    列由同一冷实际绑定提供，不生成一个零资产去代替空列表。
    """
    fault=input_fault([sequence,private],[account,condition,at,kind,resolution,policy,identity],ordinal)
    if len(balances)!=8 or not isinstance(outcomes,RecordList) or not isinstance(transported_outcomes,RecordList):
        raise TypeError('V2经济须八项原精确余额及原outcome成员向量')
    roots=account.numel()
    if (outcomes.present.ndim!=2 or outcomes.present.size(0)!=roots
            or transported_outcomes.present.shape!=outcomes.present.shape
            or len(outcomes.fields)!=2 or len(transported_outcomes.fields)!=2
            or outcomes.present.device!=account.device or transported_outcomes.present.device!=account.device
            or active.shape!=(roots,) or active.dtype!=torch.bool or active.device!=account.device):
        raise TypeError('V2经济人口和原token/资产向量须同根同设备')
    fault|=((account<0)|(kind<0)|(kind>2)|(identity<0)|
        ((kind<2)&((condition<0)|(resolution<0)|(policy<0)))|
        ((kind==2)&((condition!=-1)|(resolution!=-1)|(policy!=-1)))).to(torch.int32)
    fault|=Numbers.combine_fault_rows((outcomes.present!=transported_outcomes.present).to(torch.int32))
    width=outcomes.present.size(1)
    for vector in (outcomes,transported_outcomes):
        if (not isinstance(vector.fields[0],torch.Tensor) or vector.fields[0].dtype!=torch.int64
                or vector.fields[0].shape!=(roots*width,) or vector.fields[0].device!=account.device
                or not isinstance(vector.fields[1],ExactRational)
                or vector.fields[1].numerator.sign.shape!=(roots*width,)
                or vector.fields[1].denominator.sign.shape!=(roots*width,)
                or vector.fields[1].numerator.words.device!=account.device
                or vector.fields[1].denominator.words.device!=account.device):
            raise TypeError('V2 outcome成员须原int64身份及ExactRational份额')
        member_fault=vector.fields[1].fault.reshape(roots,width) | (vector.fields[0].reshape(roots,width)<0).to(torch.int32)
        fault|=Numbers.combine_fault_rows(torch.where(vector.present,member_fault,0))
    quantity=outcomes.fields[1].compare(transported_outcomes.fields[1])
    fault|=Numbers.combine_fault_rows(torch.where(outcomes.present,
        quantity.fault.reshape(roots,width)|(quantity.order.reshape(roots,width)!=0).to(torch.int32),0))
    for value in (*balances,settlement_liability,taker_liability):
        if (value.numerator.sign.shape!=(roots,) or value.denominator.sign.shape!=(roots,)
                or value.numerator.words.device!=account.device or value.denominator.words.device!=account.device):
            raise TypeError('V2经济余额/负债须原精确pUSD[root]')
        fault|=value.fault
    logical=encode_fields([account,torch.full_like(account,11),sequence])
    economic=encode_fields([account,torch.ones_like(account),sequence])
    semantic=encode_fields([account,condition,at,kind,resolution,policy,identity,
        *balances,outcomes,settlement_liability,taker_liability])
    zero=ExactRational(sequence.constant(0),sequence.constant(1))
    return EconomicEvidence(logical,economic,semantic,source_order(private,ordinal),tuple(balances),zero,
        outcomes.present.any(1),active&(fault==0),fault,sequence,24,None,transported_outcomes)
