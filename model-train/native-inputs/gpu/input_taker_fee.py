"""实际公共V2未付taker费，按冻结current/old scope精确归约再投影。"""
from dataclasses import dataclass
import torch
from liability_read import read_economic_liabilities


@dataclass(frozen=True)
class TakerFeeInputs:
    values: torch.Tensor  # [roots,2scopes,3]，293:296及308:311。
    fault: torch.Tensor


def taker_fee_inputs(ledger,conditions,present,applicable,*,prefix_known):
    roots=ledger.fault.numel();device=ledger.fault.device
    if (conditions.ndim!=3 or conditions.shape[:2]!=(roots,2)
            or applicable.shape!=(roots,2) or applicable.dtype!=torch.bool or applicable.device!=device):
        raise TypeError('未付taker费需原current/old scope及适用位')
    fields=[];fault=ledger.fault.clone()
    for scope in range(2):
        read=read_economic_liabilities(ledger,conditions[:,scope],present[:,scope],prefix_known=prefix_known)
        value,error=read.taker.project(operation=1)
        known=read.known&applicable[:,scope]
        fields.append(torch.stack((torch.where(known,value,0.),known.float(),applicable[:,scope].float()),-1))
        fault|=read.fault|torch.where(applicable[:,scope],error,0)
    return TakerFeeInputs(torch.stack(fields,1),fault)


def write_taker_fee_fields(slow,inputs):
    if slow.shape!=(inputs.values.size(0),433) or slow.dtype!=torch.float32 or slow.device!=inputs.values.device:
        raise TypeError('未付taker费写入同设备float32 slow433')
    result=slow.clone()
    result[:,293:296]=inputs.values[:,0]
    result[:,308:311]=inputs.values[:,1]
    return result
