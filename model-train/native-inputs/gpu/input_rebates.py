"""同一公开Ledger冻结的current/old、Maker/Taker六项返佣，不改变付款政策。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from rebate_read import read_rebates


@dataclass(frozen=True)
class RebateInputs:
    values: torch.Tensor  # [roots,2scopes,2roles,6metrics,3]，原slow212:284。
    fault: torch.Tensor


def rebate_inputs(admission, conditions, present, applicable, *, prefix_known):
    roots=admission.fault.numel();device=admission.fault.device
    if (conditions.ndim!=3 or conditions.shape[:2]!=(roots,2)
            or conditions.dtype!=torch.int64 or conditions.device!=device
            or present.shape!=conditions.shape or present.dtype!=torch.bool or present.device!=device
            or applicable.shape!=(roots,2) or applicable.dtype!=torch.bool or applicable.device!=device
            or prefix_known.shape!=(roots,) or prefix_known.dtype!=torch.bool or prefix_known.device!=device):
        raise TypeError('返佣字段需同设备原current/old作用域、适用位及完整冷前缀已知位')
    numbers=_numbers();scopes=[];fault=admission.fault.clone()
    for scope in range(2):
        # 完整空集合为Known零；非空中途前缀不能凭空目录冒充Known。
        read=read_rebates(admission,conditions[:,scope],present[:,scope],
            prefix_known | ~present[:,scope].any(1))
        metrics=[];enabled=applicable[:,scope,None]
        for index,value in enumerate((read.estimated,read.accrued_unmatured,read.below_threshold,
                read.confirmed_unpaid,read.paid,read.locked)):
            projected,error=value.project(operation=1)
            known=(read.estimated_known if index==0 else read.amounts_known)&enabled
            metrics.append(torch.stack((torch.where(known,projected.reshape(roots,2),0.),
                known.float(),enabled.expand(roots,2).float()),-1))
            fault|=numbers.combine_fault_rows(torch.where(enabled,error.reshape(roots,2),0))
        scopes.append(torch.stack(metrics,2))
        fault|=torch.where(applicable[:,scope],read.fault,0)
    return RebateInputs(torch.stack(scopes,1),fault)


def write_rebate_fields(slow, inputs):
    if slow.shape!=(inputs.values.size(0),433) or slow.dtype!=torch.float32 or slow.device!=inputs.values.device:
        raise TypeError('返佣字段写入同设备float32 slow433')
    result=slow.clone();result[:,212:284]=inputs.values.flatten(1)
    return result
