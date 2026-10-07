"""同Ledger冻结前缀的公开实际执行量、本金与净费，不以成功量代替。"""
from dataclasses import dataclass
import torch
from prepare import _numbers


@dataclass(frozen=True)
class ExecutedInputs:
    values: torch.Tensor  # [roots,2scopes,2roles,3metrics,3]，原176—211。
    fault: torch.Tensor


def executed_inputs(trades, conditions, present, applicable, *, prefix_known):
    roots = trades.has_fact.size(0)
    device = trades.has_fact.device
    if (conditions.ndim != 3 or conditions.shape[:2] != (roots,2)
            or conditions.dtype != torch.int64 or conditions.device != device
            or present.shape != conditions.shape or present.dtype != torch.bool or present.device != device
            or applicable.shape != (roots,2) or applicable.dtype != torch.bool or applicable.device != device):
        raise TypeError('执行字段需要同设备原current/old condition作用域和适用位')
    if prefix_known.shape != (roots,) or prefix_known.dtype != torch.bool or prefix_known.device != device:
        raise TypeError('执行人口需同设备真实冷初始化前缀known，不由空目录推断')
    numbers = _numbers()
    fields = []
    fault = torch.zeros(roots,dtype=torch.int32,device=device)
    for scope in range(2):
        read = trades.executed(conditions[:,scope],present[:,scope])
        enabled = applicable[:,scope,None]
        metrics = []
        for value in (read.quantity,read.principal,read.actual_fee):
            projected,error = value.project(operation=1)
            known = read.known & enabled & (prefix_known | ~present[:,scope].any(1))[:,None]
            metrics.append(torch.stack((torch.where(known,projected.reshape(roots,2),0.0),
                known.float(),enabled.expand(roots,2).float()),-1))
            fault |= numbers.combine_fault_rows(torch.where(enabled,error.reshape(roots,2),0))
        fields.append(torch.stack(metrics,2))
        fault |= torch.where(applicable[:,scope],read.fault,0)
    return ExecutedInputs(torch.stack(fields,1),fault)


def write_executed_fields(slow, inputs):
    if slow.shape != (inputs.values.size(0),433) or slow.dtype != torch.float32 or slow.device != inputs.values.device:
        raise TypeError('执行字段写入同设备float32 slow433')
    result = slow.clone()
    result[:,176:212] = inputs.values.reshape(slow.size(0),36)
    return result
