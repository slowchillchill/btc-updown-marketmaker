"""原Ledger市场计数和精确all-in的Native末端投影，不消费Registry计数。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from account_read import read_market_account


@dataclass(frozen=True)
class MarketAccountInputs:
    counts: torch.Tensor  # [roots,2scopes,2fields,3]
    all_in: torch.Tensor  # [roots,3]，current。
    fault: torch.Tensor


def market_account_inputs(ledger,conditions,present,applicable,*,prefix_known):
    roots=ledger.fault.numel();device=ledger.fault.device;numbers=_numbers()
    if (conditions.ndim!=3 or conditions.shape[:2]!=(roots,2) or conditions.dtype!=torch.int64 or conditions.device!=device
            or present.shape!=conditions.shape or present.dtype!=torch.bool or present.device!=device
            or applicable.shape!=(roots,2) or applicable.dtype!=torch.bool or applicable.device!=device):
        raise TypeError('账本市场输入需同设备current/old scope及适用位')
    fields=[];fault=ledger.fault.clone();all_in=None
    for scope in range(2):
        read=read_market_account(ledger,conditions[:,scope],present[:,scope],history_known=prefix_known)
        enabled=applicable[:,scope]
        counts=numbers.ExactInt.from_i64_tensor(read.counts.flatten())
        projected,error=counts.project(counts.constant(1),operation=1)
        known=enabled&read.counts_known
        fields.append(torch.stack((torch.where(known[:,None],projected.reshape(roots,2),0.),
            known[:,None].expand(roots,2).float(),enabled[:,None].expand(roots,2).float()),-1))
        fault|=read.fault|numbers.combine_fault_rows(torch.where(enabled[:,None],error.reshape(roots,2),0))
        if scope==0:
            projected,error=read.all_in.project(operation=1)
            known=enabled&read.known
            all_in=torch.stack((torch.where(known,projected,0.),known.float(),enabled.float()),-1)
            fault|=torch.where(enabled,error,0)
    return MarketAccountInputs(torch.stack(fields,1),all_in,fault)


def write_market_account_fields(slow,inputs):
    if slow.shape!=(inputs.counts.size(0),433) or slow.dtype!=torch.float32 or slow.device!=inputs.counts.device:
        raise TypeError('账本市场字段写入同设备float32 slow433')
    result=slow.clone()
    result[:,81:93]=inputs.counts.flatten(1)
    result[:,95:98]=inputs.all_in
    return result
