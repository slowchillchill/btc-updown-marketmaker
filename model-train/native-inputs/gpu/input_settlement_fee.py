"""公共Cost累计recognized−paid，按冻结scope精确归约后投影。"""
import torch


def settlement_fee_inputs(ledger,conditions,present,applicable):
    roots=ledger.fault.numel();device=ledger.fault.device
    if (conditions.ndim!=3 or conditions.shape[:2]!=(roots,2) or conditions.dtype!=torch.int64
            or conditions.device!=device or present.shape!=conditions.shape or present.dtype!=torch.bool
            or present.device!=device or applicable.shape!=(roots,2) or applicable.dtype!=torch.bool
            or applicable.device!=device):
        raise TypeError('结算费用需原current/old scope及适用位')
    if ledger.settlement_costs is None:
        raise ValueError('结算费用输入须先安装实际Cost冷历史')
    fields=[];fault=ledger.fault.clone()
    for scope in range(2):
        recognized,paid,known,error=ledger.settlement_costs.read(conditions[:,scope],present[:,scope])
        unpaid=recognized-paid
        value,projection_fault=unpaid.project(operation=1)
        known &= applicable[:,scope]
        fields.append(torch.stack((torch.where(known,value,0.),known.float(),applicable[:,scope].float()),-1))
        fault |= error | unpaid.fault | torch.where(applicable[:,scope],projection_fault,0)
    return torch.stack(fields,1),fault
