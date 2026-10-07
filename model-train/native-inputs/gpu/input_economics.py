"""公共市场经济精确读面到Native字段；不归约交易、费用或结算业务。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from market_economic_read import read_market_economics


@dataclass(frozen=True)
class EconomicInputs:
    fields: torch.Tensor  # [roots,2scopes,7,3]
    current_state: torch.Tensor  # 六one-hot与Known/适用位。
    old_states: torch.Tensor  # [roots,6,3]精确计数末投影。
    fault: torch.Tensor


def economic_inputs(ledger,conditions,present,applicable,*,economic_prefix_known,success_prefix_known,settlement_prefix_known):
    roots=ledger.fault.numel();device=ledger.fault.device;numbers=_numbers()
    if (conditions.ndim!=3 or conditions.shape[:2]!=(roots,2) or applicable.shape!=(roots,2)
            or applicable.dtype!=torch.bool or applicable.device!=device):
        raise TypeError('经济输入需原current/old scope及适用位')
    fields=[];reads=[];fault=ledger.fault.clone()
    for scope in range(2):
        read=read_market_economics(ledger,conditions[:,scope],present[:,scope],
            economic_prefix_known=economic_prefix_known,success_prefix_known=success_prefix_known,
            settlement_prefix_known=settlement_prefix_known)
        reads.append(read);fault|=read.fault;values=[]
        for field,amount in enumerate(read.amounts):
            value,error=amount.project(operation=1)
            known=read.known[:,field]&applicable[:,scope]
            values.append(torch.stack((torch.where(known,value,0.),known.float(),applicable[:,scope].float()),-1))
            fault|=torch.where(known,error,0)
        fields.append(torch.stack(values,1))
    current=reads[0];current_known=current.states_known&applicable[:,0]
    # 原current只有一市场。读面先精确计数，再无损投影one-hot，不能取任意一项。
    current_values=torch.where(current_known[:,None],(current.states.sign>0).reshape(roots,6).float(),0.)
    current_state=torch.cat((current_values,current_known[:,None].float(),applicable[:,0,None].float()),1)
    fault|=torch.where(applicable[:,0],(present[:,0].to(torch.int64).sum(1)>1).to(torch.int32),0)
    old=reads[1];old_known=old.states_known&applicable[:,1]
    counts,error=old.states.project(old.states.constant(1),operation=1)
    old_values=torch.where(old_known[:,None],counts.reshape(roots,6),0.)
    old_states=torch.stack((old_values,old_known[:,None].expand(roots,6).float(),
        applicable[:,1,None].expand(roots,6).float()),-1)
    fault|=numbers.combine_fault_rows(torch.where(old_known[:,None],error.reshape(roots,6),0))
    return EconomicInputs(torch.stack(fields,1),current_state,old_states,fault)


def write_economic_fields(slow,inputs):
    if slow.shape!=(inputs.fields.size(0),433) or slow.dtype!=torch.float32 or slow.device!=inputs.fields.device:
        raise TypeError('经济字段写入同设备float32 slow433')
    result=slow.clone()
    for scope in range(2):
        result[:,284+scope*15:293+scope*15]=inputs.fields[:,scope,:3].flatten(1)
        result[:,332+scope*12:344+scope*12]=inputs.fields[:,scope,3:].flatten(1)
    result[:,356:364]=inputs.current_state
    result[:,364:382]=inputs.old_states.flatten(1)
    return result
