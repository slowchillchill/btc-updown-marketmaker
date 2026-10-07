"""公共全账户WCM与other读面末端投影；不使用模型current/old市场子集。"""
from dataclasses import dataclass
import torch
from portfolio_economics import read_portfolio_economics


@dataclass(frozen=True)
class PortfolioInputs:
    fields: torch.Tensor  # [roots,4,3]
    fault: torch.Tensor


def portfolio_inputs(ledger,**history):
    read=read_portfolio_economics(ledger,**history)
    rows=[];fault=read.fault.clone()
    for index,amount in enumerate(read.amounts):
        value,error=amount.project(operation=1)
        known=read.known[:,index]
        rows.append(torch.stack((torch.where(known,value,0.),known.float(),torch.ones_like(value)),-1))
        fault|=torch.where(known,error,0)
    return PortfolioInputs(torch.stack(rows,1),fault)


def write_portfolio_fields(slow,inputs):
    if slow.shape!=(inputs.fields.size(0),433) or slow.dtype!=torch.float32 or slow.device!=inputs.fields.device:
        raise TypeError('全账户经济字段写入同设备float32 slow433')
    result=slow.clone()
    for index,offset in enumerate((317,323,326,329)):
        result[:,offset:offset+3]=inputs.fields[:,index]
    return result
