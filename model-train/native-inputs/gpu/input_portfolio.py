"""公共全账户WCM与other读面末端投影；不使用模型current/old市场子集。"""
from dataclasses import dataclass
import torch
from portfolio_economics import read_portfolio_economics
from portfolio_nav import read_portfolio_nav


@dataclass(frozen=True)
class PortfolioInputs:
    fields: torch.Tensor  # [roots,4或6,3]，两NAV只在实际同冻结估值安装时提供。
    fault: torch.Tensor


def portfolio_inputs(ledger,*,valuation=None,cutoff=None,**history):
    read=read_portfolio_economics(ledger,**history)
    rows=[];fault=read.fault.clone()
    for index,amount in enumerate(read.amounts):
        value,error=amount.project(operation=1)
        known=read.known[:,index]
        rows.append(torch.stack((torch.where(known,value,0.),known.float(),torch.ones_like(value)),-1))
        fault|=torch.where(known,error,0)
    if valuation is not None:
        if cutoff is None:raise ValueError('全账户NAV须消费同冻结实际cutoff')
        nav=read_portfolio_nav(ledger,valuation,cutoff,portfolio=read,**history)
        fault|=nav.fault
        for index,amount in enumerate((nav.conservative,nav.directional)):
            value,error=amount.project(operation=1)
            known=nav.known[:,index]
            rows.append(torch.stack((torch.where(known,value,0.),known.float(),torch.ones_like(value)),-1))
            fault|=torch.where(known,error,0)
    return PortfolioInputs(torch.stack(rows,1),fault)


def write_portfolio_fields(slow,inputs):
    if slow.shape!=(inputs.fields.size(0),433) or slow.dtype!=torch.float32 or slow.device!=inputs.fields.device:
        raise TypeError('全账户经济字段写入同设备float32 slow433')
    result=slow.clone()
    if inputs.fields.size(1) not in (4,6):raise ValueError('公共账户经济需四字段或加完整两NAV字段')
    for index,offset in enumerate((317,323,326,329,314,320)[:inputs.fields.size(1)]):
        result[:,offset:offset+3]=inputs.fields[:,index]
    return result
