"""原公开Ledger市场读面作oracle；不提供设备逐条应用控制。"""
from dataclasses import replace
from fractions import Fraction
import torch
from queues import Numbers
from market_economics import UnresolvedMarkets
from validate_base_execution import rational,values


def exact(value):
    if isinstance(value,dict):
        return Fraction(int(value['numerator']),int(value['denominator']))
    return Fraction(value)


def open_prefix(pools,checkpoint,condition):
    # 当前有限来源只装配该真实市场两资产；未读其它市场不冒充全账户。
    original=next(row for row in checkpoint['markets'] if row['condition_id']==condition)
    if original['settlement_state']!='Unresolved':
        raise ValueError('此读面尚需真实Resolution/请求式结算状态')
    state=UnresolvedMarkets.open(pools,torch.tensor([0],device='cuda',dtype=torch.int64))
    return replace(state,profit=rational([str(exact(original['confirmed_net_pnl_pusd']))]),
        maximum_loss=rational([str(exact(original['maximum_confirmed_net_loss_pusd']))]))


def check(admission,checkpoint,condition):
    read=Numbers.run_exact_batch(lambda:admission.unresolved_markets.read(
        admission.success_pools,admission.trade_success,admission.rebate_days))
    original=next(row for row in checkpoint['markets'] if row['condition_id']==condition)
    assert read.known.cpu().tolist()==[True] and read.fault.cpu().tolist()==[0]
    for value,key in [(read.guarantee,'guaranteed_position_value_pusd'),
        (read.orphan_cost,'unresolved_orphan_principal_cost_pusd'),
        (read.profit,'confirmed_net_pnl_pusd'),(read.maximum_loss,'maximum_confirmed_net_loss_pusd')]:
        actual=[Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
        assert actual==[exact(original[key])],(key,actual,original[key])
    return True
