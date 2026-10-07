"""独立原CPU市场经济字段对照；不把CPU预期用于GPU业务归约。"""
from fractions import Fraction
import math
import torch


# 独立沿原Rust account.rs::status槽位；不从GPU生产状态表生成预期。
STATES=('Unresolved','ResolvedNoCurrentPayout','ReceivableNotRequested','RequestPending','Redeemed','RequestFailedNoEffect')


def amount(value):
    return Fraction(value['numerator'])/Fraction(value['denominator']) if isinstance(value,dict) else Fraction(value)


def triple(value):
    return [math.copysign(math.log1p(abs(float(value))),float(value)),1.,1.]


def check(native,market,*,profit_known=True):
    for offset,keys in [(284,('guaranteed_position_value_pusd','paired_resolution_receivable_pusd','winner_orphan_receivable_pusd')),
            (332,('confirmed_net_pnl_pusd','confirmed_net_loss_pusd','maximum_confirmed_net_loss_pusd','unresolved_orphan_principal_cost_pusd'))]:
        rows=[triple(amount(market[key])) if profit_known or i==3 or offset==284 else [0.,0.,1.]
            for i,key in enumerate(keys)]
        torch.testing.assert_close(native.slow[0,offset:offset+len(keys)*3].cpu(),
            torch.tensor([v for row in rows for v in row]),rtol=1e-6,atol=1e-7)
        assert native.provided[0,offset:offset+len(keys)*3].all().cpu().item()
    for first,last in [(299,308),(344,356),(364,382)]:
        assert native.slow[0,first:last].cpu().tolist()==[0.,1.,1.]*((last-first)//3)
        assert native.provided[0,first:last].all().cpu().item()
    state=[float(index==STATES.index(market['settlement_state'])) for index in range(6)]+[1.,1.]
    assert native.slow[0,356:364].cpu().tolist()==state
    assert native.provided[0,356:364].all().cpu().item()
