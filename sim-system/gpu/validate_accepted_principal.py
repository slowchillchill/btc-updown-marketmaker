"""原SIM/NOE→CPU Ledger累计本金oracle；不以当前占款或GPU自身结果充当预期。"""
from fractions import Fraction
import torch
from queues import Numbers
from accepted_principal import AcceptedPrincipal
from validate_base_execution import values


def amounts(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def open_prefix(pools, checkpoint):
    projection=checkpoint['projection']
    # 此既有真实三成交场景从零累计开户，完整market图尚空；不是中途值的猜测。
    assert not projection['market_projections']
    assert Fraction(projection['platform_confirmed_projection']['cumulative_accepted_principal_pusd'])==0
    conditions=torch.unique(pools.condition_slots)
    # 真实零开户历史尚无接纳成员，完整冷前缀仍给其实际scope Known零。
    empty=torch.empty(0,dtype=torch.int64,device=conditions.device)
    present=torch.ones((1,conditions.numel()),dtype=torch.bool,device=conditions.device)
    for complete in (True,False):
        state=AcceptedPrincipal.open(1,empty,known=complete)
        value,known,fault=state.read(conditions[None],present)
        assert amounts(value)==[Fraction(0)] and known.cpu().tolist()==[complete] and fault.cpu().tolist()==[0]
    return AcceptedPrincipal.open(1,conditions,known=True)


def check(admission, checkpoint):
    projection=checkpoint['projection'];state=admission.accepted_principal
    total,known,fault=Numbers.run_exact_batch(lambda:state.read(state.conditions[None],
        torch.ones_like(state.conditions[None],dtype=torch.bool)))
    expected=Fraction(projection['platform_confirmed_projection']['cumulative_accepted_principal_pusd'])
    assert amounts(total)==[expected] and known.cpu().tolist()==[True] and fault.cpu().tolist()==[0]
    for market in projection['market_projections']:
        # 原入口是唯一condition0且由实际SIM市场固定绑定，不由金额反推身份。
        value,known,fault=Numbers.run_exact_batch(lambda:state.read(torch.zeros((1,1),dtype=torch.int64,device='cuda'),
            torch.ones((1,1),dtype=torch.bool,device='cuda')))
        assert amounts(value)==[Fraction(market['cumulative_accepted_principal_pusd'])]
        assert known.cpu().tolist()==[True] and fault.cpu().tolist()==[0]
    assert bool(admission.orders.accepted_principal_counted.any().cpu())==(expected>0)
    return expected


def check_growth(admission,source,delivered):
    from ledger_drain import _owned
    # 原场景已通过真实MarketEnrollment接纳另一个市场；只安装它的冷字典槽，不生成业务NOE。
    original_condition=source['initial'][0]['market']['condition_id']
    enrollments=[row['kind']['MarketEnrollment']['result']['market']['condition_id']
        for batch in source['normalization_reference'] if batch['name']=='actual_source'
        for row in batch['input']['evidence'] if 'MarketEnrollment' in row['kind']]
    dictionary=[original_condition,*sorted(set(enrollments)-{original_condition})]
    assert len(dictionary)>1
    before=admission.accepted_principal
    assert before.conditions.cpu().tolist()==[dictionary.index(original_condition)]
    checkpoint=source['reservation_checkpoints'][-1]
    expected=check(admission,checkpoint)
    assert expected>0
    frozen=_owned(admission,admission)
    saved={(part,name):getattr(getattr(frozen.accepted_principal.amounts,part),name).clone()
        for part in ('numerator','denominator') for name in ('words','sign','fault')}
    counted=frozen.orders.accepted_principal_counted.clone()
    # 重排原非零成员，并增加已经合法登记、累计仍为零的成员。
    directory=torch.tensor([dictionary.index(dictionary[1]),dictionary.index(original_condition)],
        dtype=torch.int64,device=before.conditions.device)
    grown=Numbers.run_exact_batch(lambda:admission.extended_principal_conditions(directory))
    assert amounts(grown.accepted_principal.amounts)==[Fraction(0),expected]
    assert torch.equal(grown.accepted_principal.known,before.known)
    assert torch.equal(grown.orders.accepted_principal_counted,counted)
    assert check(grown,checkpoint)==expected
    repeated,remaining=Numbers.run_exact_batch(lambda:grown.apply_closed(delivered,
        torch.ones_like(grown.columns.enabled),maximum_retry_rounds=1))
    assert not remaining.any().cpu().item() and check(repeated,checkpoint)==expected
    assert torch.equal(repeated.orders.accepted_principal_counted,counted)
    assert frozen.accepted_principal.conditions.cpu().tolist()==[0]
    assert check(frozen,checkpoint)==expected
    for (part,name),value in saved.items():
        assert torch.equal(getattr(getattr(frozen.accepted_principal.amounts,part),name),value)
    assert torch.equal(frozen.orders.accepted_principal_counted,counted)
