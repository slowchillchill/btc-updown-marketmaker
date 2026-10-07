"""当前有限原市场返佣读面的逐项精确oracle。"""
from fractions import Fraction
import torch
from queues import Numbers
from rebate_read import read_rebates
from validate_base_execution import values


def check(admission,checkpoint,condition):
    roots=admission.fault.numel()
    assert roots==1
    yes=torch.ones(roots,dtype=torch.bool,device='cuda')
    read=Numbers.run_exact_batch(lambda:read_rebates(admission,torch.zeros((roots,1),dtype=torch.int64,device='cuda'),yes[:,None],yes))
    original=next(row for row in checkpoint['markets'] if row['condition_id']==condition)['rebates']
    assert read.fault.cpu().tolist()==[0] and read.amounts_known.cpu().tolist()==[[True,True]]
    for value,key in [(read.accrued_unmatured,'accrued_unmatured_pusd'),(read.below_threshold,'below_threshold_pusd'),
        (read.confirmed_unpaid,'confirmed_unpaid_pusd'),(read.paid,'paid_pusd'),(read.locked,'locked_remainder_pusd')]:
        actual=[Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
        expected=[Fraction(int(row[key]['numerator']),int(row[key]['denominator'])) for row in original]
        assert actual==expected,(key,actual,expected)
    for role,known in enumerate(read.estimated_known.cpu().tolist()[0]):
        expected=original[role]['estimated_pusd']
        assert known==(isinstance(expected,dict) and 'Known' in expected)
        if known:
            amount=expected['Known']
            n,d=values(read.estimated.numerator),values(read.estimated.denominator)
            assert Fraction(n[role],d[role])==Fraction(int(amount['numerator']),int(amount['denominator']))
    return True
