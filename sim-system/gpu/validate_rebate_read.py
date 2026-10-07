"""当前有限原市场返佣读面的逐项精确oracle。"""
from fractions import Fraction
from pathlib import Path
import math
import sys
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
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'model-train/native-inputs/gpu'),str(root/'data-pipeline/gpu')]
    from input_rebates import rebate_inputs,write_rebate_fields
    # 原oracle的condition0显式装入old输入布局，current为NA；不证明根old选择器。
    conditions=torch.tensor([[[-1],[0]]],device='cuda')
    present=conditions>=0;applicable=torch.tensor([[False,True]],device='cuda')
    encoded=Numbers.run_exact_batch(lambda:rebate_inputs(admission,conditions,present,applicable,prefix_known=yes))
    actual=write_rebate_fields(torch.zeros((roots,433),device='cuda'),encoded)
    expected=[]
    for role in original:
        for key in ('estimated_pusd','accrued_unmatured_pusd','below_threshold_pusd',
                'confirmed_unpaid_pusd','paid_pusd','locked_remainder_pusd'):
            value=role[key]
            known=key!='estimated_pusd' or (isinstance(value,dict) and 'Known' in value)
            if key=='estimated_pusd' and known: value=value['Known']
            amount=Fraction(int(value['numerator']),int(value['denominator'])) if known else Fraction(0)
            expected.extend((math.copysign(math.log1p(abs(float(amount))),float(amount)),float(known),1.))
    assert torch.equal(actual[0,212:248],torch.zeros(36,device='cuda'))
    assert torch.equal(actual[0,248:284],torch.tensor(expected,dtype=torch.float32,device='cuda'))
    assert encoded.fault.cpu().tolist()==[0]
    unknown=Numbers.run_exact_batch(lambda:rebate_inputs(admission,conditions,present,applicable,prefix_known=~yes))
    assert torch.equal(unknown.values[0,1],torch.tensor([0.,0.,1.],device='cuda').expand(2,6,3))
    # 原完整空old集合是Known零；没有完整非空前缀时仍独立保持这个语义。
    empty=Numbers.run_exact_batch(lambda:rebate_inputs(admission,conditions,torch.zeros_like(present),
        torch.tensor([[False,True]],device='cuda'),prefix_known=~yes))
    assert torch.equal(empty.values[0,1],torch.tensor([0.,1.,1.],device='cuda').expand(2,6,3))
    return True
