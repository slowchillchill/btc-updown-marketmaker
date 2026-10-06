"""原两市场maker真实NOE日结/付款的GPU归属对照；不运行正式训练。"""
import json
import sys
import time
from pathlib import Path
from fractions import Fraction
import torch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'sim-system/gpu'))
from queues import Numbers
from validate_base_execution import integers,rational,values
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ledger/gpu'))
from rebate_contribution import RebateContributions
from rebate_day import RebateDays
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'execution-normalization/gpu'))
from fields import IntegerList,encode_fields,decode_fields


def amounts(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def identity(value):
    return int(json.loads(value.split(':',1)[1])[-1]['Trade']['source_trade_id'].removeprefix('sim-'))


def settlement_results(value):
    if isinstance(value,dict):
        if 'SettlementLifecycleV2' in value:
            yield value['SettlementLifecycleV2']['result']
        else:
            for child in value.values():
                yield from settlement_results(child)
    elif isinstance(value,list):
        for child in value:
            yield from settlement_results(child)


def main():
    source=json.load(sys.stdin)
    started=time.perf_counter()
    results=list(settlement_results(source['records']))
    vectors=[value['MakerAccrualFinalized'] for value in results if 'MakerAccrualFinalized' in value]
    finalized=next(value['DayFinalized'] for value in results if 'DayFinalized' in value)
    payment=next(value['Payment'] for value in results if 'Payment' in value)
    assert len(vectors)==2 and finalized['kind']=='Maker' and payment['kind']=='Maker'
    condition={name:i for i,name in enumerate(source['conditions'])}
    enabled=torch.tensor([True],device='cuda')
    kind=torch.tensor([0],device='cuda')
    day=torch.tensor([finalized['attribution_day_start_utc_ms']],device='cuda')
    refs=integers([identity(value) for value in finalized['contribution_refs']])
    ref_mask=torch.ones((1,len(finalized['contribution_refs'])),dtype=torch.bool,device='cuda')
    # 原Day贡献引用列表保留实际人口/原顺序；物理字宽或人口尾补齐不得改变正文。
    encoded=Numbers.run_exact_batch(lambda:encode_fields([kind,day,IntegerList(refs,ref_mask)]))
    padded_ids=[identity(value) for value in finalized['contribution_refs']]+[0,0]
    padded_refs=Numbers.run_exact_batch(lambda:integers(padded_ids),initial_word_capacity=8)
    padded_mask=torch.nn.functional.pad(ref_mask,(0,2),value=False)
    padded=Numbers.run_exact_batch(lambda:encode_fields([kind,day,IntegerList(padded_refs,padded_mask)]))
    common=max(encoded.size(1),padded.size(1))
    assert torch.equal(torch.nn.functional.pad(encoded,(0,common-encoded.size(1))),
        torch.nn.functional.pad(padded,(0,common-padded.size(1))))
    decoded,error=Numbers.run_exact_batch(lambda:decode_fields(padded,['scalar','scalar','integer_list'],enabled))
    assert error.cpu().tolist()==[0] and decoded[0].cpu().tolist()==[0] and torch.equal(decoded[1],day)
    ref_values=values(decoded[2].values)
    assert [value for value,present in zip(ref_values,decoded[2].present.flatten().cpu().tolist()) if present]==padded_ids[:-2]
    index=Numbers.run_exact_batch(lambda:RebateContributions.empty(1,len(source['contributions']),day.device))
    state=Numbers.run_exact_batch(lambda:RebateDays.empty(torch.tensor([list(range(len(condition)))],device='cuda'),1))
    def finalize(current):
        return current.finalize(index,kind,day,rational([finalized['accrued_pusd']]),
            rational([finalized['payable_pusd']]),rational([finalized['locked_remainder_pusd']]),refs,ref_mask,enabled)
    state,ready,changed=Numbers.run_exact_batch(lambda:finalize(state))
    assert ready.cpu().tolist()==[False] and not state.occupied.any().cpu().item() and state.fault.cpu().tolist()==[0]
    state,ready,changed=Numbers.run_exact_batch(lambda:state.payment(kind,day,rational([payment['paid_pusd']]),enabled))
    assert ready.cpu().tolist()==[False] and not state.payment_observed.any().cpu().item()
    for vector in vectors:
        for value in vector['contributions']:
            index,required,used=Numbers.run_exact_batch(lambda:index.add(integers([identity(value['trade_identity'])]),
                torch.tensor([0],device='cuda'),torch.tensor([value['attribution_day_start_utc_ms']],device='cuda'),
                torch.tensor([condition[value['condition_id']]],device='cuda'),rational([value['contribution_pusd']]),enabled))
            assert used.cpu().tolist()==[True] and index.fault.cpu().tolist()==[0]
    state,ready,changed=Numbers.run_exact_batch(lambda:finalize(state))
    assert ready.cpu().tolist()==[True] and changed.cpu().tolist()==[True] and state.fault.cpu().tolist()==[0]
    exact=lambda value:Fraction(int(value['numerator']),int(value['denominator']))
    assert amounts(state.condition_payable)==[exact(value) for value in source['expected_pay']]
    assert amounts(state.condition_locked)==[exact(value) for value in source['expected_lock']]
    assert sum(amounts(state.condition_payable))==Fraction(finalized['payable_pusd'])
    assert sum(amounts(state.condition_locked))==Fraction(finalized['locked_remainder_pusd'])
    state,ready,changed=Numbers.run_exact_batch(lambda:state.payment(kind,day,rational([payment['paid_pusd']]),enabled))
    assert ready.cpu().tolist()==[True] and changed.cpu().tolist()==[True]
    again,ready,changed=Numbers.run_exact_batch(lambda:state.payment(kind,day,rational([payment['paid_pusd']]),enabled))
    assert ready.cpu().tolist()==[True] and changed.cpu().tolist()==[False]
    assert amounts(again.condition_payable)==amounts(state.condition_payable)
    restored=Numbers.run_exact_batch(lambda:state.resized(2),initial_word_capacity=8)
    assert amounts(restored.condition_payable)[:len(condition)]==amounts(state.condition_payable)
    assert restored.payment_observed.cpu().tolist()==[[True,False]]
    print(json.dumps({'actual_maker_vectors':len(vectors),'actual_contributions':len(source['contributions']),
        'actual_DayFinalized_and_Payment':True,'missing_predecessors_no_commit':True,
        'condition_finite_payable_locked_checked':True,'source_totals_conserved':True,'payment_repeat_no_second_allocation':True,
        'actual_day_reference_integer_list_and_padding_checked':True,
        'elapsed_seconds':time.perf_counter()-started,'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'full_NOE_Ledger_day_coordinator_delivered':False}))


if __name__=='__main__':
    main()
