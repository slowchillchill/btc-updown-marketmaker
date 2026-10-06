"""原合法两腿成交成功人口→GPU成本池，沿原公共源短链验证。"""
import sys
from pathlib import Path
import json
import time
from fractions import Fraction
import torch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'sim-system/gpu'))
from queues import Numbers
from validate_base_execution import rational,values
from success_pool import SuccessPools


def fractions(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def ratio(value):
    return Fraction(int(value['numerator']),int(value['denominator']))


def main():
    source=json.load(sys.stdin)
    started=time.perf_counter()
    zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    pool=Numbers.run_exact_batch(lambda:SuccessPools.open(torch.tensor([0,0],device='cuda'),
        torch.tensor([0,0],device='cuda'),torch.tensor([0,1],device='cuda'),torch.tensor([1,0],device='cuda'),1))
    for row in source['trace']:
        fact=row['fact']
        assert fact['economic_disposition']=='FinalSuccess'
        assert fact['economic_application_account_transition_seq'] is not None
        token=0 if fact['token_id']==source['up_token'] else 1
        assert fact['token_id'] in [source['up_token'],source['down_token']]
        pool,used=Numbers.run_exact_batch(lambda:pool.add(zero,torch.full_like(zero,token),rational([fact['base_quantity_shares']]),
            rational([fact['quote_amount_pusd']]),torch.tensor([True],device='cuda')))
        assert used.cpu().tolist()==[True] and pool.fault.cpu().tolist()==[0]
    market=source['market']
    up=next(i for i,a in enumerate(market['assets']) if a['token_id']==source['up_token'])
    assert fractions(pool.success_quantity)==[Fraction(market['success_shares'][i]) for i in [up,1-up]]
    assert fractions(pool.success_cost)==[ratio(market['success_principal_cost_pusd'][i]) for i in [up,1-up]]
    assert fractions(pool.quantity)==[Fraction(market['orphan_shares'][i]) for i in [up,1-up]]
    assert fractions(pool.paired_quantity)==[Fraction(market['paired_shares'])]*2
    assert fractions(pool.paired_cost)==[ratio(source['allocated_up_cost']),ratio(market['success_principal_cost_pusd'][1-up])]
    assert fractions(pool.cost)==[ratio(source['remaining_up_cost']),Fraction(0)]
    assert fractions(pool.cost)[0]+fractions(pool.paired_cost)[0]==Fraction(1)
    print(json.dumps({'actual_success_population':len(source['trace']),'fractional':source['fractional'],
        'paired_cost_and_source_tail_checked':True,'elapsed_seconds':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),'contribution_and_rebate_day_policy_delivered':False}))


if __name__=='__main__':
    main()
