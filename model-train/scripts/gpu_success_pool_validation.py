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
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'ledger/gpu'))
from success_pool import SuccessPools
from trade_success import TradeSuccess
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'execution-normalization/gpu'))
from device_identity import sim_trade_order_key
from types import SimpleNamespace
from validate_base_execution import integers


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
    rows=len(source['trace'])
    directory=SimpleNamespace(keys=integers([row['fact']['fill_sequence'] for row in source['trace']]),
        occupied=torch.ones((1,rows),dtype=torch.bool,device='cuda'))
    trade=Numbers.run_exact_batch(lambda:TradeSuccess.open(1,rows,zero.device))
    for slot,row in enumerate(source['trace']):
        fact=row['fact']
        assert fact['economic_disposition']=='FinalSuccess'
        assert fact['economic_application_account_transition_seq'] is not None
        token=0 if fact['token_id']==source['up_token'] else 1
        assert fact['token_id'] in [source['up_token'],source['down_token']]
        enabled=torch.tensor([True],device='cuda')
        at=torch.tensor([slot],device='cuda')
        trade=Numbers.run_exact_batch(lambda:trade.update(at,zero,torch.full_like(zero,token),rational([fact['base_quantity_shares']]),
            rational([fact['quote_amount_pusd']]),torch.full_like(zero,2),enabled,enabled,enabled))
        trade,pool=Numbers.run_exact_batch(lambda:trade.apply_success(pool,directory,at,enabled))
        assert pool.fault.cpu().tolist()==[0]
    market=source['market']
    up=next(i for i,a in enumerate(market['assets']) if a['token_id']==source['up_token'])
    assert fractions(pool.success_quantity)==[Fraction(market['success_shares'][i]) for i in [up,1-up]]
    assert fractions(pool.success_cost)==[ratio(market['success_principal_cost_pusd'][i]) for i in [up,1-up]]
    assert fractions(pool.quantity)==[Fraction(market['orphan_shares'][i]) for i in [up,1-up]]
    assert fractions(pool.paired_quantity)==[Fraction(market['paired_shares'])]*2
    assert fractions(pool.paired_cost)==[ratio(source['allocated_up_cost']),ratio(market['success_principal_cost_pusd'][1-up])]
    assert fractions(pool.cost)==[ratio(source['remaining_up_cost']),Fraction(0)]
    assert fractions(pool.cost)[0]+fractions(pool.paired_cost)[0]==Fraction(1)
    expected_contribution={row['fill_sequence']:ratio(row['remaining']) for row in source['contribution_expected']}
    for row,value in zip(source['trace'],fractions(trade.unpaired_contribution)):
        fact=row['fact']
        assert value==(expected_contribution[fact['fill_sequence']] if fact['token_id']==source['up_token'] else Fraction(0))
    sys.path[:0]=[str(Path(__file__).resolve().parents[1]/'native-inputs/gpu'),
                  str(Path(__file__).resolve().parents[2]/'data-pipeline/gpu')]
    from input_success import success_inputs,write_success_fields
    native=Numbers.run_exact_batch(lambda:success_inputs(pool,torch.tensor([[[0],[-1]]],device='cuda'),
        torch.tensor([[[True],[False]]],device='cuda'),torch.ones((1,2),dtype=torch.bool,device='cuda'),
        torch.tensor([0,1],device='cuda'),prefix_known=torch.ones(1,dtype=torch.bool,device='cuda')))
    assert native.fault.cpu().tolist()==[0]
    slow=write_success_fields(torch.zeros((1,433),device='cuda'),native)
    import math
    def project(value):
        value=float(value)
        return [math.copysign(math.log1p(abs(value)),value),1.,1.]
    for first,source_values in [(122,[Fraction(market['success_shares'][i]) for i in [up,1-up]]),
        (134,[ratio(market['success_principal_cost_pusd'][i]) for i in [up,1-up]]),
        (149,[Fraction(market['orphan_shares'][i]) for i in [up,1-up]])]:
        torch.testing.assert_close(slow[0,first:first+6].cpu(),torch.tensor([v for amount in source_values for v in project(amount)]),
            rtol=1e-6,atol=1e-7)
    torch.testing.assert_close(slow[0,146:149].cpu(),torch.tensor(project(Fraction(market['paired_shares']))),rtol=1e-6,atol=1e-7)
    for first,last in [(128,134),(140,146),(155,164)]:
        assert slow[0,first:last].cpu().tolist()==[0.,1.,1.]*((last-first)//3)
    identifiers=[1,10,2,(1<<64)-1]
    keys=Numbers.run_exact_batch(lambda:sim_trade_order_key(integers(identifiers)))
    assert keys.fault.cpu().tolist()==[0]*len(identifiers)
    encoded=[bytes(row).rstrip(b'\0').decode() for row in keys.bytes.cpu().tolist()]
    assert encoded==[str(value) for value in identifiers]
    assert sorted(range(len(encoded)),key=lambda i:encoded[i])==[0,1,3,2]
    print(json.dumps({'actual_success_population':len(source['trace']),'fractional':source['fractional'],
        'paired_cost_and_source_tail_checked':True,'Native_actual_success_pair_orphan_fields_122_to_163_checked':True,'elapsed_seconds':time.perf_counter()-started,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),'actual_finite_contribution_checked':True,'full_u64_legacy_string_order_checked':True,'rebate_day_policy_delivered':False}))


if __name__=='__main__':
    main()
