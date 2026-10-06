"""原SIM真实FOK→MINED→成熟屏障→CONFIRMED→应计/日结/付款对照。"""
import json
import sys
import time
from fractions import Fraction
import torch
from pathlib import Path
from latency import EmpiricalDelaySampler
from feedback import FeedbackEmitter
from queues import Numbers
from account_state import PlatformAccountState
from fill_finality import FillRecord
from fill_pool import FillPool
from taker_days import TakerDays
from validate_base_execution import integers,rational,values,units


def ratios(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def main():
    source=json.load(sys.stdin)
    start=time.perf_counter()
    enabled=torch.tensor([True],device='cuda')
    disabled=~enabled
    matched=torch.tensor([source['matched_at']],device='cuda')
    base=integers([int(source['base_units'])])
    quote=integers([int(source['quote_units'])])
    fee=integers([int(source['fee_units'])])
    def prepare():
        account=PlatformAccountState.open(integers([units(source['opening_cash'])]),torch.tensor([0],device='cuda'))
        account,delta,rejected=account.accept_order(quote,fee,quote+fee,enabled)
        record=FillRecord.create(base,quote,fee,torch.tensor([0],device='cuda'),matched,enabled,source['finality_path'])
        pool=FillPool.empty(record,capacity=1)
        days=TakerDays.empty(1,'cuda',fills=1,days=1)
        pool,days,event,demand=pool.enqueue_with_days(record,days)
        return account,pool,days,delta,rejected,event,demand
    account,pool,days,delta,rejected,event,demand=Numbers.run_exact_batch(prepare)
    assert delta.emitted.cpu().tolist()==[True] and rejected.cpu().tolist()==[False]
    assert event.emitted.cpu().tolist()==[True] and values(event.sequence)==[source['fill_sequence']]
    assert days.pending.cpu().tolist()==[[1]] and days.fault.cpu().tolist()==[0]
    rate=Numbers.run_exact_batch(lambda:rational([source['taker_rebate_rate']]))
    pool,account,days,frame=Numbers.run_exact_batch(lambda:pool.advance_with_days(account,days,matched,rate))
    assert frame.event.emitted.cpu().tolist()==[True] and not frame.event.terminal.any().cpu().item()
    mined=matched+source['finality_path']['steps'][1]['offset_ms']
    pool,account,days,frame=Numbers.run_exact_batch(lambda:pool.advance_with_days(account,days,mined,rate))
    assert values(frame.event.economic_delta.sequence)==[2]
    assert days.pending.cpu().tolist()==[[1]]
    maturity=torch.tensor([source['maturity_at']],device='cuda')
    days,clock=Numbers.run_exact_batch(lambda:days.advance_clock(maturity,enabled))
    assert clock.cpu().tolist()==[True] and days.mature.cpu().tolist()==[[True]]
    days,account,result=Numbers.run_exact_batch(lambda:days.finalize_one(account,enabled))
    assert result.emitted.cpu().tolist()==[False] and values(account.next_sequence)==[3]
    at=torch.tensor([source['confirmed_at']],device='cuda')
    pool,account,days,frame=Numbers.run_exact_batch(lambda:pool.advance_with_days(account,days,at,rate))
    original=source['settlements'][0]['event']['TakerRebateAccrualApplied']
    assert frame.event.terminal.cpu().tolist()==[True] and days.pending.cpu().tolist()==[[0]]
    assert values(frame.event.rebate_delta.sequence)==[original['account_delta']['account_transition_seq']]
    assert ratios(days.accrued)==[Fraction(original['accrual_pusd'])]
    days,account,final=Numbers.run_exact_batch(lambda:days.finalize_one(account,enabled))
    original=source['settlements'][1]['event']['TakerRebateDayFinalized']
    assert final.emitted.cpu().tolist()==[True] and final.day.cpu().tolist()==[original['attribution_day_start_utc_ms']]
    assert ratios(final.accrued)==[Fraction(original['accrual_pusd'])]
    assert ratios(final.payable)==[Fraction(original['payable_pusd'])] and ratios(final.locked)==[Fraction(original['locked_remainder_pusd'])]
    assert values(final.account_delta.sequence)==[original['account_delta']['account_transition_seq']]
    assert values(final.contribution_refs)==[1] and final.reference_mask.cpu().tolist()==[[True]]
    assert original['contribution_trade_ids']==['sim-1']
    confirmed_frame=frame
    confirmed_pool=pool
    days,account,paid=Numbers.run_exact_batch(lambda:days.pay_one(account,enabled))
    original=source['settlements'][2]['event']['TakerRebatePaid']
    assert paid.emitted.cpu().tolist()==[True] and ratios(paid.payable)==[Fraction(original['paid_pusd'])]
    assert values(paid.account_delta.sequence)==[original['account_delta']['account_transition_seq']]
    assert values(account.available)==[units(source['final_available'])]
    assert values(account.next_sequence)==[source['next_account_sequence']]
    assert ratios(account.rebates)==[0,0,0,Fraction(source['settlements'][1]['event']['TakerRebateDayFinalized']['locked_remainder_pusd'])]
    # 同一次CONFIRMED：Trade/Accrual/Day/Payment必须共享一个batch，非各发一个批。
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'execution-normalization/gpu'))
    from projection import Projection
    from fill_pool import _take_record
    from core import OUT_EVIDENCE,OUT_FACT
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    path=Path(__file__).resolve().parents[1]/'config/latency-profiles'/sha/'profile.json'
    sampler=EmpiricalDelaySampler.build(path.read_bytes(),sha,[b'7'*64])
    zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    projection=Projection.open(FeedbackEmitter.open(1),zero,zero,zero,zero,
        torch.tensor([0],device='cuda'),integers([1]),zero,zero,zero,zero)
    record=_take_record(confirmed_pool.records,confirmed_frame.slot)
    projection,batch=Numbers.run_exact_batch(lambda:projection.finality(sampler,record,confirmed_frame,
        rational([source['source_price']]),rate,at,finalized=(final,),payments=(paid,),kind=torch.ones_like(zero)))
    assert batch.fault.cpu().tolist()==[0]
    assert batch.emitted.cpu().tolist()==[[True,False,True,True,True,True,True,True]]
    assert batch.normalized.outcomes.cpu().tolist()==[[OUT_EVIDENCE,0,OUT_EVIDENCE,OUT_FACT,OUT_EVIDENCE,OUT_FACT,OUT_EVIDENCE,OUT_FACT]]
    assert values(batch.private_sequences)==[1,1,2,2,3,3,4,4]
    assert values(batch.batch_sequences)==[2]*8
    assert projection.normalizer.bank.source_order[:,3].cpu().tolist()==[0,1,1,2,3,4,5,6]
    assert [values(batch.economic_sequences)[index] for index in [3,5,7]]==[3,4,5]
    days,account,repeated=Numbers.run_exact_batch(lambda:days.pay_one(account,enabled))
    assert repeated.emitted.cpu().tolist()==[False] and values(account.next_sequence)==[source['next_account_sequence']]
    pool,account,days,frame=Numbers.run_exact_batch(lambda:pool.advance_with_days(account,days,at,rate))
    assert frame.event.emitted.cpu().tolist()==[False] and days.pending.cpu().tolist()==[[0]]
    # 物理容量不足不接受新fill；增长后同原输入登记，不改原完整身份/金额。
    future=matched+86_400_000
    old_days=days
    deferred,request=Numbers.run_exact_batch(lambda:days.register(integers([2]),future,fee,enabled))
    assert request.committed.cpu().tolist()==[False] and request.fill_capacity.cpu().tolist()==[2] and request.day_capacity.cpu().tolist()==[2]
    assert deferred.pending.cpu().tolist()==[[0]] and values(deferred.fill_sequences)==[1]
    days=Numbers.run_exact_batch(lambda:days.resized(fills=2,days=2))
    days,request=Numbers.run_exact_batch(lambda:days.register(integers([2]),future,fee,enabled))
    assert request.committed.cpu().tolist()==[True] and values(days.fill_sequences)==[1,2]
    assert days.pending.cpu().tolist()==[[0,1]] and ratios(days.accrued)==ratios(old_days.accrued)+[0]
    print(json.dumps({'original_SIM_mature_Taker_source':True,'maturity_before_terminal_checked':True,
        'actual_accrual_day_payment_sequences':[3,4,5],'repeat_no_second_payment':True,
        'fill_pool_terminal_predecessor_checked':True,'capacity_retry_preserves_source':True,
        'same_finality_batch_trade_accrual_day_payment_checked':True,
        'delay_profile_identity':'engineering_only_not_formal_training',
        'elapsed_seconds_with_reference_reads':time.perf_counter()-start,
        'device_peak_allocated_bytes':torch.cuda.max_memory_allocated(),
        'global_Maker_clock_NOE_ledger_day_pipeline_delivered':False}))


if __name__=='__main__':
    main()
