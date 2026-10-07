"""原CPU Ledger市场投影独立核all-in/计数，含实际非零冷起点和经济先到。"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import math
import sys
import torch
from queues import Numbers
from account_read import MarketBalanceOrigin,read_market_account
from validate_base_execution import integers,rational,values


def origin(checkpoint,condition):
    projection=checkpoint['projection'];prefix=integers([projection['platform_source_prefix']])
    markets=projection['market_projections'];yes=torch.ones(1,dtype=torch.bool,device='cuda')
    if not markets:
        return MarketBalanceOrigin.zero_opening(prefix,known=yes)
    assert {m['condition_id'] for m in markets}=={condition}
    market=markets[0]
    # 公共projection本金/费含PlatformPending；冷经济起点只安装实际平台余额。
    principal=Fraction(market['principal_encumbrance_pusd'])-Fraction(market['platform_confirmation_pending_economic_principal_pusd'])
    fee=Fraction(market['immediate_taker_fee_encumbrance_pusd'])-Fraction(market['platform_confirmation_pending_economic_immediate_fee_pusd'])
    return MarketBalanceOrigin.open(torch.zeros(1,dtype=torch.int64,device='cuda'),
        rational([principal]),rational([fee]),prefix,yes)


def check(ledger,checkpoint,condition):
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'model-train/native-inputs/gpu'),str(root/'data-pipeline/gpu')]
    from input_market_account import market_account_inputs,write_market_account_fields
    market=next((m for m in checkpoint['projection']['market_projections'] if m['condition_id']==condition),None)
    expected=Fraction(0) if market is None else Fraction(market['current_all_in_commitment_pusd'])
    counts=[0,0] if market is None else [market['open_order_count'],market['pending_fill_count']]
    yes=torch.ones(1,dtype=torch.bool,device='cuda');zero=torch.zeros((1,1),dtype=torch.int64,device='cuda')
    read=Numbers.run_exact_batch(lambda:read_market_account(ledger,zero,yes[:,None],history_known=yes))
    actual=[Fraction(n,d) for n,d in zip(values(read.all_in.numerator),values(read.all_in.denominator))]
    assert actual==[expected] and read.known.cpu().tolist()==[True] and read.fault.cpu().tolist()==[0]
    assert read.counts.cpu().tolist()==[counts] and read.counts_known.cpu().tolist()==[True]
    scopes=torch.tensor([[[0],[-1]]],device='cuda');present=scopes>=0;applicable=torch.ones((1,2),dtype=torch.bool,device='cuda')
    encoded=Numbers.run_exact_batch(lambda:market_account_inputs(ledger,scopes,present,applicable,prefix_known=yes))
    slow=write_market_account_fields(torch.zeros((1,433),device='cuda'),encoded)
    def triple(v):return [math.copysign(math.log1p(abs(float(v))),float(v)),1.,1.]
    torch.testing.assert_close(slow[0,81:93].cpu(),torch.tensor([*triple(counts[0]),*triple(counts[1]),0.,1.,1.,0.,1.,1.]),rtol=1e-6,atol=1e-7)
    torch.testing.assert_close(slow[0,95:98].cpu(),torch.tensor(triple(expected)),rtol=1e-6,atol=1e-7)
    assert encoded.fault.cpu().tolist()==[0]
    return expected


def check_nonzero_origin(before,after,reference,condition):
    from ledger_drain import _owned,_pairs,installed_word_capacity
    cold=Numbers.run_exact_batch(lambda:origin(reference['before'],condition))
    assert any(Fraction(n,d)>0 for n,d in zip(values(cold.principal.numerator),values(cold.principal.denominator)))
    # CPU原seq1余额已经包括首个实际Economic；该列仍在GPUapplied历史，不能再加一遍。
    assert values(cold.source_prefix)==[1]
    checkpointed=replace(before,market_balance_origin=cold)
    expected=check(checkpointed,reference['before'],condition)
    frozen=_owned(checkpointed,checkpointed)
    # late Submission只释放对应预留，另一0.25 BeforeSend责任保留。
    resumed=replace(after,market_balance_origin=cold)
    check(resumed,reference['after'],condition)
    grown=resumed.resized_lifecycle(trades=resumed.trades.occupied.size(1)+1)
    assert grown.market_balance_origin is cold and check(grown,reference['after'],condition)==check(resumed,reference['after'],condition)
    # 同一真实经济先到→迟到Submission，用固定活动地址捕获读面；预留释放改变all-in。
    yes=torch.ones(1,dtype=torch.bool,device='cuda');zero=torch.zeros((1,1),dtype=torch.int64,device='cuda')
    words=max(8,installed_word_capacity((checkpointed,resumed)))
    live=_owned(checkpointed,resumed,words)
    def read_live():return read_market_account(live,zero,yes[:,None],history_known=yes)
    Numbers.device_exact_batch(read_live,word_capacity=words,device='cuda')
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        captured=Numbers.device_exact_batch(read_live,word_capacity=words,device='cuda')
    graph.replay()
    before_amount=[Fraction(n,d) for n,d in zip(values(captured.value.all_in.numerator),values(captured.value.all_in.denominator))]
    assert captured.fits.cpu().item() and before_amount==[expected]
    incoming=_owned(resumed,live,words)
    for target,source in _pairs(live,incoming):target.copy_(source)
    graph.replay()
    market=next(m for m in reference['after']['projection']['market_projections'] if m['condition_id']==condition)
    after_amount=[Fraction(n,d) for n,d in zip(values(captured.value.all_in.numerator),values(captured.value.all_in.denominator))]
    assert captured.fits.cpu().item() and captured.value.fault.cpu().tolist()==[0]
    assert captured.value.known.cpu().tolist()==[True] and after_amount==[Fraction(market['current_all_in_commitment_pusd'])]
    assert before_amount!=after_amount
    assert captured.value.counts.cpu().tolist()==[[market['open_order_count'],market['pending_fill_count']]]
    assert check(frozen,reference['before'],condition)==expected
    other=Numbers.run_exact_batch(lambda:origin(reference['after'],condition))
    next_root=replace(resumed,market_balance_origin=other)
    assert next_root.market_balance_origin is not frozen.market_balance_origin
    assert frozen.market_balance_origin is cold and check(frozen,reference['before'],condition)==expected
    unknown=replace(checkpointed,market_balance_origin=None)
    yes=torch.ones(1,dtype=torch.bool,device='cuda');zero=torch.zeros((1,1),dtype=torch.int64,device='cuda')
    read=Numbers.run_exact_batch(lambda:read_market_account(unknown,zero,yes[:,None],history_known=yes))
    assert read.known.cpu().tolist()==[False] and read.counts_known.cpu().tolist()==[True]
    return True
