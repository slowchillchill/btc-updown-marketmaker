"""原CPU账户经济值与Native全账户投影对照；不把模型scope当全账户人口。"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
import math
import sys
import torch
from queues import Numbers
from validate_base_execution import integers,rational,values

KEYS=('worst_case_maker_nav_pusd','other_account_residual_pusd','other_receivable_pusd','other_liability_pusd')
OFFSETS=(317,323,326,329)


def amount(value):
    return Fraction(value['numerator'])/Fraction(value['denominator']) if isinstance(value,dict) else Fraction(value)


def check(native,reference,*,wcm_known=True):
    for index,(key,offset) in enumerate(zip(KEYS,OFFSETS)):
        value=amount(reference[key]);known=wcm_known or index!=0
        triple=[math.copysign(math.log1p(abs(float(value))),float(value)),1.,1.] if known else [0.,0.,1.]
        torch.testing.assert_close(native.slow[0,offset:offset+3].cpu(),torch.tensor(triple),rtol=1e-6,atol=1e-7)
        assert native.provided[0,offset:offset+3].all().cpu().item()


def checkpoint_reference(checkpoint):
    # SIM的原Ledger读面导出；测试独立沿shared-run::AccountEconomicsReadV1公式求和。
    cash=checkpoint['cash']
    wcm=amount(cash['maker_available_pusd'])+amount(cash['maker_reserved_pusd'])
    for market in checkpoint['markets']:
        wcm+=amount(market['guaranteed_position_value_pusd'])+amount(market['paired_resolution_receivable_pusd'])
        wcm+=sum((amount(row['confirmed_unpaid_pusd']) for row in market['rebates']),Fraction(0))
        wcm-=amount(market['unpaid_taker_fee_pusd'])
        if market['unpaid_settlement_fee_attribution']=={'Known':'Maker'}:
            wcm-=amount(market['unpaid_settlement_fee_pusd'])
    return dict(zip(KEYS,(wcm,amount(checkpoint['other_cash']),Fraction(0),Fraction(0))))


def check_complete(ledger,checkpoint):
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'model-train/native-inputs/gpu'),str(root/'data-pipeline/gpu')]
    from portfolio_economics import read_portfolio_economics
    from input_portfolio import portfolio_inputs
    from input_account import frozen_account_inputs
    from consumption import PendingRecords
    from closed import ClosedAdmission
    from registry import PublicRegistryState,PublicRegistryEvents,FrozenIntentOrders
    from public_ledger import PublicLedgerState
    run=Numbers.run_exact_batch;yes=torch.ones(1,dtype=torch.bool,device='cuda')
    history={key:yes for key in ('economic_prefix_known','success_prefix_known','settlement_prefix_known',
        'rebate_prefix_known','economic_liability_prefix_known')}
    reference=checkpoint_reference(checkpoint)
    read=run(lambda:read_portfolio_economics(ledger,**history))
    assert read.known.cpu().tolist()==[[True]*4] and read.fault.cpu().tolist()==[0]
    assert [Fraction(values(v.numerator)[0],values(v.denominator)[0]) for v in read.amounts]==[reference[k] for k in KEYS]
    active=torch.zeros_like(ledger.columns.enabled);times=torch.zeros_like(ledger.columns.row_refs)
    pending=run(lambda:PendingRecords.from_columns(active,times,integers([0]*active.numel()),
        integers([0]*active.numel()),ledger.columns.row_refs,integers([0])))
    state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),
        ClosedAdmission.empty(ledger.columns),ledger)
    blank=torch.empty((1,0),dtype=torch.bool,device='cuda');empty=run(lambda:integers([]));ratio=run(lambda:rational([]))
    intents=FrozenIntentOrders(blank,empty,blank,ratio,blank,blank)
    cutoff=torch.maximum(ledger.available_through,torch.zeros_like(ledger.available_through))
    frozen=run(lambda:state.freeze_account(intents,cutoff))
    # 模型没有current/old子集，也必须消费完整账户WCM，不能变成cash-only或NA。
    scopes=torch.full((1,2,1),-1,dtype=torch.int64,device='cuda');present=scopes>=0
    known=torch.ones((1,2),dtype=torch.bool,device='cuda');applicable=known.clone();applicable[:,0]=False
    def encode(account,args=history):
        return frozen_account_inputs(account,torch.empty((1,2,0),dtype=torch.bool,device='cuda'),known,applicable,
            scope_conditions=scopes,scope_present=present,**args)
    native=run(lambda:encode(frozen));check(native,reference)
    assert native.fault.cpu().tolist()==[0]
    missing=run(lambda:encode(frozen,dict(history,rebate_prefix_known=~yes)))
    check(missing,reference,wcm_known=False)
    incomplete=replace(frozen,ledger=replace(frozen.ledger,projected_markets=None))
    check(run(lambda:encode(incomplete)),reference,wcm_known=False)
    # 同设备固定读面可捕获；历史门变化只改变Known，旧freeze与已发布投影保持原值。
    live=yes.clone()
    def graph_read():return portfolio_inputs(frozen.ledger,**dict(history,rebate_prefix_known=live))
    Numbers.device_exact_batch(graph_read,word_capacity=8,device='cuda')
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(graph_read,word_capacity=8,device='cuda')
    graph.replay()
    assert captured.fits.cpu().item()
    torch.testing.assert_close(captured.value.fields[0,0].cpu(),native.slow[0,317:320].cpu())
    live.zero_();graph.replay()
    assert captured.fits.cpu().item() and captured.value.fields[0,0].cpu().tolist()==[0.,0.,1.]
    check(native,reference);check(run(lambda:encode(frozen)),reference)
    return reference
