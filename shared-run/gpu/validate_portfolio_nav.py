"""原Native合法工程场景→真实冷Ledger输入→共享GPU NAV与完整账户捕获。

每个原cutoff只冷安装实际已提交来源；不以原CPU提交序驱动GPU热宿主。
"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
import importlib.util
import json
import sys
import os
import time
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','execution-normalization/gpu','ledger/gpu',
    'data-pipeline/gpu','shared-run/gpu','model-train/native-inputs/gpu')]
from queues import Numbers
from validate_base_execution import integers,rational,values
from fields import encode_fields
from economic import EconomicEvidence
from core import ECONOMIC
from feedback import FeedbackEmitter
from projection import Projection
from memory import EconomicColumns
from admission import AdmissionLifecycle
from success_pool import SuccessPools
from market_economics import UnresolvedMarkets
from resolution import MarketResolutions,ResolutionSources
from settlement_accounting import SettlementCosts,ExternalCashIdentities
from projected_markets import ProjectedMarkets
from rebate_day import RebateDays
from position_cost import PositionCosts
from consumption import PendingRecords
from closed import ClosedAdmission
from registry import PublicRegistryState,PublicRegistryEvents,FrozenIntentOrders
from public_ledger import PublicLedgerState
from public_device_page import PreparedPublicPage,build_public_device_page
from portfolio_nav import PortfolioValuation,read_portfolio_nav
from input_account import account_inputs_at_prefix

spec=importlib.util.spec_from_file_location('nav_ledger_platform',ROOT/'ledger/gpu/platform.py')
platform_module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=platform_module
spec.loader.exec_module(platform_module)
BALANCES=('cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
    'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd')


def exact(value):return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
def amount(value):return Fraction(int(value['numerator']),int(value['denominator'])) if isinstance(value,dict) else Fraction(value)


def cold_ledger(checkpoint,pm):
    """实际来源转换；余额已含截至prefix的delta，applied列只供同前缀读取。"""
    run=Numbers.run_exact_batch
    tensor=lambda x,dtype=torch.int64:torch.tensor(x,dtype=dtype,device='cuda')
    zero=tensor([0]);yes=tensor([True],torch.bool)
    markets=pm.markets;assets=pm.assets;tokens=[a['token_id'] for a in assets]
    conditions={m['condition_id']:i for i,m in enumerate(markets)}
    token_slots={token:i for i,token in enumerate(tokens)}
    owner=tensor([0]*len(assets));condition_slots=[-1]*len(assets);counterpart=[-1]*len(assets)
    for market in range(len(markets)):
        pair=[a for m,a,_ in pm.market_assets if m==market]
        assert len(pair)==2
        for a,b in (pair,pair[::-1]):condition_slots[a]=market;counterpart[a]=b
    pool=run(lambda:SuccessPools.open(owner,tensor(condition_slots),tensor(list(range(len(assets)))),tensor(counterpart),1))
    for source in checkpoint['successful_trades']:
        fact=source['fact'];slot=token_slots[fact['token_id']]
        assert condition_slots[slot]==conditions[fact['condition_id']]
        pool,_=run(lambda:pool.add(tensor([condition_slots[slot]]),tensor([slot]),
            rational([fact['base_quantity_shares']]),rational([fact['quote_amount_pusd']]),yes))
    projection=run(lambda:Projection.open(FeedbackEmitter.open(1),zero,zero,zero,zero,owner,integers([0]),zero,zero,zero,zero))
    batches=[];sequences=[]
    for row in checkpoint['records']:
        body=next(iter(row['record'].values()))
        if 'fact' not in body:continue
        fact=body['fact']
        if 'AccountTransition' not in fact['disposition']:continue
        value=fact['disposition']['AccountTransition'];transition=value['transition']
        if isinstance(transition,dict) and 'Existing' in transition:transition=transition['Existing']
        name,fields=(transition,{}) if isinstance(transition,str) else next(iter(transition.items()))
        tag={'OrderAcceptedEncumbranceApplied':0,'FillFinalityEconomicApplied':6,
            'MakerRebateAccrualFinalized':9,'TakerRebateAccrualApplied':10}[name]
        seq=fact['economic_identity']['AccountTransition']['account_transition_seq'];sequences.append(seq)
        delta=value['balance_delta'];v2=next(iter(row['record']))=='EconomicFactV2'
        balance=delta['existing'] if v2 else delta;members=balance['outcome_balance_deltas']
        assert len(members)<=1
        source_order=body['source_order'];private=source_order['sequence']['PrivateFeedback']['private_feedback_sequence']
        token=token_slots[members[0]['token_id']] if members else 0
        condition=conditions[value['condition_id']]
        entity=0 if tag==9 else json.loads(fields['order_identity' if tag==0 else 'trade_identity'].split(':',1)[1])[-1]
        if isinstance(entity,dict):entity=int(next(iter(next(iter(entity.values())).values())).removeprefix('sim-'))
        def map_original():
            balances=tuple(rational([balance[n+'_delta']]) for n in BALANCES)
            quantity=rational([members[0]['balance_shares_delta'] if members else '0'])
            sequence=integers([seq]);private_sequence=integers([private]);entity_sequence=integers([entity])
            enabled=tensor([bool(members)],torch.bool)
            semantic=encode_fields([zero,tensor([condition]),tensor([value['economic_effective_at_utc_ms']]),
                tensor([tag]),entity_sequence,tensor([fields.get('path_ordinal',0)]),*balances,
                enabled,tensor([token]),quantity,rational([delta['settlement_fee_liability_pusd_delta'] if v2 else '0']),
                rational([delta['actual_taker_fee_liability_pusd_delta'] if v2 else '0'])])
            return EconomicEvidence(encode_fields([zero,tensor([11]),sequence]),
                encode_fields([zero,tensor([1]),sequence]),semantic,
                tensor([[2,private&0xffffffff,private>>32,source_order['ordinal']]]),balances,quantity,
                enabled,yes,tensor([0],torch.int32),sequence,asset_indices=tensor([token]))
        mapped=run(map_original)
        kind,source=next(iter(fact['source_reference']['record'].items()))
        category={'DirectResponse':0,'OrderLifecycle':1,'TradeLifecycle':2,'SettlementLifecycle':3}[kind]
        received=source.get('feedback_received_at_utc_ms',source.get('response_received_at_utc_ms'))
        platform_at=source.get('platform_event_at_utc_ms',source.get('platform_responded_at_utc_ms'))
        assert source['private_feedback_sequence']==private
        projection,batch=run(lambda:projection._finish(projection.emitter,[(mapped,ECONOMIC,integers([private]),
            integers([row['position']['source_watermark']['projection_batch_sequence']]),
            tensor([received]),tensor([category]),tensor([platform_at]),yes)],
            tensor([0],torch.int32),record_ordinal_base=integers([row['position']['record_ordinal']])))
        assert batch.fault.cpu().tolist()==[0]
        batches.append(batch)
    prefix=checkpoint['projection']['platform_source_prefix']
    assert sorted(sequences)==list(range(1,prefix+1)),('完整实际经济来源',sequences,prefix)
    columns=run(lambda:EconomicColumns.from_projection_batches(batches)) if batches else EconomicColumns.empty(1,'cuda')
    baseline=checkpoint['projection']['platform_confirmed_projection']
    outcomes={row['token_id']:row['balance_shares'] for row in baseline['outcome_balances']}
    platform=run(lambda:platform_module.PlatformBalances.open(rational([baseline[n] for n in BALANCES]),
        rational([outcomes.get(token,'0') for token in tokens]),owner,integers([prefix])))
    representatives=tensor([next(a for m,a,_ in pm.market_assets if m==i) for i in range(len(markets))])
    unresolved=run(lambda:UnresolvedMarkets.open(pool,representatives))
    # 这两个实际原场景还没有Resolved/Cost/成熟日金额；从已导出的完整事实核边界。
    assert not checkpoint['resolutions'] and not checkpoint['costs']
    assert all(amount(day['amounts']['confirmed_unpaid_pusd'])==0 for day in checkpoint['rebate_days'])
    resolutions=run(lambda:MarketResolutions.open(pool,unresolved,ResolutionSources.bind(
        SimpleNamespace(terminals=[],canonical_ids=[]),tokens)))
    present={row['condition_id'] for row in checkpoint['projection']['market_projections']}
    ledger=run(lambda:AdmissionLifecycle.open(platform,projection.normalizer.bank,columns,
        integers([checkpoint['projection']['ledger_entry_seq']]),integers([checkpoint['projection']['ledger_local_prefix']]),pool,
        unresolved_markets=unresolved,resolutions=resolutions,
        projected_markets=ProjectedMarkets.open(tensor(list(range(len(markets)))),tensor([[m['condition_id'] in present for m in markets]],torch.bool),yes),
        position_costs=PositionCosts.empty(1,max(1,len(checkpoint['successful_trades'])),'cuda',known=False),
        settlement_costs=SettlementCosts.empty(1,1,'cuda',known=True),
        external_cash=ExternalCashIdentities.empty(1,1,'cuda',known=True),
        rebate_days=RebateDays.empty(tensor([list(range(len(markets)))]),1),
        other_cash=rational([checkpoint['residual']['other_cash_pusd']])))
    cash=checkpoint['cash']
    ledger=run(lambda:replace(ledger,applied=columns.enabled.clone(),
        maker_cash=rational([amount(cash['maker_available_pusd'])+amount(cash['maker_reserved_pusd'])])))
    assert amount(checkpoint['residual']['directional_cash_pusd'])==0
    return ledger


def validate(source,artifact):
    started=time.perf_counter()
    progress_every=max(0,int(os.environ.get('B_GPU_NAV_PROGRESS_EVERY','1')))
    run=Numbers.run_exact_batch
    build_public_device_page(Path(os.environ['B_GPU_COLUMNS_BINARY']),Path(source['compact']),artifact/'public-v3-page')
    prepared=PreparedPublicPage(artifact/'public-v3-page',Path(source['compact']))
    pm,market,_,_=prepared.install()
    yes=torch.ones(1,dtype=torch.bool,device='cuda');zero=torch.zeros(1,dtype=torch.int64,device='cuda')
    blank=torch.empty((1,0),dtype=torch.bool,device='cuda');integer=run(lambda:integers([]));ratio=run(lambda:rational([]))
    intents=FrozenIntentOrders(blank,integer,blank,ratio,blank,blank)
    history={key:yes for key in ('economic_prefix_known','success_prefix_known','settlement_prefix_known',
        'rebate_prefix_known','economic_liability_prefix_known')}
    condition_ids=torch.arange(len(pm.markets),device='cuda')
    order_conditions=torch.empty((1,0),dtype=torch.int64,device='cuda')
    asset_outcomes=torch.tensor([a['outcome']['index'] for a in pm.assets],device='cuda')
    root_started=zero-10_000
    frozen_old=None;old_values=None;old_native=None;old_native_values=None;reports=[]
    total=len(source['checkpoints'])
    for ordinal,checkpoint in enumerate(source['checkpoints'],1):
        notify=progress_every>0 and (ordinal==1 or ordinal==total or ordinal%progress_every==0)
        def progress(stage):
            if notify:print(json.dumps({'stage':stage,'cutoff_index':ordinal,'cutoff_total':total,
                'cutoff':checkpoint['cutoff'],'elapsed_seconds':time.perf_counter()-started}),flush=True)
        progress('cutoff_started')
        ledger=cold_ledger(checkpoint,pm)
        cutoff=torch.tensor([checkpoint['cutoff']],device='cuda');prefix=pm.prefix_at(cutoff)
        quote=run(lambda:PortfolioValuation(pm,[a['token_id'] for a in pm.assets],ledger.success_pools.asset_roots).freeze(prefix,cutoff))
        def nav(args=history):return read_portfolio_nav(ledger,quote,cutoff,**args)
        read=run(nav)
        assert read.fault.cpu().tolist()==[0]
        wanted=[]
        for index,key in enumerate(('conservative_nav_pusd','directional_residual_pusd')):
            expected=checkpoint['economics'][key];known=isinstance(expected,dict) and 'Known' in expected
            assert read.known[0,index].cpu().item()==known,(checkpoint['cutoff'],key,expected)
            if known:assert exact((read.conservative,read.directional)[index])==[amount(expected['Known'])]
            wanted.append(known)
        # Directional不借无关Maker返佣完整性；Conservative确实保留它自己的门。
        missing=run(lambda:nav(dict(history,rebate_prefix_known=~yes)))
        assert not missing.known[0,0].cpu().item() and missing.known[0,1].cpu().item()==wanted[1]
        active=torch.zeros_like(ledger.columns.enabled)
        pending=run(lambda:PendingRecords.from_columns(active,ledger.columns.row_refs,
            integers([0]*active.numel()),integers([0]*active.numel()),ledger.columns.row_refs,integers([0])))
        state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),ClosedAdmission.empty(ledger.columns),ledger)
        account=run(lambda:state.freeze_account(intents,cutoff))
        absent_history=~yes
        rows=checkpoint['native']['rows'] if checkpoint['native'] is not None else []
        role=torch.tensor([{'ContextOnly':0,'DecisionRequired':1}[rows[0]['role']] if rows else -1],device='cuda')
        def encode():return account_inputs_at_prefix(account,market,
            condition_ids,prefix,root_started,order_conditions,blank,asset_outcomes,valuation=quote,
            executed_prefix_known=absent_history,ledger_prefix_known=absent_history,row_role=role,**history)
        native=run(encode)
        assert native.inputs.fault.cpu().tolist()==[0]
        assert native.inputs.provided[0,314:332].all().cpu().item()
        if rows:
            expected=torch.tensor(rows[0]['slow'][314:332],dtype=torch.float32)
            torch.testing.assert_close(native.inputs.slow[0,314:332].cpu(),expected,rtol=1e-6,atol=1e-7)
        Numbers.device_exact_batch(encode,word_capacity=16,device='cuda')
        graph=torch.cuda.CUDAGraph()
        with torch.cuda.graph(graph):captured=Numbers.device_exact_batch(encode,word_capacity=16,device='cuda')
        graph.replay();assert captured.fits.cpu().item()
        torch.testing.assert_close(captured.value.inputs.slow,native.inputs.slow,rtol=0,atol=0)
        torch.testing.assert_close(captured.value.inputs.provided,native.inputs.provided,rtol=0,atol=0)
        # 同图实际old人口缺口分支与已拥有旧冻结互不覆盖。
        account.ledger.projected_markets.known.zero_();graph.replay();assert captured.fits.cpu().item()
        assert captured.value.inputs.slow[0,87:93].cpu().tolist()==[0.,0.,1.]*2
        account.ledger.projected_markets.known.fill_(True)
        if frozen_old is None:
            frozen_old=quote;old_values=(exact(quote.prices),quote.known.clone(),quote.prefixes.clone())
            old_native=native.inputs;old_native_values=(old_native.slow.clone(),old_native.provided.clone())
        assert exact(frozen_old.prices)==old_values[0] and torch.equal(frozen_old.known,old_values[1]) and torch.equal(frozen_old.prefixes,old_values[2])
        assert torch.equal(old_native.slow,old_native_values[0]) and torch.equal(old_native.provided,old_native_values[1])
        reports.append({'cutoff':checkpoint['cutoff'],'prefix':prefix.cpu().tolist(),'NAV_known':wanted,
            'account_same_graph_capture':True,'actual_cold_source_only':True})
        progress('cutoff_finished')
    report={'original_native_NAV_scenario':source['name'],'checkpoints':reports,'continuous_GPU_account':False}
    (artifact/'NAV-report.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report),flush=True)


if __name__=='__main__':validate(json.loads(Path(sys.argv[1]).read_text()),Path(sys.argv[2]))
