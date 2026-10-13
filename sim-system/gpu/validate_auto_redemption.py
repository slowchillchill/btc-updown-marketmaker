"""原 PR278 真实已提交前缀→SIM Auto→NOE→Ledger数值对照。

SIM冷前缀来自CPU实际材料；另由原NOE按真实received验证Ledger迟确认等待。
本代表不冒称从开户推进完整GPU撮合。
"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
import importlib.util,json,os,sys,time
import torch
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/"ledger/gpu"))
from queues import Numbers
from validate_base_execution import integers,rational,values,units
from account_state import PlatformAccountState
from base_runtime import BaseSimRuntime
from hidden_resolution import TerminalColumns,SourceQualifiedResolutions,ResolutionObligations
from settlement_redemption import RedemptionSources
from latency import EmpiricalDelaySampler
from feedback import FeedbackEmitter
from projection import Projection
from validate_projection import expected_semantic
from consumption import PendingRecords,ReceiveGates
from memory import EconomicColumns
from admission import AdmissionLifecycle
from order_funds import OrderFunds
from position_cost import PositionCosts
from success_pool import SuccessPools
from market_economics import UnresolvedMarkets
from resolution import ResolutionSources,MarketResolutions
from rebate_day import RebateDays
from closed import ClosedAdmission


def check(source,sampler):
    t=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    zero=t([0]);yes=t([True],torch.bool)
    before,after=source['before'],source['after'];references=source['auto_records']
    life=references[0]['record']['Evidence']
    metadata=life['source_reference']['record']['SettlementLifecycle']
    automatic=life['evidence']['SettlementLifecycle']['result']['AutoRedeemApplied']
    at=metadata['platform_event_at_utc_ms'];source_at=automatic['resolution_source_event_at_utc_ms']
    balance=before['projection']['platform_confirmed_projection']
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    assets=before['markets'][0]['assets'];tokens=[v['token_id'] for v in assets]
    slots={token:i for i,token in enumerate(tokens)}
    existing={v['token_id']:v for v in balance['outcome_balances']}
    terminal=before['resolved']['payout_vector']
    columns=TerminalColumns(t([[1,source_at,0,0,0,2],[0,0,-1,-1,3,0]]),t([[0,0],[0,0]]),
        (terminal,),(automatic['resolution_identity'],))
    root=Path(__file__).resolve().parents[2]
    spec=importlib.util.spec_from_file_location('auto_ledger_platform',root/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    def initialize():
        quantities=[existing.get(token,{}).get('balance_shares','0') for token in tokens]
        account=replace(PlatformAccountState.open(integers([units(balance['cash_pusd'])]),t([0,0])),
            available=integers([units(balance['available_pusd'])]),principal=integers([units(balance['principal_encumbrance_pusd'])]),
            fee=integers([units(balance['immediate_taker_fee_encumbrance_pusd'])]),outcomes=integers([units(v) for v in quantities]),
            rebates=rational([balance[name] for name in names[4:]]),next_sequence=integers([before['projection']['platform_source_prefix']+1]))
        obligations=ResolutionObligations.empty(1,1,SimpleNamespace(occupied=t([[False]],torch.bool),capacity=1,sequences=integers([0])))
        obligations=replace(obligations,reference_indices=t([[0]]))
        source_resolutions=replace(SourceQualifiedResolutions.empty(columns,1,1,'cuda'),reference_indices=t([[0]]),
            source_times=t([[source_at]]),present=yes[:,None])
        world=BaseSimRuntime(None,None,None,account,None,SimpleNamespace(maker=SimpleNamespace(market_ids=zero[:,None],market_result=t([[0]]),fault=t([0],torch.int32))),
            source_resolutions=source_resolutions,resolution_obligations=obligations,
            auto_redemption_sources=RedemptionSources.bind(columns,tokens,[0,0],[0,1],1))
        private=metadata['private_feedback_sequence'];batch=references[0]['position']['source_watermark']['projection_batch_sequence']
        emitter=replace(FeedbackEmitter.open(1),next_private_sequence=integers([private]),next_batch_sequence=integers([batch]))
        projection=Projection.open(emitter,zero,zero,zero,zero,t([0,0]),integers([0]),zero,zero,zero,zero)
        return world,projection
    world,projection=Numbers.run_exact_batch(initialize)
    if os.environ.get('B_GPU_AUTO_DEVICE'):
        from auto_redemption_loop import AutoRedemptionDeviceLoop
        auto_loop=AutoRedemptionDeviceLoop()
        def automatic(world,projection,at,*,preceding=None):
            return auto_loop.run(world,projection,t([at]),yes,sampler,preceding=preceding)
    else:
        def automatic(world,projection,at,*,preceding=None):
            return Numbers.run_exact_batch(lambda:world.try_auto_redeem(t([at]),yes,projection,sampler,preceding=preceding))

    # 原before属于触发批已经消费的真实前缀；只承接其位置，不重放前缀账户。
    ordinal=references[0]['position']['record_ordinal']
    previous=SimpleNamespace(emitted=t([[True]*ordinal],torch.bool),
        batch_sequences=integers([references[0]['position']['source_watermark']['projection_batch_sequence']]*ordinal))
    world,projection,batches=automatic(world,projection,at,preceding=(previous,) if ordinal else ())
    assert len(batches)==1
    batch=batches[0]
    assert batch.fault.cpu().tolist()==[0] and batch.emitted.sum(1).cpu().tolist()==[len(references)]
    assert values(world.account.outcomes)==[0,0] and world.resolution_obligations.auto_redeemed.cpu().tolist()==[[True]]
    assert values(world.account.cash)==[units(after['projection']['platform_confirmed_projection']['cash_pusd'])]
    for index,reference in enumerate(references):
        expected=Numbers.run_exact_batch(lambda:expected_semantic(reference,token_slots=slots))
        actual=projection.normalizer.bank.semantic[batch.rows[0,index]]
        assert torch.equal(actual[:expected.size(1)],expected[0]) and not actual[expected.size(1):].any().cpu().item()
        assert values(batch.record_ordinals)[index]==reference['position']['record_ordinal']
        assert values(batch.batch_sequences)[index]==reference['position']['source_watermark']['projection_batch_sequence']
        assert values(batch.private_sequences)[index]==metadata['private_feedback_sequence']
        assert batch.received_at[0,index].cpu().item()==metadata['feedback_received_at_utc_ms']
        body=next(iter(reference['record'].values()))
        assert projection.normalizer.bank.source_order[batch.rows[0,index],3].cpu().item()==body['source_order']['ordinal']
    unchanged=world
    world,projection,repeated=automatic(world,projection,at+1)
    assert not repeated and world is unchanged
    economic=EconomicColumns.from_projection_batches([batch])
    def open_ledger():
        pools=SuccessPools.open(t([0,0]),t([0,0]),t([0,1]),t([1,0]),1)
        for trade in before['successful_trades']:
            cost=trade['remaining_principal']
            pools,_=pools.add(zero,t([slots[trade['fact']['token_id']]]),rational([trade['remaining_shares']]),
                rational([Fraction(cost['numerator'])/Fraction(cost['denominator'])]),yes)
        unresolved=UnresolvedMarkets.open(pools,zero)
        resolutions=MarketResolutions.open(pools,unresolved,ResolutionSources.bind(columns,tokens))
        resolutions=resolutions.observe(zero,zero,zero,pools,yes)
        resolutions=replace(resolutions,realized_pnl=rational([balance['realized_pnl_pusd']]),directional_cash=rational([before['directional_cash']]))
        platform=module.PlatformBalances.open(rational([balance[n] for n in names]),rational([existing.get(token,{}).get('balance_shares','0') for token in tokens]),
            t([0,0]),integers([before['projection']['platform_source_prefix']]))
        costs=PositionCosts.empty(1,2,'cuda')
        costs=replace(costs,active=t([[token in existing for token in tokens]],torch.bool),conditions=t([[0,0]]),assets=t([[0,1]]),
            cost=rational([existing.get(token,{}).get('position_cost_basis_pusd','0') for token in tokens]),
            fee=rational([sum((Fraction(trade['fact']['immediate_taker_fee_pusd']) for trade in before['successful_trades'] if trade['fact']['token_id']==token),Fraction(0)) for token in tokens]))
        days=replace(RebateDays.empty(zero[:,None],1),maker_accrual_finalized=yes[:,None])
        ledger=AdmissionLifecycle.open(platform,projection.normalizer.bank,economic,integers([before['projection']['ledger_entry_seq']]),
            integers([before['projection']['ledger_local_prefix']]),pools,position_costs=costs,rebate_days=days,
            order_funds=OrderFunds.empty(1,2,2,2,'cuda',known=True),unresolved_markets=unresolved,resolutions=resolutions)
        return replace(ledger,maker_cash=rational([Fraction(before['cash']['maker_available_pusd'])+Fraction(before['cash']['maker_reserved_pusd'])]))
    ledger=Numbers.run_exact_batch(open_ledger)
    ordinal_values=integers(projection.normalizer.bank.source_order[batch.rows.flatten(),3].cpu().tolist())
    pending=PendingRecords.from_columns(batch.emitted,batch.received_at,batch.private_sequences,ordinal_values,batch.rows,integers([0]))
    gates=ReceiveGates.unset(1).set_receive(t([metadata['feedback_received_at_utc_ms']]),yes).set_public(t([metadata['feedback_received_at_utc_ms']]),yes)
    pending,closed=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    waiting=ClosedAdmission.empty(economic).accept(economic,closed)
    missing=replace(ledger,resolutions=replace(ledger.resolutions,present=~ledger.resolutions.present))
    missing,unresolved_waiting=Numbers.run_exact_batch(lambda:waiting.drain(missing,torch.ones_like(economic.enabled)))
    assert missing.fault.cpu().tolist()==[0] and not unresolved_waiting.consumed.any().cpu().item()
    missing=replace(missing,resolutions=ledger.resolutions)
    ledger,waiting=Numbers.run_exact_batch(lambda:unresolved_waiting.drain(missing,torch.ones_like(economic.enabled)))
    assert ledger.fault.cpu().tolist()==[0] and waiting.consumed[waiting.received].all().cpu().item(), dict(
        case=source['case'],fault=ledger.fault.cpu().tolist(),lifecycle_fault=ledger.lifecycle.fault.cpu().tolist(),
        schemas=ledger.lifecycle.schemas.cpu().tolist(),conditions=ledger.lifecycle.conditions.cpu().tolist(),
        resolution_ids=ledger.lifecycle.resolution_identities.cpu().tolist(),outcomes=ledger.lifecycle.normalization_outcomes.cpu().tolist(),
        received=waiting.received.cpu().tolist(),consumed=waiting.consumed.cpu().tolist(),waiting_fault=waiting.fault.cpu().tolist(),
        platform_prefix=values(ledger.platform.platform_source_prefix),sequences=values(ledger.columns.sequences),
        predecessor=ledger.resolutions.economic_predecessors(ledger.trade_success,ledger.rebate_days))
    fraction=lambda value:[Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
    expected=after['projection']['platform_confirmed_projection']
    assert fraction(ledger.platform.balances)==[Fraction(expected[n]) for n in names]
    assert fraction(ledger.maker_cash)==[Fraction(after['cash']['maker_available_pusd'])+Fraction(after['cash']['maker_reserved_pusd'])]
    assert fraction(ledger.resolutions.directional_cash)==[Fraction(after['directional_cash'])]
    assert ledger.resolutions.read(ledger.success_pools).settlement_state.cpu().tolist()==[4]
    assert ledger.resolutions.requests.present.cpu().tolist()==[False]
    assert fraction(ledger.resolutions.read(ledger.success_pools).paired_receivable)==[0]
    assert fraction(ledger.resolutions.read(ledger.success_pools).winner_orphan_receivable)==[0]
    assert values(ledger.current_entry_sequence)==[after['projection']['ledger_entry_seq']]
    old=values(ledger.current_entry_sequence);old_cash=fraction(ledger.platform.balances)
    ledger,waiting=Numbers.run_exact_batch(lambda:waiting.drain(ledger,torch.ones_like(economic.enabled)))
    assert values(ledger.current_entry_sequence)==old and fraction(ledger.platform.balances)==old_cash
    return dict(case=source['case'],records=len(references),cash=expected['cash_pusd'],received=metadata['feedback_received_at_utc_ms'],
        zero_holdings_lifecycle=(source['case']=='empty'),zero_cash_burn=(source['case']=='loser'))



def check_original_arrivals(source):
    """原CPU NOE仅机械编码；原公开领取/receiver按真实received推进。

    CPU有限驱动在71000首次看见这批反馈；本代表的GPU公共门按原收到时刻
    领取，first_available由take_closed实际建立，绝不改写原参考的visibility。
    """
    from core import EvidenceBank,NormalizedBatch,EVIDENCE,ECONOMIC,OUT_EVIDENCE,OUT_FACT
    from projection import ProjectionBatch
    from fields import RecordList
    from public_ledger import PublicLedgerState
    from registry import PublicRegistryState,PublicRegistryEvents
    from ledger_drain import PublicLedgerReceiver
    t=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    zero=t([0]);yes=t([True],torch.bool);run=Numbers.run_exact_batch
    assets=source['before']['markets'][0]['assets'];tokens=[a['token_id'] for a in assets]
    token_slots={token:i for i,token in enumerate(tokens)}
    bank=EvidenceBank.empty('cuda');batches=[];auto_rows=[];confirmed_rows=[]
    for reference in source['all_records'][2:]:
        body=next(iter(reference['record'].values()));fact=body.get('fact')
        raw=body.get('source_reference') or fact['source_reference']
        category,metadata=next(iter(raw['record'].items()))
        if fact:
            transition=fact['disposition']['AccountTransition'];delta=transition['balance_delta']
            existing=delta.get('existing',delta);outcome=existing['outcome_balance_deltas']
            economic_sequence=fact['economic_identity']['AccountTransition']['account_transition_seq']
            automatic='AutoRedeemApplied' in transition['transition']
            schema=29 if automatic else 11
            token=outcome[0]['token_id'] if outcome else tokens[0]
        else:
            kind,value=next(iter(body['evidence'].items()));result=value['result']
            schema={'OrderLifecycle':7,'Control':3,'TradeLifecycleV3':8,'SettlementLifecycle':28}.get(kind)
            if schema is None:
                schema=14 if 'MakerAccrualFinalized' in result else 17 if 'Resolved' in result else 9
            detail=result['result'] if kind=='TradeLifecycleV3' else result['Submission']['outcome']['Accepted']['order'] if kind=='Control' else result
            token=detail.get('token_id',tokens[0]);existing=None;outcome=[];economic_sequence=0
        semantic=run(lambda:expected_semantic(reference,token_slots=token_slots,token_index=token_slots[token]))
        private=metadata['private_feedback_sequence'];ordinal=body['source_order']['ordinal']
        source_order=t([[2,private&0xffffffff,private>>32,ordinal]])
        row=bank.kinds.numel()
        incoming=EvidenceBank(t([[row]]),t([[row]]),semantic,t([[0]]),t([ECONOMIC if fact else EVIDENCE]),
            source_order,zero,t([0],torch.int32),t([schema]),frozenset([schema]))
        bank=bank.append(incoming)
        names=['cash_pusd_delta','available_pusd_delta','principal_encumbrance_pusd_delta','immediate_taker_fee_encumbrance_pusd_delta',
            'maker_rebate_receivable_pusd_delta','taker_rebate_receivable_pusd_delta','maker_rebate_locked_remainder_pusd_delta','taker_rebate_locked_remainder_pusd_delta']
        received=metadata.get('feedback_received_at_utc_ms',metadata.get('response_received_at_utc_ms'))
        at=metadata.get('platform_event_at_utc_ms',metadata.get('platform_responded_at_utc_ms'))
        normalized=NormalizedBatch(t([[row]]),t([[OUT_FACT if fact else OUT_EVIDENCE]]),t([[row if fact else -1]]),yes[:,None],t([0],torch.int32))
        vector=(run(lambda:RecordList((t([token_slots[e['token_id']] for e in outcome]),rational([e['balance_shares_delta'] for e in outcome])),t([[True]*len(outcome)],torch.bool)))
            if len(outcome)>1 else None)
        batch=run(lambda:ProjectionBatch(normalized,integers([private]),integers([reference['position']['source_watermark']['projection_batch_sequence']]),
            integers([reference['position']['record_ordinal']]),t([[received]]),t([[row]]),yes[:,None],t([0],torch.int32),
            t([[{'DirectResponse':0,'OrderLifecycle':1,'TradeLifecycle':2,'SettlementLifecycle':3}[category]]]),t([[at]]),
            t([[0 if token==tokens[0] else 1]]),t([[0]]),t([[0]]),t([[0]]),integers([economic_sequence]),
            rational([existing[n] if existing else '0' for n in names]),rational([outcome[0]['balance_shares_delta'] if outcome else '0']),
            t([[token_slots[token]]]),t([[fact is not None]],torch.bool),vector))
        batches.append(batch)
        if schema in (28,29):auto_rows.append(row)
        if schema==8 and result['result']['state']=='Confirmed':confirmed_rows.append((row,received))
    terminal=source['before']['resolved']
    columns=SimpleNamespace(terminals=[terminal['payout_vector']],canonical_ids=[terminal['resolution_identity']])
    root=Path(__file__).resolve().parents[2]
    spec=importlib.util.spec_from_file_location('auto_arrival_platform',root/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    def initialize():
        # 实际opening+InitialMarket已经消费；不安装未来成功/应收/已确认Trade。
        platform=module.PlatformBalances.open(rational(['100','100','0','0','0','0','0','0']),rational(['0','0']),t([0,0]),integers([0]))
        pools=SuccessPools.open(t([0,0]),t([0,0]),t([0,1]),t([1,0]),1)
        markets=UnresolvedMarkets.open(pools,zero)
        economic=EconomicColumns.from_projection_batches(batches)
        # 成本/资金关联使用原Trade目录槽人口，不能用已发生成交笔数代替。
        capacity=max(1,economic.row_refs.size(1))
        ledger=AdmissionLifecycle.open(platform,bank,economic,integers([2]),integers([0]),pools,
            rebate_days=RebateDays.empty(t([[0]]),1),position_costs=PositionCosts.empty(1,capacity,'cuda'),
            order_funds=OrderFunds.empty(1,capacity,capacity,economic.row_refs.size(1),'cuda',known=True),
            unresolved_markets=markets,resolutions=MarketResolutions.open(pools,markets,ResolutionSources.bind(columns,tokens)))
        pending=PendingRecords.from_columns(t([[True]*len(batches)],torch.bool),t([[int(b.received_at[0,0]) for b in batches]]),
            integers([values(b.private_sequences)[0] for b in batches]),integers(bank.source_order[:,3].cpu().tolist()),t([list(range(len(batches)))]),integers([0]))
        return PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),ClosedAdmission.empty(economic),ledger)
    state=run(initialize);receiver=PublicLedgerReceiver(word_capacity=16);proof=[]
    for at in sorted(set(int(batch.received_at[0,0]) for batch in batches)):
        gates=ReceiveGates.unset(1).set_receive(t([at]),yes).set_public(t([at]),yes)
        state,delivery=run(lambda:state.receive_first(bank,gates))
        state,complete=receiver(state,delivery,yes)
        assert complete.cpu().tolist()==[True] and state.ledger.fault.cpu().tolist()==[0]
        if at==67032:
            assert state.waiting.received[0,auto_rows].all().cpu().item()
            assert not state.waiting.consumed[0,auto_rows].any().cpu().item()
            assert any(state.pending_noe.active[0,row].cpu().item() and received==67033 for row,received in confirmed_rows)
            assert not state.ledger.resolutions.auto_redeemed.any().cpu().item()
            assert values(state.ledger.platform.platform_source_prefix)==[6]
            proof.append(dict(at=at,auto_available=state.waiting.available_at[0,auto_rows].cpu().tolist(),prefix=6,waiting=True))
        if at==67033:
            assert state.waiting.consumed[0,auto_rows].all().cpu().item()
            assert state.ledger.resolutions.auto_redeemed.cpu().tolist()==[True]
            assert state.waiting.available_at[0,auto_rows].cpu().tolist()==[67032,67032]
            proof.append(dict(at=at,waiting=False,prefix=values(state.ledger.platform.platform_source_prefix)))
    assert len(proof)==2
    expected=source['after']['projection']['platform_confirmed_projection']
    fraction=lambda value:[Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd']
    assert fraction(state.ledger.platform.balances)[:3]==[Fraction(expected[n]) for n in names[:3]]
    assert fraction(state.ledger.platform.outcomes)==[0,0]
    assert fraction(state.ledger.maker_cash)==[Fraction('95.748')]
    assert fraction(state.ledger.resolutions.directional_cash)==[5]
    return proof


def main():
    source=json.load(sys.stdin);started=time.perf_counter();root=Path(__file__).resolve().parents[2]
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    sampler=EmpiricalDelaySampler.build((root/'sim-system/config/latency-profiles'/sha/'profile.json').read_bytes(),sha,[b'e'*64])
    result=[check(case,sampler) for case in source['cases']]
    arrival=check_original_arrivals(source['cases'][0])
    print(json.dumps(dict(cold_prefix_auto=result,original_arrival_wait=arrival,elapsed_seconds=time.perf_counter()-started)))

if __name__=='__main__':main()
