"""实际SIM Cost/Cash切片：原提交before冷输入，after为独立CPU预期。

目录沿所有费用块连续承接；非费用事件由原CPU场景产生，不冒称GPU全根。
"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
import importlib.util
import json
import math
import sys
import time
sys.path=[p for p in sys.path if Path(p).resolve()!=Path(__file__).resolve().parent]
import torch

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('sim-system/gpu','execution-normalization/gpu','ledger/gpu',
    'shared-run/gpu','data-pipeline/gpu','model-train/native-inputs/gpu')]
from queues import Numbers
from validate_base_execution import integers,rational,values
from latency import EmpiricalDelaySampler
from feedback import FeedbackEmitter
from projection import Projection
from fields import RecordList,encode_fields
from consumption import PendingRecords,ReceiveGates
from memory import EconomicColumns
from admission import AdmissionLifecycle
from success_pool import SuccessPools
from position_cost import PositionCosts
from order_funds import OrderFunds
from market_economics import UnresolvedMarkets
from resolution import ResolutionSources,MarketResolutions
from settlement_accounting import SettlementCosts,ExternalCashIdentities
from accepted_principal import AcceptedPrincipal
from asset_buy import take_ratio
from closed import ClosedAdmission
from registry import PublicRegistryState,PublicRegistryEvents,FrozenIntentOrders
from public_ledger import PublicLedgerState
from ledger_drain import _owned
from input_account import frozen_account_inputs
from validate_append import _device_scan
from validate_native_market_economics import check as check_economics
from validate_portfolio_economics import check as check_portfolio

spec=importlib.util.spec_from_file_location('settlement_cash_platform',ROOT/'ledger/gpu/platform.py')
platform_module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=platform_module
spec.loader.exec_module(platform_module)

BALANCES=('cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
    'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd')


def exact(value):
    return [Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]


def body(row):return next(iter(row['record'].values()))


def position_key(row):
    position=row['position'];watermark=position['source_watermark']
    return (position['context_reference'],watermark['run_id'],watermark['projection_batch_sequence'],position['record_ordinal'])


def feedback_key(row):
    return (*position_key(row)[:3],body(row)['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence'])


def main(source):
    started=time.perf_counter();run=Numbers.run_exact_batch
    tensor=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device='cuda')
    zero=tensor([0]);yes=tensor([True],torch.bool);no=~yes
    costs=cash_ids=None;previous=None;scans=[];liability_only=False;negative=False;resource_exits=0
    zero_cash_sequence=None;zero_cash_continued=False;association_retry=False;split_economic=False
    committed_positions={}
    identities=sorted({identity for block in source['blocks'] for row in block['records']
        if 'evidence' in body(row) for kind,fact in body(row)['evidence']['SettlementLifecycleV2']['result'].items()
        for identity in [fact['cost_identity' if kind=='Cost' else 'identity'],fact.get('correction_of')]
        if identity is not None})
    slots={identity:index for index,identity in enumerate(identities)}
    assert len(slots)==len(identities)
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    sampler=EmpiricalDelaySampler.build((ROOT/'sim-system/config/latency-profiles'/sha/'profile.json').read_bytes(),
        sha,[source['seed'].encode('ascii')])
    empty=tensor([[]],torch.bool);empty_int=run(lambda:integers([]));empty_ratio=run(lambda:rational([]))
    intents=FrozenIntentOrders(empty,empty_int,empty,empty_ratio,empty,empty)
    for block_index,block in enumerate(source['blocks']):
        before,after=block['before'],block['after'];consumed_rows=block['records']
        assert after['projection']['ledger_entry_seq']-before['projection']['ledger_entry_seq']==len(consumed_rows), (
            source['label'],block_index,'非费用材料跨过了切片边界',before['projection']['ledger_entry_seq'],
            after['projection']['ledger_entry_seq'],[row['ledger_entry_seq'] for row in consumed_rows])
        feedback=feedback_key(consumed_rows[0])
        assert all(feedback_key(row)==feedback for row in consumed_rows)
        # 原同反馈完整来源与实际Ledger提交切片分别保留，不能以切片行数猜Option。
        rows=sorted((row for row in source['source_records'] if feedback_key(row)==feedback),
            key=lambda row:row['position']['record_ordinal'])
        life=body(rows[0]);kind,fact=next(iter(life['evidence']['SettlementLifecycleV2']['result'].items()))
        assert kind in ('Cost','ExternalCash') and len(rows) in (1,2)
        assert len({position_key(row) for row in rows})==len(rows)
        keys=[position_key(row) for row in rows]+[None]*(2-len(rows))
        current_keys={position_key(row) for row in consumed_rows}
        assert current_keys.issubset({key for key in keys if key is not None})
        current_mask=tensor([[key in current_keys for key in keys]],torch.bool)
        split_economic|=len(rows)==2 and keys[1] in current_keys and keys[0] not in current_keys and keys[0] in committed_positions
        condition=fact['settlement_request_id']['condition_id'] if kind=='Cost' else source['initials'][0]['market']['condition_id']
        initial=next(v for v in source['initials'] if v['market']['condition_id']==condition)
        assets=initial['assets'];tokens=[asset['token_id'] for asset in assets]
        assert len(assets)==2 and {a['outcome']['index'] for a in assets}=={0,1}
        token_slots={token:index for index,token in enumerate(tokens)}
        private=life['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
        batch_sequence=rows[0]['position']['source_watermark']['projection_batch_sequence']
        source_ref=life['source_reference']['record']['SettlementLifecycle']
        at=tensor([source_ref['platform_event_at_utc_ms']])
        issuer=run(lambda:replace(FeedbackEmitter.open(1),next_private_sequence=integers([private]),
            next_batch_sequence=integers([batch_sequence])))
        projection=run(lambda:Projection.open(issuer,zero,zero,zero,zero,tensor([0,0]),integers([0]),zero,zero,zero,zero))
        has_correction=tensor([fact['correction_of'] is not None],torch.bool)
        # 两条原NOE表示真实Some(delta)，即使八余额/份额/负债全部为零。
        economic_present=tensor([len(rows)==2],torch.bool)
        source_ordinal=tensor([life['source_order']['ordinal']])
        record_ordinal=run(lambda:integers([rows[0]['position']['record_ordinal']]))
        correction=tensor([slots.get(fact['correction_of'],0)])
        identity=tensor([slots[fact['cost_identity' if kind=='Cost' else 'identity']]])
        balance={'existing':{name+'_delta':'0' for name in BALANCES},
            'settlement_fee_liability_pusd_delta':'0','actual_taker_fee_liability_pusd_delta':'0'}
        balance['existing']['outcome_balance_deltas']=[]
        sequence=0;economic_kind=2 if kind=='ExternalCash' else 0
        if len(rows)==2:
            economic=body(rows[1])['fact'];value=economic['disposition']['AccountTransition']
            name,fields=next(iter(value['transition'].items()))
            economic_kind={'SettlementCostRecognized':0,'SettlementCostPaid':1,'ExternalCashApplied':2}[name]
            balance=value['balance_delta'];sequence=economic['economic_identity']['AccountTransition']['account_transition_seq']
            assert value['economic_effective_at_utc_ms']==source_ref['platform_event_at_utc_ms']
            if economic_kind<2:
                assert fields['cost_identity']==fact['cost_identity'] and fields['settlement_request_id']==fact['settlement_request_id']
                assert value['condition_id']==condition
            else:assert fields['cash_identity']==fact['identity'] and value['condition_id'] is None
        existing=balance['existing'];members=existing['outcome_balance_deltas']
        original_vector=run(lambda:RecordList((tensor([token_slots[m['token_id']] for m in members]),
            rational([m['balance_shares_delta'] for m in members])),tensor([[True]*len(members)],torch.bool)))
        balances=run(lambda:tuple(rational([existing[name+'_delta']]) for name in BALANCES))
        liabilities=run(lambda:(rational([balance['settlement_fee_liability_pusd_delta']]),rational([balance['actual_taker_fee_liability_pusd_delta']])))
        if kind=='Cost':
            request=fact['settlement_request_id']
            assert request['policy_version']=='RESOLUTION_REDEEM_ONLY' and request['condition_id']==condition
            assert request['account']==before['projection']['account_instance_key']
            resolutions=[r for r in before['resolutions'] if r['resolved']['condition_id']==condition]
            assert len(resolutions)==1 and resolutions[0]['resolved']['resolution_identity']==request['resolution_identity']
            projection,batch=run(lambda:projection.settlement_cost(sampler,zero,zero,zero,identity,
                rational([fact['recognized_pusd']]),rational([fact['paid_pusd']]),has_correction,correction,
                tensor([economic_kind]),integers([sequence]),balances,original_vector,original_vector,*liabilities,at,yes,
                tensor([0],torch.int32),economic_present=economic_present,
                source_ordinal=source_ordinal,record_ordinal=record_ordinal))
            expected_life=run(lambda:encode_fields([zero,zero,zero,zero,identity,rational([fact['recognized_pusd']]),
                rational([fact['paid_pusd']]),has_correction,correction]))
        else:
            attribution=['Deposit','ManualAdjustment','UnknownActivity'].index(fact['attribution'])
            projection,batch=run(lambda:projection.external_cash_feedback(sampler,identity,rational([fact['signed_delta_pusd']]),
                tensor([attribution]),has_correction,correction,integers([sequence]),balances,original_vector,original_vector,
                *liabilities,at,yes,tensor([0],torch.int32),economic_present=economic_present,
                source_ordinal=source_ordinal,record_ordinal=record_ordinal))
            expected_life=run(lambda:encode_fields([zero,identity,rational([fact['signed_delta_pusd']]),
                tensor([attribution]),has_correction,correction]))
        assert batch.fault.cpu().tolist()==[0] and batch.emitted.cpu().tolist()==[[True,len(rows)==2]]
        expected=[expected_life]
        if len(rows)==2:
            absent=tensor([-1]) if kind=='ExternalCash' else zero
            expected.append(run(lambda:encode_fields([zero,absent,at,tensor([economic_kind]),absent,absent,identity,
                *balances,original_vector,*liabilities])))
            liability_only|=all(Fraction(existing[n+'_delta'])==0 for n in BALANCES) and not members and (
                Fraction(balance['settlement_fee_liability_pusd_delta'])!=0 or Fraction(balance['actual_taker_fee_liability_pusd_delta'])!=0)
        if kind=='ExternalCash':
            if len(rows)==2 and keys[1] in current_keys and Fraction(fact['signed_delta_pusd'])==0:
                assert len(rows)==2 and not members
                assert all(Fraction(existing[n+'_delta'])==0 for n in BALANCES)
                assert Fraction(balance['settlement_fee_liability_pusd_delta'])==0
                assert Fraction(balance['actual_taker_fee_liability_pusd_delta'])==0
                assert sequence==before['projection']['platform_source_prefix']+1
                assert after['projection']['platform_source_prefix']==sequence
                zero_cash_sequence=sequence
            elif len(rows)==2 and keys[1] in current_keys and zero_cash_sequence is not None and not zero_cash_continued:
                assert len(rows)==2 and sequence==zero_cash_sequence+1
                zero_cash_continued=True
        for column,row in enumerate(rows):
            raw=body(row);material=projection.normalizer.bank.semantic[batch.rows[0,column]]
            assert torch.equal(material[:expected[column].size(1)],expected[column][0])
            assert not material[expected[column].size(1):].any().cpu().item()
            order=raw['source_order'];actual_order=projection.normalizer.bank.source_order[batch.rows[0,column]].cpu().tolist()
            original_private=order['sequence']['PrivateFeedback']['private_feedback_sequence']
            assert actual_order==[2,original_private&0xffffffff,original_private>>32,order['ordinal']]
            assert values(batch.record_ordinals)[column]==row['position']['record_ordinal']
            assert values(batch.batch_sequences)[column]==row['position']['source_watermark']['projection_batch_sequence']
            assert values(batch.private_sequences)[column]==original_private
            ref=raw.get('source_reference',raw.get('fact',{}).get('source_reference'))['record']['SettlementLifecycle']
            assert batch.received_at[0,column].cpu().item()==ref['feedback_received_at_utc_ms']
            assert batch.platform_at[0,column].cpu().item()==ref['platform_event_at_utc_ms']
        columns=run(lambda:EconomicColumns.from_projection_batches([batch]))
        baseline=before['projection']['platform_confirmed_projection']
        original_outcomes={v['token_id']:v for v in baseline['outcome_balances']}
        def initialize():
            platform=platform_module.PlatformBalances.open(rational([baseline[n] for n in BALANCES]),
                rational([original_outcomes[token]['balance_shares'] if token in original_outcomes else '0' for token in tokens]),
                tensor([0,0]),integers([before['projection']['platform_source_prefix']]))
            pools=SuccessPools.open(tensor([0,0]),tensor([0,0]),tensor([0,1]),tensor([1,0]),1)
            for trade in before['successful_trades']:
                if trade['fact']['condition_id']==condition:
                    pools,_=pools.add(zero,tensor([token_slots[trade['fact']['token_id']]]),
                        rational([trade['fact']['base_quantity_shares']]),rational([trade['fact']['quote_amount_pusd']]),yes)
            markets=UnresolvedMarkets.open(pools,zero)
            resolved=None
            if kind=='Cost':
                r=resolutions[0]
                sources=ResolutionSources.bind(SimpleNamespace(terminals=[r['resolved']['payout_vector']],
                    canonical_ids=[r['resolved']['resolution_identity']]),tokens)
                resolved=MarketResolutions.open(pools,markets,sources).observe(zero,zero,zero,pools,yes)
                resolved=replace(resolved,maker_category=tensor([r['attribution']=='Maker'],torch.bool),
                    directional_cash=rational([before['directional_cash']]))
                m=next(m for m in before['markets'] if m['condition_id']==condition)
                # 冷起点只取真实before净量与原成功池；after仍仅用于独立预期。
                market_tokens=[asset['token_id'] for asset in m['assets']]
                gross_pair=take_ratio(pools.paired_quantity,zero)
                net_pair=rational([m['paired_shares']])
                gross_orphan=take_ratio(pools.quantity,tensor([0,1]))
                net_orphan=rational([m['orphan_shares'][market_tokens.index(token)] for token in tokens])
                resolved=replace(resolved,redeemed_pair=gross_pair-net_pair,redeemed_orphan=gross_orphan-net_orphan)
                if r['request'] is not None:
                    request=r['request']
                    assert request['settlement_request_id']==fact['settlement_request_id']
                    outcomes=('Registered','Pending','FinalSuccess','FinalFailureNoEffect')
                    observations=('Registered','SubmissionUnknown','ObservationTimeout','QueryUnknown','QueryAcceptedPending',
                        'QueryFinalSuccess','QueryConfirmedNoTransaction','RetryAuthorized','RetryNotAuthorized','TerminalSuccess','TerminalFailureNoEffect')
                    resolved,_=resolved.observe_request(zero,zero,zero,tensor([outcomes.index(request['outcome'])]),
                        tensor([observations.index(request['observation'])]),yes)
                markets=replace(markets,profit=rational([Fraction(m['confirmed_net_pnl_pusd']['numerator'])/
                    Fraction(m['confirmed_net_pnl_pusd']['denominator'])]),maximum_loss=rational([
                        Fraction(m['maximum_confirmed_net_loss_pusd']['numerator'])/Fraction(m['maximum_confirmed_net_loss_pusd']['denominator'])]))
            ledger=AdmissionLifecycle.open(platform,projection.normalizer.bank,columns,
                integers([before['projection']['ledger_entry_seq']]),integers([before['projection']['ledger_local_prefix']]),pools,
                resolutions=resolved,unresolved_markets=markets if kind=='Cost' else None,
                position_costs=PositionCosts.empty(1,max(1,len(before['trades'])),'cuda',known=False),
                order_funds=OrderFunds.empty(1,2,max(1,len(before['trades'])),2,'cuda',known=False),
                accepted_principal=AcceptedPrincipal.open(1,zero,known=False),
                settlement_costs=costs,external_cash=cash_ids,other_cash=rational([before['other_cash']]))
            actual=[t for t in before['trades'] if t['fact']['condition_id']==condition]
            trades=ledger.trade_success.resized(max(ledger.trade_success.has_fact.size(1),len(actual)))
            width=trades.has_fact.size(1);pad=width-len(actual)
            trades=replace(trades,has_fact=tensor([[True]*len(actual)+[False]*pad],torch.bool),
                conditions=tensor([[0]*len(actual)+[-1]*pad]),details_known=tensor([[True]*len(actual)+[False]*pad],torch.bool),
                economic_applied=tensor([[t['economic_applied'] for t in actual]+[False]*pad],torch.bool),
                success_applied=tensor([[t['success_applied'] for t in actual]+[False]*pad],torch.bool),
                immediate_fee=rational([t['actual_net_fee'] for t in actual]+['0']*pad))
            return replace(ledger,trade_success=trades,
                applied=tensor([[key in committed_positions for key in keys]],torch.bool),
                applied_sequences=integers([committed_positions.get(key,0) for key in keys]),maker_cash=rational([
                Fraction(before['cash']['maker_available_pusd'])+Fraction(before['cash']['maker_reserved_pusd'])]))
        if costs is None:
            # 绑定在原开户前，所有Cost/Cash材料都参与此序列；不从净余额推身份历史。
            assert block_index==0 and before['costs']==[]
            costs=run(lambda:SettlementCosts.empty(1,1,'cuda',known=True))
            cash_ids=run(lambda:ExternalCashIdentities.empty(1,1,'cuda',known=True))
        ledger=run(initialize)
        old=run(lambda:_owned(ledger,ledger))
        visible=rows[0]['visibility']['Feedback']['available_at_utc_ms']
        delivery=rows[0]['visibility']['Feedback']['delivery_sequence']
        pending=run(lambda:PendingRecords.from_columns(batch.emitted&current_mask,batch.received_at,batch.private_sequences,
            Numbers.ExactInt.from_i64_tensor(projection.normalizer.bank.source_order[batch.rows.flatten(),3]),
            batch.rows,integers([delivery-1])))
        state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),
            ClosedAdmission.empty(columns),ledger)
        gates=ReceiveGates.unset(1).set_receive(tensor([visible]),yes).set_public(tensor([visible]),yes)
        state,_=run(lambda:state.receive_first(projection.normalizer.bank,gates))
        # 一个失败候选不能先提交费用、现金或平台经济前缀。
        damaged=replace(state.ledger,fault=tensor([1],torch.int32))
        rejected,_=run(lambda:damaged.apply_record(zero,yes,tensor([visible]),yes))
        assert torch.equal(rejected.applied,damaged.applied)
        assert exact(rejected.maker_cash)==exact(damaged.maker_cash)
        assert values(rejected.platform.platform_source_prefix)==values(damaged.platform.platform_source_prefix)
        if kind=='ExternalCash' and block_index==0:
            # 原首条Cash尚无公开身份：用同一许可张量等待Evidence后重试Economic。
            # 该切片没有Resolution，避免分支先分配新Tensor掩盖caller被原地修改。
            assert state.ledger.resolutions is None and len(rows)==2
            permit=tensor([True],torch.bool)
            delayed,consumed=run(lambda:state.ledger.apply_record(tensor([1]),permit,tensor([visible]),permit))
            assert permit.cpu().tolist()==[True] and consumed.cpu().tolist()==[False]
            assert torch.equal(delayed.applied,state.ledger.applied)
            assert values(delayed.platform.platform_source_prefix)==values(state.ledger.platform.platform_source_prefix)
            prepared,consumed=run(lambda:delayed.apply_record(zero,permit,tensor([visible]),permit))
            assert permit.cpu().tolist()==[True] and consumed.cpu().tolist()==[True]
            resumed,consumed=run(lambda:prepared.apply_record(tensor([1]),permit,tensor([visible]),permit))
            assert permit.cpu().tolist()==[True] and consumed.cpu().tolist()==[True]
            assert values(resumed.platform.platform_source_prefix)==[after['projection']['platform_source_prefix']]
            assert values(resumed.current_entry_sequence)==[after['projection']['ledger_entry_seq']]
            assert exact(resumed.platform.balances)==[Fraction(after['projection']['platform_confirmed_projection'][n]) for n in BALANCES]
            association_retry=True
        state,scan=_device_scan(state,current_mask,'cost_cash_'+str(block_index))
        scans.append(scan);resource_exits+=scan['resource_retries'];ledger=state.ledger
        assert ledger.fault.cpu().tolist()==[0], (source['label'],block_index,'ledger fault',ledger.fault.cpu().tolist())
        assert not (state.waiting.received&~state.waiting.consumed).any().cpu().item(), (
            source['label'],block_index,'waiting',values(ledger.platform.platform_source_prefix),sequence,
            state.waiting.received.cpu().tolist(),state.waiting.consumed.cpu().tolist())
        assert values(ledger.current_entry_sequence)==[after['projection']['ledger_entry_seq']]
        assert values(ledger.platform.platform_source_prefix)==[after['projection']['platform_source_prefix']]
        assert exact(ledger.platform.balances)==[Fraction(after['projection']['platform_confirmed_projection'][n]) for n in BALANCES]
        assert exact(ledger.maker_cash)==[Fraction(after['cash']['maker_available_pusd'])+Fraction(after['cash']['maker_reserved_pusd'])]
        assert exact(ledger.other_cash)==[Fraction(after['other_cash'])]
        if kind=='Cost':
            assert exact(ledger.resolutions.directional_cash)==[Fraction(after['directional_cash'])]
            m=next(m for m in after['markets'] if m['condition_id']==condition)
            expected_profit=Fraction(m['confirmed_net_pnl_pusd']['numerator'])/Fraction(m['confirmed_net_pnl_pusd']['denominator'])
            expected_max=Fraction(m['maximum_confirmed_net_loss_pusd']['numerator'])/Fraction(m['maximum_confirmed_net_loss_pusd']['denominator'])
            assert exact(ledger.unresolved_markets.profit)==[expected_profit]
            assert exact(ledger.unresolved_markets.maximum_loss)==[expected_max]
        known=ledger.settlement_costs.read(zero[:,None],yes[:,None])
        assert known[2].cpu().tolist()==[True]
        expected_rec=sum((Fraction(c['fact']['recognized_pusd']) for c in after['costs']),Fraction(0))
        expected_paid=sum((Fraction(c['fact']['paid_pusd']) for c in after['costs']),Fraction(0))
        assert exact(known[0])==[expected_rec] and exact(known[1])==[expected_paid]
        for expected_cost in after['costs']:
            bound=ledger.settlement_costs.occupied & (ledger.settlement_costs.identities==slots[expected_cost['fact']['cost_identity']])
            assert bound.sum().cpu().item()==1
            assert ledger.settlement_costs.maker_category[bound].cpu().tolist()==[expected_cost['attribution']=='Maker']
        negative|=kind=='Cost' and Fraction(fact['recognized_pusd'])<0
        scopes=tensor([[[0],[-1]]]);present=scopes>=0;applicable=tensor([[True,True]],torch.bool)
        cutoff=max(visible,after['projection'].get('available_through_utc_ms') or visible)
        frozen=run(lambda:state.freeze_account(intents,tensor([cutoff])))
        economic_args=dict(economic_prefix_known=yes,success_prefix_known=yes,settlement_prefix_known=yes)
        packed=run(lambda:frozen_account_inputs(frozen,tensor([[[],[]]],torch.bool),applicable,applicable,
            scope_conditions=scopes,scope_present=present,**economic_args))
        if kind=='Cost':check_economics(packed,m)
        # 此实际冷切片未保留开户起全部Economic，不能宣称WCM已知；other由真实before连续推进。
        check_portfolio(packed,after['portfolio_reference'],wcm_known=False)
        if kind=='Cost' and block_index==0:
            # 同一原已成熟市场的old消费布局；不声称重新选择了真实Root。
            old_scopes=tensor([[[-1],[0]]]);old_present=old_scopes>=0
            old_only=run(lambda:frozen_account_inputs(frozen,tensor([[[],[]]],torch.bool),applicable,
                tensor([[False,True]],torch.bool),scope_conditions=old_scopes,scope_present=old_present,**economic_args))
            assert old_only.slow[0,356:364].cpu().tolist()==[0.]*8
            expected_counts=[triple for state in range(6) for triple in (
                math.log1p(float(state==('Unresolved','ResolvedNoCurrentPayout','ReceivableNotRequested',
                    'RequestPending','Redeemed','RequestFailedNoEffect').index(m['settlement_state']))),1.,1.)]
            torch.testing.assert_close(old_only.slow[0,364:382].cpu(),torch.tensor(expected_counts),rtol=1e-6,atol=1e-7)
        unpaid=expected_rec-expected_paid
        triple=[math.copysign(math.log1p(abs(float(unpaid))),float(unpaid)),1.,1.]
        torch.testing.assert_close(packed.slow[0,296:299].cpu(),torch.tensor(triple),rtol=1e-6,atol=1e-7)
        assert packed.slow[0,311:314].cpu().tolist()==[0.,1.,1.]
        assert packed.provided[0,296:299].all().cpu().item() and packed.provided[0,311:314].all().cpu().item()
        # 缺Cost历史只令非空scope Unknown；空scope Known0，当前不存在仍NA。
        missing=replace(frozen,ledger=replace(ledger,settlement_costs=replace(ledger.settlement_costs,known=no)))
        unknown=run(lambda:frozen_account_inputs(missing,tensor([[[],[]]],torch.bool),applicable,applicable,
            scope_conditions=scopes,scope_present=present,**economic_args))
        assert unknown.slow[0,296:299].cpu().tolist()==[0.,0.,1.]
        assert unknown.slow[0,311:314].cpu().tolist()==[0.,1.,1.]
        if kind=='Cost':
            check_economics(unknown,m,profit_known=False)
            if block_index==0 and resolutions[0]['request'] is None:
                # 原late切片的Resolved无Request：应收分支缺成功历史必须Unknown。
                incomplete_args=dict(economic_args,success_prefix_known=no)
                incomplete=run(lambda:frozen_account_inputs(frozen,tensor([[[],[]]],torch.bool),applicable,applicable,
                    scope_conditions=scopes,scope_present=present,**incomplete_args))
                assert incomplete.slow[0,356:364].cpu().tolist()==[0.]*6+[0.,1.]
                assert incomplete.slow[0,364:382].cpu().tolist()==[0.,1.,1.]*6
            # 缺结算前缀时，不把缺目录当已知Unresolved；空old仍Known0。
            absent_resolution=replace(frozen,ledger=replace(frozen.ledger,resolutions=None))
            unavailable=run(lambda:frozen_account_inputs(absent_resolution,tensor([[[],[]]],torch.bool),applicable,applicable,
                scope_conditions=scopes,scope_present=present,**economic_args))
            assert unavailable.slow[0,284:293].cpu().tolist()==[0.,0.,1.]*3
            assert unavailable.slow[0,356:364].cpu().tolist()==[0.]*6+[0.,1.]
        na=run(lambda:frozen_account_inputs(frozen,tensor([[[],[]]],torch.bool),applicable,
            tensor([[False,True]],torch.bool),scope_conditions=scopes,scope_present=present,**economic_args))
        assert na.slow[0,296:299].cpu().tolist()==[0.,0.,0.]
        if kind=='Cost':assert na.slow[0,356:364].cpu().tolist()==[0.]*8
        if previous is not None:
            assert exact(previous[0].ledger.settlement_costs.recognized)==previous[1]
            assert exact(previous[0].ledger.maker_cash)==previous[2]
        assert exact(old.settlement_costs.recognized)==exact(costs.recognized)
        grown=ledger.resized_settlement_identities(costs=ledger.settlement_costs.occupied.size(1)+1,
            cash=ledger.external_cash.occupied.size(1)+1)
        assert exact(grown.settlement_costs.read(zero[:,None],yes[:,None])[0])==[expected_rec]
        assert exact(grown.maker_cash)==exact(ledger.maker_cash)
        for name in ('identities','conditions','resolutions','policies','source_orders','maker_category','occupied'):
            old_width=ledger.settlement_costs.occupied.size(1)
            assert torch.equal(getattr(grown.settlement_costs,name)[:,:old_width],getattr(ledger.settlement_costs,name))
        for name in ('identities','occupied','maker_category'):
            old_width=ledger.external_cash.occupied.size(1)
            assert torch.equal(getattr(grown.external_cash,name)[:,:old_width],getattr(ledger.external_cash,name))
        previous=(frozen,exact(ledger.settlement_costs.recognized),exact(ledger.maker_cash))
        costs,cash_ids=ledger.settlement_costs,ledger.external_cash
        for row in consumed_rows:
            committed_positions[position_key(row)]=row['ledger_entry_seq']
        print(json.dumps({'stage':'cost_cash_block','label':source['label'],'completed':block_index+1,
            'total':len(source['blocks']),'elapsed_seconds':time.perf_counter()-started}),file=sys.stderr,flush=True)
    assert source['label']=='external-cash' or liability_only
    if source['label']=='settlement-case-8':assert negative and resource_exits>0
    if source['label']=='settlement-case-0':assert split_economic
    if source['label']=='external-cash':
        assert resource_exits>0 and zero_cash_sequence is not None and zero_cash_continued and association_retry and split_economic
    print(json.dumps({'status':'PASS','label':source['label'],'actual_fee_blocks':len(source['blocks']),
        'liability_only_economic':liability_only,'negative_correction':negative,'resource_exits':resource_exits,
        'zero_cash_economic_continued':zero_cash_continued,'same_permission_association_retry':association_retry,
        'actual_split_Evidence_then_Economic_slices':split_economic,
        'scans':scans,'seconds':time.perf_counter()-started}))


if __name__=='__main__':main(json.load(sys.stdin))
