"""原双腿V4清偿前缀→实际SIM金额→NOE两成员→Ledger共同提交。

原已提交before只冷安装一次；after只作oracle，不代表从开户运行完整1716根。
"""
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace
import importlib.util
import json
import sys
import time
import torch
from validate_base_execution import integers,rational,values,units
from account_state import PlatformAccountState
from queues import Numbers
from latency import EmpiricalDelaySampler


def main():
    source=json.load(sys.stdin)
    started=time.perf_counter()
    root=Path(__file__).resolve().parents[2]
    sys.path.insert(0,str(root/'data-pipeline/gpu'))
    tensor=lambda v,dtype=torch.int64:torch.tensor(v,dtype=dtype,device='cuda')
    zero=tensor([0]);enabled=tensor([True],torch.bool)
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd','maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    before,after=source['before'],source['after']
    balances=before['projection']['platform_confirmed_projection']
    # 原平台向量排序与运行资产坐标分别保留，不能把UP提交顺序当token排序。
    assets=before['markets'][0]['assets']
    tokens=[asset['token_id'] for asset in assets]
    token_slots={token:i for i,token in enumerate(tokens)}
    original_outcomes={item['token_id']:item for item in balances['outcome_balances']}
    def initialize():
        state=PlatformAccountState.open(integers([units(balances['cash_pusd'])]),tensor([0,0]))
        return replace(state,available=integers([units(balances['available_pusd'])]),
            principal=integers([units(balances['principal_encumbrance_pusd'])]),
            fee=integers([units(balances['immediate_taker_fee_encumbrance_pusd'])]),
            rebates=rational([balances[name] for name in names[4:]]),
            outcomes=integers([units(original_outcomes[token]['balance_shares']) for token in tokens]),
            next_sequence=integers([before['projection']['platform_source_prefix']+1]))
    account=Numbers.run_exact_batch(initialize)
    payout_map={item['asset']['token_id']:item['payout_pusd_per_share'] for item in before['resolved']['payout_vector']['payout_vector']}
    account,delta=Numbers.run_exact_batch(lambda:account.redeem_outcomes(tensor([[0,1]]),
        rational([payout_map[token] for token in tokens]),enabled))
    assert delta.emitted.cpu().tolist()==[True] and values(account.outcomes)==[0,0]
    assert values(account.cash)==[units(after['projection']['platform_confirmed_projection']['cash_pusd'])]
    sys.path.insert(0,str(root/'execution-normalization/gpu'))
    from projection import Projection
    from feedback import FeedbackEmitter
    from validate_projection import expected_semantic
    from fields import encode_fields
    from consumption import PendingRecords,ReceiveGates
    original=source['record']['EconomicFactV2']
    private=original['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
    batch_seq=source['position']['source_watermark']['projection_batch_sequence']
    at=original['fact']['disposition']['AccountTransition']['economic_effective_at_utc_ms']
    sha='98fd40f843275db8994a29cec1de6a72f95bbaa4309df9779c7ed1285cdff596'
    sampler=EmpiricalDelaySampler.build((root/'sim-system/config/latency-profiles'/sha/'profile.json').read_bytes(),sha,[b'e'*64])
    emitter=replace(FeedbackEmitter.open(1),next_private_sequence=integers([private]),next_batch_sequence=integers([batch_seq]))
    projection=Projection.open(emitter,zero,zero,zero,zero,tensor([0,0]),integers([0]),zero,zero,zero,zero)
    projection,batch=Numbers.run_exact_batch(lambda:projection.settlement_success(sampler,zero,zero,zero,
        tensor([[0,1]]),tensor([[0,1]]),delta,tensor([at]),enabled,tensor([0],torch.int32)))
    assert batch.emitted.cpu().tolist()==[[True,True]] and batch.fault.cpu().tolist()==[0]
    for column,reference in enumerate([source['request_record'],source]):
        body=next(iter(reference['record'].values()))
        expected=Numbers.run_exact_batch(lambda:expected_semantic(reference,token_slots=token_slots))
        semantic=projection.normalizer.bank.semantic[batch.rows[0,column]]
        assert torch.equal(semantic[:expected.size(1)],expected[0]) and not semantic[expected.size(1):].any().cpu().item()
        assert values(batch.batch_sequences)[column]==reference['position']['source_watermark']['projection_batch_sequence']
        assert values(batch.record_ordinals)[column]==reference['position']['record_ordinal']
        assert values(batch.private_sequences)[column]==body['source_order']['sequence']['PrivateFeedback']['private_feedback_sequence']
        assert projection.normalizer.bank.source_order[batch.rows[0,column],3].cpu().item()==body['source_order']['ordinal']
        record=body.get('source_reference',body.get('fact',{}).get('source_reference'))['record']['SettlementLifecycle']
        assert batch.received_at[0,column].cpu().item()==record['feedback_received_at_utc_ms']
        assert batch.platform_at[0,column].cpu().item()==record['platform_event_at_utc_ms']
    assert values(batch.economic_sequences)[1]==original['fact']['economic_identity']['AccountTransition']['account_transition_seq']
    sequence=original['fact']['economic_identity']['AccountTransition']['account_transition_seq']
    expected_keys=[encode_fields([zero,tensor([9]),integers([private])]),encode_fields([zero,tensor([11]),integers([sequence])])]
    for column,key in enumerate(expected_keys):
        actual=projection.normalizer.bank.logical_keys[batch.rows[0,column]]
        assert torch.equal(actual[:key.size(1)],key[0]) and not actual[key.size(1):].any().cpu().item()
    members=original['fact']['disposition']['AccountTransition']['balance_delta']['existing']['outcome_balance_deltas']
    assert [(v['token_id'],v['balance_shares_delta']) for v in members]==[('3','-5'),('20','-6')]
    sys.path.insert(0,str(root/'ledger/gpu'))
    from memory import EconomicColumns
    from admission import AdmissionLifecycle
    from order_funds import OrderFunds
    from position_cost import PositionCosts
    from success_pool import SuccessPools
    from market_economics import UnresolvedMarkets
    from resolution import ResolutionSources,MarketResolutions
    from closed import ClosedAdmission
    spec=importlib.util.spec_from_file_location('pair_redemption_platform',root/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec);sys.modules[spec.name]=module;spec.loader.exec_module(module)
    columns=EconomicColumns.from_projection_batches([batch])
    def initialize_ledger():
        platform=module.PlatformBalances.open(rational([balances[name] for name in names]),
            rational([original_outcomes[token]['balance_shares'] for token in tokens]),tensor([0,0]),
            integers([before['projection']['platform_source_prefix']]))
        pools=SuccessPools.open(tensor([0,0]),tensor([0,0]),tensor([0,1]),tensor([1,0]),1)
        for trade in before['successful_trades']:
            amount=trade['remaining_principal']
            pools,_=pools.add(zero,tensor([token_slots[trade['fact']['token_id']]]),rational([trade['remaining_shares']]),
                rational([Fraction(amount['numerator'])/Fraction(amount['denominator'])]),enabled)
        market=UnresolvedMarkets.open(pools,zero)
        public_source=SimpleNamespace(terminals=[before['resolved']['payout_vector']],canonical_ids=[before['resolved']['resolution_identity']])
        resolutions=MarketResolutions.open(pools,market,ResolutionSources.bind(public_source,tokens))
        resolutions=resolutions.observe(zero,zero,zero,pools,enabled)
        request=before['request']
        consumed_request=source['request_record']['record']['Evidence']['evidence']['SettlementLifecycleV2']['result']['Request']
        assert request['settlement_request_id']==consumed_request['settlement_request_id']
        assert request['index_sets']==['0x1','0x2']
        outcomes=['Registered','Pending','FinalSuccess','FinalFailureNoEffect']
        observations=['Registered','SubmissionUnknown','ObservationTimeout','QueryUnknown','QueryAcceptedPending',
            'QueryFinalSuccess','QueryConfirmedNoTransaction','RetryAuthorized','RetryNotAuthorized','TerminalSuccess','TerminalFailureNoEffect']
        resolutions,ready=resolutions.observe_request(zero,zero,zero,tensor([outcomes.index(request['outcome'])]),
            tensor([observations.index(request['observation'])]),enabled)
        assert ready.cpu().tolist()==[True]
        resolutions=replace(resolutions,realized_pnl=rational([balances['realized_pnl_pusd']]),directional_cash=rational([before['directional_cash']]))
        costs=PositionCosts.empty(1,2,'cuda')
        costs=replace(costs,active=tensor([[True,True]],torch.bool),conditions=tensor([[0,0]]),assets=tensor([[0,1]]),
            cost=rational([original_outcomes[token]['position_cost_basis_pusd'] for token in tokens]),
            fee=rational([sum((Fraction(trade['fact']['immediate_taker_fee_pusd']) for trade in before['successful_trades']
                if trade['fact']['token_id']==token),Fraction(0)) for token in tokens]))
        ledger=AdmissionLifecycle.open(platform,projection.normalizer.bank,columns,
            integers([before['projection']['ledger_entry_seq']]),integers([before['projection']['ledger_local_prefix']]),pools,
            position_costs=costs,order_funds=OrderFunds.empty(1,2,2,2,'cuda',known=True),resolutions=resolutions)
        # before为成功Request已实际消费、Economic尚未消费的原提交前缀。
        return replace(ledger,applied=tensor([[True,False]],torch.bool),
            applied_sequences=integers([source['request_record']['ledger_entry_seq'],0]),
            maker_cash=rational([Fraction(before['cash']['maker_available_pusd'])+Fraction(before['cash']['maker_reserved_pusd'])]))
    ledger=Numbers.run_exact_batch(initialize_ledger)
    old=ledger
    assert old.resolutions.requests.present.cpu().tolist()==[True]
    assert old.resolutions.requests.outcomes.cpu().tolist()==[2]
    assert old.resolutions.requests.observations.cpu().tolist()==[9]
    assert old.resolutions.requests.policies.cpu().tolist()==[0]
    visible=source['visibility']['Feedback']['available_at_utc_ms']
    delivered=source['visibility']['Feedback']['delivery_sequence']
    pending=PendingRecords.from_columns(batch.emitted,batch.received_at,batch.private_sequences,
        integers([0,1]),batch.rows,integers([delivered-1]))
    gates=ReceiveGates.unset(1).set_receive(tensor([visible]),enabled).set_public(tensor([visible]),enabled)
    pending,closed=Numbers.run_exact_batch(lambda:pending.take_closed(gates))
    assert values(closed.delivered_sequences)==[delivered,delivered]
    waiting=ClosedAdmission.empty(columns).accept(columns,closed)
    original_waiting=waiting
    # 沿真实公共冻结入口消费同一before/after的typed resolution净读面。
    sys.path[:0]=[str(root/'shared-run/gpu'),str(root/'model-train/native-inputs/gpu')]
    from public_ledger import PublicLedgerState
    from registry import PublicRegistryState,PublicRegistryEvents,FrozenIntentOrders
    from input_account import frozen_account_inputs
    from input_success import success_inputs,write_success_fields
    from success_read import net_success
    import math
    empty=tensor([[]],torch.bool);integer=integers([])
    ratio_empty=type(rational(['0']))(integer,integer.constant(1))
    intents=FrozenIntentOrders(empty,integer,empty,ratio_empty,empty,empty)
    registry=PublicRegistryState(PublicRegistryEvents.empty(1,'cuda'))
    scopes=tensor([[[0],[-1]]]);present=tensor([[[True],[False]]],torch.bool)
    applicable=tensor([[True,True]],torch.bool)
    source_sides=[asset['outcome']['index'] for asset in assets]
    outcomes=tensor(source_sides)
    order_members=tensor([[[],[]]],torch.bool)
    def frozen_inputs(current,current_waiting):
        public=PublicLedgerState(pending,registry,current_waiting,current)
        frozen=public.freeze_account(intents,tensor([visible]))
        inputs=frozen_account_inputs(frozen,order_members,applicable,applicable,
            scope_conditions=scopes,scope_present=present,asset_outcomes=outcomes,
            success_prefix_known=enabled,settlement_prefix_known=enabled)
        return frozen,inputs
    before_frozen,before_native=Numbers.run_exact_batch(lambda:frozen_inputs(old,original_waiting))
    ledger,waiting=Numbers.run_exact_batch(lambda:waiting.drain(ledger,torch.ones_like(columns.enabled)))
    assert ledger.fault.cpu().tolist()==[0] and not (waiting.received&~waiting.consumed).any().cpu().item()
    assert values(ledger.current_entry_sequence)==[after['projection']['ledger_entry_seq']]
    assert values(ledger.platform.platform_source_prefix)==[after['projection']['platform_source_prefix']]
    assert values(ledger.current_local_prefix)==[after['projection']['ledger_local_prefix']]
    assert values(ledger.applied_sequences)==[source['request_record']['ledger_entry_seq'],after['projection']['ledger_entry_seq']]
    assert ledger.available_at.cpu().tolist()[0][1]==visible
    ratio=lambda value:[Fraction(n,d) for n,d in zip(values(value.numerator),values(value.denominator))]
    assert ratio(ledger.platform.balances)==[Fraction(after['projection']['platform_confirmed_projection'][name]) for name in names]
    assert ratio(ledger.platform.outcomes)==[Fraction(0),Fraction(0)]
    assert ratio(ledger.position_costs.read().cost)==[Fraction(0)]
    assert ratio(ledger.position_costs.read().fee)==[Fraction(after['projection']['platform_confirmed_projection']['fees_pusd'])]
    assert ratio(ledger.resolutions.realized_pnl)==[Fraction(after['projection']['platform_confirmed_projection']['realized_pnl_pusd'])]
    assert ratio(ledger.maker_cash)==[Fraction(after['cash']['maker_available_pusd'])+Fraction(after['cash']['maker_reserved_pusd'])]
    assert ratio(ledger.resolutions.directional_cash)==[Fraction(after['directional_cash'])]
    assert before['request']==after['request']
    for name in ('present','policies','outcomes','observations'):
        assert torch.equal(getattr(ledger.resolutions.requests,name),getattr(old.resolutions.requests,name))
    read=Numbers.run_exact_batch(lambda:ledger.resolutions.read(ledger.success_pools))
    assert ratio(read.paired_receivable)==[0] and ratio(read.winner_orphan_receivable)==[0]
    assert ratio(old.platform.outcomes)==[Fraction(5),Fraction(6)] and ratio(old.position_costs.read().cost)==[Fraction('6.6')]
    after_frozen,after_native=Numbers.run_exact_batch(lambda:frozen_inputs(ledger,waiting))
    def source_cost(value):return Fraction(value['numerator'])/Fraction(value['denominator'])
    def triple(value):return [math.copysign(math.log1p(abs(float(value))),float(value)),1.,1.]
    for current,reference,native in [(old,before,before_native),(ledger,after,after_native)]:
        expected=reference['markets'][0]
        read=Numbers.run_exact_batch(lambda:net_success(current.success_pools,current.resolutions,prefix_known=enabled))
        assert read.known.cpu().tolist()==[True,True] and read.fault.cpu().tolist()==[0]
        for field,key,first in [('quantity','success_shares',122),('cost','success_principal_cost_pusd',134),('orphan','orphan_shares',149)]:
            # CPU数组沿其assets存储顺序；公共read也沿原运行资产坐标，不先换outcome序。
            original=[(source_cost if field=='cost' else Fraction)(expected[key][position]) for position in range(len(assets))]
            assert ratio(getattr(read,field))==original
            # 此单市场作用域只核消费布局，不冒称Root current/old选择。
            by_side=[original[source_sides.index(side)] for side in range(2)]
            torch.testing.assert_close(native.slow[0,first:first+6].cpu(),torch.tensor([v for q in by_side for v in triple(q)],dtype=torch.float32),rtol=1e-6,atol=1e-7)
        pair=Fraction(expected['paired_shares'])
        assert ratio(read.paired)==[pair,pair]
        torch.testing.assert_close(native.slow[0,146:149].cpu(),torch.tensor(triple(pair)),rtol=1e-6,atol=1e-7)
        assert native.fault.cpu().tolist()==[0] and native.provided[0,122:164].all().cpu().item()
        for start,end in [(128,134),(140,146),(155,164)]:
            assert native.slow[0,start:end].cpu().tolist()==[0.,1.,1.]*((end-start)//3)
    # 缺结算历史不是已知Unresolved；完整空scope仍为Known零。
    unavailable=replace(after_frozen,ledger=replace(ledger,resolutions=None))
    unknown=Numbers.run_exact_batch(lambda:frozen_account_inputs(unavailable,order_members,applicable,applicable,
        scope_conditions=scopes,scope_present=present,asset_outcomes=outcomes,success_prefix_known=enabled))
    for start,end in [(122,128),(134,140),(146,155)]:
        assert unknown.slow[0,start:end].cpu().tolist()==[0.,0.,1.]*((end-start)//3)
    _,old_again=Numbers.run_exact_batch(lambda:frozen_inputs(before_frozen.ledger,original_waiting))
    assert torch.equal(old_again.slow[:,122:164],before_native.slow[:,122:164])
    # 同一固定地址typed resolution图从真实未兑付切到实际兑付；不改历史成功池。
    from ledger_drain import _owned,_pairs,installed_word_capacity
    live=_owned(old.resolutions,ledger.resolutions)
    words=installed_word_capacity(ledger)
    def encode_live():
        return success_inputs(old.success_pools,scopes,present,applicable,outcomes,
            prefix_known=enabled,resolutions=live,settlement_prefix_known=enabled)
    Numbers.device_exact_batch(encode_live,word_capacity=words,device='cuda')
    graph=torch.cuda.CUDAGraph()
    with torch.cuda.graph(graph):
        candidate=Numbers.device_exact_batch(encode_live,word_capacity=words,device='cuda')
    graph.replay()
    assert candidate.fits.cpu().item()
    old_graph=write_success_fields(torch.zeros((1,433),device='cuda'),candidate.value)
    torch.testing.assert_close(old_graph[:,122:164],before_native.slow[:,122:164],rtol=1e-6,atol=1e-7)
    saved_graph=old_graph.clone()
    incoming_resolution=_owned(ledger.resolutions,live)
    with torch.no_grad():
        for target,incoming in _pairs(live,incoming_resolution):target.copy_(incoming)
    graph.replay()
    assert candidate.fits.cpu().item() and candidate.value.fault.cpu().tolist()==[0]
    new_graph=write_success_fields(torch.zeros((1,433),device='cuda'),candidate.value)
    torch.testing.assert_close(new_graph[:,122:164],after_native.slow[:,122:164],rtol=1e-6,atol=1e-7)
    assert torch.equal(old_graph,saved_graph)
    repeated,waiting=Numbers.run_exact_batch(lambda:waiting.drain(ledger,torch.ones_like(columns.enabled)))
    assert values(repeated.current_entry_sequence)==values(ledger.current_entry_sequence)
    print(json.dumps({'actual_V4_two_positive_assets_one_economic_fact_and_original_vector_order_checked':True,
        'actual_Request_Economic_semantic_positions_private_source_times_checked':True,
        'same_NOE_closed_first_visibility_direct_platform_prefix_and_no_double_apply_checked':True,
        'actual_pair_five_maker_and_orphan_one_directional_attribution_checked':True,
        'actual_already_consumed_FinalSuccess_Request_and_sticky_state_cold_prefix_preserved_checked':True,
        'same_public_freeze_Native_net_success_122_to_163_and_missing_settlement_Unknown_checked':True,
        'same_device_typed_resolution_net_success_graph_replay_and_old_frozen_checked':True,
        'cold_actual_committed_before_initialized_once':True,'continuous_full_1716_root_checked':False,
        'settlement_cost_query_retry_consumers_checked':False,'wall_seconds_including_cold_reference':time.perf_counter()-started,
        'peak_allocated_bytes':torch.cuda.max_memory_allocated()}))


if __name__=='__main__':main()
