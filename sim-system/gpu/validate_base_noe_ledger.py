"""设备请求/订单/行情效果自然批至同一公开账户；原值仅初始化开户并核末端。

原场景提供合法关闭前沿，不提供逐条Ledger尝试顺序；当前不是全根自动时钟。
"""
from pathlib import Path
from fractions import Fraction
import importlib.util
import sys
import torch
from queues import Numbers
from validate_base_execution import integers,rational,values


def check(source,noe,batches,order,receipt,target,cancel_batch,*,return_state=False):
    root=Path(__file__).resolve().parents[2]
    sys.path[:0]=[str(root/'ledger/gpu'),str(root/'shared-run/gpu')]
    from memory import EconomicColumns
    from admission import AdmissionLifecycle
    from closed import ClosedAdmission
    from position_cost import PositionCosts
    from order_funds import OrderFunds
    from success_pool import SuccessPools
    from market_economics import UnresolvedMarkets
    from public_ledger import PublicLedgerState
    from registry import PublicRegistryState,PublicRegistryEvents,FrozenIntentOrders
    from consumption import PendingRecords,ReceiveGates
    from transport import compact_committed_batches
    from validate_order_funds import check as check_funds
    from validate_market_economics import check as check_markets
    from validate_market_account import origin as market_origin,check as check_market_account
    run=Numbers.run_exact_batch
    tensor=lambda value,dtype=torch.int64:torch.tensor(value,dtype=dtype,device='cuda')
    zero=tensor([0])
    active=tensor([True],torch.bool)
    # 原开户后、首次经营前的公开账本，只安装一次。
    initial=source['opening_ledger']['projection']
    opening=initial['platform_confirmed_projection']
    names=['cash_pusd','available_pusd','principal_encumbrance_pusd','immediate_taker_fee_encumbrance_pusd',
        'maker_rebate_receivable_pusd','taker_rebate_receivable_pusd',
        'maker_rebate_locked_remainder_pusd','taker_rebate_locked_remainder_pusd']
    assert not initial['market_projections'] and not opening['outcome_balances']
    assert not source['opening_ledger']['order_funds']
    spec=importlib.util.spec_from_file_location('gpu_dynamic_base_ledger_platform',root/'ledger/gpu/platform.py')
    module=importlib.util.module_from_spec(spec)
    sys.modules[spec.name]=module
    spec.loader.exec_module(module)
    platform=run(lambda:module.PlatformBalances.open(rational([opening[name] for name in names]),
        rational(['0','0']),tensor([0,0]),integers([initial['platform_source_prefix']])))
    (first,),_=run(lambda:compact_committed_batches([batches[0]]))
    columns=run(lambda:EconomicColumns.from_projection_batches([first]))
    width=columns.row_refs.size(1)
    pools=run(lambda:SuccessPools.open(tensor([0,0]),tensor([0,0]),tensor([0,1]),tensor([1,0]),1))
    admission=run(lambda:AdmissionLifecycle.open(platform,noe.normalizer.bank,columns,
        integers([initial['ledger_entry_seq']]),integers([initial['ledger_local_prefix']]),pools,
        position_costs=PositionCosts.empty(1,max(1,width),'cuda'),
        order_funds=OrderFunds.empty(1,max(1,width),max(1,width),width,'cuda'),
        unresolved_markets=UnresolvedMarkets.open(pools,zero),
        market_balance_origin=market_origin(source['opening_ledger'],source['initial'][0]['market']['condition_id'])))
    pending=run(lambda:PendingRecords.from_columns(first.emitted,first.received_at,first.private_sequences,
        Numbers.ExactInt.from_i64_tensor(noe.normalizer.bank.source_order[first.rows.flatten(),3]),first.rows,integers([0])))
    state=PublicLedgerState(pending,PublicRegistryState(PublicRegistryEvents.empty(1,'cuda')),
        ClosedAdmission.empty(columns),admission)
    for batch in batches[1:]:
        state=run(lambda:state.append_batch(noe.normalizer.bank,batch))
    cutoff=tensor([source['prior_closed_at']])
    gates=ReceiveGates.unset(1).set_receive(cutoff,active).set_public(cutoff,active)
    state,delivery=run(lambda:state.receive(noe.normalizer.bank,gates,
        torch.ones_like(state.ledger.columns.enabled)))
    assert delivery.fault.cpu().tolist()==[0]
    assert not (state.waiting.received & ~state.waiting.consumed).any().cpu().item()
    assert delivery.closed.emitted.sum().cpu().item()==len(source['prior_records'])
    expected=source['cancel_ledger_before']['projection']
    for actual,key in [(state.ledger.current_entry_sequence,'ledger_entry_seq'),
        (state.ledger.current_local_prefix,'ledger_local_prefix'),
        (state.ledger.platform.platform_source_prefix,'platform_source_prefix')]:
        assert values(actual)==[expected[key]],key
    def amounts(ratio):
        return [Fraction(n,d) for n,d in zip(values(ratio.numerator),values(ratio.denominator))]
    original=expected['platform_confirmed_projection']
    assert amounts(state.ledger.platform.balances)==[Fraction(original[name]) for name in names]
    assets={row['token_id']:row['balance_shares'] for row in original['outcome_balances']}
    assert amounts(state.ledger.platform.outcomes)==[Fraction(assets.get(token,'0')) for token in ['1','2']]
    assert check_funds(state.ledger,source['cancel_ledger_before'])
    assert check_markets(state.ledger,source['cancel_ledger_before'],source['initial'][0]['market']['condition_id'])
    check_market_account(state.ledger,source['cancel_ledger_before'],source['initial'][0]['market']['condition_id'])
    # 本场景订单来源与receipt来自实际设备请求/接纳，不从oracle派生承诺初始化。
    intents=FrozenIntentOrders(order.present[:,None],order.sequence,active[:,None],target,
        torch.zeros((1,1),dtype=torch.bool,device='cuda'),(receipt.registered|receipt.duplicate)[:,None])
    frozen=run(lambda:state.freeze_account(intents,cutoff))
    assert frozen.fault.cpu().tolist()==[0] and values(frozen.pending_noe_count)==[0]
    assert amounts(frozen.funds.actual_available)==[Fraction(source['cancel_ledger_before']['cash']['actual_available_pusd'])]
    assert amounts(frozen.funds.maker_available)==[Fraction(source['cancel_ledger_before']['cash']['maker_available_pusd'])]
    saved=values(state.ledger.current_entry_sequence)
    state,repeat=run(lambda:state.receive(noe.normalizer.bank,gates,
        torch.ones_like(state.ledger.columns.enabled)))
    assert not repeat.closed.emitted.any().cpu().item() and values(state.ledger.current_entry_sequence)==saved
    assert amounts(frozen.funds.actual_available)==[Fraction(source['cancel_ledger_before']['cash']['actual_available_pusd'])]
    # 同一个请求、发射器、Normalizer和公开账户接实际取消批，不另装取消冷前缀。
    from validate_projection import expected_semantic
    original_cancel={record['position']['record_ordinal']:record for record in source['cancel_records']}
    row_reference={}
    for column,present in enumerate(cancel_batch.emitted.cpu().tolist()[0]):
        if not present:continue
        reference=original_cancel[values(cancel_batch.record_ordinals)[column]]
        assert values(cancel_batch.batch_sequences)[column]==reference['position']['source_watermark']['projection_batch_sequence']
        body=next(iter(reference['record'].values()))
        original_ref=body.get('source_reference') or body['fact']['source_reference']
        category,metadata=next(iter(original_ref['record'].items()))
        assert values(cancel_batch.private_sequences)[column]==metadata['private_feedback_sequence']
        time_name='response_received_at_utc_ms' if category=='DirectResponse' else 'feedback_received_at_utc_ms'
        assert int(cancel_batch.received_at[0,column].cpu())==metadata[time_name]
        row=int(cancel_batch.rows[0,column].cpu())
        semantic=run(lambda:expected_semantic(reference))
        actual=noe.normalizer.bank.semantic[row:row+1]
        width=max(actual.size(1),semantic.size(1))
        assert torch.equal(torch.nn.functional.pad(actual,(0,width-actual.size(1))),
            torch.nn.functional.pad(semantic,(0,width-semantic.size(1))))
        row_reference[row]=reference
    assert len(row_reference)==4
    state=run(lambda:state.append_batch(noe.normalizer.bank,cancel_batch))
    at=tensor([source['cancel_closed_at']])
    gates=gates.set_receive(at,active).set_public(at,active)
    state,delivery=run(lambda:state.receive(noe.normalizer.bank,gates,
        torch.ones_like(state.ledger.columns.enabled)))
    assert delivery.fault.cpu().tolist()==[0]
    actual=[row_reference[row]['position'] for row,present in zip(delivery.closed.record_refs.cpu().tolist()[0],
        delivery.closed.emitted.cpu().tolist()[0]) if present]
    assert actual==[reference['position'] for reference in source['cancel_records']]
    expected=source['cancel_ledger_after']['projection']
    for actual,key in [(state.ledger.current_entry_sequence,'ledger_entry_seq'),
        (state.ledger.current_local_prefix,'ledger_local_prefix'),
        (state.ledger.platform.platform_source_prefix,'platform_source_prefix')]:
        assert values(actual)==[expected[key]],key
    assert not (state.waiting.received & ~state.waiting.consumed).any().cpu().item()
    original=expected['platform_confirmed_projection']
    assert amounts(state.ledger.platform.balances)==[Fraction(original[name]) for name in names]
    assert check_funds(state.ledger,source['cancel_ledger_after'])
    check_market_account(state.ledger,source['cancel_ledger_after'],source['initial'][0]['market']['condition_id'])
    canceled=run(lambda:state.freeze_account(intents,at))
    assert canceled.fault.cpu().tolist()==[0] and canceled.orders.cancellable.cpu().tolist()==[[False]]
    assert amounts(canceled.funds.actual_available)==[Fraction(source['cancel_ledger_after']['cash']['actual_available_pusd'])]
    assert amounts(frozen.funds.actual_available)==[Fraction(source['cancel_ledger_before']['cash']['actual_available_pusd'])]
    if return_state:return state
    return {'natural_dynamic_base_batches_to_single_public_ledger_checked':True,
        'initialization_only_at_original_opening_checked':True,
        'actual_NOE_population':len(source['prior_records']),
        'device_ledger_attempt_order_from_original_reference':False,
        'dynamic_cancel_request_to_same_account_and_old_freeze_checked':True,
        'dynamic_cancel_NOE_population':4,
        'original_public_closure_frontier_supplied':True}
