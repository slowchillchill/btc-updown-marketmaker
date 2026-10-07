"""原Native实际consume/encode冻结输出的定向读面核对。

这里只核开户后已公开但未进Ledger市场图的真实前缀，不以逐前缀冷初始化
冒充连续GPU账户。原post_only夹具只作为公共来源，非B Actor动作支持。
"""
from pathlib import Path
from types import SimpleNamespace
import json
import sys
import torch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'sim-system/gpu'), str(ROOT/'ledger/gpu'),
               str(ROOT/'data-pipeline/gpu'), str(ROOT/'shared-run/gpu'), str(Path(__file__).resolve().parent)]
from queues import Numbers
from validate_base_execution import integers, rational
from position_cost import PositionCosts
from projected_markets import ProjectedMarkets
from input_positions import position_inputs, write_position_fields
from input_account import account_inputs_at_prefix
from reservation import ReservationBook
from trade_success import TradeSuccess
from registry import PublicRegistryState, PublicRegistryEvents, FrozenIntentOrders


def validate(groups):
    group = next(group for group in groups if group['current'] is not None
                 and not group['frozen_account_source']['ledger']['market_projections'])
    source = group['frozen_account_source']
    condition = group['current']
    market = next(m for m in source['economics']['markets'] if m['condition_id'] == condition)
    assets = market['assets']
    assert len(assets) == 2 and {a['outcome']['index'] for a in assets} == {0, 1}
    platform = source['ledger']['platform_confirmed_projection']
    balances = {item['token_id']: item['balance_shares'] for item in platform['outcome_balances']}
    assert all(token in {a['token_id'] for a in assets} for token in balances)
    # 这个真实公开前缀未发生任何市场账本提交。冷资产已知、成本未安装相互独立。
    ledger = SimpleNamespace(
        fault=torch.zeros(1, dtype=torch.int32, device='cuda'),
        platform=SimpleNamespace(asset_roots=torch.zeros(2, dtype=torch.int64, device='cuda'),
            outcomes=rational([balances.get(a['token_id'], '0') for a in assets])),
        success_pools=SimpleNamespace(condition_slots=torch.zeros(2, dtype=torch.int64, device='cuda')),
        position_costs=PositionCosts.empty(1, 1, 'cuda', known=False),
        projected_markets=ProjectedMarkets.open(torch.tensor([0], device='cuda'),
            torch.zeros((1, 1), dtype=torch.bool, device='cuda'), torch.ones(1, dtype=torch.bool, device='cuda')))
    inputs = Numbers.run_exact_batch(lambda:position_inputs(ledger,
        torch.tensor([[[0], [-1]]], device='cuda'), torch.tensor([[[True], [False]]], device='cuda'),
        torch.ones((1, 2), dtype=torch.bool, device='cuda'),
        torch.tensor([a['outcome']['index'] for a in assets], device='cuda')))
    assert inputs.fault.cpu().tolist() == [0]
    encoded = write_position_fields(torch.zeros((1, 433), device='cuda'), inputs)
    expected = torch.tensor(group['rows'][0]['slow'][54:78], dtype=torch.float32)
    torch.testing.assert_close(encoded[0, 54:78].cpu(), expected, rtol=0, atol=0)
    assert encoded[0, 54:78].cpu().tolist() == [0., 1., 1.]*8
    # 原Root实际绑定产生冷公开前缀目录，不从expected current/old反推选择。
    catalog = {entry['market']['condition_id']:entry for g in groups for entry in g['bound_markets']}
    identities = sorted(catalog)
    slots = {identity:index for index,identity in enumerate(identities)}
    visibility = torch.tensor([[identity in {entry['market']['condition_id'] for entry in g['bound_markets']}
        for identity in identities] for g in groups],dtype=torch.bool,device='cuda')
    times = torch.tensor([[catalog[i]['market']['window']['start_at_utc_ms'],
        catalog[i]['market']['window']['end_at_utc_ms']] for i in identities],device='cuda')
    class BindingPrefix:
        condition_order = torch.arange(len(identities),device='cuda')
        def frame(self,prefixes):
            return SimpleNamespace(admitted=visibility[prefixes],
                prefix_fault=torch.zeros_like(prefixes,dtype=torch.int32),
                lifecycle=SimpleNamespace(fault=torch.zeros((len(prefixes),len(identities)),dtype=torch.int32,device='cuda')))
    market_prefix = BindingPrefix()
    market_prefix.times = times
    zero = integers([])
    ratio = type(rational(['0']))(zero,zero.constant(1))
    empty = torch.zeros((1,0),dtype=torch.bool,device='cuda')
    intents = FrozenIntentOrders(empty,zero,empty,ratio,empty,empty)
    registry = PublicRegistryState(PublicRegistryEvents.empty(1,'cuda'))
    checked = 0
    for index,g in enumerate(groups):
        original = g['frozen_account_source']
        if original['ledger']['market_projections'] or not g['rows']:
            continue
        cash = original['economics']['cash']
        cutoff = torch.tensor([g['cutoff']['decision_time']['logical_time_utc_ms']],device='cuda')
        ledger.reservations = ReservationBook.empty(1)
        ledger.trade_success = TradeSuccess.open(1,1,'cuda')
        frozen = SimpleNamespace(ledger=ledger,cutoff=cutoff,fault=ledger.fault,
            orders=registry.freeze_orders(intents,cutoff),order_sources=registry.freeze_order_sources(intents,cutoff),
            funds=SimpleNamespace(**{name:rational([cash[source]]) for name,source in [
                ('actual_available','actual_available_pusd'),('maker_available','maker_available_pusd'),
                ('actual_reserved','actual_reserved_pusd'),('maker_reserved','maker_reserved_pusd')]}),
            pending_noe_count=integers([original['pending_noe_record_count']]),cancellations=None)
        packed = Numbers.run_exact_batch(lambda:account_inputs_at_prefix(frozen,market_prefix,
            torch.arange(len(identities),device='cuda'),torch.tensor([index],device='cuda'),
            torch.tensor([-10_000],device='cuda'),torch.empty((1,0),dtype=torch.int64,device='cuda'),
            empty,torch.tensor([a['outcome']['index'] for a in assets],device='cuda')))
        expected_current = -1 if g['current'] is None else slots[g['current']]
        assert packed.scopes.current_market.cpu().tolist() == [expected_current]
        assert packed.inputs.fault.cpu().tolist() == [0]
        expected = torch.tensor(g['rows'][0]['slow'],dtype=torch.float32)
        torch.testing.assert_close(packed.inputs.slow[0,:78].cpu(),expected[:78],rtol=0,atol=0)
        checked += 1
    assert checked >= 2
    return {'original_NativeInputRoot_encoder_invoked': True,
        'actual_public_bound_prefix_scope_selector_and_native_0_to_77_checked': checked,
        'same_ActionBeforeRead_source_used': True,
        'public_current_without_Ledger_market_cold_cost_unknown_still_Known_zero_checked': True,
        'checked_native_fields': [0, 77], 'continuous_GPU_account_or_full_slow433_checked': False}


if __name__ == '__main__':
    groups=json.loads(Path(sys.argv[1]).read_text())
    result=validate(groups)
    from validate_original_commitments import validate_commitments
    result.update(validate_commitments(groups))
    print(json.dumps(result,sort_keys=True))
