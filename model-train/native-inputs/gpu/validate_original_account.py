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
               str(ROOT/'data-pipeline/gpu'), str(Path(__file__).resolve().parent)]
from queues import Numbers
from validate_base_execution import integers, rational
from position_cost import PositionCosts
from projected_markets import ProjectedMarkets
from input_positions import position_inputs, write_position_fields


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
    return {'original_NativeInputRoot_encoder_invoked': True,
        'same_ActionBeforeRead_source_used': True,
        'public_current_without_Ledger_market_cold_cost_unknown_still_Known_zero_checked': True,
        'checked_native_fields': [54, 77], 'continuous_GPU_account_or_full_slow433_checked': False}


if __name__ == '__main__':
    print(json.dumps(validate(json.loads(Path(sys.argv[1]).read_text())), sort_keys=True))
