"""真实V4失败记录的成本候选对照；参考尝试仅驱动验证，不是生产采集器。"""
from fractions import Fraction
import json
from pathlib import Path
import sys
import torch

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT/'model-train'), str(ROOT/'sim-system/gpu'), str(ROOT/'ledger/gpu'),
    str(ROOT/'execution-normalization/gpu')]
from queues import Numbers
from position_cost import PositionCosts
from trade_success import TradeSuccess
from validate_base_execution import rational, values, integers
from admission_lifecycle import LifecycleDirectory
from rebate_contribution import RebateContributions


def amounts(value):
    return [Fraction(n, d) for n, d in zip(values(value.numerator), values(value.denominator))]


def main():
    source = json.loads(Path(sys.argv[1]).read_text())
    zero = torch.zeros(1, dtype=torch.int64, device='cuda')
    yes = torch.ones_like(zero, dtype=torch.bool)
    no = ~yes
    costs, trades = Numbers.run_exact_batch(lambda: (
        PositionCosts.empty(1, 1, 'cuda'), TradeSuccess.open(1, 1, 'cuda')))
    directory = Numbers.run_exact_batch(lambda: LifecycleDirectory.empty(1, 1, device='cuda'))
    directory, error = Numbers.run_exact_batch(lambda: directory.set(integers([1]), zero, no, integers([0]), yes))
    assert error.cpu().tolist() == [0]
    contributions = Numbers.run_exact_batch(lambda: RebateContributions.empty(1, 1, 'cuda'))
    unknown = Numbers.run_exact_batch(lambda: PositionCosts.empty(1, 1, 'cuda', known=False).asset_cost(zero))
    assert unknown.known.cpu().tolist() == [False] and unknown.fault.cpu().tolist() == [0]
    prefix = 0
    checked = 0
    for checkpoint in source['checkpoints']:
        body = next(iter(checkpoint['record'].values()))
        after = checkpoint['after']['projection']
        committed = after['platform_source_prefix'] != prefix
        prefix = after['platform_source_prefix']
        name, ordinal, principal, fee = None, 0, '0', '0'
        if 'fact' in body:
            value = body['fact']['disposition']['AccountTransition']
            transition = value['transition']
            if 'Existing' in transition:
                transition = transition['Existing']
            name, fields = (transition, {}) if isinstance(transition, str) else next(iter(transition.items()))
            ordinal = fields.get('path_ordinal', 0)
            delta = value['balance_delta']
            if 'existing' in delta:
                delta = delta['existing']
            principal = str(-Fraction(delta['principal_encumbrance_pusd_delta']))
            fee = str(-Fraction(delta['immediate_taker_fee_encumbrance_pusd_delta']))
        applied = yes if committed and name == 'FillFinalityEconomicApplied' else no
        compensated = yes if committed and name == 'FillFinalityCompensationApplied' else no
        # 原金额来自真实NOE delta；FAILED ordinal不决定之前存储的应用项。
        costs = Numbers.run_exact_batch(lambda: costs.transition(zero, zero, zero,
            torch.full_like(zero, ordinal), rational([principal]), rational([fee]), applied, compensated))
        economic = yes if committed and name in ['FillFinalityEconomicApplied',
            'FillFinalityUnappliedFailureReleased', 'FillFinalityCompensationApplied'] else no
        evidence = body.get('evidence', {}).get('TradeLifecycleV3')
        if evidence:
            fact = evidence['result']['result']
            disposition = {'PendingUnapplied': 0, 'PendingApplied': 1, 'FinalSuccess': 2,
                'FinalFailureUnapplied': 3, 'FinalFailureCompensated': 4}[fact['economic_disposition']]
            trades = Numbers.run_exact_batch(lambda: trades.update(zero, zero, zero,
                rational([fact['base_quantity_shares']]), rational([fact['quote_amount_pusd']]),
                torch.full_like(zero, disposition), yes, no, no,
                role=zero, matched_at=torch.tensor([fact['matched_at_utc_ms']], device='cuda'),
                immediate_fee=rational([fact['immediate_taker_fee_pusd']]),
                maker_fee_read_kind=torch.full_like(zero, 2), maker_fee_equivalent=rational(['0'])))
        elif economic.any().cpu().item():
            # 仅验证器按真实参考尝试安排输入；生产门由Admission的result.applied给出。
            trades = Numbers.run_exact_batch(lambda: trades.update(zero, zero, zero,
                rational(['0']), rational(['0']), zero, no, economic, applied))
        read = Numbers.run_exact_batch(costs.read)
        expected = after['platform_confirmed_projection']
        assert read.known.cpu().tolist() == [True] and read.fault.cpu().tolist() == [0]
        assert amounts(read.cost) == [Fraction(expected['position_cost_basis_pusd'])]
        assert amounts(read.fee) == [Fraction(expected['fees_pusd'])]
        asset = Numbers.run_exact_batch(lambda: costs.asset_cost(zero))
        expected_token = next((row for row in expected['outcome_balances'] if row['token_id'] == '1'), None)
        assert asset.known.cpu().tolist() == [True] and asset.fault.cpu().tolist() == [0]
        assert amounts(asset.cost) == [Fraction(expected_token['position_cost_basis_pusd']) if expected_token else 0]
        executed = Numbers.run_exact_batch(lambda: trades.executed(zero[:, None], yes[:, None]))
        original = checkpoint['after']['markets'][0]['executed']
        for field, key in [('quantity', 'shares'), ('principal', 'principal_pusd'), ('actual_fee', 'actual_fee_pusd')]:
            assert amounts(getattr(executed, field)) == [Fraction(row[key]) for row in original]
        estimate = Numbers.run_exact_batch(lambda: trades.estimated_rebate(directory, contributions, zero[:, None], yes[:, None]))
        expected_estimate = [row['estimated_pusd']['Known'] for row in checkpoint['after']['markets'][0]['rebates']]
        assert estimate.known.cpu().tolist() == [[True, True]] and estimate.fault.cpu().tolist() == [0]
        assert amounts(estimate.amount) == [Fraction(int(row['numerator']), int(row['denominator'])) for row in expected_estimate]
        checked += 1
    assert not costs.active.any().cpu().item()
    print(json.dumps({'actual_failed_V4_attempts': checked,
        'unapplied_release_and_applied_compensation_checked': True,
        'actual_applied_cost_fee_token_basis_and_executed_net_fee_checked': True,
        'actual_pending_and_failed_rebate_estimate_checked': True,
        'unknown_cold_prefix_asset_cost_not_known_zero_checked': True,
        'reference_attempt_order_drives_validation': True,
        'full_device_NOE_failure_clock_or_activity_restore_delivered': False}))


if __name__ == '__main__':
    main()
