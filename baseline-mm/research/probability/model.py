"""有限研究假设；全局来源采样不等同生产记忆。"""
from __future__ import annotations
from dataclasses import asdict, dataclass
from math import erf, sqrt

WINDOWS = (15, 30, 60, 120, 300)
INITIAL = (0.5, 1.0, 2.0)
ERROR = (0.0, 0.5, 1.0, 2.0)
WEIGHTS = (0.0, -1.0, -0.5, 0.5, 1.0)  # 精确并列时优先零修正。
FEATURES = ('innovation', 'depth_pressure', 'trade_pressure')
REMAINING = (300, 240, 180, 120, 60, 30, 10, 1)

@dataclass(frozen=True)
class Candidate:
    window_s: int
    initial_multiplier: float
    error_multiplier: float
    feature: str = 'none'
    weight: float = 0.0

    def identity(self):
        return asdict(self)


def baseline_candidates():
    return [Candidate(w, i, e) for w in WINDOWS for i in INITIAL for e in ERROR]


def predict(row, candidate, sigma_fit):
    """返回预测或明确缺失原因；零方差保留为已知值。"""
    if row['remaining_s'] <= 0:
        return None, 'market_ended'
    if row.get('rule_window_s') is None:
        return None, 'missing_settlement_rule'
    if row.get('reference') is None:
        return None, 'missing_reference'
    if row.get('price') is None:
        return None, 'missing_settlement_price'
    pair = row['variance'][str(candidate.window_s)]
    fallback = pair['seconds'] <= 0
    if fallback and sigma_fit is None:
        return None, 'missing_fit_sigma'
    sigma = (sigma_fit * candidate.initial_multiplier if fallback
             else sqrt(pair['squared_change'] / pair['seconds']))
    if candidate.error_multiplier and sigma_fit is None:
        return None, 'missing_fit_sigma'
    innovation = 0.0
    if candidate.feature != 'none' and candidate.weight:
        value = row.get(candidate.feature)
        if value is None:
            return None, 'missing_' + candidate.feature
        scale = 1.0 if candidate.feature == 'innovation' else sigma_fit
        if scale is None:
            return None, 'missing_fit_sigma'
        innovation = candidate.weight * scale * value
    s, tau, w = row['price'], row['remaining_s'], row['rule_window_s']
    if not w:
        mu, variance = s + innovation, sigma * sigma * tau
    elif tau >= w:
        mu, variance = s + innovation, sigma * sigma * (tau - 2 * w / 3)
    else:
        if row.get('integral') is None:
            return None, 'missing_window_history'
        mu = (row['integral'] + tau * (s + innovation)) / w
        variance = sigma * sigma * tau**3 / (3 * w * w)
    epsilon = candidate.error_multiplier * (sigma_fit or 0.0)  # 乘 sqrt(1秒)。
    uncertainty = sqrt(variance + epsilon * epsilon)
    z = (mu - row['reference']) / uncertainty if uncertainty else None
    p = 0.5 * (1 + erf(z / sqrt(2))) if z is not None else float(mu >= row['reference'])
    return dict(p=p, mean=mu, uncertainty=uncertainty, z=z, fallback=fallback,
                zero_variance=uncertainty == 0, sigma=sigma), None


def innovation_steps(rows):
    """每次采样先按旧基差计算，再刷新基差。"""
    basis = None
    for row in rows:
        price, trade, received = row.get('spot_price',row.get('price')), row.get('trade_price'), row.get('spot_received',row.get('price_received'))
        if price is None or trade is None or received is None:
            row['innovation'] = None
            continue
        if basis is None:
            basis = (trade, price, received)
        row['innovation'] = (trade - basis[0]) - (price - basis[1])
        if received > basis[2]:
            basis = (trade, price, received)
    return rows
