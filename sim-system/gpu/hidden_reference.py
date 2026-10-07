"""原hidden MarketFact控制字段；初始化Active与Terminal经济效果仍由原SIM路径负责。"""
from dataclasses import dataclass, replace
import json
import torch

REFERENCE_TYPES = frozenset(('chainlink_btc_usd', 'binance_depth_state', 'binance_aggregate_trade',
    'market_price_anchor_update', 'market_settlement_reference_update', 'chainlink_btc_usd_twap',
    'rtds_observed_anomaly'))


def reference_columns(buckets, scene, assets, intern_decimal, quality, device):
    """从reader已核验的原完整事实建立只读分派列，不安装任何未来业务状态。"""
    markets = {condition:(index, value[0], value[1]) for index,(condition,value) in enumerate(scene.items())}
    asset_ids = {value['token_id']:index for index,value in enumerate(assets)}
    lifecycle_values, lifecycle_ids = [], {}
    def lifecycle(value):
        if value is None:
            return -1
        key = json.dumps(value, sort_keys=True, separators=(',', ':'))
        if key not in lifecycle_ids:
            lifecycle_ids[key] = len(lifecycle_values)
            lifecycle_values.append(value)
        return lifecycle_ids[key]
    rows = []
    for bucket in buckets:
        for reference in bucket['references']:
            payload = reference['payload']
            row = [0, -1, -1, 0, -1, -1, -1, -1, -1, 3, 3, 0]
            if 'BinanceDepthFromPrefix' in payload:
                row[0] = 1
            elif 'Inline' in payload and 'ReferenceFact' in payload['Inline']:
                if payload['Inline']['ReferenceFact']['reference_type'] not in REFERENCE_TYPES:
                    raise ValueError('未知hidden纯引用类型不得作为noop消费')
                row[0] = 1
            elif 'Inline' in payload and 'MarketFact' in payload['Inline']:
                fact = payload['Inline']['MarketFact']
                bound = markets.get(fact['market']['condition_id'])
                if bound is None or bound[1] != fact['market']:
                    raise ValueError('hidden市场事实不属于原SIM固定目录')
                row[1] = bound[0]
                tag, fields = next(iter(fact['payload'].items()))
                def member(asset):
                    if asset not in bound[2]:
                        raise ValueError('hidden市场事实资产不属于原市场完整身份')
                    return asset_ids.get(asset['token_id'], -1)
                if tag == 'FixedIdentity':
                    for asset in fields['assets']:
                        member(asset)
                    row[0] = 2
                elif tag == 'InitialTick':
                    row[0:4] = [3, bound[0], member(fields['asset']), intern_decimal(fields['tick_size_pusd_per_share'])]
                elif tag == 'LifecycleTransition':
                    for asset in fields['assets']:
                        member(asset)
                    row[0], row[4], row[5] = 4, lifecycle(fields['previous_state']), lifecycle(fields['current_state'])
                elif tag == 'MarketLifecycleSnapshot':
                    for asset in fields['assets']:
                        member(asset)
                    row[0], row[5] = 5, lifecycle(fields['current_state'])
                elif tag == 'TradingState':
                    row[0] = 6
                    row[6:9] = [-1 if fields[name] is None else int(fields[name])
                                for name in ('active', 'closed', 'accepting_orders')]
                elif tag == 'QualityBoundary':
                    scope = fields['scope']
                    if scope['kind'] == 'MARKET':
                        row[0] = 7
                    else:
                        row[0], row[2] = 8, member(scope['asset'])
                    row[9:12] = [quality[fields['previous_quality']], quality[fields['current_quality']],
                                  int(reference['quality']['resync'] == 'REQUIRED')]
                elif tag == 'Terminal':
                    row[0] = 9  # 等真实Resolution/义务/输出共同提交，不将它标成noop。
                else:
                    raise ValueError('尚未接线的MarketFact类型不得静默消费')
            else:
                raise ValueError('hidden参考人口必须是原ReferenceFact或MarketFact')
            rows.append(row)
    return torch.tensor(rows + [[0, -1, -1, 0, -1, -1, -1, -1, -1, 3, 3, 0]],
                        dtype=torch.int64, device=device), tuple(lifecycle_values)


@dataclass(frozen=True)
class HiddenMarketControls:
    mapped_lifecycle: torch.Tensor  # 原完整mapped值的同源字典索引，-1仅表示None。
    trading: torch.Tensor  # active/closed/accepting，-1原None，不是false。
    quality: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def empty(cls, roots, markets, device):
        return cls(torch.full((roots,markets), -1, dtype=torch.int64, device=device),
                   torch.full((roots,markets,3), -1, dtype=torch.int64, device=device),
                   torch.full((roots,markets), 3, dtype=torch.int64, device=device),
                   torch.zeros(roots, dtype=torch.int32, device=device))

    def apply(self, rows, enabled):
        count = self.mapped_lifecycle.size(1)
        axis = torch.arange(count, device=rows.device)
        selected = enabled[:,None] & (axis[None,:] == rows[:,1,None])
        kind = rows[:,0]
        lifecycle = selected & ((kind == 4) | (kind == 5))[:,None]
        conflict = selected & (kind == 4)[:,None] & (self.mapped_lifecycle != -1)
        conflict = conflict & (self.mapped_lifecycle != rows[:,4,None])
        quality = selected & (kind == 7)[:,None]
        conflict = conflict | (quality & (self.quality != rows[:,9,None]))
        fault = self.fault | conflict.any(1).to(torch.int32)
        fault = fault | (enabled & ((rows[:,1] < 0) | (rows[:,1] >= count)) & (kind > 1)).to(torch.int32)
        valid = (fault == 0)[:,None]
        return replace(self, mapped_lifecycle=torch.where(lifecycle & valid, rows[:,5,None], self.mapped_lifecycle),
            trading=torch.where((selected & (kind == 6)[:,None] & valid)[:,:,None], rows[:,None,6:9], self.trading),
            quality=torch.where(quality & valid, rows[:,10,None], self.quality), fault=fault)

    def coordinate_result(self, original, fault):
        valid = (fault == 0)[:,None]
        return replace(self, mapped_lifecycle=torch.where(valid,self.mapped_lifecycle,original.mapped_lifecycle),
            trading=torch.where(valid[:,:,None],self.trading,original.trading),
            quality=torch.where(valid,self.quality,original.quality), fault=self.fault | original.fault | fault)
