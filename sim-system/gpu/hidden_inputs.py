"""已验收compact hidden桶的一次冷装箱；GPU批量解析精确值并生成资产事件列。

保留原完整桶/参考/派生身份给SIM协调器。这里只装配资产步骤，市场参考尚须其处理器；
没有把冷读取水位当作运行闭合水位，也不读取公开PM步骤。
"""
from pathlib import Path
from dataclasses import replace
import sys
import torch
from hidden_market import HiddenAssetState, rational_take, rational_select, zeros
from queues import Numbers, ExactInt, ExactRational

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'data-pipeline/gpu'))
from prepare import decimal_columns

QUALITY = {'fresh': 0, 'stale': 1, 'missing': 2, 'unknown_not_observed': 3, 'confirmed_gap': 4, 'invalid': 5}


class HiddenPmBlock:
    def __init__(self, buckets, assets, slots, *, device='cuda'):
        self.buckets = buckets  # 原完整身份、闭桶证明、参考和步骤均保留。
        self.assets = assets
        self.slots = slots
        asset_ids = {asset['token_id']: index for index, asset in enumerate(assets)}
        self.level_assets = torch.tensor([asset_ids[token] for token, _, _ in slots], dtype=torch.int64, device=device)
        self.level_sides = torch.tensor([0 if side == 'BID' else 1 for _, side, _ in slots], dtype=torch.int64, device=device)
        texts = ['0', '1']
        table = {'0': 0, '1': 1}

        def intern(text):
            if text not in table:
                table[text] = len(texts)
                texts.append(text)
            return table[text]

        price_ids = [intern(price) for _, _, price in slots]
        pending = []
        for bucket in buckets:
            if bucket['version'] != 'compact_hidden_gpu_bucket_v1':
                raise ValueError('不是已恢复的compact独立hidden桶')
            if len(bucket['steps']) != len(bucket['bucket']['steps']):
                raise ValueError('步骤人口与完整桶引用不闭合')
            for (market, step), reference in zip(bucket['steps'], bucket['bucket']['steps']):
                if step['derived_id'] != reference['derived_id']:
                    raise ValueError('步骤不属于完整桶原派生身份')
                tag, fields = next(iter(step['change'].items()))
                entry = {'tag': tag, 'fields': fields, 'quality': QUALITY[step['quality']['availability']],
                         'required': step['quality']['resync'] == 'REQUIRED', 'step': step,
                         'source_time': bucket['bucket']['bucket_time_utc_ms'], 'market': market}
                if tag == 'InitialSnapshot':
                    entry['asset'] = asset_ids[fields['asset']['token_id']]
                    entry['tick'] = intern(fields['initial_tick_size_pusd_per_share'])
                    entry['levels'] = [(side, intern(level['price_pusd_per_share']), intern(level['quantity_shares']))
                                       for side, levels in enumerate((fields['bids'], fields['asks'])) for level in levels]
                elif tag == 'LevelChange':
                    transition = fields['transition']
                    entry.update(asset=asset_ids[transition['asset']['token_id']],
                                 side=0 if transition['side'] == 'BID' else 1,
                                 price=intern(transition['price_pusd_per_share']),
                                 before=intern(transition['quantity_before_shares']),
                                 after=intern(transition['quantity_after_shares']),
                                 resync=transition['basis'] == 'BOOK_RESYNC_DELTA_INFERENCE')
                elif tag == 'Tick':
                    tick = fields['tick']
                    previous = tick['old_tick_size_pusd_per_share']
                    entry.update(asset=asset_ids[tick['asset']['token_id']], previous_present=previous is not None,
                                 previous=intern(previous) if previous is not None else 0,
                                 current=intern(tick['new_tick_size_pusd_per_share']))
                elif tag == 'Quality':
                    boundary = fields['boundary']
                    if boundary['scope'].get('kind') != 'ASSET':
                        raise ValueError('市场质量须交所属市场处理器；不能作为资产noop消费')
                    entry.update(asset=asset_ids[boundary['scope']['asset']['token_id']],
                                 previous_quality=QUALITY[boundary['previous_quality']],
                                 current_quality=QUALITY[boundary['current_quality']])
                elif tag not in ('Epoch', 'ResetAsset', 'LevelEvidenceOnly', 'Selection', 'AbsenceBaseline',
                                 'BookReady', 'BestBidAsk', 'PublicConfirmation', 'PublicTrade'):
                    raise ValueError('尚未接线的hidden步骤不得静默丢弃')
                pending.append(entry)
        raw, offsets = bytearray(), [0]
        for text in texts:
            raw.extend(text.encode('ascii'))
            offsets.append(len(raw))
        raw_gpu = torch.tensor(list(raw), dtype=torch.uint8).pin_memory().to(device, non_blocking=True)
        offsets_gpu = torch.tensor(offsets, dtype=torch.int64).pin_memory().to(device, non_blocking=True)
        columns = decimal_columns(raw_gpu, offsets_gpu, torch.ones(len(texts), dtype=torch.bool, device=device))
        self.decimals = Numbers.run_exact_batch(columns.rational)
        self.decimal_fault = self.decimals.fault | (~columns.present).to(torch.int32)
        self.prices = self._values(price_ids)
        self.operations = []
        assets_axis = torch.arange(len(assets), device=device)
        for entry in pending:
            enabled = assets_axis == entry.get('asset', -1)
            entry['enabled'] = enabled
            # 冷归一化/身份关联，只构建共享设备源列；此阶段不推进业务状态。
            if entry['tag'] == 'InitialSnapshot':
                quantities = zeros(self.prices)
                count = torch.zeros(len(slots), dtype=torch.int64, device=device)
                missing = torch.zeros((), dtype=torch.int32, device=device)
                for side, price, quantity in entry['levels']:
                    target = self._level_mask(entry['asset'], side, price)
                    missing = missing | (target.sum() != 1).to(torch.int32)
                    count = count + target.to(torch.int64)
                    quantities = rational_select(self._broadcast(quantity, len(slots)), target, quantities)
                entry['shares'] = quantities
                entry['source_fault'] = (count > 1).to(torch.int32)
                entry['asset_source_fault'] = missing * enabled.to(torch.int32)
                entry['tick_value'] = self._broadcast(entry['tick'], len(assets))
            elif entry['tag'] == 'LevelChange':
                entry['level_enabled'] = self._level_mask(entry['asset'], entry['side'], entry['price'])
                entry['asset_source_fault'] = (entry['level_enabled'].sum() != 1).to(torch.int32) * enabled.to(torch.int32)
                entry['before_value'] = self._broadcast(entry['before'], len(slots))
                entry['after_value'] = self._broadcast(entry['after'], len(slots))
            elif entry['tag'] == 'Tick':
                entry['previous_value'] = self._broadcast(entry['previous'], len(assets))
                entry['current_value'] = self._broadcast(entry['current'], len(assets))
            self.operations.append(entry)

    def _values(self, ids):
        return rational_take(self.decimals, torch.tensor(ids, dtype=torch.int64, device=self.level_assets.device))

    def _broadcast(self, index, count):
        return self._values([index] * count)

    def _level_mask(self, asset, side, price):
        relation = Numbers.run_exact_batch(lambda: self.prices.compare(self._broadcast(price, len(self.slots))))
        if relation.fault.shape != self.level_assets.shape:
            raise ValueError('设备精确价位形状错误')
        # 关联结果保留fault；不得把解析失败误判成“该价位不存在”。
        self.price_fault = getattr(self, 'price_fault', torch.zeros_like(relation.fault)) | relation.fault
        return (self.level_assets == asset) & (self.level_sides == side) & (relation.order == 0) & (relation.fault == 0)

    def empty_state(self, *, segment_capacity=4):
        state = HiddenAssetState.empty(self.level_assets, self.prices, len(self.assets), segment_capacity=segment_capacity)
        cold_fault = state._asset_fault(getattr(self, 'price_fault', torch.zeros_like(self.level_assets, dtype=torch.int32)))
        decimal_fault = Numbers.combine_fault_rows(self.decimal_fault.reshape(1, -1))[0]
        return replace(state, fault=state.fault | cold_fault | decimal_fault)

    def apply(self, state, index):
        """原源类型在冷边界已解码；这里不读任何设备条件回CPU决定业务。

        协调器仍须先应用同桶references/到期义务，并在整桶成功后才推进运行前沿。
        返回资产候选，不承担此协调器或账户事务。
        """
        from hidden_market import run_hidden_asset_batch
        entry = self.operations[index]
        tag = entry['tag']
        if tag == 'InitialSnapshot':
            quality = torch.full_like(state.quality, entry['quality'])
            state, _ = run_hidden_asset_batch(state, lambda value: value.initial_snapshot(
                entry['enabled'], entry['shares'], entry['tick_value'], quality))
            source_fault = state._asset_fault(entry['source_fault'])
            return replace(state, fault=state.fault | source_fault | entry['asset_source_fault'])
        if tag == 'LevelChange':
            state, _ = run_hidden_asset_batch(state, lambda value: value.level_transition(
                entry['level_enabled'], entry['before_value'], entry['after_value'],
                torch.full_like(entry['level_enabled'], entry['resync'])))
            return replace(state, fault=state.fault | entry['asset_source_fault'])
        if tag == 'Tick':
            return Numbers.run_exact_batch(lambda: state.tick_change(entry['enabled'], entry['previous_value'],
                torch.full_like(entry['enabled'], entry['previous_present']), entry['current_value']))
        if tag == 'Quality':
            return Numbers.run_exact_batch(lambda: state.quality_boundary(entry['enabled'],
                torch.full_like(state.quality, entry['previous_quality']), torch.full_like(state.quality, entry['current_quality']),
                torch.full_like(entry['enabled'], entry['required'])))
        # 与原normalize_pm_step_v1及ExecutionStateV2::apply_market_event一致：
        # 选择/证据/公开成交/BBA不驱动隐藏FIFO，也不抬高公开可见性。
        return state
