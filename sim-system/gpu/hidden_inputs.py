"""已验收compact hidden桶的一次冷装箱；GPU批量解析精确值并生成资产事件列。

保留原完整桶/参考/派生身份给SIM协调器。这里只装配资产步骤，市场参考尚须其处理器；
没有把冷读取水位当作运行闭合水位，也不读取公开PM步骤。
"""
from pathlib import Path
from dataclasses import replace
import sys
import json
import subprocess
import torch
from hidden_market import HiddenAssetState, rational_take, rational_select, zeros
from queues import Numbers, ExactInt, ExactRational

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'data-pipeline/gpu'))
from prepare import decimal_columns

QUALITY = {'fresh': 0, 'stale': 1, 'missing': 2, 'unknown_not_observed': 3, 'confirmed_gap': 4, 'invalid': 5}
STEP_TAG = {'InitialSnapshot': 1, 'LevelChange': 2, 'Tick': 3, 'Quality': 4}


class HiddenPmBlock:
    def __init__(self, buckets, assets, slots, *, source_binding, device='cuda'):
        # JSON是新的可达输入边界。绑定原材料reader再次恢复内容，不能只信自有hash或ID。
        command = [source_binding['binary'], '--progress-interval-seconds', '0', '--verify-gpu-pm-buckets',
                   source_binding['hidden'], source_binding['public'], str(source_binding['rows']),
                   str(source_binding['bytes']), str(source_binding['max_batch_bytes'])]
        verified = subprocess.run(command, input=json.dumps(buckets).encode(), stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE, check=False)
        if verified.returncode:
            raise ValueError('hidden冷批次与实际原材料不一致: ' + verified.stderr.decode())
        self.source_verification = json.loads(verified.stdout)
        # 原首次FixedIdentity的完整市场成员来自reader；不从当前PM步骤人口推断Active。
        scene = self.source_verification['source_scene']
        registry = {}
        scene_members = {}
        for market_index, (_, members) in enumerate(scene.values()):
            scene_members[market_index] = {asset['token_id'] for asset in members}
            for asset in members:
                if asset['token_id'] in registry:
                    raise ValueError('SIM冷目录资产身份冲突')
                registry[asset['token_id']] = (asset, market_index)
        selected = set()
        coordinates = []
        for asset in assets:
            token = asset['token_id']
            if token in selected or token not in registry or registry[token][0] != asset:
                raise ValueError('资产不属于原SIM冷目录身份')
            selected.add(token)
            coordinates.append(registry[token][1])
        for market_index in set(coordinates):
            if not scene_members[market_index] <= selected:
                raise ValueError('SIM市场须包含原完整资产人口，不能提前判定Active')
        self.asset_markets = torch.tensor(coordinates, dtype=torch.int64, device=device)
        self.market_count = len(scene)
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
        self.operations = pending
        self.asset_axis = torch.arange(len(assets), device=device)
        self.price_fault = self.prices.fault  # 冷不可变价格列的源故障
        events = []
        snapshot_levels = []
        for entry in pending:
            # 常驻只保存共享字典索引及源ragged盘口；不保存N×L mask或精确广播字张量。
            entry['snapshot_start'] = len(snapshot_levels)
            snapshot_levels.extend(entry.get('levels', []))
            entry['snapshot_end'] = len(snapshot_levels)
            events.append([entry.get('asset', -1), entry.get('side', -1), entry.get('price', 0),
                entry.get('before', 0), entry.get('after', 0), entry.get('tick', 0),
                entry.get('previous', 0), entry.get('current', 0), int(entry.get('previous_present', False)),
                int(entry.get('resync', False)), entry['quality'], entry.get('previous_quality', 3),
                entry.get('current_quality', 3), int(entry['required'])])
        self.event_indices = torch.tensor(events, dtype=torch.int64, device=device).reshape(-1, 14)
        self.snapshot_levels = torch.tensor(snapshot_levels, dtype=torch.int64, device=device).reshape(-1, 3)
        self.step_tags = torch.tensor([STEP_TAG.get(entry['tag'], 0) for entry in pending],
                                      dtype=torch.int64, device=device)
        self.snapshot_offsets = torch.tensor([entry['snapshot_start'] for entry in pending] +
                                            [len(snapshot_levels)], dtype=torch.int64, device=device)
        # 补零哨兵在冷安装时建立一次；每步仅index_select，不复制整页源列。
        self.event_lookup = torch.cat((self.event_indices, torch.zeros((1, 14), dtype=torch.int64, device=device)), 0)
        self.tag_lookup = torch.cat((self.step_tags, torch.zeros(1, dtype=torch.int64, device=device)), 0)

    def selected_rows(self, indices):
        """设备游标选原源步；补零行只供无步根的掩码候选，不算来源人口。"""
        count = self.event_indices.size(0)
        safe = indices.clamp(0, count)
        rows = self.event_lookup.index_select(0, safe)
        tags = self.tag_lookup.index_select(0, safe)
        return rows, tags, (indices >= 0) & (indices < count)

    def selected_lookup(self, state, rows, asset_roots, source_assets, level_sides):
        """运行资产按根隔离，价格按精确值关联，不以共享字典编号判相等。"""
        owners = asset_roots[state.level_assets]
        price = self._values(rows[:, 2])
        relation = state.queues.price.compare(rational_take(price, owners))
        fault = relation.fault | state.queues.price.fault
        target = (source_assets[state.level_assets] == rows[owners, 0])
        target = target & (level_sides == rows[owners, 1])
        return target & (relation.order == 0) & (fault == 0), fault

    def snapshot_values(self, state, indices, rows, asset_roots, source_assets, level_sides, enabled):
        """设备只扫描选中快照ragged范围；其它源类型不做精确快照匹配。"""
        price = state.queues.price
        dictionary = self.decimals
        ids, counts, fault = Numbers.public_extension().hidden_snapshot_gather([
            indices, rows[:, 0].contiguous(), enabled, source_assets, asset_roots,
            state.level_assets, level_sides, self.snapshot_offsets, self.snapshot_levels,
            price.numerator.words, price.numerator.sign, price.denominator.words, price.fault,
            dictionary.numerator.words, dictionary.numerator.sign, dictionary.denominator.words,
            self.decimal_fault, state.book_closed])
        return self._values(ids), counts, fault

    def _values(self, ids):
        indices = ids if isinstance(ids, torch.Tensor) else torch.tensor(ids, dtype=torch.int64, device=self.level_assets.device)
        return rational_take(self.decimals, indices.reshape(-1))

    def _broadcast(self, index, count):
        if isinstance(index, torch.Tensor):
            return self._values(index.expand(count))
        return self._values(torch.full((count,), index, dtype=torch.int64, device=self.level_assets.device))

    def _level_lookup(self, asset, side, price):
        relation = Numbers.run_exact_batch(lambda: self.prices.compare(self._broadcast(price, len(self.slots))))
        if relation.fault.shape != self.level_assets.shape:
            raise ValueError('设备精确价位形状错误')
        # 关联故障随本次候选返回，不覆盖共享源列，也不让资源失败污染原输入。
        fault = self.price_fault | relation.fault
        return (self.level_assets == asset) & (self.level_sides == side) & (relation.order == 0) & (fault == 0), fault

    def empty_state(self, *, segment_capacity=4):
        state = HiddenAssetState.empty(self.level_assets, self.prices, self.asset_markets, self.market_count, segment_capacity=segment_capacity)
        cold_fault = state._asset_fault(getattr(self, 'price_fault', torch.zeros_like(self.level_assets, dtype=torch.int32)))
        decimal_fault = Numbers.combine_fault_rows(self.decimal_fault.reshape(1, -1))[0]
        return replace(state, fault=state.fault | cold_fault | decimal_fault)

    def apply(self, state, index, *, enabled_assets=None):
        """原源类型在冷边界已解码；这里不读任何设备条件回CPU决定业务。

        协调器仍须先应用同桶references/到期义务，并在整桶成功后才推进运行前沿。
        返回资产候选，不承担此协调器或账户事务。
        """
        from hidden_market import run_hidden_asset_batch
        entry = self.operations[index]
        tag = entry['tag']
        row = self.event_indices[index]
        enabled = self.asset_axis == row[0]
        if enabled_assets is not None:
            enabled = enabled & state._mask(enabled_assets)
        if tag == 'InitialSnapshot':
            active = enabled & ~state.book_closed
            quantities = zeros(self.prices)
            count = torch.zeros(len(self.slots), dtype=torch.int64, device=self.level_assets.device)
            missing = torch.zeros((), dtype=torch.int32, device=self.level_assets.device)
            price_fault = self.price_fault
            for level_index in range(entry['snapshot_start'], entry['snapshot_end']):
                side, price, quantity = self.snapshot_levels[level_index].unbind()
                target, fault = self._level_lookup(row[0], side, price)
                price_fault = price_fault | fault
                missing = missing | (target.sum() != 1).to(torch.int32)
                count = count + target.to(torch.int64)
                quantities = rational_select(self._broadcast(quantity, len(self.slots)), target, quantities)
            quality = row[10].expand_as(state.quality)
            state, _ = run_hidden_asset_batch(state, lambda value: value.initial_snapshot(
                enabled, quantities, self._broadcast(row[5], len(self.assets)), quality))
            source_fault = state._asset_fault((count > 1).to(torch.int32) | price_fault)
            return replace(state, fault=state.fault | source_fault | missing * active.to(torch.int32)), None
        if tag == 'LevelChange':
            active = enabled & ~state.book_closed
            level_enabled, price_fault = self._level_lookup(row[0], row[1], row[2])
            if enabled_assets is not None:
                level_enabled = level_enabled & enabled_assets[state.level_assets]
            state, effects = run_hidden_asset_batch(state, lambda value: value.level_transition(
                level_enabled, self._broadcast(row[3], len(self.slots)), self._broadcast(row[4], len(self.slots)),
                (row[9] != 0).expand_as(level_enabled)))
            source_fault = state._asset_fault(price_fault) | (level_enabled.sum() != 1).to(torch.int32) * active.to(torch.int32)
            state = replace(state, fault=state.fault | source_fault)
            effects = replace(effects, fault=effects.fault | state.fault[state.level_assets])
            return state, effects
        if tag == 'Tick':
            return Numbers.run_exact_batch(lambda: state.tick_change(enabled, self._broadcast(row[6], len(self.assets)),
                (row[8] != 0).expand_as(enabled), self._broadcast(row[7], len(self.assets)))), None
        if tag == 'Quality':
            return Numbers.run_exact_batch(lambda: state.quality_boundary(enabled,
                row[11].expand_as(state.quality), row[12].expand_as(state.quality),
                (row[13] != 0).expand_as(enabled))), None
        # 与原normalize_pm_step_v1及ExecutionStateV2::apply_market_event一致：
        # 选择/证据/公开成交/BBA不驱动隐藏FIFO，也不抬高公开可见性。
        return state, None
