"""Native盘口9字段的GPU实现。公共事实归DPT，特征/排序/模型装箱归Native。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from public_state import _take


@dataclass(frozen=True)
class PackedPmBooks:
    levels: torch.Tensor  # [roots,4,capacity,9]；本入口只负责四个PM侧。
    mask: torch.Tensor
    quality: torch.Tensor  # [roots,2,6]，原Native Q6。
    fault: torch.Tensor


class PmBookInputs:
    def __init__(self, state, *, initial_capacity=128):
        if initial_capacity < 1:
            raise ValueError('初始资源容量须为正')
        self.state = state
        self.capacity = initial_capacity
        numbers = _numbers()
        # SourceDecimal最多96位、scale<=28；统一10^28整数排序保持全部源精度。
        # 这仅是已证明的源十进制域，经济运算仍使用公共可增长arena。
        def keys():
            source = state.decimal_columns
            scale = source.scale
            factor = source.coefficient.constant(1)
            for exponent in range(1, 29):
                factor = factor.constant(10**exponent).select(28-scale == exponent, factor)
            return source.coefficient * factor
        key = numbers.run_exact_batch(keys)
        order = torch.arange(state.slots.shape[0], device=state.slots.device)
        price_keys = key.words[state.slots[:, 2]].to(torch.int64) & 0xffffffff
        # 字典中PM价格由原准入保证合法；仍保留错误供调用方禁止提交。
        self.sort_fault = key.fault[state.slots[:, 2]]
        signs = key.sign[state.slots[:, 2]]
        for word in range(price_keys.shape[1]):
            digit = torch.where(signs < 0, 0xffffffff-price_keys[:, word], price_keys[:, word])
            order = order[torch.argsort(digit[order], stable=True)]
        order = order[torch.argsort(signs[order], stable=True)]
        self.ask_order = order
        self.bid_order = order.flip(0)

    def pack(self, frame, asset_indices, observation_ms):
        """asset_indices[roots,2]由原市场身份给定，观察时间与模型墙钟无关。

        容量检查仅回传一次资源请求；扩池只消耗墙钟，不丢档位或改变交易支持。
        调用方须在fault为零后提交观察，不把错误转换为缺盘口。
        """
        numbers = _numbers()
        state = self.state
        roots, slots = frame.present.shape
        if asset_indices.shape != (roots, 2) or observation_ms.shape != (roots,):
            raise ValueError('市场资产/观察时间批次错误')
        if asset_indices.dtype != torch.int64 or observation_ms.dtype != torch.int64:
            raise TypeError('需设备int64资产和UTC观察时间')
        if asset_indices.device != state.slots.device or observation_ms.device != state.slots.device:
            raise ValueError('设备坐标不在同一GPU')
        ids_valid = (asset_indices >= 0) & (asset_indices < len(state.assets))
        safe_assets = asset_indices.clamp(0, max(0, len(state.assets)-1))
        visible = frame.ready & ((frame.quality == 0) | (frame.quality == 1))
        quantity_positive = frame.quantity.numerator.sign.reshape(roots, slots) > 0
        candidates = []
        for outcome in range(2):
            for side in range(2):
                order = self.bid_order if side == 0 else self.ask_order
                selected = (frame.present & quantity_positive &
                    (state.slots[:, 0][None] == safe_assets[:, outcome, None]) &
                    (state.slots[:, 1][None] == side) &
                    visible.gather(1, safe_assets[:, outcome, None]) & ids_valid[:, outcome, None])
                # 原精确价格顺序内稳定压紧；未来/零量槽位不进入配对树。
                local = torch.argsort((~selected[:, order]).to(torch.int8), dim=1, stable=True)
                candidates.append((order[local], selected.sum(dim=1)))
        required = int(torch.stack([count for _, count in candidates]).max().cpu())
        while self.capacity < required:
            self.capacity *= 2
        capacity = self.capacity
        levels = torch.zeros((roots, 4, capacity, 9), dtype=torch.float32, device=state.slots.device)
        mask = torch.zeros((roots, 4, capacity), dtype=torch.bool, device=state.slots.device)
        fault = numbers.combine_fault_rows(frame.fault.bitwise_or(self.sort_fault[None]))
        fault = fault | (~ids_valid.all(dim=1)).to(torch.int32)
        for side, (ordered, counts) in enumerate(candidates):
            count = min(capacity, slots)
            ordered = ordered[:, :count]
            flat = (torch.arange(roots, device=ordered.device)[:, None]*slots + ordered).reshape(-1)
            prices = state.slots[ordered, 2].reshape(-1)
            price = numbers.ExactRational(_take(state.decimals.numerator, prices),
                                           _take(state.decimals.denominator, prices))
            quantity = numbers.ExactRational(_take(frame.quantity.numerator, flat),
                                              _take(frame.quantity.denominator, flat))
            valid = torch.arange(count, device=ordered.device)[None] < counts[:, None]
            def project():
                phi = price.numerator.constant(2)*price.numerator-price.denominator
                p, pf = phi.project(price.denominator)
                q, qf = quantity.project(operation=1)
                best_ids = state.slots[ordered[:, :1], 2].expand(-1, count).reshape(-1)
                best = numbers.ExactRational(_take(state.decimals.numerator, best_ids),
                                              _take(state.decimals.denominator, best_ids))
                distance = best-price if side % 2 == 0 else price-best
                distance_value, df = distance.project(operation=1)
                anchor_id = frame.anchor_ids.gather(1, ordered)+1
                times = state.anchor_times[anchor_id]
                has_anchor = frame.anchor_present.gather(1, ordered)
                has_source = has_anchor & times[:, :, 0].bool()
                now = observation_ms[:, None].expand(-1, count)
                source = times[:, :, 1]
                received = times[:, :, 2]
                time_features, time_fault = [], torch.zeros_like(pf)
                for left, right, present in ((now, source, has_source),
                        (now, received, has_anchor), (received, source, has_source)):
                    delta = numbers.ExactInt.from_i64_tensor(left)-numbers.ExactInt.from_i64_tensor(right)
                    value, error = delta.project(delta.constant(1000), operation=1)
                    time_features.append(torch.where(present, value.reshape(roots, count), 0.0))
                    time_fault = time_fault | error
                rows = torch.stack((p.reshape(roots, count), q.reshape(roots, count), valid.float(),
                    *time_features, has_source.float(), has_anchor.float(),
                    distance_value.reshape(roots, count)), dim=-1)
                return rows, (pf | qf | df | time_fault).reshape(roots, count)
            rows, errors = numbers.run_exact_batch(project)
            levels[:, side, :count] = torch.where(valid[:, :, None], rows, 0.0)
            mask[:, side, :count] = valid
            fault = fault | numbers.combine_fault_rows(errors)
        quality = torch.nn.functional.one_hot(frame.quality.gather(1, safe_assets), 6).float()
        return PackedPmBooks(levels, mask, quality, fault)
