"""公开Resolved事实的首次安装和未兑付应收，归Ledger经济层。

完整Terminal与canonical身份借同一不可变来源字典。冷绑定按原token匹配
payout，不按vector位置猜资产。请求、费用和兑付仍须各自真实事实接入。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt, ExactRational, Numbers
from asset_buy import take_ratio
from success_pool import select


@dataclass(frozen=True)
class ResolutionSources:
    columns: object
    payouts: object  # [terminal,运行资产]；只转换源金额，不生成根内判定。
    matched: torch.Tensor

    @classmethod
    def bind(cls, columns, asset_tokens, *, device='cuda'):
        from prepare import decimal_columns
        texts, matched = [], []
        for terminal in columns.terminals:
            vector = terminal['payout_vector']
            for token in asset_tokens:
                matches = [entry for entry in vector if entry['asset']['token_id'] == token]
                matched.append(len(matches) == 1)
                texts.append(matches[0]['payout_pusd_per_share'] if len(matches) == 1 else '0')
        raw, offsets = bytearray(), [0]
        for value in texts:
            raw.extend(value.encode('ascii'))
            offsets.append(len(raw))
        parsed = decimal_columns(torch.tensor(list(raw), dtype=torch.uint8).pin_memory().to(device),
            torch.tensor(offsets, dtype=torch.int64).pin_memory().to(device),
            torch.ones(len(texts), dtype=torch.bool, device=device))
        payouts = parsed.rational()
        return cls(columns, payouts, torch.tensor(matched, dtype=torch.bool, device=device).reshape(
            len(columns.terminals), len(asset_tokens)))


@dataclass(frozen=True)
class ResolutionRead:
    present: torch.Tensor
    identities: torch.Tensor
    terminals: torch.Tensor
    maker_category: torch.Tensor
    paired_receivable: object
    winner_orphan_receivable: object
    known: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class MarketResolutions:
    sources: object
    roots: torch.Tensor
    conditions: torch.Tensor
    assets: torch.Tensor
    present: torch.Tensor
    identities: torch.Tensor
    terminals: torch.Tensor
    maker_category: torch.Tensor  # 首次判定时冻结，不随后来成功成交重分。
    payouts: object  # 每market两腿，按原固定资产绑定。
    known: torch.Tensor
    fault: torch.Tensor
    root_fault: torch.Tensor

    @classmethod
    def open(cls, pools, markets, sources, *, known=True):
        assets = torch.stack([markets.asset_indices, pools.counterpart[markets.asset_indices]], 1)
        count = assets.size(0)
        zero = ExactInt.from_i64_tensor(torch.zeros(count*2, dtype=torch.int64, device=assets.device))
        return cls(sources, markets.roots, markets.conditions, assets,
            torch.zeros(count, dtype=torch.bool, device=assets.device), torch.full_like(markets.conditions, -1),
            torch.full_like(markets.conditions, -1), torch.zeros(count, dtype=torch.bool, device=assets.device),
            ExactRational(zero, zero.constant(1)), torch.full((count,), known, dtype=torch.bool, device=assets.device),
            markets.fault.clone(), pools.fault.clone())

    def observe(self, condition, identity, terminal, pools, enabled):
        roots = pools.fault.numel()
        members = (self.roots[:, None] == torch.arange(roots, device=condition.device)[None, :]) & (
            self.conditions[:, None] == condition[None, :])
        count = members.to(torch.int64).sum(0)
        source = self.sources
        source_count, asset_count = source.matched.shape
        if source_count == 0:
            return replace(self, root_fault=self.root_fault | enabled.to(torch.int32))
        safe = terminal.clamp(0, source_count-1)
        row = safe[self.roots, None]*asset_count + self.assets
        incoming = take_ratio(source.payouts, row.flatten())
        mapped = source.matched[safe[self.roots, None], self.assets].all(1)
        selected = members.any(1) & enabled[self.roots]
        invalid = (terminal < 0) | (terminal >= source_count) | (identity < 0) | (
            identity >= len(source.columns.canonical_ids)) | (count != 1)
        error = invalid[self.roots].to(torch.int32) | (~mapped).to(torch.int32)
        error |= Numbers.combine_fault_rows(incoming.fault.reshape(-1, 2))
        fault = self.fault | torch.where(selected, error, 0)
        fresh = selected & ~self.present & (fault == 0)
        paired = take_ratio(pools.paired_quantity, self.assets[:, 0])
        fault |= torch.where(fresh, paired.fault, 0)
        fresh &= fault == 0
        root_fault = self.root_fault | torch.where(enabled, invalid.to(torch.int32), 0)
        root_fault |= Numbers.combine_fault_rows(torch.where(
            self.roots[None, :] == torch.arange(roots, device=condition.device)[:, None], fault[None, :], 0))
        fresh &= root_fault[self.roots] == 0
        return replace(self, present=self.present | fresh,
            identities=torch.where(fresh, identity[self.roots], self.identities),
            terminals=torch.where(fresh, terminal[self.roots], self.terminals),
            maker_category=torch.where(fresh, paired.numerator.sign > 0, self.maker_category),
            payouts=select(incoming, fresh.repeat_interleave(2), self.payouts),
            known=self.known | fresh, fault=fault, root_fault=root_fault)

    def coordinated(self, original, valid_roots):
        use = valid_roots[self.roots]
        return replace(self, **{name:torch.where(use, getattr(self, name), getattr(original, name))
            for name in ('present', 'identities', 'terminals', 'maker_category', 'known')},
            payouts=select(self.payouts, use.repeat_interleave(2), original.payouts))

    def orphan_value(self, pools):
        quantities = take_ratio(pools.quantity, self.assets.flatten())
        amounts = quantities*self.payouts
        axes = torch.arange(self.roots.numel(), device=self.roots.device)*2
        return take_ratio(amounts, axes) + take_ratio(amounts, axes+1)

    def read(self, pools):
        pair = take_ratio(pools.paired_quantity, self.assets[:, 0])
        orphan = self.orphan_value(pools)
        zero = ExactRational(pair.numerator.constant(0), pair.denominator.constant(1))
        return ResolutionRead(self.present, self.identities, self.terminals, self.maker_category,
            select(pair, self.present, zero), select(orphan, self.present, zero), self.known,
            self.fault | self.root_fault[self.roots] | pair.fault | orphan.fault)
