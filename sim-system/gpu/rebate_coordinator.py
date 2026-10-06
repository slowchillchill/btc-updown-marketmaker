"""SIM两类返佣时钟和原Day先于Payment的共同候选。

只负责返佣义务；完整毫秒调度仍须先处理其它原SIM义务并协调hidden与请求。
市场目录顺序继承原condition字串序，下一时刻读取不推进任何业务状态。
"""
from dataclasses import dataclass, replace
import torch
from taker_days import coordinate_calendar


@dataclass(frozen=True)
class RebatePhases:
    finalized: tuple  # (kind:int, result)，原Maker→Taker、日期升序。
    payments: tuple
    fault: torch.Tensor


@dataclass(frozen=True)
class RebateClockFrame:
    consumed: torch.Tensor
    market_results: tuple
    phases: RebatePhases
    fault: torch.Tensor


def settle_ready_days(account, maker, taker, enabled):
    original_account, original_maker, original_taker = account, maker, taker
    finalized, payments = [], []
    # 与原settle_ready_days一致：所有Day完成后才生成任何Payment。
    for kind in [0, 1]:
        calendar = maker if kind == 0 else taker
        for _ in range(calendar.occupied.size(1)):
            calendar, account, result = calendar.finalize_one(account, enabled)
            finalized.append((kind, result))
        if kind == 0:
            maker = calendar
        else:
            taker = calendar
    for kind in [0, 1]:
        calendar = maker if kind == 0 else taker
        for _ in range(calendar.occupied.size(1)):
            calendar, account, result = calendar.pay_one(account, enabled)
            payments.append((kind, result))
        if kind == 0:
            maker = calendar
        else:
            taker = calendar
    fault = account.fault | maker.fault | taker.fault
    valid = fault == 0
    account = account.coordinate_result(original_account, fault)
    maker = coordinate_calendar(maker, original_maker, valid, fault)
    taker = coordinate_calendar(taker, original_taker, valid, fault)
    def commit(population):
        return tuple((kind, replace(result, emitted=result.emitted & valid, fault=fault,
            account_delta=account.coordinate_delta(result.account_delta, valid, fault))) for kind, result in population)
    return account, maker, taker, RebatePhases(commit(finalized), commit(payments), fault)


@dataclass(frozen=True)
class RebateCoordinator:
    maker: object
    taker: object

    def next_clock(self):
        end, _, has_end = self.maker.next_end()
        maker, has_maker = self.maker.next_clock()
        taker, has_taker = self.taker.next_clock()
        times = torch.stack([end, maker, taker], dim=1)
        valid = torch.stack([has_end, has_maker, has_taker], dim=1)
        at = torch.where(valid, times, torch.full_like(times, (1 << 63) - 1)).min(1).values
        # 同刻原优先级：MakerMarketEnd→MakerDay→TakerDay。
        which = (valid & (times == at[:, None])).to(torch.int64).argmax(1)
        return at, which, valid.any(1)

    def advance_clock(self, account, at, enabled):
        roots = self.maker.fault.numel()
        if at.shape != (roots,) or at.dtype != torch.int64 or at.device != self.maker.days.device:
            raise TypeError('返佣时刻须同设备int64[roots]')
        original_account = account
        scheduled, which, exists = self.next_clock()
        fault = self.maker.fault | self.taker.fault | account.fault | ((at > scheduled) & exists & enabled).to(torch.int32)
        use = enabled & exists & (at == scheduled) & (fault == 0)
        maker, slot, ended = self.maker.advance_end(at, use & (which == 0))
        maker, account, market = maker.finalize_market(account, slot, ended)
        maker, _ = maker.advance_clock(at, use & (which == 1))
        taker, _ = self.taker.advance_clock(at, use & (which == 2))
        account, maker, taker, phases = settle_ready_days(account, maker, taker, use)
        fault = fault | phases.fault
        valid = fault == 0
        account = account.coordinate_result(original_account, fault)
        maker = coordinate_calendar(maker, self.maker, valid, fault)
        taker = coordinate_calendar(taker, self.taker, valid, fault)
        market = replace(market, emitted=market.emitted & valid, fault=fault,
            account_delta=account.coordinate_delta(market.account_delta, valid, fault))
        def commit(population):
            return tuple((kind, replace(result, emitted=result.emitted & valid, fault=fault,
                account_delta=account.coordinate_delta(result.account_delta, valid, fault))) for kind, result in population)
        phases = RebatePhases(commit(phases.finalized), commit(phases.payments), fault)
        return RebateCoordinator(maker, taker), account, RebateClockFrame(use & valid, (market,), phases, fault)
