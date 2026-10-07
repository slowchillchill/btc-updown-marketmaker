"""持续Base SIM运行状态：唯一hidden队列与请求/订单/账户/义务的装配。

保留各原模块的状态和纯候选边界；本装配不持有公共Ledger或推进NOE公开门。
已准入GTC/单target与原单条hidden入口先接通，FOK/完整桶/selector继续由原后端补齐。
"""
from dataclasses import dataclass,replace
import torch
from queues import Numbers
from scheduling import next_sim_drive
from pending_base_execution import run_pending_base_acceptance
from pending_cancel_execution import run_pending_base_cancel
from hidden_base_execution import run_hidden_base_transition
from rebate_coordinator import RebateCoordinator


@dataclass(frozen=True)
class BaseSimRuntime:
    hidden: object
    requests: object
    orders: object
    account: object
    fills: object
    calendars: object

    def choose(self, hidden_source, hidden_eof, advance_until, enabled, *, settlement=None,gtd=None):
        from account_state import grouped_fault
        fault=self.account.fault | self.orders.fault | grouped_fault(
            self.hidden.fault,self.account.asset_roots,self.orders.roots)
        source=replace(hidden_source,fault=hidden_source.fault | fault)
        return next_sim_drive(self.requests,self.fills,self.calendars,source,hidden_eof,
            advance_until,enabled,settlement=settlement,gtd=gtd)

    def _source(self, choice):
        # 对象绑定只确认纯候选来源；不读设备phase或业务值回CPU选择执行分支。
        if choice.requests is not self.requests or choice.fills is not self.fills or choice.calendars is not self.calendars:
            raise ValueError('SIM阶段候选只能应用于选择时的同一活动状态')

    def accept_base(self, choice, terms, ask_rows, fee_rate, fee_exponent, selected_paths,
                    taker_rate, row_roots, result_refs):
        self._source(choice)
        requests,orders,queue,account,fills,effects=run_pending_base_acceptance(self.requests,self.orders,
            self.hidden.queues,self.account,self.fills,terms,ask_rows,fee_rate,fee_exponent,
            selected_paths,taker_rate,row_roots,result_refs,calendars=self.calendars,enabled=choice.enabled(5))
        return replace(self,requests=requests,orders=orders,hidden=replace(self.hidden,queues=queue),
            account=account,fills=fills,calendars=effects.calendars),effects

    def cancel_base(self, choice, target, row_roots, result_refs):
        self._source(choice)
        read=self.orders.find(target.order_sequence)
        safe=read.assets.clamp(0,self.hidden.initialized.numel()-1)
        closed=self.hidden.matching_closed[safe]
        requests,orders,queue,account,effects=run_pending_base_cancel(self.requests,self.orders,
            self.hidden.queues,self.account,target,row_roots,closed,result_refs,choice.enabled(5))
        return replace(self,requests=requests,orders=orders,hidden=replace(self.hidden,queues=queue),account=account),effects

    def hidden_base(self, choice, rows, before, after, source_price, condition, resync_inference,
                    selected_paths, row_roots, fee_rate, fee_exponent, taker_rate):
        self._source(choice)
        hidden,orders,account,fills,calendars,effects=run_hidden_base_transition(self.hidden,self.orders,
            self.account,self.fills,self.calendars,rows,before,after,source_price,condition,choice.at,
            choice.enabled(4),resync_inference,selected_paths,row_roots,fee_rate,fee_exponent,taker_rate)
        return replace(self,hidden=hidden,orders=orders,account=account,fills=fills,calendars=calendars),effects

    def finality(self, choice, maker_rate, taker_rate, market_end):
        self._source(choice)
        fills,account,maker,taker,effect,market=Numbers.run_exact_batch(lambda:self.fills.advance_mixed(
            self.account,self.calendars.maker,self.calendars.taker,choice.at,maker_rate,taker_rate,
            enabled=choice.enabled(2)))
        candidate=replace(self,fills=fills,account=account,calendars=RebateCoordinator(maker,taker))
        return candidate._finish_obligation_time(choice.at,effect.event.emitted,market_end),effect,market

    def rebate_clock(self, choice, market_end):
        self._source(choice)
        calendars,account,effect=Numbers.run_exact_batch(lambda:self.calendars.advance_clock(
            self.account,choice.at,choice.enabled(3)))
        candidate=replace(self,calendars=calendars,account=account)
        return candidate._finish_obligation_time(choice.at,effect.consumed,market_end),effect

    def _finish_obligation_time(self, at, consumed, market_end):
        """本Base装配实际义务已消费且本毫秒义务耗尽后，才推进role时间。

        同刻还有其它fill或返佣钟时不提前收尾。Settlement/GTD未来装配仍须
        在原更早阶段耗尽其义务，本方法不代替未交后端或原hidden桶关闭证明。
        """
        fill_at,_,has_fill=self.fills.next_obligation()
        clock_at,_,has_clock=self.calendars.next_clock()
        finished=consumed & ~(has_fill & (fill_at<=at)) & ~(has_clock & (clock_at<=at))
        hidden=Numbers.run_exact_batch(lambda:self.hidden.advance_matching_time(at[self.account.asset_roots],
            market_end,enabled=finished[self.account.asset_roots]))
        return replace(self,hidden=hidden)

    def advance_role_time(self, choice, market_end):
        self._source(choice)
        # 原义务阶段结束后、原hidden/请求之前调用；end只清撮合段，不释放订单/预留。
        enabled=choice.enabled(4) | choice.enabled(5)
        now=choice.at[self.account.asset_roots]
        hidden=Numbers.run_exact_batch(lambda:self.hidden.advance_matching_time(now,market_end,
            enabled=enabled[self.account.asset_roots]))
        return replace(self,hidden=hidden)
