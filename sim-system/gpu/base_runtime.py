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
    hidden_frontier: object = None

    def choose(self, hidden_source, hidden_eof, advance_until, enabled, *, settlement=None,gtd=None):
        from account_state import grouped_fault
        fault=self.account.fault | self.orders.fault | grouped_fault(
            self.hidden.fault,self.account.asset_roots,self.orders.roots)
        if self.hidden_frontier is not None:
            hidden_source,hidden_eof=self.hidden_frontier.scheduled()
        source=replace(hidden_source,fault=hidden_source.fault | fault)
        choice=next_sim_drive(self.requests,self.fills,self.calendars,source,hidden_eof,
            advance_until,enabled,settlement=settlement,gtd=gtd)
        return replace(choice,hidden_frontier=self.hidden_frontier)

    def begin_hidden_bucket(self, choice):
        self._source(choice)
        return replace(self,hidden_frontier=self.hidden_frontier.begin(choice))

    def apply_hidden_step(self, choice, index, condition_by_asset, selected_paths,
                          fee_rate, fee_exponent, taker_rate):
        """固定原源step发射，根是否执行由完整桶设备游标决定。

        当前列与运行资产/价位同一绑定。参考仍交原参考处理器；本方法不能
        给未处理参考签发完成，也不把LevelChange另送一次资产FIFO入口。
        """
        from account_state import grouped_fault
        self._source(choice)
        block=self.hidden_frontier.columns.source
        if (condition_by_asset.shape != self.hidden.initialized.shape or
            condition_by_asset.dtype != torch.int64 or condition_by_asset.device != self.account.asset_roots.device):
            raise TypeError('原condition绑定须为同设备完整资产int64列')
        enabled=choice.enabled(4) & self.hidden_frontier.step_enabled(index)
        row=block.event_indices[index]
        owner=self.account.asset_roots
        source_fault=grouped_fault((block.level_assets != self.hidden.level_assets).to(torch.int32),
                                  owner[self.hidden.level_assets],self.orders.roots)
        enabled=enabled & (source_fault == 0)
        if block.operations[index]['tag'] == 'LevelChange':
            target,price_fault=block._level_lookup(row[0],row[1],row[2])
            source_fault=source_fault | grouped_fault(self.hidden._asset_fault(price_fault),owner,self.orders.roots)
            level_roots=owner[self.hidden.level_assets]
            counts=torch.zeros_like(enabled,dtype=torch.int64).scatter_add(0,level_roots,target.to(torch.int64))
            rows=torch.full_like(counts,-1).scatter_reduce(0,level_roots,
                torch.where(target,torch.arange(target.numel(),device=owner.device),-1),reduce='amax')
            source_fault=source_fault | torch.where(enabled,(counts != 1).to(torch.int32),0)
            choice=replace(choice,present=enabled & (source_fault == 0))
            condition=condition_by_asset.index_select(0,row[0].reshape(1)).expand(self.orders.roots)
            candidate,effect=self.hidden_base(choice,rows,block._broadcast(row[3],self.orders.roots),
                block._broadcast(row[4],self.orders.roots),block._broadcast(row[2],self.orders.roots),
                condition,(row[9] != 0).expand(self.orders.roots),selected_paths,level_roots,
                fee_rate,fee_exponent,taker_rate)
            fault=effect.fault | source_fault
        else:
            hidden,effect=block.apply(self.hidden,index,enabled_assets=enabled[owner])
            fault=grouped_fault(hidden.fault,owner,self.orders.roots) | source_fault
            candidate=replace(self,hidden=hidden.coordinate_result(self.hidden,owner,fault))
        return candidate.hidden_step_applied(index,enabled & (fault == 0),fault),effect

    def hidden_step_applied(self, index, committed, fault):
        # 原处理器完整提交的step才记进度；不能拿材料预读或公开前沿作committed。
        return replace(self,hidden_frontier=self.hidden_frontier.step_applied(index,committed,fault))

    def apply_hidden_selected(self, choice, indices, condition_by_asset, selected_paths,
                              fee_rate, fee_exponent, taker_rate, source_assets, level_sides):
        """每根本轮只读取一个原源步；固定类型候选都使用同一进入游标。"""
        from account_state import grouped_fault
        from hidden_market import rational_take
        self._source(choice)
        block = self.hidden_frontier.columns.source
        owner = self.account.asset_roots
        roots = self.orders.roots
        for binding, shape in ((source_assets, owner.shape), (level_sides, self.hidden.level_assets.shape),
                               (condition_by_asset, owner.shape)):
            if binding.dtype != torch.int64 or binding.device != owner.device or binding.shape != shape:
                raise TypeError('hidden源资产、侧与condition绑定须为同设备完整int64列')
        rows, tags, present = block.selected_rows(indices)
        enabled = choice.enabled(4) & self.hidden_frontier.step_enabled(indices)
        source_invalid = (source_assets < 0) | (source_assets >= len(block.assets))
        source_fault = grouped_fault(source_invalid.to(torch.int32), owner, roots)
        source_fault = source_fault | (enabled & ~present).to(torch.int32)
        permitted = enabled & present & (source_fault == 0)
        level_enabled = permitted & (tags == 2)
        target, price_fault = block.selected_lookup(self.hidden, rows, owner, source_assets, level_sides)
        level_roots = owner[self.hidden.level_assets]
        counts = torch.zeros(roots, dtype=torch.int64, device=owner.device).scatter_add(
            0, level_roots, target.to(torch.int64))
        source_fault = source_fault | torch.where(level_enabled,
            grouped_fault(self.hidden._asset_fault(price_fault), owner, roots) | (counts != 1).to(torch.int32), 0)
        queue_rows = torch.full_like(counts, -1).scatter_reduce(0, level_roots,
            torch.where(target, torch.arange(target.numel(), device=owner.device), -1), reduce='amax')
        assets = (source_assets == rows[owner, 0])
        conditions = torch.zeros(roots, dtype=torch.int64, device=owner.device).scatter_reduce(
            0, owner, torch.where(assets, condition_by_asset, 0), reduce='amax')
        level_choice = replace(choice, present=level_enabled & (source_fault == 0))
        candidate, effect = self.hidden_base(level_choice, queue_rows,
            block._values(rows[:, 3]), block._values(rows[:, 4]), block._values(rows[:, 2]),
            conditions, rows[:, 9] != 0, selected_paths, level_roots, fee_rate, fee_exponent, taker_rate)
        hidden = candidate.hidden
        quantities, snapshot_counts, missing, snapshot_price_fault = block.snapshot_values(
            hidden, indices, rows, owner, source_assets, level_sides)
        snapshot_enabled = assets & (permitted & (tags == 1))[owner]
        active_snapshot = snapshot_enabled & ~hidden.book_closed
        tick = rational_take(block._values(rows[:, 5]), owner)
        hidden, snapshot_required = hidden.initial_snapshot(snapshot_enabled, quantities, tick, rows[owner, 10])
        snapshot_fault = hidden._asset_fault((snapshot_counts > 1).to(torch.int32) | snapshot_price_fault)
        hidden = replace(hidden, fault=hidden.fault | (snapshot_fault | missing[owner]) * active_snapshot.to(torch.int32))
        tick_enabled = assets & (permitted & (tags == 3))[owner]
        hidden = hidden.tick_change(tick_enabled, rational_take(block._values(rows[:, 6]), owner),
                                    rows[owner, 8] != 0, rational_take(block._values(rows[:, 7]), owner))
        quality_enabled = assets & (permitted & (tags == 4))[owner]
        hidden = hidden.quality_boundary(quality_enabled, rows[owner, 11], rows[owner, 12], rows[owner, 13] != 0)
        fault = grouped_fault(hidden.fault, owner, roots) | source_fault | effect.fault
        hidden = hidden.coordinate_result(self.hidden, owner, fault)
        candidate = replace(candidate, hidden=hidden)
        effect = replace(effect, fault=fault,
                         required_segments=torch.maximum(effect.required_segments, snapshot_required))
        return candidate.hidden_step_applied(indices, enabled & (fault == 0), fault), effect

    def hidden_reference_applied(self, index, committed, fault):
        return replace(self,hidden_frontier=self.hidden_frontier.reference_applied(index,committed,fault))

    def finish_hidden_bucket(self):
        from account_state import grouped_fault
        fault=self.account.fault | self.orders.fault | self.fills.fault | self.requests.fault
        fault=fault | self.calendars.maker.fault | self.calendars.taker.fault
        fault=fault | grouped_fault(self.hidden.fault,self.account.asset_roots,self.orders.roots)
        frontier,closed=self.hidden_frontier.finish(fault)
        return replace(self,hidden_frontier=frontier),closed

    def _source(self, choice):
        # 对象绑定只确认纯候选来源；不读设备phase或业务值回CPU选择执行分支。
        if (choice.requests is not self.requests or choice.fills is not self.fills or
            choice.calendars is not self.calendars or choice.hidden_frontier is not self.hidden_frontier):
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
