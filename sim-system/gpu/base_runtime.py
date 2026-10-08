"""持续Base SIM运行状态：唯一hidden队列与请求/订单/账户/义务的装配。

保留各原模块的状态和纯候选边界；本装配不持有公共Ledger或推进NOE公开门。
支持原GTC/FOK pending接单、逐腿结果和末项完整响应；有界Actor发送由shared-run接线，持续宿主仍未交付。
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
    market_controls: object = None  # MarketFact映射/交易/质量；不冒充Terminal经济状态。
    source_resolutions: object = None
    resolution_obligations: object = None
    settlement_registrations: object = None  # 显式Registered/成功与有限查询场景；不代表完整结算后端。
    order_query_history: object = None  # 显式原订单保留水位；不代表列表查询RootSnapshot退役。

    input_frontier: object = None  # 所属整根实际输入提交前沿，非请求专用计数。

    post_results: object = None  # 原逐项Post正文，末项封口前保留真实第一结果。
    cancel_results: object = None  # 原逐目标取消正文，完整末项封口后仍保原决定。

    def install_order_query_history(self,history):
        if self.order_query_history is not None:
            raise ValueError('当前订单查询水位须沿原显式输入推进，不能冷覆盖')
        return replace(self,order_query_history=history)

    def advance_order_retention(self,retained_from,has_retained,enabled):
        if self.order_query_history is None:
            raise ValueError('须先安装同根实际订单查询历史')
        return replace(self,order_query_history=self.order_query_history.advance(retained_from,has_retained,enabled))

    def install_settlement_registration(self,conditions,receipt_offsets,success_offsets=None):
        from settlement_registration import SettlementRegistrations
        if self.settlement_registrations is not None:
            return self
        return replace(self,settlement_registrations=SettlementRegistrations.empty(conditions,receipt_offsets,success_offsets))

    def register_resolution_request(self,actions,sampler,identity_bytes,identity_offsets):
        if self.settlement_registrations is None or self.resolution_obligations is None:
            raise ValueError('请求首登记需实际已安装scenario和判定义务')
        registered=self.settlement_registrations.register(actions,sampler,identity_bytes,identity_offsets,
            self.resolution_obligations,self.calendars.maker,self.hidden_frontier.columns.source.terminal_columns)
        return replace(self,settlement_registrations=registered)

    def settlement_registered(self,choice,projection,sampler,policy):
        """真实阶段0回执与NOE共同候选；映射故障不能先消费scenario步骤。"""
        self._source(choice)
        original=self.settlement_registrations
        if original is None:
            raise ValueError('Registered消费需已安装实际scenario')
        backend,feedback=original.advance_registered(choice.at,choice.enabled(0))
        zeros=torch.zeros_like(feedback.conditions)
        projected,batch=projection.settlement_requests(sampler,feedback.conditions,feedback.resolutions,
            policy,zeros,zeros,feedback.at,feedback.emitted,feedback.fault | choice.fault)
        fault=backend.fault | batch.fault | choice.fault
        backend=replace(backend.coordinated(original,fault==0),fault=fault)
        batch=replace(batch,emitted=batch.emitted & (fault==0)[:,None],
            economic_enabled=batch.economic_enabled & (fault==0)[:,None],fault=fault)
        return replace(self,settlement_registrations=backend),projected,batch

    def settlement_success(self,choice,projection,sampler,policy,sources):
        self._source(choice)
        if sources.columns is not self.hidden_frontier.columns.source.terminal_columns:
            raise ValueError('兑付须绑定同一实际原Terminal字典')
        backend,account,feedback=self.settlement_registrations.advance_success(choice.at,choice.enabled(0),
            self.account,sources,self.resolution_obligations)
        condition,identity,assets,tokens,delta,emitted,fault=feedback
        projected,batch=projection.settlement_success(sampler,condition,identity,policy,assets,tokens,delta,choice.at,emitted,fault | choice.fault)
        fault |= batch.fault | choice.fault
        valid=fault==0
        backend=replace(backend.coordinated(self.settlement_registrations,valid),fault=fault)
        account=account.coordinate_result(self.account,fault)
        batch=replace(batch,emitted=batch.emitted & valid[:,None],economic_enabled=batch.economic_enabled & valid[:,None],fault=fault)
        return replace(self,settlement_registrations=backend,account=account),projected,batch

    def install_settlement_queries(self,timeout_offsets,reply_offsets,replies):
        if self.settlement_registrations is None:
            raise ValueError('查询场景需同一实际首登记目录')
        return replace(self,settlement_registrations=self.settlement_registrations.install_queries(timeout_offsets,reply_offsets,replies))

    def submit_resolution_queries(self,actions):
        if self.settlement_registrations is None:
            raise ValueError('查询需要既有请求')
        return replace(self,settlement_registrations=self.settlement_registrations.submit_queries(actions))

    def settlement_timeout(self,choice,projection,sampler,policy):
        self._source(choice)
        original=self.settlement_registrations
        backend,feedback=original.advance_timeout(choice.at,choice.enabled(0))
        projected,batch=projection.settlement_requests(sampler,feedback.conditions,feedback.resolutions,policy,
            torch.ones_like(feedback.conditions),torch.full_like(feedback.conditions,2),feedback.at,feedback.emitted,
            feedback.fault | choice.fault)
        fault=backend.fault | batch.fault | choice.fault
        backend=replace(backend.coordinated(original,fault==0),fault=fault)
        return replace(self,settlement_registrations=backend),projected,replace(batch,
            emitted=batch.emitted & (fault==0)[:,None],economic_enabled=batch.economic_enabled & (fault==0)[:,None],fault=fault)

    def settlement_query_reply(self,choice,projection,sampler,policy,sources):
        self._source(choice)
        if sources.columns is not self.hidden_frontier.columns.source.terminal_columns:
            raise ValueError('查询到账须实际原Terminal字典')
        backend,account,feedback=self.settlement_registrations.advance_query_reply(choice.at,choice.enabled(0),
            self.account,sources,self.resolution_obligations)
        condition,identity,assets,tokens,delta,emitted,fault,observation=feedback
        projected,batch=projection.settlement_success(sampler,condition,identity,policy,assets,tokens,delta,choice.at,emitted,
            fault | choice.fault,observation=observation)
        fault |= batch.fault | choice.fault
        valid=fault==0
        backend=replace(backend.coordinated(self.settlement_registrations,valid),fault=fault)
        account=account.coordinate_result(self.account,fault)
        return replace(self,settlement_registrations=backend,account=account),projected,replace(batch,
            emitted=batch.emitted & valid[:,None],economic_enabled=batch.economic_enabled & valid[:,None],fault=fault)

    def install_hidden_resolutions(self):
        """冷安装判定存储；不把未来Terminal、订单或成交当已应用事实。"""
        from hidden_resolution import SourceQualifiedResolutions,ResolutionObligations
        if self.source_resolutions is not None:
            return replace(self,resolution_obligations=self.resolution_obligations.resized(self.fills.capacity))
        source = self.hidden_frontier.columns.source
        roots = self.orders.roots
        return replace(self,source_resolutions=SourceQualifiedResolutions.empty(
            source.terminal_columns,roots,source.market_count,self.account.asset_roots.device),
            resolution_obligations=ResolutionObligations.empty(roots,source.market_count,self.fills))

    def prepare_hidden_terminal(self, choice, indices, condition_by_market, market_end,
                                source_assets=None):
        """原Terminal的市场/整订单/账户/执行义务共同候选及实际反馈。

        本方法不签发引用完成。调用方须把返回的完整反馈与候选、引用游标放在
        同一资源提交门；没有反馈存储或后继失败时保留整个进入前缀。
        请求式兑付不在此处自动到账，首次Resolved也不等请求发送屏障。
        """
        from account_state import grouped_fault
        from hidden_resolution import HiddenTerminalEffects
        from market_resolution_orders import resolve_market_base_orders
        self._source(choice)
        if self.source_resolutions is None or self.resolution_obligations is None or self.market_controls is None:
            raise ValueError('Terminal候选须先安装原市场/源判定/执行义务的冷容量')
        block = self.hidden_frontier.columns.source
        roots, markets = self.source_resolutions.present.shape
        owner = self.account.asset_roots
        if any(value.shape != (roots,markets) or value.dtype != torch.int64 or value.device != owner.device
               for value in (condition_by_market,market_end)):
            raise TypeError('原condition与end须为同设备完整root×market冷目录')
        source_assets = block.asset_axis if source_assets is None else source_assets
        if source_assets.shape != owner.shape or source_assets.dtype != torch.int64 or source_assets.device != owner.device:
            raise TypeError('判定源资产绑定须为同设备完整资产int64列')
        rows = block.reference_rows[indices.clamp(0,block.reference_rows.size(0)-1)]
        enabled = choice.enabled(4) & self.hidden_frontier.reference_enabled(indices) & (rows[:,0] == 9)
        axis = torch.arange(roots,device=owner.device)
        market = rows[:,1]
        if markets == 0:
            raise ValueError('Terminal来源必须属于已安装的原市场目录')
        safe_market = market.clamp(0,markets-1)
        end = market_end[axis,safe_market]
        condition = condition_by_market[axis,safe_market]
        source_invalid = (source_assets<0)|(source_assets>=len(block.assets))
        source_market = block.asset_markets[source_assets.clamp(0,len(block.assets)-1)]
        members = source_market == market[owner]
        population = torch.zeros(roots,dtype=torch.int64,device=owner.device).scatter_add(0,owner,members.to(torch.int64))
        uninitialized = torch.zeros(roots,dtype=torch.int32,device=owner.device).scatter_reduce(
            0,owner,(members & ~self.hidden.initialized).to(torch.int32),reduce='amax') != 0
        initialized = (population == block.asset_markets.eq(market[:,None]).sum(1)) & ~uninitialized
        initialized &= population > 0
        original_fault = grouped_fault(self.hidden.fault,owner,roots) | self.orders.fault | self.account.fault
        fault = original_fault | self.market_controls.fault | self.resolution_obligations.fault
        fault |= torch.where(enabled,grouped_fault(source_invalid.to(torch.int32),owner,roots),0)
        stable_reference = block.stable_reference(indices)
        source, transition = self.source_resolutions.observe(stable_reference,market,end,initialized,
            enabled & (fault == 0),torch.zeros_like(enabled))
        fault |= transition.fault
        obligations, resolved = self.resolution_obligations.register(transition,self.fills,condition,
            enabled & (fault == 0))
        matching_open = torch.zeros(roots,dtype=torch.int32,device=owner.device).scatter_reduce(
            0,owner,(members & ~self.hidden.matching_closed).to(torch.int32),reduce='amax') != 0
        orders, queue, account, effects = resolve_market_base_orders(self.orders,self.hidden.queues,
            self.account,condition,owner[self.hidden.level_assets],~matching_open,resolved,choice.at)
        fault |= obligations.fault | effects.fault
        committed = enabled & (fault == 0)
        selected = (torch.arange(markets,device=owner.device)[None,:] == market[:,None]) & committed[:,None]
        quality = block.terminal_columns.fields[stable_reference.clamp(0,block.terminal_columns.fields.size(0)-1),4]
        controls = replace(self.market_controls,quality=torch.where(selected,quality[:,None],self.market_controls.quality))
        from pending_base_execution import _coordinate_orders
        from base_acceptance import _coordinate_queue
        orders = _coordinate_orders(orders,self.orders,fault)
        queue = _coordinate_queue(queue,self.hidden.queues,owner[self.hidden.level_assets],fault)
        account = account.coordinate_result(self.account,fault)
        source = source.coordinate_result(self.source_resolutions,fault)
        obligations = obligations.coordinate_result(self.resolution_obligations,fault)
        controls = controls.coordinate_result(self.market_controls,fault)
        # 最后一个协调错误也不能让早先order effects在整市场回退后公开。
        events = tuple(replace(event,effect=replace(event.effect,closed=event.effect.closed & committed,
            delta=account.coordinate_delta(event.effect.delta,committed,fault),fault=fault)) for event in effects.events)
        effect = HiddenTerminalEffects(market,transition.reference,source.source_times[axis,safe_market],
            choice.at,replace(effects,events=events,fault=fault),resolved & committed,fault)
        candidate = replace(self,hidden=replace(self.hidden,queues=queue),orders=orders,account=account,
            market_controls=controls,source_resolutions=source,resolution_obligations=obligations)
        return candidate,effect

    def apply_hidden_terminal(self,choice,indices,condition_by_market,market_end,projection,sampler,asset_tokens,
                              source_assets=None):
        """真实Base订单市场关闭、释放和Resolved完整反馈共同成功后才消费引用。"""
        block=self.hidden_frontier.columns.source
        rows=block.reference_rows[indices.clamp(0,block.reference_rows.size(0)-1)]
        axis=torch.arange(self.orders.roots,device=indices.device)
        condition=condition_by_market[axis,rows[:,1].clamp(0,condition_by_market.size(1)-1)]
        candidate,effect=self.prepare_hidden_terminal(choice,indices,condition_by_market,market_end,source_assets)
        projected,batch=projection.market_resolution(sampler,effect,condition,block.terminal_columns,asset_tokens)
        return self._commit_hidden_terminal(candidate,effect,projected,batch,indices,choice.enabled(4),rows)

    def _commit_hidden_terminal(self,candidate,effect,projected,batch,indices,permitted,rows):
        fault = effect.fault | batch.fault
        from pending_base_execution import _coordinate_orders
        from base_acceptance import _coordinate_queue
        owner = self.account.asset_roots
        candidate = replace(candidate,
            hidden=replace(candidate.hidden,queues=_coordinate_queue(candidate.hidden.queues,self.hidden.queues,
                owner[self.hidden.level_assets],fault)),
            orders=_coordinate_orders(candidate.orders,self.orders,fault),
            account=candidate.account.coordinate_result(self.account,fault),
            market_controls=candidate.market_controls.coordinate_result(self.market_controls,fault),
            source_resolutions=candidate.source_resolutions.coordinate_result(self.source_resolutions,fault),
            resolution_obligations=candidate.resolution_obligations.coordinate_result(self.resolution_obligations,fault))
        committed = permitted & self.hidden_frontier.reference_enabled(indices) & (rows[:,0]==9) & (fault==0)
        events = tuple(replace(event,effect=replace(event.effect,closed=event.effect.closed & committed,
            delta=candidate.account.coordinate_delta(event.effect.delta,committed,fault),fault=fault))
            for event in effect.orders.events)
        effect = replace(effect,orders=replace(effect.orders,events=events,fault=fault),
            resolved=effect.resolved & committed,fault=fault)
        return candidate.hidden_reference_applied(indices,committed,fault),projected,batch,effect

    def install_hidden_market_controls(self):
        """冷安装原市场默认状态，不从未来参考或公开市场读面初始化业务字段。"""
        from hidden_reference import HiddenMarketControls
        if self.market_controls is not None:
            return self
        source = self.hidden_frontier.columns.source
        return replace(self, market_controls=HiddenMarketControls.empty(
            self.orders.roots, source.market_count, self.account.asset_roots.device))

    def choose(self, hidden_source, hidden_eof, advance_until, enabled, *, settlement=None,gtd=None,before=False):
        from account_state import grouped_fault
        fault=self.account.fault | self.orders.fault | grouped_fault(
            self.hidden.fault,self.account.asset_roots,self.orders.roots)
        if self.market_controls is not None:
            fault = fault | self.market_controls.fault
        if self.source_resolutions is not None:
            fault |= self.source_resolutions.fault | self.resolution_obligations.fault
        if self.order_query_history is not None:
            fault|=self.order_query_history.fault
        if self.hidden_frontier is not None:
            hidden_source,hidden_eof=self.hidden_frontier.scheduled()
        next_at=None
        if self.hidden_frontier is not None:
            bound=self.hidden_frontier.columns.next_bucket_at
            if bound is not None:
                next_at=bound.expand_as(advance_until)
        source=replace(hidden_source,fault=hidden_source.fault | fault)
        if settlement is None and self.settlement_registrations is not None:
            settlement=self.settlement_registrations.next_clock()
        choice=next_sim_drive(self.requests,self.fills,self.calendars,source,hidden_eof,
            advance_until,enabled,settlement=settlement,gtd=gtd,hidden_next_at=next_at,before=before)
        return replace(choice,hidden_frontier=self.hidden_frontier,settlement_registrations=self.settlement_registrations,
            order_query_history=self.order_query_history)

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
        snapshot_enabled = assets & (permitted & (tags == 1))[owner]
        active_snapshot = snapshot_enabled & ~hidden.book_closed
        snapshot_roots = torch.zeros(roots, dtype=torch.int32, device=owner.device).scatter_reduce(
            0, owner, active_snapshot.to(torch.int32), reduce='amax') != 0
        quantities, snapshot_counts, snapshot_fault = block.snapshot_values(
            hidden, indices, rows, owner, source_assets, level_sides, snapshot_roots)
        tick = rational_take(block._values(rows[:, 5]), owner)
        hidden, snapshot_required = hidden.initial_snapshot(snapshot_enabled, quantities, tick, rows[owner, 10])
        snapshot_fault = snapshot_fault | grouped_fault(hidden._asset_fault((snapshot_counts > 1).to(torch.int32)), owner, roots)
        hidden = replace(hidden, fault=hidden.fault | snapshot_fault[owner] * active_snapshot.to(torch.int32))
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

    def apply_hidden_reference(self, choice, indices, source_assets=None):
        """原ReferenceEvent/control MarketFact候选；Terminal等待真实经济共同处理器。"""
        from account_state import grouped_fault
        from hidden_market import rational_take
        self._source(choice)
        block = self.hidden_frontier.columns.source
        rows = block.reference_rows.index_select(0, indices.clamp(0, block.reference_rows.size(0)-1))
        tags = rows[:,0]
        enabled = choice.enabled(4) & self.hidden_frontier.reference_enabled(indices)
        if self.market_controls is None:
            return self.hidden_reference_applied(indices, enabled & (tags == 1),
                torch.zeros_like(self.hidden_frontier.fault))
        source_assets = block.asset_axis if source_assets is None else source_assets
        owner = self.account.asset_roots
        if source_assets.dtype != torch.int64 or source_assets.device != owner.device or source_assets.shape != owner.shape:
            raise TypeError('hidden参考源资产须为同设备完整int64绑定')
        supported = enabled & (tags >= 1) & (tags <= 8)
        control = self.market_controls.apply(rows, supported)
        if self.source_resolutions is not None:
            # 非Fresh Terminal后首次转Fresh会产生Resolution效果。该效果尚未
            # 接入此控制入口时继续等待，不能只改quality并消费掉原事实。
            markets = self.source_resolutions.present.size(1)
            if markets:
                axis = torch.arange(self.orders.roots,device=indices.device)
                safe = rows[:,1].clamp(0,markets-1)
                needs_resolution = self.source_resolutions.present[axis,safe] & (
                    self.resolution_obligations.reference_indices[axis,safe]<0) & (control.quality[axis,safe]==0)
                supported &= ~needs_resolution
                control = self.market_controls.apply(rows,supported)
        asset_event = supported & ((tags == 3) | (tags == 8))
        selected = asset_event[owner] & (source_assets == rows[owner,2])
        counts = torch.zeros_like(indices).scatter_add(0, owner, selected.to(torch.int64))
        fault = control.fault | (asset_event & (counts != 1)).to(torch.int32)
        hidden = self.hidden.initial_tick(selected & (tags == 3)[owner],
            rational_take(block._values(rows[:,3]), owner))
        hidden = hidden.quality_boundary(selected & (tags == 8)[owner],
            rows[owner,9], rows[owner,10], rows[owner,11] != 0)
        fault = fault | grouped_fault(hidden.fault, owner, self.orders.roots)
        candidate = replace(self, hidden=hidden.coordinate_result(self.hidden, owner, fault),
                            market_controls=control.coordinate_result(self.market_controls, fault))
        return candidate.hidden_reference_applied(indices, supported & (fault == 0), fault)

    def finish_hidden_bucket(self):
        from account_state import grouped_fault
        fault=self.account.fault | self.orders.fault | self.fills.fault | self.requests.fault
        fault=fault | self.calendars.maker.fault | self.calendars.taker.fault
        fault=fault | grouped_fault(self.hidden.fault,self.account.asset_roots,self.orders.roots)
        if self.market_controls is not None:
            fault = fault | self.market_controls.fault
        if self.source_resolutions is not None:
            fault |= self.source_resolutions.fault | self.resolution_obligations.fault
        frontier,closed=self.hidden_frontier.finish(fault)
        return replace(self,hidden_frontier=frontier),closed

    def _source(self, choice):
        # 对象绑定只确认纯候选来源；不读设备phase或业务值回CPU选择执行分支。
        if (choice.requests is not self.requests or choice.fills is not self.fills or
            choice.calendars is not self.calendars or choice.hidden_frontier is not self.hidden_frontier or
            choice.settlement_registrations is not self.settlement_registrations or
            choice.order_query_history is not self.order_query_history):
            raise ValueError('SIM阶段候选只能应用于选择时的同一活动状态')

    def accept_base(self, choice, terms, ask_rows, fee_rate, fee_exponent, selected_paths,
                    taker_rate, row_roots, result_refs):
        self._source(choice)
        requests,orders,queue,account,fills,effects=run_pending_base_acceptance(self.requests,self.orders,
            self.hidden.queues,self.account,self.fills,terms,ask_rows,fee_rate,fee_exponent,
            selected_paths,taker_rate,row_roots,result_refs,calendars=self.calendars,enabled=choice.enabled(5))
        return replace(self,requests=requests,orders=orders,hidden=replace(self.hidden,queues=queue),
            account=account,fills=fills,calendars=effects.calendars),effects

    def post_pending(self,choice,intents,rule,codec,ask_rows,selected_paths,taker_rate,row_roots,
                     result_refs,projection,sampler,asset_tokens):
        """实际阶段5：处理时准入/SDK、共同接单候选、各腿feedback与末项完整响应。"""
        from pending_base_execution import _coordinate_orders
        from base_acceptance import _coordinate_queue,_coordinate_pool
        from taker_days import coordinate_calendar
        from post_results import coordinate_results
        from asset_buy import take_ratio
        self._source(choice)
        if self.market_controls is None or self.post_results is None:
            raise ValueError('Post处理须有原hidden控制与逐项正文资源')
        pending=self.requests.next_pending()
        terms=intents.encode(pending,self.hidden,self.market_controls,rule,codec,choice.enabled(5))
        if self.hidden.level_sides is not None:
            hidden,terms=Numbers.run_exact_batch(lambda:self.hidden.pending_bid_levels(terms,self.account.asset_roots,choice.enabled(5)))
            self=replace(self,hidden=hidden)
            row_roots=self.account.asset_roots[hidden.level_assets]
        requests,orders,queue,account,fills,effects=run_pending_base_acceptance(self.requests,self.orders,
            self.hidden.queues,self.account,self.fills,terms,ask_rows,take_ratio(rule.values,
                torch.full((self.orders.roots,),2,dtype=torch.int64,device=ask_rows.device)),
            rule.context['fee_exponent'],selected_paths,taker_rate,row_roots,result_refs,
            calendars=self.calendars,enabled=choice.enabled(5),post_results=self.post_results)
        # Resource growth preserves earlier legs. Coordinate against the same expanded predecessor.
        original_orders=self.orders.resized(orders.capacity)
        original_queue=self.hidden.queues.resized(queue.capacity)
        original_fills=self.fills.resized(fills.capacity)
        original_results=self.post_results.resized(effects.post_results.capacity,effects.post_results.fill_capacity)
        original_calendars=replace(self.calendars,taker=self.calendars.taker.resized(
            fills=effects.calendars.taker.fill_occupied.size(1),days=effects.calendars.taker.occupied.size(1)))
        projected,batch=projection.pending_base_acceptance(sampler,orders,fills,effects,asset_tokens,
            post_results=effects.post_results)
        fault=effects.fault|batch.fault|choice.fault
        committed=fault==0
        # NOE故障只回退本项候选，不回退此前原成功前缀。
        requests=requests.coordinate_result(self.requests,committed,fault)
        orders=_coordinate_orders(orders,original_orders,fault)
        queue=_coordinate_queue(queue,original_queue,row_roots,fault)
        account=account.coordinate_result(self.account,fault)
        fills=_coordinate_pool(fills,original_fills,fault)
        calendars=replace(effects.calendars,
            maker=coordinate_calendar(effects.calendars.maker,original_calendars.maker,committed,fault),
            taker=coordinate_calendar(effects.calendars.taker,original_calendars.taker,committed,fault))
        results=coordinate_results(effects.post_results,original_results,committed)
        # Each Projection batch retains its own wanted masks and real source time.
        batch=replace(batch,emitted=batch.emitted&committed[:,None],economic_enabled=batch.economic_enabled&committed[:,None],fault=fault)
        emitter=replace(projected.emitter,
            next_private_sequence=projected.emitter.next_private_sequence.select(committed,projection.emitter.next_private_sequence),
            next_batch_sequence=projected.emitter.next_batch_sequence.select(committed,projection.emitter.next_batch_sequence),fault=fault)
        # Grow only physical storage; failed roots retain the original logical/economic directories.
        normalizer=projected.normalizer
        if projection.normalizer is None:
            original_logical=torch.full_like(normalizer.logical_rows,-1)
            original_economic=torch.full_like(normalizer.economic_rows,-1)
        else:
            addition=normalizer.logical_rows.size(1)-projection.normalizer.logical_rows.size(1)
            original_logical=torch.nn.functional.pad(projection.normalizer.logical_rows,(0,addition),value=-1)
            original_economic=torch.nn.functional.pad(projection.normalizer.economic_rows,(0,addition),value=-1)
        normalizer=replace(normalizer,logical_rows=torch.where(committed[:,None],normalizer.logical_rows,original_logical),
            economic_rows=torch.where(committed[:,None],normalizer.economic_rows,original_economic),fault=fault)
        projected=replace(projected,emitter=emitter,normalizer=normalizer)
        return replace(self,requests=requests,orders=orders,hidden=replace(self.hidden,queues=queue),account=account,
            fills=fills,calendars=calendars,post_results=results),projected,batch,replace(effects,fault=fault)

    def cancel_base(self, choice, target, row_roots, result_refs):
        self._source(choice)
        read=self.orders.find(target.order_sequence)
        safe=read.assets.clamp(0,self.hidden.initialized.numel()-1)
        closed=self.hidden.matching_closed[safe]
        requests,orders,queue,account,effects=run_pending_base_cancel(self.requests,self.orders,
            self.hidden.queues,self.account,target,row_roots,closed,result_refs,choice.enabled(5))
        return replace(self,requests=requests,orders=orders,hidden=replace(self.hidden,queues=queue),account=account),effects

    def prepare_cancel(self,choice,intents):
        self._source(choice)
        return intents.expand(self.requests.next_pending(),self.orders,choice.enabled(5))

    def cancel_pending(self,choice,work,row_roots,result_refs,projection,sampler,asset_tokens,scenario=None):
        from pending_cancel_request import run_cancel_pending_request
        self._source(choice)
        return run_cancel_pending_request(self,work,row_roots,result_refs,projection,sampler,asset_tokens,
            choice.enabled(5),scenario)

    def query_base(self,choice,target,result_refs,projection,sampler,asset_tokens):
        """实际阶段5的Query完成与NOE发射共同提交；未完成不消耗请求。"""
        from pending_order_query import run_pending_base_query
        self._source(choice)
        if self.order_query_history is None:
            raise ValueError('普通Query须绑定同根实际保留历史')
        history=self.order_query_history
        requests,effects=run_pending_base_query(self.requests,self.orders,target,history.retained_from,
            history.has_retained,result_refs,choice.enabled(5))
        projected,batch=projection.order_query(sampler,effects,asset_tokens)
        fault=effects.fault|batch.fault|choice.fault
        valid=fault==0
        requests=requests.coordinate_result(self.requests,valid,fault)
        response=replace(effects.response,emitted=effects.response.emitted&valid,fault=fault)
        effects=replace(effects,processed=effects.processed&valid,response=response,fault=fault)
        return replace(self,requests=requests),projected,replace(batch,
            emitted=batch.emitted&valid[:,None],economic_enabled=batch.economic_enabled&valid[:,None],fault=fault),effects

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
        from pending_base_execution import _coordinate_orders
        from taker_days import coordinate_calendar
        roots=torch.arange(self.orders.roots,device=choice.at.device)
        slot=effect.slot.clamp(0,self.fills.capacity-1)
        before_applied=self.fills.records.economic_applied[roots*self.fills.capacity+slot]
        # 原fee释放点即使fee为0仍更新；economic后的FAILED补偿不刷新订单时钟。
        changed=effect.event.emitted&(effect.event.economic_delta.emitted|
            ((effect.event.state==5)&~before_applied))
        read=self.orders.read(effect.origin.order_slots)
        same=read.sequence.compare(effect.origin.order_sequences)
        fault=effect.event.fault|torch.where(changed,read.fault|same.fault|
            (~read.present|(same.order!=0)).to(torch.int32),0)
        orders=self.orders.update(read,read.quantity,changed&(fault==0),choice.at)
        fault|=orders.fault
        committed=effect.event.emitted&(fault==0)
        fills=fills.coordinate_result(self.fills,committed,fault)
        account=account.coordinate_result(self.account,fault)
        maker=coordinate_calendar(maker,self.calendars.maker,committed,fault)
        taker=coordinate_calendar(taker,self.calendars.taker,committed,fault)
        event=replace(effect.event,emitted=committed,terminal=effect.event.terminal&committed,fault=fault,
            economic_delta=account.coordinate_delta(effect.event.economic_delta,committed,fault),
            failure_delta=account.coordinate_delta(effect.event.failure_delta,committed,fault),
            rebate_delta=account.coordinate_delta(effect.event.rebate_delta,committed,fault))
        effect=replace(effect,event=event)
        market=replace(market,emitted=market.emitted&committed,fault=fault,
            account_delta=account.coordinate_delta(market.account_delta,committed,fault))
        obligations = self.resolution_obligations
        if obligations is not None:
            from taker_days import coordinate_calendar
            obligations=Numbers.run_exact_batch(lambda:obligations.complete(effect))
            fault=effect.event.fault | obligations.fault
            committed=effect.event.emitted & (fault==0)
            fills=fills.coordinate_result(self.fills,committed,fault)
            account=account.coordinate_result(self.account,fault)
            maker=coordinate_calendar(maker,self.calendars.maker,committed,fault)
            taker=coordinate_calendar(taker,self.calendars.taker,committed,fault)
            obligations=obligations.coordinate_result(self.resolution_obligations,fault)
            event=replace(effect.event,emitted=committed,terminal=effect.event.terminal & committed,fault=fault,
                economic_delta=account.coordinate_delta(effect.event.economic_delta,committed,fault),
                failure_delta=account.coordinate_delta(effect.event.failure_delta,committed,fault),
                rebate_delta=account.coordinate_delta(effect.event.rebate_delta,committed,fault))
            effect=replace(effect,event=event)
            market=replace(market,emitted=market.emitted & committed,fault=fault,
                account_delta=account.coordinate_delta(market.account_delta,committed,fault))
        orders=_coordinate_orders(orders,self.orders,effect.event.fault)
        candidate=replace(self,fills=fills,account=account,orders=orders,calendars=RebateCoordinator(maker,taker),
            resolution_obligations=obligations)
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
