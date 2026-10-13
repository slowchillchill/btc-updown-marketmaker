"""规范PublicTrade的同资产、价格优先、FIFO共享数量；复用原Maker经济消费者。"""
from dataclasses import replace
from fractions import Fraction
import torch
from queues import Numbers,QueueEffects,_take,_repeat
from asset_buy import take_ratio
from hidden_market import micro_units
from hidden_base_execution import apply_base_level_transition
from account_state import grouped_fault


class PublicTradeExecution:
    def __init__(self):
        from public_trade_loop import PublicTradeDeviceLoop
        self.binding=None;self.bid_rows=None;self.loop=PublicTradeDeviceLoop()

    def rows(self,hidden):
        # 只读价位目录的冷排序；新合法报价/布局安装时重建，不按每条成交回读。
        price=hidden.queues.price
        binding=(price.numerator.words.data_ptr(),price.denominator.words.data_ptr(),
            hidden.level_assets.data_ptr(),hidden.level_sides.data_ptr())
        if binding!=self.binding:
            values=[Fraction(a,b) for a,b in zip(price.numerator.report_values(),price.denominator.report_values())]
            assets=hidden.level_assets.cpu().tolist();sides=hidden.level_sides.cpu().tolist()
            rows=[sorted((i for i,a in enumerate(assets) if a==asset and sides[i]==0),key=lambda i:values[i],reverse=True)
                for asset in range(hidden.initialized.numel())]
            width=max(map(len,rows),default=0)
            self.bid_rows=torch.tensor([v+[-1]*(width-len(v)) for v in rows],dtype=torch.int64,device=hidden.level_assets.device)
            self.binding=binding
        return self.bid_rows

    def _apply(self,world,choice,index,condition_by_asset,source_assets,pairs,paths,fee,exponent,rebate,*,fifo_loop,maker_loop):
        block=world.hidden_frontier.columns.source
        indices=torch.full_like(choice.at,index);rows,tags,present=block.selected_rows(indices)
        roots=world.orders.roots;owner=world.account.asset_roots
        enabled=choice.enabled(4)&world.hidden_frontier.step_enabled(indices)&present&(tags==5)
        source_asset=rows[:,0]
        selected=(source_assets==source_asset[owner])&enabled[owner]
        matching=torch.full_like(choice.at,-1).scatter_reduce(0,owner,
            torch.where(selected,torch.arange(owner.numel(),device=owner.device),-1),reduce='amax')
        buy=rows[:,1]==0
        asset=torch.where(buy,pairs[matching.clamp_min(0)],matching)
        safe=asset.clamp(0,owner.numel()-1)
        enabled&=(matching>=0)&~world.hidden.matching_closed[safe]
        price=block._values(rows[:,2]);one=price.numerator.constant(1)
        from queues import ExactRational
        complement=ExactRational.ratio(one,one)-price
        lowest=replace(price,numerator=complement.numerator.select(buy,price.numerator),
            denominator=complement.denominator.select(buy,price.denominator))
        quantity=micro_units(block._values(rows[:,14])).select(enabled,world.account.cash.constant(0))
        conditions=condition_by_asset[safe]
        order=self.rows(world.hidden)[safe]
        original=world
        if order.size(1):
            output,effects=self.loop.take(world,order,quantity,lowest,enabled,conditions,choice.at,paths,fee,exponent,rebate)
            world=replace(world,hidden=replace(world.hidden,queues=replace(world.hidden.queues,
                conservative=output.rank,fault=output.queue_fault)),orders=output.orders,account=output.account,
                fills=output.fills,calendars=replace(world.calendars,maker=output.maker,
                    taker=replace(world.calendars.taker,fault=output.taker_fault)),
                resolution_obligations=None if world.resolution_obligations is None else world.resolution_obligations.resized(output.fills.capacity))
        else:effects=None
        fault=world.orders.fault|world.account.fault|world.fills.fault
        if effects is not None:
            population=torch.arange(effects.capacity,device=owner.device)<effects.count
            values=effects.member_buffer(effects.template.fault)
            fault|=grouped_fault(torch.where(population[:,None],values,0).flatten(),
                torch.arange(roots,device=owner.device).repeat(effects.capacity),roots)
        # 整条成交只推进一次原步骤，跨价位失败保原进入业务状态。
        from base_acceptance import _coordinate_queue,_coordinate_pool
        from pending_base_execution import _coordinate_orders
        from taker_days import coordinate_calendar
        world=replace(world,hidden=world.hidden.coordinate_result(original.hidden,owner,fault),
            orders=_coordinate_orders(world.orders,original.orders,fault),account=world.account.coordinate_result(original.account,fault),
            fills=_coordinate_pool(world.fills,original.fills.resized(world.fills.capacity),fault),
            calendars=replace(world.calendars,
                maker=coordinate_calendar(world.calendars.maker,original.calendars.maker.resized(
                    fills=world.calendars.maker.fill_occupied.size(1),days=world.calendars.maker.occupied.size(1)),fault==0,fault),
                taker=coordinate_calendar(world.calendars.taker,original.calendars.taker,fault==0,fault)))
        return world.hidden_step_applied(indices,enabled&(fault==0),fault),effects

    def run(self,world,choice,index,condition_by_asset,source_assets,pairs,paths,fee,exponent,rebate,*,fifo_loop,maker_loop):
        self.rows(world.hidden)
        while True:
            result=self.loop.run(lambda:Numbers.run_exact_batch(lambda:
                self._apply(world,choice,index,condition_by_asset,source_assets,pairs,paths,fee,exponent,rebate,
                    fifo_loop=fifo_loop,maker_loop=maker_loop)))
            candidate,effects=result
            if effects is None:return candidate,None,0
            population=torch.arange(effects.capacity,device=effects.count.device)<effects.count
            required=torch.stack([torch.where(population[:,None],effects.member_buffer(getattr(effects.template,name)),0).amax()
                for name in ('required_records','required_maker_fills','required_maker_days')])
            frames=torch.where(population,effects.member_buffer(effects.template.maker_page.count),0).sum()
            records,fills,days,frames=torch.cat((required,frames.reshape(1))).cpu().tolist()
            if (records<=world.fills.capacity and fills<=world.calendars.maker.fill_occupied.size(1)
                and days<=world.calendars.maker.occupied.size(1)):return candidate,effects,frames
            world=replace(world,fills=world.fills.resized(max(world.fills.capacity,1<<(max(1,records)-1).bit_length())),
                calendars=replace(world.calendars,maker=world.calendars.maker.resized(
                    fills=max(world.calendars.maker.fill_occupied.size(1),1<<(max(1,fills)-1).bit_length()),
                    days=max(world.calendars.maker.occupied.size(1),1<<(max(1,days)-1).bit_length()))))


def prepare_public_price(queue,orders,account,rows,budget,lowest,enabled,owners):
    roots=orders.roots;device=rows.device
    safe=rows.clamp(0,max(0,queue.rows-1));valid=rows>=0
    zero=budget.constant(0)
    price=take_ratio(queue.price,safe)
    relation=price.compare(lowest)
    active=enabled&valid&(relation.order>=0)
    selected=active[owners]&(torch.arange(queue.rows,device=device)==rows[owners])
    rank=queue.conservative
    has_orders=(rank.kind==2).any(1)
    row_budget=_take(budget,owners).select(selected&has_orders,queue.debt.constant(0))
    return dict(safe=safe,zero=zero,price=price,relation=relation,active=active,
        selected=selected,rank=rank,has_orders=has_orders,row_budget=row_budget)


def finish_public_price(queue,orders,account,budget,owners,plan,rank,left,ids,maker,actual):
    safe,zero,price,relation,active,has_orders,row_budget=(plan[n] for n in
        ('safe','zero','price','relation','active','has_orders','row_budget'))
    roots=orders.roots
    # 没有Account排位的更好价位只占本条成交预算；不建立可给后续订单复用的历史。
    taken=(row_budget-left).select(has_orders,actual)
    per_root=_take(taken,safe).select(active,zero)
    relation_budget=per_root.compare(budget)
    consumed=per_root.select(relation_budget.order<=0,budget)
    budget=budget-consumed
    faults=orders.fault|account.fault|grouped_fault(queue.fault,owners,roots)
    faults|=(relation.fault|relation_budget.fault|budget.fault)*active.to(torch.int32)
    effects=QueueEffects(ids,maker,queue.debt.constant(0),torch.zeros_like(queue.fault,dtype=torch.bool),
        (rank.kind!=0).sum(1),queue.fault|grouped_fault(maker.fault.reshape(queue.rows,queue.capacity).flatten(),
            owners.repeat_interleave(queue.capacity),roots)[owners])
    queue=replace(queue,conservative=rank)
    return queue,replace(orders,fault=faults),budget,effects
