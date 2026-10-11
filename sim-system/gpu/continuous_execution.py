"""持续Post/Cancel原正文→原SIM处理器→原NOE批；不建立CPU业务宿主。"""
from dataclasses import replace
import torch
from queues import Numbers


class ContinuousExecution:
    def __init__(self,population,rule,codec,sampler,ask_rows,asset_tokens,market_end,
            taker_rate,maker_rate,condition_by_asset,source_assets,fill_paths,cancel_scenario,*,
            condition_by_market=None,market_end_by_market=None,redemption_sources=None,settlement_policy=None,
            resolution_identity_bytes=None,resolution_identity_offsets=None):
        self.population=population;self.rule=rule;self.codec=codec;self.sampler=sampler
        self.ask_rows=ask_rows;self.asset_tokens=asset_tokens;self.market_end=market_end
        self.taker_rate=taker_rate;self.maker_rate=maker_rate
        self.condition_by_asset=condition_by_asset;self.source_assets=source_assets
        self.fill_paths=fill_paths;self.cancel_scenario=cancel_scenario
        self.expanded_cancel=None
        self.condition_by_market=condition_by_market;self.market_end_by_market=market_end_by_market
        self.redemption_sources=redemption_sources;self.settlement_policy=settlement_policy
        self.resolution_identity_bytes=resolution_identity_bytes;self.resolution_identity_offsets=resolution_identity_offsets

    @staticmethod
    def refresh(world,choice):
        return replace(choice,requests=world.requests,fills=world.fills,calendars=world.calendars,
            hidden_frontier=world.hidden_frontier,settlement_registrations=world.settlement_registrations,
            order_query_history=world.order_query_history)

    def mutation(self,world,projection,choice):
        world=world.advance_role_time(choice,self.market_end)
        pending=world.requests.next_pending()
        method=pending.source.methods.gather(1,pending.request_slot[:,None])[:,0]
        post=(method==0)|(method==1)
        frames=[];effect=None
        for is_post in (True,False):
            active=choice.present&(post if is_post else ~post)
            if not bool(active.any().cpu()):continue
            selected=replace(self.refresh(world,choice),present=active)
            pending=world.requests.next_pending()
            owners=world.account.asset_roots[world.hidden.level_assets]
            if is_post:
                intents=self.population.post_intents
                asset=intents.assets.gather(1,pending.item_ref.clamp(0,intents.assets.size(1)-1)[:,None])[:,0]
                asks=self.ask_rows[asset.clamp(0,self.ask_rows.size(0)-1)]
                paths=self.fill_paths('taker',world,asks)
                world,projection,batch,effect=world.post_pending(selected,intents,self.rule,self.codec.post_terms_batch_v2,
                    asks,paths,self.taker_rate,owners,pending.item_ref,projection,self.sampler,self.asset_tokens)
            else:
                if self.expanded_cancel is None:
                    self.expanded_cancel=Numbers.run_exact_batch(lambda:world.prepare_cancel(selected,self.population.cancellation.intents))
                work=self.expanded_cancel
                # 原ExpandedCancel绑定原pending；容量退出仅续这份目标，不再展开市场人口。
                world,projection,batch,effect=world.cancel_pending(selected,work,owners,pending.item_ref,
                    projection,self.sampler,self.asset_tokens,self.cancel_scenario)
                if not bool(batch.fault.any().cpu()):self.expanded_cancel=None
            frames.append(batch)
            if bool(batch.fault.any().cpu()):break
        return world,projection,tuple(frames),effect

    def finality(self,world,projection,choice):
        world,frame,effect=Numbers.run_exact_batch(lambda:world.finality(choice,self.maker_rate,self.taker_rate,self.market_end))
        from rebate_coordinator import settle_ready_days,RebateCoordinator
        account,maker,taker,phases=Numbers.run_exact_batch(lambda:settle_ready_days(world.account,
            world.calendars.maker,world.calendars.taker,frame.event.emitted))
        world=replace(world,account=account,calendars=RebateCoordinator(maker,taker))
        effect=replace(effect,fault=effect.fault|phases.fault)
        projection,batch=Numbers.run_exact_batch(lambda:projection.fill_from_origin(self.sampler,world.orders,world.fills,
            frame,self.asset_tokens,world.calendars,self.taker_rate,choice.at,maker_results=(effect,),
            rebate_phases=phases))
        return world,projection,batch,effect

    def rebate(self,world,projection,choice):
        world,frame=Numbers.run_exact_batch(lambda:world.rebate_clock(choice,self.market_end))
        projection,batch=Numbers.run_exact_batch(lambda:projection.rebate_clock(self.sampler,frame,choice.at))
        return world,projection,batch,frame

    def hidden(self,world,projection,choice):
        from asset_buy import take_ratio
        world=world.begin_hidden_bucket(choice);block=world.hidden_frontier.columns.source
        batches=[];effect=None
        for index in range(block.reference_rows.size(0)):
            indices=torch.full_like(choice.at,index)
            # 同一原引用游标：Terminal交所属经济消费者，其余引用沿原处理器。
            if int(block.reference_rows[index,0].cpu())==9:
                world,projection,batch,effect=Numbers.run_exact_batch(lambda:world.apply_hidden_terminal(
                    self.refresh(world,choice),indices,self.condition_by_market,self.market_end_by_market,
                    projection,self.sampler,self.asset_tokens,self.source_assets))
                batches.append(batch)
                if bool((batch.fault|effect.fault).any().cpu()):return world,projection,tuple(batches),effect
                world,projection,autos=Numbers.run_exact_batch(lambda:world.try_auto_redeem(
                    choice.at,choice.present,projection,self.sampler,preceding=(batch,)))
                batches.extend(autos)
                if any(bool(value.fault.any().cpu()) for value in autos):return world,projection,tuple(batches),effect
            else:
                world=Numbers.run_exact_batch(lambda:world.apply_hidden_reference(self.refresh(world,choice),
                    indices,self.source_assets))
        for index in range(len(block.operations)):
            selected=self.refresh(world,choice)
            fee=Numbers.run_exact_batch(lambda:take_ratio(self.rule.values,torch.full_like(choice.at,2)))
            world,effect=Numbers.run_exact_batch(lambda:world.apply_hidden_selected(selected,torch.full_like(choice.at,index),
                self.condition_by_asset,self.fill_paths('maker',world,None),
                fee,self.rule.context['fee_exponent'],self.taker_rate,self.source_assets,world.hidden.level_sides))
            if bool(effect.fault.any().cpu()):return world,projection,tuple(batches),effect
            for frame in effect.fills:
                projection,batch=Numbers.run_exact_batch(lambda:projection.fill_from_origin(self.sampler,world.orders,world.fills,
                    frame,self.asset_tokens,world.calendars,self.taker_rate,choice.at))
                batches.append(batch)
                if bool(batch.fault.any().cpu()):return world,projection,tuple(batches),effect
        world,_=Numbers.run_exact_batch(world.finish_hidden_bucket)
        return world,projection,tuple(batches),effect

    def register_settlement(self,world,actions):
        """公开协调器的原请求/查询提交；不在SIM重做公开首次资格。"""
        if bool(actions.request.any().cpu()):
            world=Numbers.run_exact_batch(lambda:world.register_resolution_request(actions,self.sampler,
                self.resolution_identity_bytes,self.resolution_identity_offsets))
        if bool(actions.query.any().cpu()):
            world=Numbers.run_exact_batch(lambda:world.submit_resolution_queries(actions))
        return world

    def settlement(self,world,projection,choice):
        """阶段0只分发原已安装scenario当前步骤，不另建兑付生命周期。"""
        backend=world.settlement_registrations
        at=choice.at[:,None];query=backend.query_state
        masks=[backend.present&~backend.received&(backend.first_arrival+backend.receipt_offsets==at)]
        if query is not None:
            masks.extend((backend.present&backend.received&~query.timeout_consumed&
                (backend.first_arrival+query.timeout_offsets==at),
                backend.present&query.timeout_consumed&~query.reply_consumed&
                (backend.first_arrival+query.reply_offsets==at)))
        done=backend.succeeded if backend.terminal_consumed is None else backend.terminal_consumed
        if backend.success_offsets is not None:
            success=backend.present&backend.received&~done&(backend.first_arrival+backend.success_offsets==at)
            if query is not None:success &= query.reply_consumed
            masks.append(success)
        methods=[world.settlement_registered]
        if query is not None:methods.extend((world.settlement_timeout,world.settlement_query_reply))
        if backend.success_offsets is not None:methods.append(world.settlement_success)
        batches=[];remaining=choice.present.clone()
        for mask,method in zip(masks,methods,strict=True):
            active=remaining&mask.any(1)
            if not bool(active.any().cpu()):continue
            selected=replace(self.refresh(world,choice),present=active)
            # 绑定最新world的方法，保同根同刻每轮一个原scenario步骤。
            function=getattr(world,method.__name__)
            args=(selected,projection,self.sampler,self.settlement_policy)
            if method.__name__ in ('settlement_success','settlement_query_reply'):
                policy=self.settlement_policy.gather(1,mask.to(torch.int64).argmax(1)[:,None])[:,0]
                args=(selected,projection,self.sampler,policy,self.redemption_sources)
            world,projection,batch=Numbers.run_exact_batch(lambda:function(*args))
            batches.append(batch);remaining &= ~active
            if bool(batch.fault.any().cpu()):break
        return world,projection,tuple(batches),None

    def handlers(self):
        def with_auto(method):
            def advance(world,projection,choice):
                world,projection,batch,effect=method(world,projection,choice)
                batches=() if batch is None else batch if isinstance(batch,tuple) else (batch,)
                fault=choice.fault.clone()
                for value in batches:fault |= value.fault
                if effect is not None and hasattr(effect,'fault'):fault |= effect.fault
                world,projection,autos=Numbers.run_exact_batch(lambda:world.try_auto_redeem(
                    choice.at,choice.present&(fault==0),projection,self.sampler,
                    preceding=batches))
                return world,projection,batches+autos,effect
            return advance
        return {phase:with_auto(method) for phase,method in
            ((0,self.settlement),(2,self.finality),(3,self.rebate),(4,self.hidden),(5,self.mutation))}
