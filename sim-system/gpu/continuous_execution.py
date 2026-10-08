"""持续Post/Cancel原正文→原SIM处理器→原NOE批；不建立CPU业务宿主。"""
from dataclasses import replace
import torch
from queues import Numbers


class ContinuousExecution:
    def __init__(self,population,rule,codec,sampler,ask_rows,asset_tokens,market_end,
            taker_rate,maker_rate,condition_by_asset,source_assets,fill_paths,cancel_scenario):
        self.population=population;self.rule=rule;self.codec=codec;self.sampler=sampler
        self.ask_rows=ask_rows;self.asset_tokens=asset_tokens;self.market_end=market_end
        self.taker_rate=taker_rate;self.maker_rate=maker_rate
        self.condition_by_asset=condition_by_asset;self.source_assets=source_assets
        self.fill_paths=fill_paths;self.cancel_scenario=cancel_scenario
        self.expanded_cancel=None

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
        projection,batch=Numbers.run_exact_batch(lambda:projection.fill_from_origin(self.sampler,world.orders,world.fills,
            frame,self.asset_tokens,world.calendars,self.taker_rate,choice.at))
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
            world=Numbers.run_exact_batch(lambda:world.apply_hidden_reference(self.refresh(world,choice),
                torch.full_like(choice.at,index),self.source_assets))
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

    def handlers(self):
        return {2:self.finality,3:self.rebate,4:self.hidden,5:self.mutation}
