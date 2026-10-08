"""冻结Actor完整正文的一次实际发送；公共层不反向导入具体模型。"""
from dataclasses import dataclass,replace
from fractions import Fraction
import json
import torch
from pending_post_intents import _integer_rows,_decimal
from reservation import RESERVED


def exact_values(value):
    return _integer_rows(value.words.detach().cpu().tolist(),value.sign.detach().cpu().tolist())


def decimal_values(value):
    return [_decimal(Fraction(n,d)) for n,d in zip(exact_values(value.numerator),
        exact_values(value.denominator),strict=True)]


@dataclass(frozen=True)
class IntentRequestIdentities:
    contexts: tuple  # 原RequestContextV2，每根稳定global_request_id/相关ID。
    conditions: tuple
    tokens: tuple
    orders: tuple  # 原完整平台身份目录，键为原完整SIM u64，不能生成sim-N冒充。

    def __post_init__(self):
        object.__setattr__(self,'contexts',tuple(dict(value) for value in self.contexts))
        object.__setattr__(self,'conditions',tuple(tuple(value) for value in self.conditions))
        object.__setattr__(self,'tokens',tuple(tuple(value) for value in self.tokens))
        object.__setattr__(self,'orders',tuple(dict(value) for value in self.orders))


def build_selected_requests(action,identities,codec):
    """一次完整人口搬运，只编码已经选定正文；不评分/抽样/推进业务。

    Native共用原intent-bridge pure builder，item ID、ordinal及类型映射归该原入口。
    本层复用原精确十进制传输，不复制SDK计量公式或ModelIntent合同。
    """
    families=action.family.detach().cpu().tolist();roots=len(families)
    legs=action.legs;terms=legs.terms
    assets=action.assets.detach().cpu().tolist();conditions=legs.condition_slots.detach().cpu().tolist()
    variants=terms.variants.detach().cpu().tolist();post_only=terms.post_only.detach().cpu().tolist()
    prices=decimal_values(terms.price);sizes=decimal_values(legs.target);spend=decimal_values(terms.spend)
    cancels=exact_values(action.cancel_orders);cancel_on=action.cancel_present.detach().cpu().tolist()
    cancel_width=action.cancel_present.size(1);width=legs.present.size(1)
    encoded=[];selected=[]
    for root,family in enumerate(families):
        if family<1:continue
        if family in (1,2):
            body=[]
            for ordinal in range(1 if family==1 else 2):
                i=root*width+ordinal
                if variants[root][ordinal]==0:
                    order={'GtcLimit':{'limit_price_pusd_per_share':prices[i],'size_shares':sizes[i],
                        'post_only':post_only[root][ordinal]}}
                else:
                    order={'FokMarket':{'spend_pusd':spend[i],'worst_price_pusd_per_share':prices[i]}}
                body.append({'token_id':identities.tokens[root][assets[root][ordinal]],'order':order})
            condition=identities.conditions[root][conditions[root][0]]
            intent={'SubmitSingleBuy':{'condition_id':condition,'order':body[0]}} if family==1 else \
                {'SubmitPairedBuy':{'condition_id':condition,'orders':body}}
        elif family in (3,4):
            ids=[identities.orders[root][cancels[root*cancel_width+i]] for i,on in enumerate(cancel_on[root]) if on]
            intent={'CancelOrder':{'platform_order_id':ids[0]}} if family==3 else {'CancelOrders':{'platform_order_ids':ids}}
        else:
            condition=action.cancellation.markets.detach().cpu().tolist()[root][0]
            intent={'CancelMarketOrders':{'condition_id':identities.conditions[root][condition]}}
        encoded.append((identities.contexts[root],intent));selected.append(root)
    requests=json.loads(codec.model_intent_requests_v2(json.dumps(encoded)))
    return tuple(selected),tuple(requests)


@dataclass(frozen=True)
class ActorSendResult:
    publication: object
    receipt: object
    sent: torch.Tensor
    complete: torch.Tensor
    advance: object = None


class FrozenActorSend:
    """调用者持有本次决定及原receipt；无全局去重库或旧账户结果镜像。

    dispatch复用原Post/Cancel submit协调，receive负责同waiting的可应用记录；
    它们返回实际物理需求，资源不足保原已发布正文和唯一RNG提交。
    """
    def __init__(self,publication,driver,control,identities,codec,dispatch,receive,observation,*,decision_cutoff):
        self.publication,self.driver=publication,driver
        self.control=replace(control,decisions=control.decisions.clone(),global_requests=control.global_requests.clone(),
            leg_requests=control.leg_requests.clone(),execute_at=control.execute_at.clone())
        self.identities,self.codec,self.dispatch,self.receive=identities,codec,dispatch,receive
        self.observation=(observation[0].clone(),*observation[1:])
        self.requests=None;self.receipt=None;self.delivery=None
        self.decision_cutoff=decision_cutoff
        self.sent=torch.zeros_like(control.execute_at,dtype=torch.bool)
        self.complete=torch.zeros_like(self.sent)
        self.received=torch.zeros_like(self.sent)

    def execute(self,clock,enabled,*,progress=None):
        active=enabled&~self.complete
        if not bool(active.any().cpu()):
            return ActorSendResult(self.publication,self.receipt,self.sent.clone(),self.complete.clone())
        advance=clock.advance_before(self.control.execute_at,active,progress=progress)
        active &= advance.complete
        if not bool(active.any().cpu()):
            return ActorSendResult(self.publication,self.receipt,self.sent.clone(),self.complete.clone(),advance)
        fresh=active & ~self.received
        if bool(fresh.any().cpu()):
            self.delivery=clock.receive_first(enabled=fresh)
            self.received |= fresh if self.delivery is None else fresh & (self.delivery.fault==0)
        clock.public,ready=self.receive(clock.public,self.delivery,active)
        active &= ready & self.received
        if not bool(active.any().cpu()):
            return ActorSendResult(self.publication,self.receipt,self.sent.clone(),self.complete.clone(),advance)
        if progress:progress("publication_boundary",input_sequence=exact_values(clock.runtime.input_frontier.next_sequence),
            ledger_prefix=exact_values(clock.public.ledger.current_local_prefix))
        clock.public,self.publication=self.driver.publish_and_reserve(self.publication,clock.public,self.control,enabled=active)
        action=self.publication.action
        if action is None:
            return ActorSendResult(self.publication,self.receipt,self.sent.clone(),self.complete.clone(),advance)
        inactive=action.family<1
        complete=self.complete | (active&inactive&self.publication.candidate.complete&(action.fault==0))
        reservation=self.publication.reservation
        if reservation is None:
            self.complete=complete
            return ActorSendResult(self.publication,self.receipt,self.sent.clone(),complete.clone(),advance)
        buy=(action.family==1)|(action.family==2)
        allowed=reservation.complete&(~buy|((reservation.decision.outcomes==RESERVED)|~action.legs.present).all(1))
        complete|=active&buy&reservation.complete&~allowed
        send=active&self.publication.published&allowed&~inactive&~self.sent
        if not bool(send.any().cpu()):
            self.complete=complete
            return ActorSendResult(self.publication,self.receipt,self.sent.clone(),complete.clone(),advance)
        if self.requests is None:self.requests=build_selected_requests(action,self.identities,self.codec)
        runtime,public,receipt,enough=self.dispatch(clock.runtime,clock.public,self.requests,action,send,cutoff=self.decision_cutoff)
        clock.runtime,clock.public=runtime,public
        committed=send&(receipt.registered|receipt.duplicate)&enough&(receipt.fault==0)
        self.sent|=committed
        from admission import coordinate
        self.receipt=receipt if self.receipt is None else coordinate(receipt,self.receipt,committed)
        self.complete=complete|committed
        return ActorSendResult(self.publication,self.receipt,self.sent.clone(),self.complete.clone(),advance)


class ActorRequestSubmission:
    """原完整BusinessRequest的实际Post/Cancel消费者。

    控制面拥有真实输入序、本地send event及身份字典；这里仅将原正文装入
    所属pending列并调用原登记入口。条款仍由处理时SDK入口计算。
    """
    def __init__(self,control,send_events,sampler,rule,identity_bytes,identity_offsets,
            conditions,tokens,bid_rows,market_end,codec,*,post_intents=None,order_sequences=None):
        self.control=control;self.send_events=send_events
        self.sampler,self.rule,self.identity_bytes,self.identity_offsets=sampler,rule,identity_bytes,identity_offsets
        self.conditions,self.tokens,self.bid_rows,self.market_end=conditions,tokens,bid_rows,market_end
        self.codec,self.post_intents,self.order_sequences=codec,post_intents,order_sequences
        self.prepared=None;self.submitted_legs=None;self.cancellation=None

    def _prepare(self,requests,action):
        from pending_post_intents import PendingPostIntents
        from cancel_submission import cancellation_catalog
        roots=action.family.numel();device=action.family.device
        selected,original=requests
        directory={root:request for root,request in zip(selected,original,strict=True)}
        families=action.family.cpu().tolist()
        posts=[[] for _ in range(roots)];refs=[[] for _ in range(roots)];ordinals=[[] for _ in range(roots)]
        methods=[0]*roots;delayed=[[] for _ in range(roots)]
        population=[];assets=[];conditions=[];bids=[];ends=[]
        for root,family in enumerate(families):
            previous=self.post_intents
            population.append(list(previous.items[root]) if previous is not None else [])
            assets.append(previous.assets[root].cpu().tolist() if previous is not None else [])
            conditions.append(previous.conditions[root].cpu().tolist() if previous is not None else [])
            bids.append(previous.bid_rows[root].cpu().tolist() if previous is not None else [])
            ends.append(previous.market_end[root].cpu().tolist() if previous is not None else [])
            if family not in (1,2):continue
            request=directory[root];body=request['request']
            methods[root]=0 if request['method']=='POST_ORDER' else 1
            posts[root]=[{'batch_item_ordinal':0,'item':body['item']}] if methods[root]==0 else body['items']
            for post in posts[root]:
                item=post['item'];order=item['order'];kind,terms=next(iter(order['kind'].items()))
                asset=self.tokens[root].index(order['token_id']);condition=self.conditions[root].index(order['condition_id'])
                price=terms.get('limit_price_pusd_per_share')
                bid=self.bid_rows[root][(asset,Fraction(price))] if price is not None else self.bid_rows[root][(asset,None)]
                refs[root].append(len(population[root]));ordinals[root].append(post['batch_item_ordinal'])
                delayed[root].append(kind=='FokMarket' and self.rule.context['taker_delay_enabled'])
                population[root].append(json.dumps(item));assets[root].append(asset);conditions[root].append(condition)
                bids[root].append(bid);ends[root].append(self.market_end[root][condition])
        width=max(1,max(map(len,population)));actions=action.legs.present.size(1)
        t=lambda values,dtype=torch.int64:torch.tensor(values,dtype=dtype,device=device)
        pad=lambda rows,n,fill:[row+[fill]*(n-len(row)) for row in rows]
        self.post_intents=PendingPostIntents(tuple(tuple(row) for row in pad(population,width,'null')),
            t(pad(assets,width,-1)),t(pad(conditions,width,-1)),t(pad(bids,width,-1)),t(pad(ends,width,0)),
            torch.zeros(roots,dtype=torch.int32,device=device))
        self.prepared=(t(methods),t(pad(refs,actions,-1)),t(pad(ordinals,actions,0)),
            t(pad([[True]*len(row) for row in refs],actions,False),torch.bool),
            t(pad(delayed,actions,False),torch.bool))
        # 取消正文由原Rust去重/目标构造入口解析；市场请求不读取订单人口。
        if any(family>=3 for family in families):
            cancel_requests=tuple((directory[root],) if family>=3 else () for root,family in enumerate(families))
            items=[]
            for row in cancel_requests:
                mapping={}
                for request in row:
                    body=request['request']
                    for item in body.get('items',[{'item':body['item']}] if 'item' in body else []):
                        mapping.setdefault(item['item']['item_id'],len(mapping))
                items.append(mapping)
            self.cancellation=cancellation_catalog(cancel_requests,
                tuple({value:i for i,value in enumerate(row)} for row in self.conditions),tuple(items),
                self.order_sequences,self.codec.cancel_targets_batch_v2,device)

    def __call__(self,runtime,public,requests,action,enabled,*,cutoff):
        from commitments import bind_submission_receipt
        from post_submission import submit_post_orders
        from cancel_submission import submit_cancellation
        from queues import Numbers
        if self.prepared is None:
            self.cutoff=cutoff
            self._prepare(requests,action)
        methods,refs,ordinals,present,delayed=self.prepared
        # 冷装完整原正文轴；所有方法共享同一请求目录及完整物理动作轴。
        cancel_columns=None if self.cancellation is None else self.cancellation.columns(torch.zeros_like(self.control.global_requests))
        width=max(refs.size(1),0 if cancel_columns is None else cancel_columns[1].size(1),runtime.requests.action_capacity)
        if runtime.requests.action_capacity<width:
            runtime=replace(runtime,requests=runtime.requests.resized(runtime.requests.capacity,width))
        def pad(value,fill):
            return torch.nn.functional.pad(value,(0,width-value.size(1)),value=fill)
        refs,ordinals,present,delayed=pad(refs,-1),pad(ordinals,0),pad(present,False),pad(delayed,False)
        if runtime.input_frontier is None:raise ValueError("Actor发送须绑定SIM整根实际输入提交前沿")
        input_sequences=runtime.input_frontier.next_sequence
        _,input_fault=Numbers.run_exact_batch(lambda:runtime.input_frontier.commit(enabled))
        buy=enabled & ((action.family==1)|(action.family==2))
        cancel=enabled & (action.family>=3)
        receipt=None;enough=torch.ones_like(enabled)
        if bool(buy.any().cpu()):
            runtime,ledger,receipt,buy_enough=Numbers.run_exact_batch(lambda:submit_post_orders(runtime,public.ledger,
                self.sampler,self.identity_bytes,self.identity_offsets,self.control.global_requests,methods,
                input_sequences,self.control.execute_at,refs,ordinals,present,delayed,
                torch.full_like(self.control.execute_at,self.rule.context['taker_delay_ms']),
                self.control.leg_requests,self.send_events,buy,action.fault|input_fault))
            frontier,_=Numbers.run_exact_batch(lambda:runtime.input_frontier.commit(buy&receipt.registered&buy_enough&(receipt.fault==0)))
            runtime=replace(runtime,input_frontier=frontier)
            public=replace(public,ledger=ledger)
            self.submitted_legs=Numbers.run_exact_batch(lambda:bind_submission_receipt(
                action.legs if self.submitted_legs is None else self.submitted_legs,
                self.control.global_requests,receipt))
            enough=torch.where(buy,buy_enough,enough)
        if bool(cancel.any().cpu()):
            from admission import coordinate
            methods,refs,ordinals,present=cancel_columns
            refs,ordinals,present=pad(refs,-1),pad(ordinals,0),pad(present,False)
            runtime,public,cancel_receipt,cancel_enough=Numbers.run_exact_batch(lambda:submit_cancellation(runtime,public,self.sampler,
                self.identity_bytes,self.identity_offsets,self.control.global_requests,methods,input_sequences,
                self.control.execute_at,refs,ordinals,present,cancel,action.fault|input_fault))
            frontier,_=Numbers.run_exact_batch(lambda:runtime.input_frontier.commit(cancel&cancel_receipt.registered&cancel_enough&(cancel_receipt.fault==0)))
            runtime=replace(runtime,input_frontier=frontier)
            receipt=cancel_receipt if receipt is None else Numbers.run_exact_batch(lambda:coordinate(cancel_receipt,receipt,cancel))
            enough=torch.where(cancel,cancel_enough,enough)
        return runtime,public,receipt,enough
