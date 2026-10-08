"""原取消请求的冷列和真实发送登记；处理仍由同一MutationRequests执行。

原Rust入口保留Single/Pair去重及input item引用。市场请求只登记condition；
目标人口不在发送时读取。身份字典和实际输入序/发送时刻来自原控制面。
"""
from dataclasses import dataclass,replace
import json
import torch
from queues import ExactInt
from fields import RecordList
from pending_cancel_intents import PendingCancelIntents


@dataclass(frozen=True)
class CancellationRequestCatalog:
    requests: tuple  # 原BusinessRequestV2正文；未知订单字符串也保留原值。
    intents: object
    methods: torch.Tensor
    item_refs: torch.Tensor
    ordinals: torch.Tensor
    present: torch.Tensor

    def columns(self,request_indices):
        roots=torch.arange(self.methods.size(0),device=self.methods.device)
        return (self.methods[roots,request_indices],self.item_refs[roots,request_indices],
            self.ordinals[roots,request_indices],self.present[roots,request_indices])

    def append(self,incoming):
        """只追加已按本次冻结身份解码的新正文，保历史目标及稳定引用。"""
        roots=self.methods.size(0);device=self.methods.device
        left,right=self.intents,incoming.intents
        old_width=left.market.size(1)
        def integer(a,b):
            capacity=max(a.words.size(1),b.words.size(1));a,b=a.with_capacity(capacity),b.with_capacity(capacity)
            return ExactInt(torch.cat((a.words.reshape(roots,-1,capacity),b.words.reshape(roots,-1,capacity)),1).flatten(0,1),
                torch.cat((a.sign.reshape(roots,-1),b.sign.reshape(roots,-1)),1).flatten(),
                torch.cat((a.fault.reshape(roots,-1),b.fault.reshape(roots,-1)),1).flatten())
        refs=max(left.input_item_refs.present.size(1),right.input_item_refs.present.size(1))
        def reference(value):
            width=value.market.size(1);original=value.input_item_refs;pad=refs-original.present.size(1)
            return tuple(torch.nn.functional.pad(field.reshape(roots,width,-1),(0,pad)) for field in original.fields),\
                torch.nn.functional.pad(original.present.reshape(roots,width,-1),(0,pad))
        lf,lp=reference(left);rf,rp=reference(right)
        inputs=RecordList(tuple(torch.cat((a,b),1).flatten() for a,b in zip(lf,rf,strict=True)),
            torch.cat((lp,rp),1).reshape(-1,refs))
        intents=PendingCancelIntents(torch.cat((left.market,right.market),1),torch.cat((left.conditions,right.conditions),1),
            integer(left.order_sequences,right.order_sequences),integer(left.target_ordinals,right.target_ordinals),inputs,left.fault|right.fault)
        requests=tuple(a+b for a,b in zip(self.requests,incoming.requests,strict=True))
        count=max(map(len,requests));actions=max(self.item_refs.size(2),incoming.item_refs.size(2))
        methods=torch.zeros((roots,count),dtype=torch.int64,device=device)
        refs_out=torch.full((roots,count,actions),-1,dtype=torch.int64,device=device)
        ordinals=torch.zeros_like(refs_out);present=torch.zeros_like(refs_out,dtype=torch.bool)
        for root in range(roots):
            start=0
            for catalog,offset in ((self,0),(incoming,old_width)):
                width=len(catalog.requests[root]);end=start+width;n=catalog.item_refs.size(2)
                methods[root,start:end]=catalog.methods[root,:width]
                on=catalog.present[root,:width]
                refs_out[root,start:end,:n]=torch.where(on,catalog.item_refs[root,:width]+offset,-1)
                ordinals[root,start:end,:n]=catalog.ordinals[root,:width]
                present[root,start:end,:n]=on;start=end
        return type(self)(requests,intents,methods,refs_out,ordinals,present)


def cancellation_catalog(requests,condition_slots,item_slots,order_sequences,codec,device):
    """原正文整批冷解码；三个字典沿已绑定原身份，不分配新业务身份。

    order_sequences由原SIM身份解码建立，保完整u64；原请求字符串保在requests。
    目标是否仍存在由处理时BaseOrders判断，不从冷身份目录猜测可撤。
    """
    requests=tuple(tuple(row) for row in requests);roots=len(requests)
    bodies=[value for row in requests for value in row]
    targets=json.loads(codec(json.dumps(bodies)))
    populations=[];registrations=[];cursor=0
    methods={'CANCEL_ORDER':2,'CANCEL_ORDERS':3,'CANCEL_MARKET_ORDERS':4}
    for root,row in enumerate(requests):
        population=[];registered=[]
        for value in row:
            body=value['request'];method=value['method'];original=targets[cursor];cursor+=1
            refs=[];ordinals=[]
            if method=='CANCEL_MARKET_ORDERS':
                refs.append(len(population));ordinals.append(0)
                population.append((True,condition_slots[root].get(body['condition_id'],-1),-1,0,[]))
            else:
                for target in original:
                    refs.append(len(population));ordinals.append(target['action_ordinal'])
                    references=[(item_slots[root][ref['item_id']],ref['batch_item_ordinal']) for ref in target['input_item_refs']]
                    population.append((False,-1,order_sequences[root][target['platform_order_id']],
                        target['target_ordinal'],references))
            registered.append((methods[method],refs,ordinals))
        populations.append(population);registrations.append(registered)
    width=max(map(len,populations));count=max(map(len,requests));actions=max(len(value[1]) for row in registrations for value in row)
    references=max(1,max(len(value[4]) for row in populations for value in row))
    market=torch.zeros((roots,width),dtype=torch.bool,device=device)
    conditions=torch.full((roots,width),-1,dtype=torch.int64,device=device)
    orders=[-1]*(roots*width);targets=[0]*(roots*width)
    item_ids=torch.zeros((roots,width,references),dtype=torch.int64,device=device);item_ord=torch.zeros_like(item_ids)
    reference_present=torch.zeros_like(item_ids,dtype=torch.bool)
    method=torch.zeros((roots,count),dtype=torch.int64,device=device)
    refs=torch.full((roots,count,actions),-1,dtype=torch.int64,device=device);ord=torch.zeros_like(refs);present=torch.zeros_like(refs,dtype=torch.bool)
    for root,row in enumerate(populations):
        for slot,(is_market,condition,sequence,target,inputs) in enumerate(row):
            market[root,slot]=is_market;conditions[root,slot]=condition;orders[root*width+slot]=sequence;targets[root*width+slot]=target
            for i,(item,ordinal) in enumerate(inputs):item_ids[root,slot,i]=item;item_ord[root,slot,i]=ordinal;reference_present[root,slot,i]=True
        for i,(kind,item_refs,ordinals) in enumerate(registrations[root]):
            method[root,i]=kind
            refs[root,i,:len(item_refs)]=torch.tensor(item_refs,device=device)
            ord[root,i,:len(ordinals)]=torch.tensor(ordinals,device=device);present[root,i,:len(item_refs)]=True
    intents=PendingCancelIntents(market,conditions,ExactInt.from_values(orders),
        ExactInt.from_values(targets),RecordList((item_ids.flatten(),item_ord.flatten()),
            reference_present.reshape(roots*width,references)),torch.zeros(roots,dtype=torch.int32,device=device))
    return CancellationRequestCatalog(requests,intents,method,refs,ord,present)


def submit_cancellation(runtime,public,sampler,identity_bytes,identity_offsets,request_keys,
        methods,input_sequences,sent_at,item_refs,ordinals,present,enabled,source_fault):
    """真实发送登记与公开receipt共用候选，不预读/修改订单或余额。"""
    candidate,receipt=runtime.requests.register(sampler,identity_bytes,identity_offsets,request_keys,methods,
        input_sequences,sent_at,item_refs,ordinals,present,torch.zeros_like(present),torch.zeros_like(sent_at),enabled,source_fault)
    enough=receipt.required_requests<=runtime.requests.capacity
    committed=enough & (receipt.fault==0)
    candidate=candidate.coordinate_result(runtime.requests,committed,receipt.fault)
    receipt=replace(receipt,registered=receipt.registered & committed,duplicate=receipt.duplicate & committed)
    from registry import identity_matches
    roots,width=public.cancellations.registered.shape
    equal,error=identity_matches(ExactInt.from_i64_tensor(request_keys),public.cancellations.requests,roots,1,width)
    received=equal[:,0,:] & (receipt.registered|receipt.duplicate)[:,None]
    cancellation=public.cancellations.register(received)
    return replace(runtime,requests=candidate),replace(public,cancellations=cancellation),receipt,enough
