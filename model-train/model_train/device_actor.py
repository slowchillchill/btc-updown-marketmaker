"""原精确Actor上的设备完整路径；模型只评分，候选不发送或预留。"""
from dataclasses import dataclass,fields,is_dataclass
import torch
from .action_random import ActionRandom,draw_condition
from .gpu_policy import score_condition


def copy_values(target,value):
    if isinstance(target,torch.Tensor):target.copy_(value)
    elif is_dataclass(target):
        for field in fields(target):copy_values(getattr(target,field.name),getattr(value,field.name))
    else:raise TypeError('捕获暂存只接张量和精确状态')


def reduce_cancel_objects(values,mask):
    """原完整全局人口两遍归约，double顺序和/均值，max保首项梯度。"""
    values=torch.where(mask[:,:,None],values,0.)
    total=values.new_zeros(values.shape[0],values.shape[-1],dtype=torch.float64)
    for index in range(values.shape[1]):total=total+values[:,index].double()
    count=mask.sum(1);exists=count>0
    maximum=values.masked_fill(~mask[:,:,None],-torch.inf).max(1).values
    maximum=torch.where(exists[:,None],maximum,0.)
    return torch.cat((total,total/count.clamp_min(1)[:,None],maximum.double()),1).float(),exists


@dataclass(frozen=True)
class ActorPathCandidate:
    prefix: object
    stages: torch.Tensor
    choices: torch.Tensor
    scored: torch.Tensor
    length: torch.Tensor
    log_prob: torch.Tensor
    random: ActionRandom
    identities: torch.Tensor
    cutoff: torch.Tensor
    policy_version: torch.Tensor
    required_words: torch.Tensor
    required_steps: torch.Tensor
    fits: torch.Tensor
    complete: torch.Tensor
    fault: torch.Tensor

    def snapshot(self):
        from actor_context import _owned
        return type(self)(**{name:_owned(value) for name,value in vars(self).items()})


class CapturedDeviceActor:
    """设备WHILE及参数头IF；NoAct/singleton不评分，不推进随机流。

    path_capacity/word_capacity是调用者物理配置，不是合法动作长度或金额上限。
    不足返回需求；外层保留原冻结/随机游标，增长后同输入重算整个候选。
    """
    def __init__(self,policy,inputs,random,*,path_capacity,word_capacity,deterministic=False):
        from actor_support import DeviceNativeActor
        from actor_context import _owned
        from prepare import _numbers
        self.numbers=_numbers();self.actor=DeviceNativeActor(inputs.actor)
        self.policy=policy;self.inputs=inputs;self.random=random;self.deterministic=deterministic
        self.capacity=path_capacity;self.word_capacity=word_capacity
        rows=self.actor.rows;device=self.actor.device
        if rows<1 or path_capacity<1 or word_capacity<4:raise ValueError('物理根/路径须为正且至少4字')
        if (random.seed.shape!=(rows,) or random.counter.shape!=(rows,4)
                or random.seed.dtype!=torch.int64 or random.counter.dtype!=torch.int64
                or random.seed.device!=device or random.counter.device!=device):
            raise TypeError('同根显式随机游标必须为CUDA int64')
        if inputs.state.shape!=(rows,policy.shape.hidden):raise ValueError('模型共享状态宽度不匹配')
        self.heads=policy.stages();self.width=max(999,inputs.cancels.present.shape[1])
        t=lambda shape,dtype:torch.zeros(shape,dtype=dtype,device=device)
        # initial中的零值可共享；捕获后原位安装的各前缀字段须有独立存储。
        self.prefix=_owned(self.numbers.device_exact_batch(
            self.actor.initial,word_capacity=word_capacity,device=device).value)
        self.stages=torch.full((rows,path_capacity),-1,dtype=torch.int64,device=device)
        self.choices=self.stages.clone();self.scored=t((rows,path_capacity),torch.bool)
        self.length=t((rows,),torch.int64);self.ell=t((rows,),torch.float32)
        self.counter=random.counter.clone();self.required_words=t((),torch.int64);self.required_steps=t((),torch.int64)
        self.fits=torch.ones((),dtype=torch.bool,device=device);self.ready=t((),torch.bool)
        self.always=torch.ones((),dtype=torch.bool,device=device);self.flags=[t((),torch.bool) for _ in range(5)]
        self.active=t((rows,),torch.bool);self.multiple=t((rows,),torch.bool)
        self.mask=t((rows,self.width),torch.bool);self.logits=t((rows,self.width),torch.float32)
        self.features=t((rows,36),torch.float32);self.rule=t((rows,policy.shape.hidden//2),torch.float32)
        count=inputs.cancels.values.shape[1]
        self.cancel_values=inputs.cancels.values if count else inputs.cancels.values.new_zeros((rows,1,184))
        self.cancel_mask=inputs.cancels.present if count else t((rows,1),torch.bool)
        self.objects=t((rows,max(1,count),policy.shape.hidden//2),torch.float32)
        self.reduced=t((rows,3*policy.shape.hidden//2),torch.float32);self.exists=t((rows,),torch.bool)
        self.price_indices=torch.arange(999,device=device)
        operations=[self.prepare,*[lambda index=i:self.score(index) for i in range(5)],self.advance]
        with torch.no_grad():
            # 冷图构建只预热既定物理形状，不提交动作/活动随机数。
            self.reset()
            for operation in operations:operation()
            torch.cuda.synchronize()
            self.graphs=[]
            for operation in [self.reset,*operations]:
                graph=torch.cuda.CUDAGraph(keep_graph=True)
                with torch.cuda.graph(graph):operation()
                self.graphs.append(graph)
            conditions=[self.always,*self.flags,self.always]
            self.loop=self.numbers.public_extension().DeviceConditionalSequenceLoop(
                self.graphs[0].raw_cuda_graph(),[g.raw_cuda_graph() for g in self.graphs[1:]],conditions,self.ready)

    def reset(self):
        initial=self.numbers.device_exact_batch(self.actor.initial,
            word_capacity=self.word_capacity,device=self.actor.device)
        copy_values(self.prefix,initial.value)
        self.prefix.fault.copy_(self.prefix.fault|self.inputs.fault
            |(~self.inputs.ready&self.inputs.actor.decision_required).to(torch.int32))
        self.stages.fill_(-1);self.choices.fill_(-1);self.scored.zero_();self.length.zero_();self.ell.zero_()
        self.counter.copy_(self.random.counter);self.required_words.copy_(initial.required_words)
        self.required_steps.zero_();self.fits.copy_(initial.fits)
        self.objects.copy_(self.heads['action_objects'](self.cancel_values,self.cancel_mask))
        reduced,exists=reduce_cancel_objects(self.objects,self.cancel_mask)
        self.reduced.copy_(reduced);self.exists.copy_(exists)
        self.rule.copy_(self.policy.action.rule(self.inputs.rules))
        self.ready.copy_(((self.prefix.stage!=7)&(self.prefix.fault==0)).any()&self.fits)

    def resources(self,candidate):
        self.required_words.copy_(torch.maximum(self.required_words,candidate.required_words))
        self.fits.logical_and_(candidate.fits)
        return candidate.value

    def prepare(self):
        def compute():return self.actor.support(self.prefix),self.actor.prefix_features(self.prefix)
        support,(features,fault)=self.resources(self.numbers.device_exact_batch(compute,
            word_capacity=self.word_capacity,device=self.actor.device))
        self.support=support
        self.features.copy_(features)
        applicable=self.prefix.stage!=7
        self.prefix.fault.copy_(self.prefix.fault|torch.where(applicable&self.fits,support.fault|fault,0))
        self.active.copy_((self.prefix.stage!=7)&(self.prefix.fault==0))
        self.required_steps.copy_(torch.maximum(self.required_steps,(self.length+self.active.long()).max()))
        self.fits.logical_and_(self.required_steps<=self.capacity)
        self.mask.zero_();self.logits.zero_()
        for stages,mask in (((0,),support.family),((1,),support.quantity),((2,3),support.variant),
                ((4,5),support.price),((6,),support.cancel)):
            selected=torch.zeros_like(self.active)
            for stage in stages:selected|=self.prefix.stage==stage
            width=mask.shape[1]
            self.mask[:,:width].logical_or_(mask&selected[:,None])
        self.multiple.copy_(self.active&(self.mask.sum(1)>1)&self.fits)
        if not self.deterministic:
            self.prefix.fault.copy_(self.prefix.fault|torch.where(self.multiple,self.random.fault(),0))
            self.active.logical_and_(self.prefix.fault==0)
            self.multiple.logical_and_(self.prefix.fault==0)
        for index,stages in enumerate(((0,),(1,),(2,3),(4,5),(6,))):
            selected=torch.zeros_like(self.active)
            for stage in stages:selected|=self.prefix.stage==stage
            self.flags[index].copy_((self.multiple&selected).any())

    def score(self,index):
        z=self.inputs.state;prefix=self.features
        if index==0:score,_=self.heads['action_family'](z,self.inputs.rules,self.reduced,self.exists)
        elif index in (1,2):score=self.heads['action_quantity' if index==1 else 'action_variant'](z,self.rule,prefix)
        elif index==3:
            fees,fault=self.resources(self.numbers.device_exact_batch(
                lambda:self.actor.price_fee_features(self.prefix,self.support),word_capacity=self.word_capacity,device=self.actor.device))
            selected=((self.prefix.stage==4)|(self.prefix.stage==5))&self.multiple
            self.prefix.fault.copy_(self.prefix.fault|torch.where(self.fits&selected,fault,0))
            score=self.heads['action_price'](z,self.rule,prefix,self.price_indices,fees)
        else:
            context=self.heads['action_cancel_context'](z,self.rule,prefix)
            score=self.heads['action_cancel'](context,self.objects,self.cancel_mask)[:,:self.inputs.cancels.present.shape[1]]
        if score.shape[1]:
            selected=((self.prefix.stage==0) if index==0 else (self.prefix.stage==1) if index==1 else
                ((self.prefix.stage==2)|(self.prefix.stage==3)) if index==2 else
                ((self.prefix.stage==4)|(self.prefix.stage==5)) if index==3 else (self.prefix.stage==6))
            width=score.shape[1]
            self.logits[:,:width].copy_(torch.where(selected[:,None]&self.multiple[:,None],score,self.logits[:,:width]))

    def advance(self):
        active=self.active&self.fits&(self.prefix.fault==0)
        if self.deterministic:choice=self.logits.masked_fill(~self.mask,-torch.inf).argmax(1)
        else:choice=draw_condition(self.logits,self.mask,ActionRandom(self.random.seed,self.counter).uniform(active&self.multiple))
        scored=score_condition(self.logits,self.mask,choice,active,self.prefix.fault)
        candidate=self.resources(self.numbers.device_exact_batch(lambda:self.actor.choose(self.prefix,choice),
            word_capacity=self.word_capacity,device=self.actor.device))
        commit=active&self.fits&(scored.fault==0)&(candidate.fault==0)
        position=self.length.clamp_max(self.capacity-1)[:,None]
        for target,value in ((self.stages,self.prefix.stage),(self.choices,choice),(self.scored,self.multiple)):
            old=target.gather(1,position).squeeze(1)
            target.scatter_(1,position,torch.where(commit,value,old)[:,None])
        self.length.add_(commit.long());self.ell.add_(torch.where(commit,scored.log_prob,0.))
        used=torch.zeros_like(commit) if self.deterministic else commit&self.multiple
        next_random=ActionRandom(self.random.seed,self.counter).advanced(used)
        self.counter.copy_(next_random.counter)
        def install(target,value):
            if isinstance(target,torch.Tensor):
                mask=commit.reshape((commit.shape[0],)+(1,)*(target.dim()-1))
                target.copy_(torch.where(mask,value,target))
            else:
                for field in fields(target):install(getattr(target,field.name),getattr(value,field.name))
        install(self.prefix,candidate)
        self.prefix.fault.copy_(self.prefix.fault|torch.where(self.fits,scored.fault,0)
            |torch.where(active&self.fits,candidate.fault,0))
        self.ready.copy_((((self.prefix.stage!=7)&(self.prefix.fault==0)).any())&self.fits)

    def replay(self):
        """图外启动整动作；返回借用候选，成功发布必须snapshot并共同提交随机游标。"""
        self.loop.replay()
        return ActorPathCandidate(self.prefix,self.stages,self.choices,self.scored,self.length,self.ell,
            ActionRandom(self.random.seed,self.counter),self.inputs.actor.identities,self.inputs.actor.cutoff,
            self.inputs.policy_version,self.required_words,self.required_steps,self.fits,
            self.prefix.stage==7,self.prefix.fault)
