"""完整更新边界一次装批的可微动作重算；金额/支持和概率继续在设备。"""
from dataclasses import dataclass
import torch
from .gpu_policy import score_condition
from .device_actor import reduce_cancel_objects


@dataclass(frozen=True)
class ReplaySchedule:
    rows_by_stage: tuple
    lengths: tuple
    path: object  # 装批来自这个owned不可变路径，不借另一个同形状路径的支路表。

    @classmethod
    def for_update(cls,path):
        """完整更新协调者调用一次；拒绝未完成候选，不逐条件读回。

        元数据只含已拥有路径的长度/阶段/评分位和完成门，用于现有autograd的
        真正参数调用装批；否则mask后zero梯度会改未用参数AdamW状态。
        同更新所有真实调用的共享参数自然形成并集，minibatch不重建这个决定。
        """
        header=torch.stack((path.length,path.complete.long(),path.fault.long(),
            path.fits.expand_as(path.length).long()),1)
        metadata=torch.cat((header,path.stages,path.scored.long()),1).cpu().tolist()
        width=path.stages.shape[1];groups=[[] for _ in range(width)];lengths=[]
        for row,values in enumerate(metadata):
            length,complete,fault,fits=values[:4]
            if not complete or fault or not fits:raise ValueError('完整学习更新不能接纳未完成动作候选')
            if not 0<=length<=width:raise ValueError('真实路径长度超出持久人口')
            lengths.append(length)
            for position in range(width):
                stage=values[4+position];scored=values[4+width+position]
                if position<length:
                    if stage not in range(7):raise ValueError('持久路径阶段不合法')
                    if scored:groups[position].append((stage,row))
                elif stage!=-1 or scored:raise ValueError('尾补齐不能冒充实际条件')
        return cls(tuple(tuple(group) for group in groups),tuple(lengths),path)


@dataclass(frozen=True)
class ReplayedAction:
    log_prob: torch.Tensor
    fault: torch.Tensor
    fits: torch.Tensor
    required_words: torch.Tensor


def replay_action(policy,inputs,path,schedule,*,word_capacity):
    """同冻结合法路径、当前参数联合ell，不采样，也不改业务/熵随机游标。

    schedule须来自整次更新的owned path。静态Python路径轴只描述持久人口；
    Actor金额/状态/支持/概率在GPU重算，每个条件不回CPU调度。
    """
    from actor_support import DeviceNativeActor
    from prepare import _numbers
    numbers=_numbers();actor=DeviceNativeActor(inputs.actor);rows=actor.rows;device=actor.device
    if (schedule.path is not path or path.stages.shape!=path.choices.shape or path.scored.shape!=path.stages.shape
            or len(schedule.lengths)!=rows or len(schedule.rows_by_stage)!=path.stages.shape[1]):
        raise ValueError('保存路径与更新装批人口不一致')
    fault=inputs.fault|path.fault
    fault|=(inputs.actor.identities!=path.identities).any(1).to(torch.int32)
    fault|=(inputs.actor.cutoff!=path.cutoff).to(torch.int32)
    heads=policy.stages()
    used_rows=sorted({row for group in schedule.rows_by_stage for _,row in group})
    used=torch.zeros(rows,dtype=torch.bool,device=device)
    if used_rows:used[torch.tensor(used_rows,dtype=torch.int64,device=device)]=True
    ell=torch.where(used[:,None],inputs.state,0.).sum(1)*0.
    mask=inputs.cancels.present;features=inputs.cancels.values
    if not features.shape[1]:
        features=features.new_zeros((rows,1,184));mask=mask.new_zeros((rows,1))
    # 整更新装批只编码真正调用的行。仅在编码后mask未调用行，会让0*NaN
    # 经LayerNorm等反传污染已用共享参数；也不能给完全未用参数造zero梯度。
    object_rows=sorted({row for group in schedule.rows_by_stage for stage,row in group if stage in (0,6)})
    rule_rows=sorted({row for group in schedule.rows_by_stage for stage,row in group if stage in range(1,7)})
    width=policy.shape.hidden//2
    objects=inputs.state.new_zeros((rows,features.shape[1],width))
    reduced=inputs.state.new_zeros((rows,3*width));exists=mask.new_zeros((rows,))
    if object_rows:
        batch=torch.tensor(object_rows,dtype=torch.int64,device=device)
        selected_mask=mask.index_select(0,batch)
        selected_features=torch.where(selected_mask[:,:,None],features.index_select(0,batch),0.)
        selected_objects=heads['action_objects'](selected_features,selected_mask)
        selected_reduced,selected_exists=reduce_cancel_objects(selected_objects,selected_mask)
        objects=objects.index_copy(0,batch,selected_objects)
        reduced=reduced.index_copy(0,batch,selected_reduced);exists=exists.index_copy(0,batch,selected_exists)
    rule=inputs.state.new_zeros((rows,width))
    if rule_rows:
        batch=torch.tensor(rule_rows,dtype=torch.int64,device=device)
        rule=rule.index_copy(0,batch,policy.action.rule(inputs.rules.index_select(0,batch)))
    required=torch.zeros((),dtype=torch.int64,device=device);fits=path.fits.clone()
    def compute(computation):
        nonlocal required,fits
        candidate=numbers.device_exact_batch(computation,word_capacity=word_capacity,device=device)
        required=torch.maximum(required,candidate.required_words);fits=fits&candidate.fits
        return candidate.value
    state=compute(actor.initial)
    indices=torch.arange(999,device=device)
    for position,group in enumerate(schedule.rows_by_stage):
        active=position<path.length
        fault=fault|((state.stage!=path.stages[:,position])&active).to(torch.int32)
        support,prefix=compute(lambda:(actor.support(state),actor.prefix_features(state)))
        encoded,error=prefix;fault=fault|torch.where(active,error|support.fault,0)
        counts=torch.zeros(rows,dtype=torch.int64,device=device)
        for stages,valid in (((0,),support.family),((1,),support.quantity),((2,3),support.variant),
                ((4,5),support.price),((6,),support.cancel)):
            selected=torch.zeros_like(active)
            for stage in stages:selected|=state.stage==stage
            counts=torch.where(selected,valid.sum(1),counts)
        fault|=((path.scored[:,position]!=(active&(counts>1)))&active).to(torch.int32)
        # 只调用原参考确实调用的参数支路；共享参数使用取整更新实际调用的并集。
        for category,stages in enumerate(((0,),(1,),(2,3),(4,5),(6,))):
            selected_rows=[row for stage,row in group if stage in stages]
            if not selected_rows:continue
            batch=torch.tensor(selected_rows,dtype=torch.int64,device=device)
            z=inputs.state.index_select(0,batch);r=rule.index_select(0,batch);f=encoded.index_select(0,batch)
            if category==0:
                logits,_=heads['action_family'](z,inputs.rules.index_select(0,batch),
                    reduced.index_select(0,batch),exists.index_select(0,batch));legal=support.family
            elif category in (1,2):
                logits=heads['action_quantity' if category==1 else 'action_variant'](z,r,f)
                legal=support.quantity if category==1 else support.variant
            elif category==3:
                fees,error=compute(lambda:actor.price_fee_features(state,support))
                fault|=torch.where(active&((state.stage==4)|(state.stage==5)),error,0)
                logits=heads['action_price'](z,r,f,indices,fees.index_select(0,batch));legal=support.price
            else:
                context=heads['action_cancel_context'](z,r,f)
                logits=heads['action_cancel'](context,objects.index_select(0,batch),mask.index_select(0,batch))
                logits=logits[:,:support.cancel.shape[1]];legal=support.cancel
            score=score_condition(logits,legal.index_select(0,batch),path.choices[:,position].index_select(0,batch),
                active.index_select(0,batch),fault.index_select(0,batch))
            ell=ell.index_add(0,batch,score.log_prob)
            fault=fault.index_copy(0,batch,score.fault)
        state=compute(lambda:actor.choose(state,path.choices[:,position]))
        fault|=torch.where(active,state.fault,0)
    fault|=(state.stage!=7).to(torch.int32)
    return ReplayedAction(ell,fault,fits,required)


def entropy_sample_objective(replayed,*,target_scale):
    """一条独立完整路径的联合熵报告/最小化surrogate，目标比例显式传入。

    调用者须用独立熵随机流采样当前评分策略，再由同引擎重算；此函数不拿行为
    路径替代独立样本，也不把条件熵相加冒充联合熵。
    """
    ell=replayed.log_prob
    return -target_scale*ell.detach(),target_scale*(ell.detach()+1.)*ell
