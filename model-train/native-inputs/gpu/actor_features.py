"""同一次精确Actor前缀的设备36列投影；不修改支持集或选择状态。"""
import torch
from prepare import _numbers


def prefix_features(actor, state):
    """仅待选择前缀进入模型；完成/ContextOnly行保持中性输出。

    精确root、区间和数量先求值，再沿公共Numbers做末端投影。
    调用者将返回fault与原支持/路径/资源门共同接纳，不按float反推动作。
    """
    numbers=_numbers();rows=actor.rows;device=actor.device
    result=torch.zeros((rows,36),dtype=torch.float32,device=device)
    active=(state.stage>=0)&(state.stage<7)
    buy=active&((state.family==1)|(state.family==2))
    # GPU内部Variant/Price各两腿，模型仍使用原五种语义stage。
    for column, stages in ((7,(0,)),(8,(1,)),(9,(2,3)),(10,(4,5)),(11,(6,))):
        selected=torch.zeros_like(active)
        for stage in stages: selected|=state.stage==stage
        result[:,column]=(active&selected).float()
    chosen=active&(state.family>=0)&(state.family<6)
    for family in range(6): result[:,family]=(chosen&(state.family==family)).float()
    result[:,6]=chosen.float()
    result[:,20]=buy.float()
    for side in (0,1):
        result[:,12+side]=(buy&((state.stage==2+side)|(state.stage==4+side))).float()
    result[:,14]=(buy&(state.stage>=2)&(state.stage<=5)).float()

    quantity_stage=buy&(state.stage==1)
    root=state.root_upper.select(quantity_stage,state.root_upper.constant(1))
    lo=state.lo.select(quantity_stage,state.lo.constant(0))
    hi=state.hi.select(quantity_stage,state.hi.constant(0))
    root_value,root_fault=root.project(root.constant(1),operation=1)
    low_value,low_fault=lo.project(root)
    high_value,high_fault=hi.project(root)
    result[:,15]=torch.where(quantity_stage,root_value,0.)
    result[:,16]=torch.where(quantity_stage,low_value,0.)
    result[:,17]=torch.where(quantity_stage,high_value,0.)
    result[:,18]=quantity_stage.float()
    selected_quantity=buy&(state.stage>=2)&(state.stage<=5)
    quantity=state.quantity.select(selected_quantity,state.quantity.constant(0))
    quantity_value,quantity_fault=quantity.project(quantity.constant(100),operation=1)
    result[:,19]=torch.where(selected_quantity,quantity_value,0.)
    result[:,21]=selected_quantity.float()
    fault=state.fault|torch.where(quantity_stage,root_fault|low_fault|high_fault,0)
    fault|=torch.where(selected_quantity,quantity_fault,0)

    for side in (0,1):
        base=22+side*7
        enabled=buy&((state.family==2)|(state.single_side==side))
        variant_known=enabled&(state.variants[:,side]>=0)
        result[:,base]=(variant_known&(state.variants[:,side]==0)).float()
        result[:,base+1]=(variant_known&(state.variants[:,side]==1)).float()
        result[:,base+2]=enabled.float()
        result[:,base+3]=variant_known.float()
        result[:,base+5]=enabled.float()
        price_known=enabled&(state.prices[:,side]>=0)
        units=actor._unit(torch.full_like(state.stage,side))
        index=numbers.ExactInt.from_i64_tensor(torch.where(price_known,state.prices[:,side],0))
        phi=units*index*units.constant(2)-units.constant(10000)
        price,price_fault=phi.project(phi.constant(10000))
        result[:,base+4]=torch.where(price_known,price,0.)
        result[:,base+6]=price_known.float()
        fault|=torch.where(price_known,price_fault,0)
    return result,fault
