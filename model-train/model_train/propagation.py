"""当前批传播标签：原冻结盘口起点、纯公共页终点；不形成策略输入或账本。"""
from dataclasses import dataclass
import numpy as np
import torch
from public_state import _take
from input_sides import SideInputs
from .gpu_exact import run_exact_batch


@dataclass(frozen=True)
class PropagationOrigin:
    cutoff: torch.Tensor
    prefix: torch.Tensor
    market: torch.Tensor
    sides: object


@dataclass(frozen=True)
class PropagationLabels:
    value: torch.Tensor
    valid: torch.Tensor
    endpoint_prefix: torch.Tensor


def frozen_origin(root,task,rows):
    """只保存真实发布人口的原Ck端点；绝不读取延迟开始时的当前盘口。"""
    identities=task.observation.frozen.identities[rows]
    locations=(identities[:,None]==root.inputs.directory_identities[None]).all(2).to(torch.int64).argmax(1)
    memory=task.observation.market.endpoint_memory.rows(rows)
    return PropagationOrigin(task.observation.frozen.cutoff[rows].clone(),
        task.observation.market.prefixes[rows].clone(),root.inputs.directory_markets[locations].clone(),memory.pm_sides)


def prepare_labels(root,origins,horizon_ms):
    """一次按页合并读取严格右开终点；不足/缺腿只屏蔽辅助损失，不阻塞PPO。"""
    cutoff=torch.cat([o.cutoff for o in origins]);market=torch.cat([o.market for o in origins])
    assets=torch.cat([o.sides.asset_indices for o in origins]);end=cutoff+horizon_ms
    prepared=root.clock.source.prepared
    causal=prepared.causal if hasattr(prepared,'causal') else prepared.arrays['causal_visible_at']
    prefix=torch.as_tensor(np.searchsorted(causal,end.cpu().numpy(),side='left')-1,device=end.device,dtype=torch.int64)
    window=root.inputs.encoder.market.times[market]
    mature=(root.clock.gates.has_public_visible[0]&(end<=root.clock.gates.public_visible_at[0])
        &(prefix>=0)&(prefix<root.clock.source.position[0])&(cutoff>=window[:,0])&(end<=window[:,1]))
    coverage=prepared.received_range_utc_ms
    mature &= False if coverage is None else ((cutoff>=coverage['start'])&(end<=coverage['end']))
    labels=torch.zeros((len(end),2),device=end.device);valid=torch.zeros_like(labels,dtype=torch.bool)
    offsets=np.cumsum([0]+[len(o.cutoff) for o in origins])
    parts=prepared.parts if hasattr(prepared,'parts') else [dict(group_begin=0,group_end=len(causal))]
    for page_index,part in enumerate(parts):
        rows=torch.nonzero(mature&(prefix>=part['group_begin'])&(prefix<part['group_end'])).flatten()
        if not len(rows):continue
        page=prepared.read(page_index) if hasattr(prepared,'parts') else prepared
        pm=page.install()[0]
        sides=SideInputs(pm).encode(pm.frame(prefix[rows]),assets[rows],end[rows])
        # 每个起点来自自己的原冻结；仅将同页实际人口拼接，保持逐腿精确差值。
        for i,origin in enumerate(origins):
            selected=rows[(rows>=int(offsets[i]))&(rows<int(offsets[i+1]))]
            if not len(selected):continue
            local=selected-int(offsets[i]);target=torch.searchsorted(rows,selected)
            def difference():
                before=_take(origin.sides.best,(local[:,None]*4+torch.arange(4,device=end.device)).flatten())
                after=_take(sides.memory.best,(target[:,None]*4+torch.arange(4,device=end.device)).flatten())
                delta=after-before
                bid=_take(delta,(torch.arange(len(local)*2,device=end.device)*2))
                ask=_take(delta,(torch.arange(len(local)*2,device=end.device)*2+1))
                total=bid+ask
                value,fault=total.project(total.constant(2*10**28),operation=0)
                return value.reshape(-1,2),fault.reshape(-1,2)
            value,fault=run_exact_batch(difference)
            before_ok=(origin.sides.known[local]&origin.sides.best_present[local]).reshape(-1,2,2).all(2)
            after_ok=(sides.memory.known[target]&sides.memory.best_present[target]).reshape(-1,2,2).all(2)
            labels[selected]=value
            valid[selected]=before_ok&after_ok&(fault==0)&(sides.fault[target,None]==0)
    return PropagationLabels(labels,valid,prefix)
