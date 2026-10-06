"""原Native bba28/pm_trade18的GPU配方；整窗归约由窗口累积器负责。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from input_projection import decimal_value, price_value, t5


@dataclass(frozen=True)
class PmFields:
    bba: torch.Tensor
    trade: torch.Tensor
    fault: torch.Tensor


def pm_fields(state, observation, asset_indices, observation_ms):
    roots = asset_indices.shape[0]
    if asset_indices.shape != (roots,2) or observation_ms.shape != (roots,):
        raise ValueError('Native公共字段市场/时间批次错误')
    if asset_indices.dtype != torch.int64 or observation_ms.dtype != torch.int64:
        raise TypeError('需设备int64公共资产/观察时间')
    if asset_indices.device != state.slots.device or observation_ms.device != state.slots.device:
        raise ValueError('Native公共字段坐标设备错误')
    numbers = _numbers()
    valid = (asset_indices >= 0) & (asset_indices < len(state.assets))
    base_fault = observation.prefix_fault | (~valid.all(dim=1)).to(torch.int32)
    if not state.assets:
        bba = torch.zeros((roots,2,28),device=asset_indices.device)
        trade = torch.zeros((roots,2,18),device=asset_indices.device)
        bba[:,:,3] = 1.0; trade[:,:,3] = 1.0
        return PmFields(bba,trade,base_fault)
    safe = asset_indices.clamp(0,len(state.assets)-1)
    batch = torch.arange(roots,device=safe.device)[:,None]
    now = observation_ms[:,None].expand(-1,2)
    def compute():
        outputs, errors = [], base_fault
        for field, width, anchor_index in ((observation.bba,28,6),(observation.trade,18,5)):
            values, present = field.values[batch,safe],field.present[batch,safe] & valid
            quality = values[:,:,-1]
            visible = present & ((quality==0) | (quality==1))
            out = torch.zeros((roots,2,width),dtype=torch.float32,device=safe.device)
            out[:,:,:6] = torch.nn.functional.one_hot(torch.where(valid,quality,3),6).float()
            out[:,:,6] = visible.float()
            anchors = state.unique_anchor_ids[values[:,:,anchor_index]]
            time, time_fault = t5(state,anchors,present & (anchors>=0),now)
            errors = errors | numbers.combine_fault_rows(time_fault | field.fault[batch,safe])
            if width == 28:
                for member in range(3):
                    start = 7+7*member
                    p,error = price_value(decimal_value(state,values[:,:,2*member+1]),pm=member<2)
                    known = visible & values[:,:,2*member].bool()
                    out[:,:,start] = torch.where(known,p.reshape(roots,2),0.0)
                    out[:,:,start+1] = known.float()
                    out[:,:,start+2:start+7] = time
                    errors = errors | numbers.combine_fault_rows(error.reshape(roots,2))
            else:
                for source,target,is_price in ((0,7,True),(1,8,False),(4,11,False)):
                    p,error = price_value(decimal_value(state,values[:,:,source]),pm=is_price)
                    known = visible & values[:,:,3].bool() if source==4 else visible
                    out[:,:,target] = torch.where(known,p.reshape(roots,2),0.0)
                    errors = errors | numbers.combine_fault_rows(error.reshape(roots,2))
                out[:,:,9] = (visible & (values[:,:,2]==0)).float()
                out[:,:,10] = (visible & (values[:,:,2]==1)).float()
                out[:,:,12] = (visible & values[:,:,3].bool()).float()
                out[:,:,13:] = time
            outputs.append(out)
        return PmFields(*outputs,errors)
    return numbers.run_exact_batch(compute)
