"""同一公共tick观察及SIM封存规则的Native rule30。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from input_projection import decimal_value,t5


@dataclass(frozen=True)
class NativeRules:
    values: torch.Tensor
    fault: torch.Tensor


class RuleInputs:
    def __init__(self, shared, process_rules):
        self.shared,self.process_rules = shared,process_rules

    def encode(self, observation, assets, now):
        roots = len(now);shared = self.shared
        if (assets.shape!=(roots,2) or now.dtype!=torch.int64 or assets.dtype!=torch.int64
                or assets.device!=shared.slots.device or now.device!=shared.slots.device):
            raise ValueError('需同设备int64资产/观察时刻')
        valid = (assets>=0)&(assets<len(shared.assets))
        base_fault = observation.prefix_fault | (~valid.all(dim=1)).to(torch.int32) | self.process_rules.fault
        if not shared.assets:
            values = torch.zeros((roots,30),device=assets.device);values[:,14:]=self.process_rules.tail
            return NativeRules(values,base_fault)
        batch = torch.arange(roots,device=assets.device)[:,None]
        field = observation.tick.values[batch,assets.clamp(0,len(shared.assets)-1)]
        present = observation.tick.present[batch,assets.clamp(0,len(shared.assets)-1)] & valid
        visible = present & (field[:,:,3]<=1)
        numbers = _numbers()
        def compute():
            values = torch.zeros((roots,30),device=assets.device)
            p,error = decimal_value(shared,field[:,:,1]).project(operation=1)
            fault = base_fault | numbers.combine_fault_rows(error.reshape(roots,2))
            anchor = shared.unique_anchor_ids[field[:,:,2]]
            time,error = t5(shared,anchor,(anchor>=0)&valid,now[:,None].expand(-1,2))
            fault |= numbers.combine_fault_rows(error | observation.tick.fault[batch,assets.clamp(0,len(shared.assets)-1)])
            for outcome in range(2):
                start = 7*outcome
                values[:,start] = torch.where(visible[:,outcome],p.reshape(roots,2)[:,outcome],0.0)
                values[:,start+1] = visible[:,outcome].float()
                values[:,start+2:start+7] = time[:,outcome]
            values[:,14:]=self.process_rules.tail
            return NativeRules(values,fault)
        return numbers.run_exact_batch(compute)
