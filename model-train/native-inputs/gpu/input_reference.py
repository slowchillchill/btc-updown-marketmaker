"""原Native reference37 GPU配方；临时值只读原准备器选择结果。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from input_projection import decimal_value, t5


@dataclass(frozen=True)
class NativeReference:
    values: torch.Tensor
    fault: torch.Tensor


class ReferenceInputs:
    def __init__(self,state):
        self.state = state
        self.shared = state.shared

    def encode(self,frame,market_frame,market_indices,observation_ms):
        roots = observation_ms.numel()
        if (market_indices.shape!=(roots,) or observation_ms.shape!=(roots,)
                or market_indices.dtype!=torch.int64 or observation_ms.dtype!=torch.int64
                or market_indices.device!=self.shared.slots.device or observation_ms.device!=self.shared.slots.device):
            raise ValueError('需同设备int64市场/观察时间')
        valid = (market_indices>=0) & (market_indices<self.state.market.market_count)
        fault = frame.prefix_fault | market_frame.prefix_fault | (~valid).to(torch.int32)
        if not self.state.market.market_count:
            return NativeReference(torch.zeros((roots,37),device=market_indices.device),fault)
        safe = market_indices.clamp(0,self.state.market.market_count-1)
        batch = torch.arange(roots,device=safe.device)
        admitted = valid & market_frame.admitted[batch,safe]
        numbers = _numbers()
        def compute():
            output = torch.zeros((roots,37),device=safe.device)
            errors = fault
            rule = frame.rule.values[batch,safe]
            official = frame.official.values[batch,safe]
            temporary = frame.temporary.values[batch,safe]
            for field in (frame.rule,frame.official,frame.temporary): errors = errors | field.fault[batch,safe]
            selected_present = admitted & frame.rule.present[batch,safe] & (rule[:,2]<=1) & (rule[:,0]<3)
            output[:,:3] = torch.nn.functional.one_hot(rule[:,0],4)[:,:3].float()*selected_present[:,None]
            op = admitted & frame.official.present[batch,safe] & official[:,0].bool()
            tp = admitted & frame.temporary.present[batch,safe] & temporary[:,0].bool()
            output[:,3] = (admitted & ~op & ~tp).float()
            output[:,4] = (~op & tp).float(); output[:,5] = op.float(); output[:,6] = tp.float()
            effective_present = (op & (official[:,3]<=1)) | (~op & tp)
            ids = torch.where(op,official[:,1],temporary[:,1])
            effective = decimal_value(self.shared,ids)
            value,error = effective.project(operation=1)
            output[:,7] = torch.where(effective_present,value,0.0); errors = errors | error
            anchors = self.shared.unique_anchor_ids[torch.where(op,official[:,2],temporary[:,2])]
            time,error = t5(self.shared,anchors,(op | tp) & (anchors>=0),observation_ms)
            output[:,8:13] = time; errors = errors | error
            window = numbers.ExactInt.from_i64_tensor(temporary[:,3])
            value,error = window.project(window.constant(1),operation=1)
            output[:,13] = torch.where(~op & tp,value,0.0); errors = errors | error
            for index in range(3):
                field = frame.global_values.values[:,index]
                present = admitted & frame.global_values.present[:,index] & (field[:,2]<=1)
                decimal = decimal_value(self.shared,field[:,0])
                value,error = decimal.project(operation=1)
                start = 14+7*index
                output[:,start] = torch.where(present,value,0.0); output[:,start+1] = present.float()
                anchors = self.shared.unique_anchor_ids[field[:,1]]
                time,tf = t5(self.shared,anchors,admitted & frame.global_values.present[:,index] & (anchors>=0),observation_ms)
                output[:,start+2:start+7] = time
                errors = errors | error | tf | frame.global_values.fault[:,index]
                delta = decimal-effective
                value,error = delta.project(operation=1)
                use = selected_present & (rule[:,0]==index) & present & effective_present
                output[:,35] = torch.where(use,value,output[:,35])
                output[:,36] = torch.where(use,1.0,output[:,36]); errors = errors | error
            return NativeReference(output,errors)
        return numbers.run_exact_batch(compute)
