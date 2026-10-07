"""原Native生命周期103字段。公共可见身份/生命周期和终态来自所属DPT状态。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from input_projection import decimal_value, price_value
from input_events import time_envelope


@dataclass(frozen=True)
class NativeLifecycle:
    values: torch.Tensor
    identity_present: torch.Tensor
    fault: torch.Tensor


class LifecycleInputs:
    def __init__(self, public_state):
        self.state = public_state
        self.shared = public_state.shared
        names = {'candidate':0,'prefetched':1,'active':2,'retired':3}
        codes = [names[state['value']] if state['mapping']=='KNOWN' else 4
                 for state in public_state.lifecycle_states]
        self.codes = torch.tensor(codes+[4],dtype=torch.int64,device=public_state.times.device)
        anchors = self.shared.anchor_sets
        capacity = max(1,max(map(len,anchors),default=0))
        self.anchor_ids = torch.full((len(anchors),capacity),-1,dtype=torch.int64,device=public_state.times.device)
        for index,ids in enumerate(anchors):
            if len(ids): self.anchor_ids[index,:len(ids)] = torch.tensor(ids,dtype=torch.int64,device=self.anchor_ids.device)

    def encode(self, frame, market_indices, asset_indices, observation_ms):
        roots = observation_ms.numel()
        if (market_indices.shape != (roots,) or asset_indices.shape != (roots,2)
                or observation_ms.dtype != torch.int64 or market_indices.dtype != torch.int64
                or asset_indices.dtype != torch.int64 or market_indices.device != self.state.times.device
                or observation_ms.device != self.state.times.device or asset_indices.device != self.state.times.device):
            raise ValueError('需同设备int64生命周期市场/资产/UTC坐标')
        numbers = _numbers()
        valid = (market_indices >= 0) & (market_indices < self.state.market_count)
        asset_valid = ((asset_indices >= 0) & (asset_indices < len(self.shared.assets))).all(dim=1)
        base_fault = frame.prefix_fault | (~valid | ~asset_valid).to(torch.int32)
        if not self.state.market_count:
            return NativeLifecycle(torch.zeros((roots,103),device=market_indices.device),valid & False,base_fault)
        safe = market_indices.clamp(0,self.state.market_count-1)
        batch = torch.arange(roots,device=safe.device)
        base_fault = base_fault | (~(asset_indices==self.state.asset_pairs[safe]).all(dim=1)).to(torch.int32)
        admitted = frame.admitted[batch,safe] & valid
        def compute():
            result = torch.zeros((roots,103),device=safe.device)
            fault = base_fault
            def envelope(anchor_set,present):
                ids = self.anchor_ids[anchor_set]
                times = self.shared.anchor_times[ids+1]
                available = present[:,None] & (ids >= 0)
                return time_envelope(times[:,:,1],times[:,:,2],available & times[:,:,0].bool(),available,observation_ms)
            for index,(left,right) in enumerate(((observation_ms,self.state.times[safe,0]),
                                                (self.state.times[safe,1],observation_ms))):
                delta = numbers.ExactInt.from_i64_tensor(left)-numbers.ExactInt.from_i64_tensor(right)
                value,error = delta.project(delta.constant(1000),operation=1)
                result[:,index] = torch.where(admitted,value,0.0); fault = fault | error
            life = frame.lifecycle.values[batch,safe]
            trading = frame.trading.values[batch,safe]
            terminal = frame.terminal.values[batch,safe]
            for field in (frame.lifecycle,frame.trading,frame.terminal): fault = fault | field.fault[batch,safe]
            result[:,2:8] = torch.nn.functional.one_hot(life[:,3],6).float()
            lp = frame.lifecycle.present[batch,safe] & life[:,0].bool() & (life[:,3] <= 1) & admitted
            result[:,8] = lp.float()
            result[:,9:14] = torch.nn.functional.one_hot(self.codes[life[:,1]],5).float()*lp[:,None]
            value,error = envelope(life[:,2],frame.lifecycle.present[batch,safe] & admitted)
            result[:,14:23] = value; fault = fault | error
            tp = frame.trading.present[batch,safe] & trading[:,:5:2].bool().any(dim=1) & (trading[:,7]<=1) & admitted
            result[:,23:29] = torch.nn.functional.one_hot(torch.where(tp,trading[:,7],3),6).float()
            result[:,29] = tp.float()
            for field,start in enumerate((30,41,52)):
                present = trading[:,2*field].bool() & frame.trading.present[batch,safe] & admitted
                visible = present & (trading[:,7]<=1)
                result[:,start] = torch.where(visible,trading[:,2*field+1],0).float()
                result[:,start+1] = visible.float()
                value,error = envelope(trading[:,6],present)
                result[:,start+2:start+11] = value; fault = fault | error
            result[:,63:69] = torch.nn.functional.one_hot(terminal[:,4],6).float()
            terminal_present = frame.terminal.present[batch,safe] & terminal[:,0].bool() & (terminal[:,4]<=1) & admitted
            result[:,69] = terminal_present.float()
            result[:,70:72] = ((asset_indices==terminal[:,1,None]) & terminal_present[:,None]).float()
            value,error = envelope(terminal[:,3],frame.terminal.present[batch,safe] & admitted)
            for start in (72,83,94): result[:,start:start+9] = value
            fault = fault | error
            payouts = self.state.payouts[terminal[:,2]]
            for side in range(2):
                matches = (payouts[:,:,0]==asset_indices[:,side,None]) & terminal_present[:,None]
                selected = matches.to(torch.int64).argmax(dim=1)
                ids = payouts[batch,selected,1]
                value,error = price_value(decimal_value(self.shared,ids),pm=True)
                present = matches.any(dim=1)
                result[:,81+11*side] = torch.where(present,value,0.0)
                result[:,82+11*side] = present.float()
                fault = fault | error
            return NativeLifecycle(result,admitted,fault)
        return numbers.run_exact_batch(compute)
