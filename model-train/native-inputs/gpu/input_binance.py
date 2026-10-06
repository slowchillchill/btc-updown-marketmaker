"""原Binance book38/六侧中的两侧9字段和最新成交15；时间保持原配方。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from input_projection import decimal_value, t5
from input_books import PmBookInputs
from input_sides import SideInputs


@dataclass(frozen=True)
class NativeBinance:
    packed: object
    side: object
    trade: torch.Tensor
    fault: torch.Tensor


class BinanceInputs:
    def __init__(self,state,*,initial_capacity=128):
        self.state = state
        self.book = PmBookInputs(state,initial_capacity=initial_capacity,pm=False,outcomes=1)
        self.side = SideInputs(state,outcomes=1)

    def encode(self,frame,observation,symbol_indices,observation_ms,previous=None):
        if symbol_indices.shape!=observation_ms.shape: raise ValueError('Binance symbol/时间批次错误')
        assets = symbol_indices[:,None]
        packed = self.book.pack(frame,assets,observation_ms)
        sides = self.side.encode(frame,assets,observation_ms,previous)
        valid = (symbol_indices>=0) & (symbol_indices<len(self.state.assets))
        fault = packed.fault | sides.fault | (~valid).to(torch.int32)
        if not self.state.assets:
            values = torch.zeros((len(symbol_indices),15),device=symbol_indices.device);values[:,3]=1.0
            return NativeBinance(packed,sides,values,fault)
        safe = symbol_indices.clamp(0,len(self.state.assets)-1)
        batch = torch.arange(len(safe),device=safe.device)
        field = observation.values[batch,safe]
        present = observation.present[batch,safe] & valid
        visible = present & (field[:,4]<=1)
        numbers = _numbers()
        def compute():
            values = torch.zeros((len(safe),15),device=safe.device)
            values[:,:6] = torch.nn.functional.one_hot(field[:,4],6).float()
            values[:,6] = visible.float();values[:,9] = torch.where(visible,field[:,2],0).float()
            errors = fault | observation.fault[batch,safe]
            for position,source in ((7,0),(8,1)):
                value,error = decimal_value(self.state,field[:,source]).project(operation=1)
                values[:,position] = torch.where(visible,value,0.0);errors = errors | error
            anchor = self.state.unique_anchor_ids[field[:,3]]
            time,error = t5(self.state,anchor,present & (anchor>=0),observation_ms)
            values[:,10:15] = time;errors = errors | error
            return NativeBinance(packed,sides,values,errors)
        return numbers.run_exact_batch(compute)
