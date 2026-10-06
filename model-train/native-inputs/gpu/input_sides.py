"""原Native PM盘口38字段与冻结端点记忆；源盘口事实归DPT，记忆归Native。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from public_state import _take
from input_events import _extreme, time_envelope
from input_trades import _sum


@dataclass(frozen=True)
class PmSideMemory:
    total: object
    best: object
    known: torch.Tensor
    best_present: torch.Tensor
    asset_indices: torch.Tensor


@dataclass(frozen=True)
class NativePmSides:
    values: torch.Tensor  # [roots,2,38] = Q6 + bid16 + ask16。
    memory: PmSideMemory
    fault: torch.Tensor


class SideInputs:
    def __init__(self, state):
        self.state = state
        numbers = _numbers()
        def price_units():
            source = state.decimal_columns
            factor = source.coefficient.constant(1)
            for exponent in range(1,29):
                factor = factor.constant(10**exponent).select(28-source.scale==exponent,factor)
            return source.coefficient*factor
        self.price_units = numbers.run_exact_batch(price_units)

    def encode(self, frame, asset_indices, observation_ms, previous=None):
        """纯计算候选记忆；成功冻结观察后由协调器一次提交，失败/扩池不能推进记忆。

        previous必须为同根/市场上次冻结的记忆，不能按源事件、块或PPO批次清空。
        输入块切换须沿原资产身份迁移坐标，完整恢复需保存该精确记忆和位置。
        """
        state, numbers = self.state,_numbers()
        roots,slots = frame.present.shape
        if (asset_indices.shape != (roots,2) or observation_ms.shape != (roots,)
                or asset_indices.dtype != torch.int64 or observation_ms.dtype != torch.int64
                or asset_indices.device != state.slots.device or observation_ms.device != state.slots.device):
            raise ValueError('需同设备int64市场资产/观察时间')
        valid = (asset_indices >= 0) & (asset_indices < len(state.assets))
        base_fault = frame.prefix_fault | (~valid.all(dim=1)).to(torch.int32)
        safe = asset_indices.clamp(0,max(0,len(state.assets)-1))
        if previous is not None:
            if previous.known.shape != (roots,4) or previous.asset_indices.shape != asset_indices.shape:
                raise ValueError('上次冻结端点形状错误')
        def compute():
            output = torch.zeros((roots,2,38),device=asset_indices.device)
            totals,bests,knowns,best_present = [],[],[],[]
            fault = base_fault
            quantity = frame.quantity.numerator
            factor,remainder = quantity.constant(10**28).divrem(frame.quantity.denominator)
            units = quantity*factor
            units = numbers.ExactInt(units.words,units.sign,units.fault | remainder.fault | (remainder.sign != 0).to(torch.int32))
            for outcome in range(2):
                if state.assets:
                    q = frame.quality.gather(1,safe[:,outcome,None]).reshape(-1)
                    ready = frame.ready.gather(1,safe[:,outcome,None]).reshape(-1)
                else:
                    q = torch.full((roots,),3,dtype=torch.int64,device=asset_indices.device)
                    ready = torch.zeros(roots,dtype=torch.bool,device=asset_indices.device)
                output[:,outcome,:6] = torch.nn.functional.one_hot(q,6).float()
                for side in range(2):
                    index = 2*outcome+side
                    side_out = output[:,outcome,6+16*side:22+16*side]
                    known = valid[:,outcome] & ready & (q <= 1)
                    if slots:
                        coordinate = (state.slots[:,0][None]==safe[:,outcome,None]) & (state.slots[:,1][None]==side)
                        mask = coordinate & known[:,None] & frame.present & (quantity.sign.reshape(roots,slots)>0)
                        total = _sum(units,mask)
                        prices = _take(self.price_units,state.slots[:,2][None].expand(roots,-1).reshape(-1))
                        best,bp,error = _extreme(prices,mask,maximum=side==0)
                        best = best.select(bp,best.constant(0))
                        fault = fault | error | numbers.combine_fault_rows(frame.fault)
                        anchor_mask = coordinate & valid[:,outcome,None] & frame.anchor_present
                        times = state.anchor_times[frame.anchor_ids+1]
                        time,error = time_envelope(times[:,:,1],times[:,:,2],anchor_mask & times[:,:,0].bool(),anchor_mask,observation_ms)
                        side_out[:,5:14] = time; fault = fault | error
                        count = mask.sum(dim=1)
                    else:
                        total = numbers.ExactInt.from_i64_tensor(torch.zeros(roots,dtype=torch.int64,device=asset_indices.device))
                        best,bp = total,torch.zeros_like(known)
                        count = torch.zeros(roots,dtype=torch.int64,device=asset_indices.device)
                    side_out[:,0] = known.float()
                    count_value = numbers.ExactInt.from_i64_tensor(count)
                    value,error = count_value.project(count_value.constant(1),operation=1)
                    side_out[:,1] = torch.where(known,value,0.0); fault = fault | error
                    value,error = total.project(total.constant(10**28),operation=1)
                    side_out[:,2] = torch.where(known,value,0.0); fault = fault | error
                    if previous is not None:
                        # 资产坐标必须已按原身份迁移；不能把另一市场的端点并入本市场。
                        same = previous.asset_indices[:,outcome]==asset_indices[:,outcome]
                        before = torch.arange(roots,device=asset_indices.device)*4+index
                        delta = total-_take(previous.total,before)
                        value,error = delta.project(delta.constant(10**28),operation=1)
                        present = known & previous.known[:,index] & same
                        side_out[:,3] = torch.where(present,value,0.0); side_out[:,4] = present.float(); fault = fault | error
                        delta = best-_take(previous.best,before)
                        value,error = delta.project(delta.constant(10**28),operation=1)
                        present = bp & previous.best_present[:,index] & same
                        side_out[:,14] = torch.where(present,value,0.0); side_out[:,15] = present.float(); fault = fault | error
                    totals.append(total); bests.append(best); knowns.append(known); best_present.append(bp)
            def combine(values):
                width = max(value.words.shape[1] for value in values)
                return numbers.ExactInt(torch.stack([torch.nn.functional.pad(v.words,(0,width-v.words.shape[1])) for v in values],dim=1).reshape(-1,width),
                                        torch.stack([v.sign for v in values],dim=1).reshape(-1),
                                        torch.stack([v.fault for v in values],dim=1).reshape(-1))
            memory = PmSideMemory(combine(totals),combine(bests),torch.stack(knowns,dim=1),
                                  torch.stack(best_present,dim=1),asset_indices.clone())
            return NativePmSides(output,memory,fault)
        return numbers.run_exact_batch(compute,initial_word_capacity=self.price_units.words.shape[1])
