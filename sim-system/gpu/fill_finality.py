"""原密封最终性路径的设备成交记录及原子经济转移候选。

每批每根至多一个当前成交。宿主按原时间/fill sequence取义务，领取终态效果后
更新返佣/分辨率前驱。这里只消费选定路径，不重新选择、不负责全局义务池。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactRational


_CODES = {'MATCHED_NOT_BROADCASTED':0,'MATCHED':1,'RETRYING':2,'MINED':3,'CONFIRMED':4,'FAILED':5}
_SUCCESS = ((1,3,4),(0,1,3,4),(1,2,3,4),(0,1,2,3,4))
_FAILURE = ((0,5),(1,5),(0,1,5),(1,3,5),(0,1,3,5),(1,2,5),(0,1,2,5))


def _path_columns(paths):
    """冷边界复用原四成功/七失败模板及经济应用位置，拒绝任意状态循环。"""
    offsets,states,economic = [],[],[]
    for path in paths:
        steps = path['steps']
        sequence = tuple(_CODES.get(step['state'],-1) for step in steps)
        if sequence not in _SUCCESS+_FAILURE:
            raise ValueError('必须是原密封成功或失败路径模板')
        times = [step['offset_ms'] for step in steps]
        if any(type(at) is not int or not 0 <= at <= (1<<63)-1 for at in times):
            raise ValueError('最终性offset须非负int64')
        if times[0] != 0 or any(a >= b for a,b in zip(times,times[1:])):
            raise ValueError('原最终性时点必须从零严格递增')
        ordinal = path['economic_application_ordinal']
        if ordinal is None:
            if sequence in _SUCCESS:
                raise ValueError('成功路径必须指定经济应用位置')
            ordinal = -1
        elif type(ordinal) is not int or not 0 <= ordinal < len(steps):
            raise ValueError('经济应用位置越界')
        elif sequence[ordinal] not in ((1,3,4) if sequence in _SUCCESS else (1,3)):
            raise ValueError('经济应用须沿原MATCHED/MINED/CONFIRMED条件')
        offsets.append(times)
        states.append(sequence)
        economic.append(ordinal)
    return offsets,states,economic


@dataclass(frozen=True)
class FillTransition:
    emitted: torch.Tensor
    ordinal: torch.Tensor
    state: torch.Tensor
    economic_delta: object
    failure_delta: object
    rebate_delta: object
    terminal: torch.Tensor  # 成功或失败都只在实际消费终态时通知前驱协调器。
    fault: torch.Tensor


@dataclass(frozen=True)
class FillRecord:
    base: object
    quote: object
    fee: object
    asset_indices: torch.Tensor
    matched_at: torch.Tensor
    active: torch.Tensor
    ordinal: torch.Tensor
    economic_applied: torch.Tensor
    economic_application_sequence: object
    selected_path: torch.Tensor
    offsets: torch.Tensor  # [paths,max_steps]，冷不可变bank，根仅保存选择坐标。
    states: torch.Tensor
    lengths: torch.Tensor
    economic_ordinals: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def create(cls, base, quote, fee, asset_indices, matched_at, active, sealed_path, *, selected_path=None):
        paths = [sealed_path] if isinstance(sealed_path,dict) else list(sealed_path)
        if not paths:
            raise ValueError('选定最终性路径bank不能为空')
        offsets,states,economic = _path_columns(paths)
        roots = base.sign.numel()
        device = base.words.device
        if asset_indices.shape != (roots,) or matched_at.shape != (roots,) or active.shape != (roots,):
            raise ValueError('成交记录布局不一致')
        if asset_indices.dtype != torch.int64 or matched_at.dtype != torch.int64 or active.dtype != torch.bool:
            raise TypeError('成交坐标/时间须int64，active须bool')
        if any(value.device != device for value in (asset_indices,matched_at,active)):
            raise ValueError('成交记录须在同一设备')
        if selected_path is None:
            selected_path = torch.zeros(roots,dtype=torch.int64,device=device)
        if selected_path.shape != (roots,) or selected_path.dtype != torch.int64 or selected_path.device != device:
            raise TypeError('选定路径须同设备int64[roots]')
        lengths = torch.tensor([len(times) for times in offsets],dtype=torch.int64,device=device)
        width = max(map(len,offsets))
        times = torch.tensor([times+[times[-1]]*(width-len(times)) for times in offsets],dtype=torch.int64,device=device)
        status = torch.tensor([list(sequence)+[sequence[-1]]*(width-len(sequence)) for sequence in states],dtype=torch.int8,device=device)
        valid_path = (selected_path >= 0) & (selected_path < len(paths))
        safe = selected_path.clamp(0,len(paths)-1)
        last_time = times[safe,lengths[safe]-1]
        error = base.fault | quote.fault | fee.fault
        error = error | ((base.sign <= 0) | (quote.sign <= 0) | (fee.sign < 0) | ~valid_path).to(torch.int32)
        error = error | (matched_at > (1<<63)-1-last_time).to(torch.int32)
        return cls(base,quote,fee,asset_indices,matched_at,active,
            torch.full((roots,),-1,dtype=torch.int64,device=device),
            torch.zeros(roots,dtype=torch.bool,device=device),base.constant(0),selected_path.clone(),times,status,lengths,
            torch.tensor(economic,dtype=torch.int64,device=device),error*active.to(torch.int32))

    def advance_one(self, account, at, taker_rebate_rate):
        """消费下一个原义务；故障原子回退本次候选，保留此前ordinal成功前缀。"""
        if at.shape != self.matched_at.shape or at.dtype != torch.int64 or at.device != self.matched_at.device:
            raise TypeError('最终性时刻须同设备int64[roots]')
        original_account = account
        next_ordinal = self.ordinal+1
        path = self.selected_path.clamp(0,self.lengths.numel()-1)
        pending = self.active & (next_ordinal < self.lengths[path])
        safe = torch.minimum(next_ordinal.clamp_min(0),self.lengths[path]-1)
        scheduled = self.matched_at+self.offsets[path,safe]
        fault = self.fault | ((at > scheduled) & pending).to(torch.int32)
        ready = pending & (at == scheduled) & (fault == 0)
        state = self.states[path,safe]
        economic = ready & (next_ordinal == self.economic_ordinals[path]) & ~self.economic_applied
        account,economic_delta = account.fill_economic(self.base,self.quote,self.fee,self.asset_indices,economic)
        failed = ready & (state == 5) & (account.fault == 0)
        account,failure_delta = account.fill_failure(self.base,self.quote,self.fee,self.asset_indices,self.economic_applied,failed)
        accrual = ExactRational.ratio(self.fee,self.fee.constant(1_000_000))*taker_rebate_rate
        success = ready & (state == 4) & (self.fee.sign > 0) & (account.fault == 0)
        account,rebate_delta = account.accrue_rebate(accrual,torch.zeros_like(ready),success)
        fault = fault | economic_delta.fault | failure_delta.fault | rebate_delta.fault
        committed = ready & (fault == 0)
        account = account.coordinate_result(original_account,fault)
        economic_delta = account.coordinate_delta(economic_delta,committed,fault)
        failure_delta = account.coordinate_delta(failure_delta,committed,fault)
        rebate_delta = account.coordinate_delta(rebate_delta,committed,fault)
        record = replace(self,ordinal=torch.where(committed,next_ordinal,self.ordinal),
            economic_applied=self.economic_applied | economic_delta.emitted,
            economic_application_sequence=economic_delta.sequence.select(economic_delta.emitted,self.economic_application_sequence),
            fault=fault)
        transition = FillTransition(committed,next_ordinal,state,economic_delta,failure_delta,rebate_delta,
            committed & ((state == 4) | (state == 5)),fault)
        return record,account,transition


# 保留现有正常路径组件调用名；create现在也接受原密封失败路径及设备选择bank。
NormalFillRecord = FillRecord
