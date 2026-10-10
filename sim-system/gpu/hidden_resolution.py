"""原hidden Terminal的共享来源列与源合格判定候选。

canonical ID和完整Terminal仅由已验收DPT材料提供；该状态不代替平台
Resolution义务、公开NOE或请求式赎回。源候选须与真实执行/反馈一起提交。
"""
from dataclasses import dataclass, replace
import json
import torch


@dataclass(frozen=True)
class TerminalColumns:
    fields: torch.Tensor  # has_time/time/完整terminal字典/原canonical ID字典/quality/payout人口。
    payout_indices: torch.Tensor
    terminals: tuple
    canonical_ids: tuple

    @classmethod
    def from_buckets(cls, buckets, intern_decimal, quality, device):
        fields, payouts, terminals, identities = [], [], [], []
        terminal_ids, identity_ids = {}, {}
        def intern(value, values, directory):
            key = json.dumps(value,sort_keys=True,separators=(',', ':'))
            if key not in directory:
                directory[key] = len(values)
                values.append(value)
            return directory[key]
        for bucket in buckets:
            for fact, event_id in zip(bucket['references'],bucket['reference_market_event_ids'],strict=True):
                row, indices = [0,0,-1,-1,3,0], [0,0]
                payload = fact['payload'].get('Inline',{}).get('MarketFact',{}).get('payload',{})
                if 'Terminal' in payload:
                    terminal = payload['Terminal']
                    time = fact['dependency_time']['source_event_at_utc_ms']
                    vector = terminal['payout_vector']
                    row = [int(time is not None),0 if time is None else time,
                        intern(terminal,terminals,terminal_ids),
                        intern(event_id,identities,identity_ids),quality[fact['quality']['availability']],len(vector)]
                    indices = [intern_decimal(entry['payout_pusd_per_share']) for entry in vector[:2]]
                    indices += [0]*(2-len(indices))
                fields.append(row)
                payouts.append(indices)
        fields.append([0,0,-1,-1,3,0])
        payouts.append([0,0])
        return cls(torch.tensor(fields,dtype=torch.int64,device=device),
                   torch.tensor(payouts,dtype=torch.int64,device=device),tuple(terminals),tuple(identities))


@dataclass(frozen=True)
class SourceTerminalTransition:
    source: object
    market: torch.Tensor
    reference: torch.Tensor
    qualified: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class SourceQualifiedResolutions:
    columns: object  # 同一不可变来源/Terminal字典；跨页须先无损承接该绑定。
    reference_indices: torch.Tensor  # 保留第一次原引用，完整ID/Terminal/payout借共享源页。
    terminal_ids: torch.Tensor
    source_times: torch.Tensor
    present: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def empty(cls, columns, roots, markets, device):
        absent = torch.full((roots,markets),-1,dtype=torch.int64,device=device)
        return cls(columns,absent,absent.clone(),torch.zeros_like(absent),
                   torch.zeros_like(absent,dtype=torch.bool),torch.zeros(roots,dtype=torch.int32,device=device))

    def observe(self, reference, market, market_end, admitted, enabled, role_terminal):
        """先保存原源判定；只有Fresh和真实资产准入/时点合格才给执行层候选。"""
        columns = self.columns
        roots, markets = self.present.shape
        if any(value.shape != (roots,) or value.dtype != torch.int64 or value.device != self.fault.device
               for value in (reference,market,market_end)):
            raise TypeError('判定源坐标、市场与end须为同设备完整int64根列')
        if any(value.shape != (roots,) or value.dtype != torch.bool or value.device != self.fault.device
               for value in (admitted,enabled,role_terminal)):
            raise TypeError('原完整资产准入、执行与角色终态须为同设备bool根列')
        if markets == 0:
            fault = self.fault | enabled.to(torch.int32)
            return replace(self,fault=fault),SourceTerminalTransition(self,market,reference,
                torch.zeros_like(enabled),fault)
        row = columns.fields[reference.clamp(0,columns.fields.size(0)-1)]
        axis = torch.arange(roots,device=self.fault.device)
        safe = market.clamp(0,markets-1)
        has_source = row[:,0] != 0
        error = ((market<0)|(market>=markets)|~admitted|(reference<0)|
                 (reference>=columns.fields.size(0)-1)|(row[:,2]<0)|(row[:,3]<0)|(row[:,5]!=2)).to(torch.int32)
        error |= (has_source & (row[:,1] < market_end)).to(torch.int32)
        error |= (has_source & self.present[axis,safe] &
                  (row[:,2] != self.terminal_ids[axis,safe])).to(torch.int32)
        fault = self.fault | torch.where(enabled,error,0)
        valid = enabled & (fault==0)
        fresh = valid & has_source & ~self.present[axis,safe]
        selected = (torch.arange(markets,device=self.fault.device)[None,:] == market[:,None]) & fresh[:,None]
        candidate = replace(self,
            reference_indices=torch.where(selected,reference[:,None],self.reference_indices),
            terminal_ids=torch.where(selected,row[:,2,None],self.terminal_ids),
            source_times=torch.where(selected,row[:,1,None],self.source_times),
            present=self.present | selected,fault=fault)
        qualified = valid & (row[:,4] == 0) & ~role_terminal & candidate.present[axis,safe]
        return candidate,SourceTerminalTransition(self,market,candidate.reference_indices[axis,safe],qualified,fault)

    def qualified(self, market, quality, enabled, role_terminal):
        """后来真实Fresh市场事件可使原首次源判定合格，不替换其身份/时点。"""
        roots, markets = self.present.shape
        if markets == 0:
            return SourceTerminalTransition(self,market,torch.full_like(market,-1),
                torch.zeros_like(enabled),self.fault | enabled.to(torch.int32))
        axis = torch.arange(roots,device=self.fault.device)
        safe = market.clamp(0,markets-1)
        fault = self.fault | (enabled & ((market<0)|(market>=markets))).to(torch.int32)
        valid = enabled & (fault==0) & (quality==0) & ~role_terminal & self.present[axis,safe]
        return SourceTerminalTransition(self,market,self.reference_indices[axis,safe],valid,fault)

    def coordinate_result(self, original, root_fault):
        valid = (root_fault == 0)[:,None]
        return replace(self,**{name:torch.where(valid,getattr(self,name),getattr(original,name))
            for name in ('reference_indices','terminal_ids','source_times','present')},fault=root_fault)


@dataclass(frozen=True)
class ResolutionObligations:
    """首次合格判定的执行义务；源合格不等于已清偿或已经公开。

    本阶段保留原非终局fill人口及完整序号。槽不复用，扩fill池时须在冷边界
    承接这份绑定；请求式结算消费者尚须保存自己的请求、清偿和失效状态。
    """
    reference_indices: torch.Tensor
    pending: torch.Tensor  # [root,market,fill slot]，仅首次登记时的非终局人口。
    sequences: object  # [root,fill slot]完整u64；不能把slot作为源fill身份。
    fault: torch.Tensor

    @classmethod
    def empty(cls, roots, markets, pool):
        return cls(torch.full((roots,markets),-1,dtype=torch.int64,device=pool.occupied.device),
            torch.zeros((roots,markets,pool.capacity),dtype=torch.bool,device=pool.occupied.device),
            pool.sequences.constant(0),torch.zeros(roots,dtype=torch.int32,device=pool.occupied.device))

    def register(self, source, pool, condition, enabled):
        """只在首次执行合格判定时冻结原pending fill集合，不重新登记重复判定。"""
        from queues import Numbers
        roots,markets,width = self.pending.shape
        if pool.occupied.shape != (roots,width):
            raise ValueError('判定义务与fill池须先在冷边界对齐容量')
        axis = torch.arange(roots,device=condition.device)
        selected = (torch.arange(markets,device=condition.device)[None,:] == source.market[:,None])
        existing = (self.reference_indices >= 0) & selected
        fresh = enabled & source.qualified & ~existing.any(1)
        record = pool.records
        safe_path = record.selected_path.clamp(0,record.states.size(0)-1)
        safe_ordinal = record.ordinal.clamp(0,record.states.size(1)-1)
        state = record.states[safe_path,safe_ordinal].reshape(roots,width)
        same_condition = pool.origins.conditions.reshape(roots,width) == condition[:,None]
        population = pool.occupied & same_condition
        invalid = (record.selected_path<0)|(record.selected_path>=record.states.size(0))|(record.ordinal<0)
        invalid |= record.ordinal >= record.lengths[safe_path]
        error = pool.fault | Numbers.combine_fault_rows(torch.where(population,
            invalid.reshape(roots,width).to(torch.int32) | record.fault.reshape(roots,width) |
            pool.sequences.fault.reshape(roots,width),0))
        fault = self.fault | source.fault | torch.where(fresh,error,0)
        fresh &= fault == 0
        pending = population & (state != 4) & (state != 5)
        install = selected & fresh[:,None]
        # 已登记市场借同一追加不复用的fill目录；保留旧绑定，补本次真实人口。
        old_members = self.pending.any(1)
        same_sequence = self.sequences.compare(pool.sequences)
        mismatch = Numbers.combine_fault_rows(torch.where(old_members,
            same_sequence.fault.reshape(roots,width) | (same_sequence.order.reshape(roots,width)!=0).to(torch.int32),0))
        fault |= mismatch
        fresh &= fault == 0
        install &= (fault == 0)[:,None]
        sequence_mask = (old_members | (pending & fresh[:,None])).flatten()
        candidate = replace(self,
            reference_indices=torch.where(install,source.reference[:,None],self.reference_indices),
            pending=torch.where(install[:,:,None],pending[:,None,:],self.pending),
            sequences=pool.sequences.select(sequence_mask,self.sequences),fault=fault)
        return candidate,fresh

    def complete(self, transition):
        """真实终态消费只移除该完整fill身份；MINED/经济应用不关闭屏障。"""
        from queues import _take
        roots, _, width = self.pending.shape
        axis = torch.arange(roots,device=self.pending.device)
        slot = transition.slot.clamp(0,width-1)
        in_range = (transition.slot>=0) & (transition.slot<width)
        members = in_range & self.pending[axis,:,slot].any(1)
        terminal = transition.event.emitted & transition.event.terminal
        identity = _take(self.sequences,axis*width+slot).compare(transition.fill_sequence)
        fault = self.fault | transition.event.fault | torch.where(terminal & members,
            identity.fault | (identity.order!=0).to(torch.int32) |
            ((transition.slot<0)|(transition.slot>=width)).to(torch.int32),0)
        selected = torch.arange(width,device=self.pending.device)[None,:] == slot[:,None]
        remove = terminal & members & (fault==0)
        return replace(self,pending=self.pending & ~(remove[:,None,None] & selected[:,None,:]),fault=fault)

    def resized(self, capacity):
        """冷容量承接保留旧市场集合和完整序号；新槽不加入已有判定义务。"""
        from queues import ExactInt
        roots, _, old = self.pending.shape
        if capacity<old:
            raise ValueError('未退役判定成交身份不能缩容')
        if capacity==old:
            return self
        value=self.sequences
        pad=torch.nn.functional.pad
        words=pad(value.words.reshape(roots,old,-1),(0,0,0,capacity-old)).flatten(0,1)
        signs=pad(value.sign.reshape(roots,old),(0,capacity-old)).flatten()
        faults=pad(value.fault.reshape(roots,old),(0,capacity-old)).flatten()
        return replace(self,pending=pad(self.pending,(0,capacity-old)),
            sequences=ExactInt(words,signs,faults))

    def coordinate_result(self, original, root_fault):
        valid = root_fault == 0
        width = self.pending.size(2)
        return replace(self,
            reference_indices=torch.where(valid[:,None],self.reference_indices,original.reference_indices),
            pending=torch.where(valid[:,None,None],self.pending,original.pending),
            sequences=self.sequences.select(valid.repeat_interleave(width),original.sequences),fault=root_fault)


@dataclass(frozen=True)
class HiddenTerminalEffects:
    market: torch.Tensor
    reference: torch.Tensor  # 首次原canonical ID、完整terminal及payout都借该不可变源列。
    source_at: torch.Tensor
    at: torch.Tensor
    orders: object
    resolved: torch.Tensor  # 原Resolved反馈人口，独立于账户delta。
    fault: torch.Tensor
