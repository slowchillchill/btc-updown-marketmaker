"""已领取原NOE位置到平台经济候选的Ledger设备缓存。

仅承担PlatformBalances子路径；Lifecycle、预留和有限派生归属仍需同一上层提交门。
原position/context/run/batch/ordinal与source reference沿共享NOE列保留，不以账户seq替代。
"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt,ExactRational,Numbers,_take


def take_ratio(value,index):
    return ExactRational(_take(value.numerator,index),_take(value.denominator,index))


def concatenate_integer(values):
    width=max(value.words.size(1) for value in values)
    return ExactInt(torch.cat([value.with_capacity(width).words for value in values]),
        torch.cat([value.sign for value in values]),torch.cat([value.fault for value in values]))


def concatenate_ratio(values):
    return ExactRational(concatenate_integer([value.numerator for value in values]),
        concatenate_integer([value.denominator for value in values]))


@dataclass(frozen=True)
class EconomicColumns:
    row_refs: torch.Tensor
    positions: torch.Tensor  # 无损字段编码(context,run,完整batch,完整record ordinal)。
    sequences: object
    balances: object
    outcomes: object
    asset_indices: torch.Tensor
    enabled: torch.Tensor
    outcome_list: object
    projection_batches: tuple  # 同一不可变共享列，保留source参考和实际收到时刻。

    def append(self, incoming):
        """追加新批共享列；旧字段不重新解码，完整原位置保持不变。"""
        from transport import append_root_fields,append_record_lists
        roots,width=self.row_refs.shape
        extra=incoming.row_refs.size(1)
        if incoming.row_refs.size(0)!=roots or incoming.row_refs.device!=self.row_refs.device:
            raise ValueError('Ledger新列须同根同设备')
        position_width=max(self.positions.size(1),incoming.positions.size(1))
        positions=[torch.nn.functional.pad(value,(0,position_width-value.size(1)))
            for value in [self.positions,incoming.positions]]
        return replace(self,row_refs=torch.cat([self.row_refs,incoming.row_refs],1),
            positions=append_root_fields(*positions,roots,width,extra,fields=position_width).reshape(-1,position_width),
            sequences=append_root_fields(self.sequences,incoming.sequences,roots,width,extra),
            balances=append_root_fields(self.balances,incoming.balances,roots,width,extra,fields=8),
            outcomes=append_root_fields(self.outcomes,incoming.outcomes,roots,width,extra),
            asset_indices=torch.cat([self.asset_indices,incoming.asset_indices],1),
            enabled=torch.cat([self.enabled,incoming.enabled],1),
            outcome_list=append_record_lists(self.outcome_list,incoming.outcome_list,roots,width,extra),
            projection_batches=self.projection_batches+incoming.projection_batches)

    @classmethod
    def from_projection_batches(cls,batches):
        from fields import encode_fields
        # 冷装配根据已知张量人口分配，设备运行只索引；不用读取GPU业务值确定格式。
        roots=batches[0].rows.size(0)
        refs=torch.cat([batch.rows for batch in batches],dim=1)
        def cat_integer(name):
            joined=concatenate_integer([getattr(batch,name) for batch in batches])
            widths=[batch.rows.size(1) for batch in batches]
            if roots==1:
                return joined
            offset=0
            indices=[]
            for width in widths:
                indices.append(torch.arange(roots,device=refs.device)[:,None]*width+
                    torch.arange(width,device=refs.device)[None,:]+offset)
                offset+=roots*width
            return _take(joined,torch.cat(indices,dim=1).flatten())
        contexts=torch.cat([batch.context_slots for batch in batches],dim=1)
        runs=torch.cat([batch.run_slots for batch in batches],dim=1)
        positions=encode_fields([contexts.flatten(),runs.flatten(),cat_integer('batch_sequences'),
            cat_integer('record_ordinals')])
        balances=concatenate_ratio([batch.economic_balances for batch in batches])
        outcomes=concatenate_ratio([batch.economic_outcomes for batch in batches])
        widths=[batch.rows.size(1) for batch in batches]
        offset=0
        order=[]
        for width in widths:
            order.append(torch.arange(roots,device=refs.device)[:,None]*width+
                torch.arange(width,device=refs.device)[None,:]+offset)
            offset+=roots*width
        indices=torch.cat(order,dim=1).flatten()
        balances=take_ratio(balances,(indices[:,None]*8+torch.arange(8,device=refs.device)).flatten())
        outcomes=take_ratio(outcomes,indices)
        from transport import batch_outcome_list,append_record_lists
        vectors=batch_outcome_list(batches[0])
        joined_width=widths[0]
        for batch in batches[1:]:
            vectors=append_record_lists(vectors,batch_outcome_list(batch),roots,joined_width,batch.rows.size(1))
            joined_width+=batch.rows.size(1)
        return cls(refs,positions,cat_integer('economic_sequences'),balances,outcomes,
            torch.cat([batch.economic_assets for batch in batches],dim=1),
            torch.cat([batch.economic_enabled for batch in batches],dim=1),vectors,tuple(batches))


    def outcome_fault(self,index,owners):
        from transport import take_record_list
        vectors=take_record_list(self.outcome_list,index)
        roots,width=vectors.present.shape
        ids=vectors.fields[0].reshape(roots,width)
        error=(ids<0)|(ids>=owners.numel())
        if owners.numel():
            error |= owners[ids.clamp(0,owners.numel()-1)]!=torch.arange(roots,device=ids.device)[:,None]
        amount_fault=vectors.fields[1].fault.reshape(roots,width)
        return Numbers.combine_fault_rows(torch.where(vectors.present,amount_fault|error.to(torch.int32),0))

    def outcome_delta(self,index,owners):
        from transport import take_record_list
        from success_pool import select
        from trade_success import sum_rows
        vectors=take_record_list(self.outcome_list,index)
        width=vectors.present.size(1)
        rows=owners[:,None]*width+torch.arange(width,device=owners.device)
        values=take_ratio(vectors.fields[1],rows.flatten())
        ids=vectors.fields[0].reshape(-1,width)[owners]
        selected=vectors.present[owners] & (ids==torch.arange(owners.numel(),device=owners.device)[:,None])
        zero=ExactRational(values.numerator.constant(0),values.denominator.constant(1))
        return sum_rows(select(values,selected.flatten(),zero),owners.numel(),width)


@dataclass(frozen=True)
class EconomicMemory:
    platform: object
    columns: EconomicColumns
    received: torch.Tensor
    applied: torch.Tensor
    available_at: torch.Tensor
    delivery_sequences: object
    fault: torch.Tensor

    @classmethod
    def open(cls,platform,columns):
        roots,width=columns.row_refs.shape
        if platform.fault.numel()!=roots:
            raise ValueError('Ledger与共享NOE经济根人口不符')
        zero=torch.zeros((roots,width),dtype=torch.bool,device=columns.row_refs.device)
        return cls(platform,columns,zero,zero.clone(),torch.zeros_like(columns.row_refs),
            ExactInt.from_i64_tensor(torch.zeros(roots*width,dtype=torch.int64,device=zero.device)),platform.fault)

    def receive(self,closed):
        """领取史原顺序保留；这里只缓存已由双门公开交付的经济Fact。"""
        roots,width=self.columns.row_refs.shape
        if closed.record_refs.size(0)!=roots or closed.record_refs.device!=self.received.device:
            raise ValueError('Ledger领取列根人口或设备不符')
        candidate=self
        for column in range(closed.record_refs.size(1)):
            reference=closed.record_refs[:,column]
            same=self.columns.row_refs==reference[:,None]
            found=same.any(dim=1)
            incoming=closed.emitted[:,column]
            fault=candidate.fault | closed.fault | (incoming & ~found).to(torch.int32)
            use=same & incoming[:,None] & self.columns.enabled & (fault==0)[:,None]
            # 重复领取不能刷新首次公开可见时刻或交付序。
            use=use & ~candidate.received
            delivered=_take(closed.delivered_sequences,
                torch.arange(roots,device=reference.device)*closed.record_refs.size(1)+column)
            delivery=_take(delivered,torch.arange(roots,device=reference.device).repeat_interleave(width))
            candidate=replace(candidate,received=candidate.received | use,
                available_at=torch.where(use,closed.available_at[:,column,None],candidate.available_at),
                delivery_sequences=delivery.select(use.flatten(),candidate.delivery_sequences),fault=fault)
        return candidate

    def apply_pending(self,association_available):
        """缺前驱或经济关联保留同一原position；成功后才在该position记应用。

        目录暂按本共享块人口分配；重复不增长。迭代上限取原输入人口，只推进可用直接前驱，
        不把物理row或排序后位置变成新NOE身份。生命周期装配者提供真实association状态。
        """
        roots,width=self.received.shape
        if association_available.shape!=self.received.shape or association_available.dtype!=torch.bool or association_available.device!=self.received.device:
            raise TypeError('Ledger经济关联须同设备bool[root,record]')
        candidate=self
        pending=self.received & ~self.applied
        # 原平台严格prefix+1。因此选可用前驱可减少空候选，但不重写原领取史。
        for _ in range(width):
            prefix=candidate.platform.platform_source_prefix+candidate.platform.platform_source_prefix.constant(1)
            expected=_take(prefix,torch.arange(roots,device=pending.device).repeat_interleave(width))
            relation=self.columns.sequences.compare(expected)
            eligible=pending & association_available & (relation.order.reshape(roots,width)==0)
            eligible=eligible & (candidate.fault==0)[:,None]
            local=eligible.to(torch.int32).argmax(dim=1)
            enabled=eligible.any(dim=1)
            index=torch.arange(roots,device=local.device)*width+local
            sequence=_take(self.columns.sequences,index)
            balances=take_ratio(self.columns.balances,(index[:,None]*8+torch.arange(8,device=index.device)).flatten())
            owners=candidate.platform.asset_roots
            outcome_delta=self.columns.outcome_delta(index,owners)
            fault=candidate.fault | torch.where(enabled,relation.fault.reshape(roots,width).gather(1,local[:,None]).flatten() | self.columns.outcome_fault(index,owners),0)
            # 无效目录/旧fault不调用可提交的经济候选。
            platform,event=candidate.platform.apply_transition(sequence,balances,outcome_delta,
                enabled & (fault==0),torch.zeros_like(enabled),torch.ones_like(enabled))
            fault=fault | event.fault
            use=event.applied & (fault==0)
            # position等值目录而不是账户seq去重；同位置另一物理引用不应二次应用。
            key=self.columns.positions[index]
            matches=(self.columns.positions.reshape(roots,width,-1)==key[:,None,:]).all(dim=2)&self.columns.enabled
            accepted=matches & use[:,None]
            candidate=replace(candidate,platform=platform,applied=candidate.applied | accepted,fault=fault)
            pending=pending & ~accepted
        return candidate
