"""已编码单Base接单/逐fill最终性输出的NOE设备来源批装配。

SIM仍拥有撮合、账户和发射器；这里只组合原证据与独立经济事实，统一准备Core。
完整请求/闭时钟/控制登记/失败补偿和预留宿主继续接线，不借本类宣称整根完成。
"""
from dataclasses import dataclass,replace
import torch
from core import EvidenceBank,Normalizer,EVIDENCE,ECONOMIC
from trade import map_trade
from economic import map_economic
from lifecycle import map_base_accepted,map_order_accepted,map_taker_contribution
from source_order import map_source_ordinals


def finality_delta(frame,record,asset_roots):
    """实际应用与实际失败退款互斥；同一次最终性已有原子提交门。"""
    from account_state import AccountMovement
    from hidden_market import rational_select
    first,second=frame.event.economic_delta,frame.event.failure_delta
    use=first.emitted
    movement=AccountMovement(*(getattr(first.movement,name).select(use,getattr(second.movement,name))
        for name in ['cash','available','principal','fee']),
        first.movement.outcomes.select(use[asset_roots.clamp(0,use.numel()-1)],second.movement.outcomes),
        rational_select(first.movement.rebates,use.repeat_interleave(4),second.movement.rebates))
    fault=first.fault | second.fault | (first.emitted & second.emitted).to(torch.int32)
    delta=replace(first,sequence=first.sequence.select(use,second.sequence),movement=movement,
        emitted=(first.emitted | second.emitted) & (fault==0),fault=fault)
    tag=torch.where(second.emitted,torch.where(record.economic_applied,8,7),6).to(torch.int64)
    return delta,tag


@dataclass(frozen=True)
class ProjectionBatch:
    normalized: object
    private_sequences: object
    batch_sequences: object
    record_ordinals: torch.Tensor
    received_at: torch.Tensor
    rows: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor
    source_categories: torch.Tensor
    platform_at: torch.Tensor
    request_slots: torch.Tensor
    item_ordinals: torch.Tensor
    context_slots: torch.Tensor
    run_slots: torch.Tensor


@dataclass(frozen=True)
class Projection:
    emitter: object
    normalizer: object
    account_slots: torch.Tensor
    condition_slots: torch.Tensor
    token_slots: torch.Tensor
    asset_indices: torch.Tensor
    asset_roots: torch.Tensor
    order_sequence: object
    request_slots: torch.Tensor
    item_ordinals: torch.Tensor
    context_slots: torch.Tensor
    run_slots: torch.Tensor

    @classmethod
    def open(cls,emitter,account,condition,token,assets,asset_roots,order_sequence,request,item,context,run):
        for value in [account,condition,token,assets,request,item,context,run]:
            if value.shape!=emitter.fault.shape or value.dtype!=torch.int64 or value.device!=emitter.fault.device:
                raise TypeError('NOE批冷身份目录需同设备int64[roots]')
        if asset_roots.ndim!=1 or asset_roots.dtype!=torch.int64 or asset_roots.device!=emitter.fault.device:
            raise TypeError('NOE批资产目录需同设备int64[assets]')
        return cls(emitter,None,account,condition,token,assets,asset_roots,order_sequence,request,item,context,run)

    def _record(self,emitter,mapped,kind,private,batch,received):
        roots=self.account_slots.numel()
        size=roots
        zero=torch.zeros(roots,dtype=torch.int64,device=self.account_slots.device)
        incoming=EvidenceBank(mapped.logical_keys,mapped.economic_keys if kind==ECONOMIC else zero[:,None],
            mapped.semantic,zero[:,None],torch.full_like(zero,kind),mapped.source_order,zero,mapped.fault)
        normalizer=Normalizer.empty(incoming,roots) if self.normalizer is None else self.normalizer.append_bank(incoming)
        offset=normalizer.bank.kinds.numel()-size
        rows=(torch.arange(size,device=zero.device)+offset)[:,None]
        return replace(self,emitter=emitter,normalizer=normalizer),rows,mapped.emitted[:,None],private,batch,received

    def _finish(self,emitter,records,source_fault):
        from queues import ExactInt
        def cat_integer(values):
            width=max(value.words.size(1) for value in values)
            words=torch.stack([value.with_capacity(width).words for value in values],dim=1).flatten(0,1)
            signs=torch.stack([value.sign for value in values],dim=1).flatten()
            faults=torch.stack([value.fault for value in values],dim=1).flatten()
            return ExactInt(words,signs,faults)
        original=self
        working=self
        coordinates=[]
        active=[]
        private=[]
        batch=[]
        received=[]
        for mapped,kind,sequence,batch_sequence,received_at,category,platform_at,wanted in records:
            working,rows,enabled,_,_,_=working._record(emitter,mapped,kind,sequence,batch_sequence,received_at)
            coordinates.append(rows)
            active.append(enabled)
            private.append(sequence)
            batch.append(batch_sequence)
            received.append(received_at)
        rows=torch.cat(coordinates,dim=1)
        enabled=torch.cat(active,dim=1)
        # emitter错误或任何已实际生成证据错误不能静默少一条记录。
        error=source_fault.clone()
        for mapped,_,_,_,_,_,_,wanted in records:
            error=error | torch.where(wanted,mapped.fault,0)
        working=replace(working,normalizer=replace(working.normalizer,fault=working.normalizer.fault | error))
        candidate,normalized=working.normalizer.prepare(rows,enabled)
        order=normalized.rows-self.normalizer.bank.kinds.numel() if self.normalizer is not None else normalized.rows
        # append布局逐记录[root]；映射回本批原column，根序号不会参与排序。
        order=torch.div(order,self.account_slots.numel(),rounding_mode='floor')
        private_values=cat_integer(private)
        batch_values=cat_integer(batch)
        roots,width=rows.shape
        from queues import _take
        locations=(torch.arange(roots,device=rows.device)[:,None]*width+order).flatten()
        private_values=_take(private_values,locations)
        batch_values=_take(batch_values,locations)
        times=torch.stack(received,dim=1).gather(1,order)
        ordinals=ExactInt.from_i64_tensor((normalized.emitted.to(torch.int64).cumsum(dim=1)-1).clamp_min(0).flatten())
        fault=normalized.fault
        committed=fault==0
        # 各来源批原子准备；发射器和目录都只提交同一成功根，旧成功批保留。
        coordinated=replace(emitter,
            next_private_sequence=emitter.next_private_sequence.select(committed,original.emitter.next_private_sequence),
            next_batch_sequence=emitter.next_batch_sequence.select(committed,original.emitter.next_batch_sequence),fault=fault)
        categories=torch.stack([record[5] for record in records],dim=1).gather(1,order)
        platforms=torch.stack([record[6] for record in records],dim=1).gather(1,order)
        requests=torch.where(categories==0,self.request_slots[:,None],-1)
        items=torch.where(categories==0,self.item_ordinals[:,None],-1)
        return replace(working,emitter=coordinated,normalizer=candidate),ProjectionBatch(normalized,
            private_values,batch_values,ordinals,times,normalized.rows,normalized.emitted,fault,
            categories,platforms,requests,items,self.context_slots[:,None].expand_as(rows),self.run_slots[:,None].expand_as(rows))

    def acceptance(self,sampler,prepared,effects,pool,at,direct_received):
        """已真实接纳Base产生Order/逐fill/直接响应；其它结果须对应producer。"""
        from queues import _take,ExactRational
        from fill_pool import _take_record
        from asset_buy import take_ratio
        roots=self.account_slots.numel()
        width=len(effects.fills)+2
        active=torch.stack([effects.submission_delta.emitted,*[frame.event.emitted for frame in effects.fills],
            effects.submission_delta.emitted | effects.rejected],dim=1)
        direct=torch.zeros_like(active)
        direct[:,-1]=True
        platform=at[:,None].expand(roots,width)
        received=torch.zeros_like(platform)
        received[:,-1]=direct_received
        source_fault=effects.fault[:,None].expand(roots,width)
        emitter,emission=self.emitter.emit(sampler,active,direct,platform,platform,received,source_fault)
        categories=torch.tensor([1]+[2]*len(effects.fills)+[0],dtype=torch.int64,device=at.device).expand(roots,width)
        counts=torch.ones_like(platform)
        counts[:,0]=1+effects.submission_delta.emitted.to(torch.int64)
        for column,frame in enumerate(effects.fills,start=1):
            counts[:,column]=1+(frame.event.economic_delta.emitted | frame.event.failure_delta.emitted).to(torch.int64)
        ordinals=map_source_ordinals(emission.emitted,categories,counts,source_fault)
        def sequence(column):
            positions=torch.arange(roots,device=at.device)*width+column
            return _take(emission.private_sequences,positions),_take(emission.batch_sequences,positions),emission.received_at[:,column]
        private,batch,time=sequence(0)
        records=[]
        mapped=map_order_accepted(prepared.quantity,self.order_sequence,private,ordinals.first_ordinals[:,0],
            self.account_slots,self.condition_slots,self.token_slots,emission.emitted[:,0])
        records.append((mapped,EVIDENCE,private,batch,time,torch.ones_like(at),at,emission.emitted[:,0]))
        mapped=map_economic(effects.submission_delta,self.account_slots,self.condition_slots,self.token_slots,
            self.asset_indices,self.asset_roots,private,ordinals.first_ordinals[:,0]+1,at,
            torch.zeros_like(at),self.order_sequence,torch.zeros_like(at))
        records.append((mapped,ECONOMIC,private,batch,time,torch.ones_like(at),at,effects.submission_delta.emitted))
        zero=ExactRational.ratio(prepared.taker_bases.constant(0),prepared.taker_bases.constant(1))
        for column,frame in enumerate(effects.fills):
            private,batch,time=sequence(column+1)
            position=torch.arange(roots,device=at.device)*pool.capacity+frame.slot
            record=_take_record(pool.records,position)
            price=take_ratio(prepared.taker_prices,torch.arange(roots,device=at.device)*prepared.maximum_fills+column)
            maker_zero=take_ratio(zero,torch.arange(roots,device=at.device))
            mapped=map_trade(record,frame,self.order_sequence,price,self.account_slots,self.condition_slots,self.token_slots,
                private,ordinals.first_ordinals[:,column+1],liquidity_role=torch.zeros_like(at),
                maker_fee_equivalent=maker_zero,maker_fee_read_kind=torch.full_like(at,2))
            records.append((mapped,EVIDENCE,private,batch,time,torch.full_like(at,2),at,frame.event.emitted))
            mapped=map_economic(frame.event.economic_delta,self.account_slots,self.condition_slots,self.token_slots,
                self.asset_indices,self.asset_roots,private,ordinals.first_ordinals[:,column+1]+1,at,
                torch.full_like(at,6),frame.fill_sequence,frame.event.ordinal)
            records.append((mapped,ECONOMIC,private,batch,time,torch.full_like(at,2),at,frame.event.economic_delta.emitted))
        private,batch,time=sequence(width-1)
        mapped=map_base_accepted(prepared.quantity,self.order_sequence,private,ordinals.first_ordinals[:,-1],
            self.account_slots,self.condition_slots,self.token_slots,self.request_slots,self.item_ordinals,at,
            emission.emitted[:,-1],effects.submission_delta.emitted)
        records.append((mapped,EVIDENCE,private,batch,time,torch.zeros_like(at),at,emission.emitted[:,-1]))
        return self._finish(emitter,records,emission.root_fault | ordinals.fault)

    def finality(self,sampler,record,frame,price,rebate_rate,at):
        from queues import _take,ExactRational
        roots=self.account_slots.numel()
        active=torch.stack([frame.event.emitted,frame.event.rebate_delta.emitted],dim=1)
        platform=at[:,None].expand(roots,2)
        direct=torch.zeros_like(active)
        emitter,emission=self.emitter.emit(sampler,active,direct,platform,platform,platform,
            frame.event.fault[:,None].expand(roots,2))
        counts=torch.stack([1+(frame.event.economic_delta.emitted | frame.event.failure_delta.emitted).to(torch.int64),
            1+frame.event.rebate_delta.emitted.to(torch.int64)],dim=1)
        categories=torch.tensor([2,3],device=at.device).expand(roots,2)
        ordinals=map_source_ordinals(emission.emitted,categories,counts,frame.event.fault[:,None].expand(roots,2))
        def sequence(column):
            positions=torch.arange(roots,device=at.device)*2+column
            return _take(emission.private_sequences,positions),_take(emission.batch_sequences,positions),emission.received_at[:,column]
        private,batch,time=sequence(0)
        zero=ExactRational.ratio(record.base.constant(0),record.base.constant(1))
        mapped=map_trade(record,frame,self.order_sequence,price,self.account_slots,self.condition_slots,self.token_slots,
            private,ordinals.first_ordinals[:,0],liquidity_role=torch.zeros_like(at),maker_fee_equivalent=zero,
            maker_fee_read_kind=torch.full_like(at,2))
        records=[(mapped,EVIDENCE,private,batch,time,torch.full_like(at,2),at,frame.event.emitted)]
        delta,tag=finality_delta(frame,record,self.asset_roots)
        mapped=map_economic(delta,self.account_slots,self.condition_slots,self.token_slots,
            self.asset_indices,self.asset_roots,private,ordinals.first_ordinals[:,0]+1,at,
            tag,frame.fill_sequence,frame.event.ordinal)
        records.append((mapped,ECONOMIC,private,batch,time,torch.full_like(at,2),at,delta.emitted))
        private,batch,time=sequence(1)
        mapped=map_taker_contribution(record,frame,rebate_rate,private,ordinals.first_ordinals[:,1],
            self.account_slots,self.condition_slots)
        records.append((mapped,EVIDENCE,private,batch,time,torch.full_like(at,3),at,frame.event.rebate_delta.emitted))
        mapped=map_economic(frame.event.rebate_delta,self.account_slots,self.condition_slots,self.token_slots,
            self.asset_indices,self.asset_roots,private,ordinals.first_ordinals[:,1]+1,at,
            torch.full_like(at,10),frame.fill_sequence,torch.zeros_like(at))
        records.append((mapped,ECONOMIC,private,batch,time,torch.full_like(at,3),at,frame.event.rebate_delta.emitted))
        return self._finish(emitter,records,emission.root_fault | ordinals.fault)
