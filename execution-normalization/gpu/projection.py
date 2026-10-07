"""已编码单Base接单/逐fill最终性输出的NOE设备来源批装配。

SIM仍拥有撮合、账户和发射器；这里只组合原证据与独立经济事实，统一准备Core。
完整请求/闭时钟/控制登记/失败补偿和预留宿主继续接线，不借本类宣称整根完成。
"""
from dataclasses import dataclass,replace
import torch
from queues import ExactInt
from core import EvidenceBank,Normalizer,EVIDENCE,ECONOMIC,OUT_FACT
from trade import map_trade
from economic import map_economic
from lifecycle import (map_base_accepted,map_order_accepted,map_taker_contribution,
    map_cancellation,map_cancellation_completed,map_order_canceled)
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
    record_ordinals: ExactInt
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
    economic_sequences: object
    economic_balances: object
    economic_outcomes: object
    economic_assets: torch.Tensor
    economic_enabled: torch.Tensor
    economic_outcome_list: object = None  # 多资产经济原成员；旧单资产列保无损兼容视图。
    cancellation_method: torch.Tensor | None = None  # 原直接响应method冷字典坐标，非反馈为-1。



@dataclass(frozen=True)
class CancelTargetFeedback:
    order_sequence: object
    target_ordinal: object
    input_item_refs: object
    outcome: torch.Tensor  # Canceled/NotCanceledNoEffect/EvidenceUnknownNoEffect。
    active: torch.Tensor
    delta: object  # 原SIM实际释放delta；Control本身不重复释放资金。
    fault: torch.Tensor


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
    resolution_columns: object = None  # 原canonical/完整Terminal共享字典，记录与冻结引用存活时保持绑定。

    @classmethod
    def open(cls,emitter,account,condition,token,assets,asset_roots,order_sequence,request,item,context,run):
        for value in [account,condition,token,assets,request,item,context,run]:
            if value.shape!=emitter.fault.shape or value.dtype!=torch.int64 or value.device!=emitter.fault.device:
                raise TypeError('NOE批冷身份目录需同设备int64[roots]')
        if asset_roots.ndim!=1 or asset_roots.dtype!=torch.int64 or asset_roots.device!=emitter.fault.device:
            raise TypeError('NOE批资产目录需同设备int64[assets]')
        return cls(emitter,None,account,condition,token,assets,asset_roots,order_sequence,request,item,context,run)

    def bind_base_order(self,order,asset_tokens,enabled):
        """只替换本次来源绑定；Emitter/Core及其此前成功批保持同一连续状态。

        order来自SIM当前BaseOrders读取，asset_tokens是原资产→token不可变目录。
        物理order slot不充当NOE身份；完整order u64和原request/item均从实际目录取。
        """
        roots=self.account_slots.numel()
        if enabled.shape!=(roots,) or enabled.dtype!=torch.bool or enabled.device!=self.account_slots.device:
            raise TypeError('实际反馈人口需同设备bool[roots]')
        if asset_tokens.shape!=self.asset_roots.shape or asset_tokens.dtype!=torch.int64 or asset_tokens.device!=enabled.device:
            raise TypeError('原token目录需同设备int64[assets]')
        if order.conditions is None:
            raise ValueError('动态Base反馈须保留实际市场绑定')
        if asset_tokens.numel()==0:
            token=torch.full_like(order.assets,-1)
            invalid=enabled
        else:
            safe=order.assets.clamp(0,asset_tokens.numel()-1)
            token=asset_tokens[safe]
            invalid=(order.assets<0)|(order.assets>=asset_tokens.numel())|(token<0)|(
                self.asset_roots[safe]!=torch.arange(roots,device=enabled.device))
        error=order.sequence.fault | order.quantity.fault | order.price.fault
        error=error | (~order.present | invalid | (order.conditions<0) |
            (order.request_keys<0) | (order.item_ordinals<0) | (order.item_ordinals>65535)).to(torch.int32)
        fault=self.emitter.fault | order.fault | torch.where(enabled,error,0)
        return replace(self,emitter=replace(self.emitter,fault=fault),
            condition_slots=order.conditions,token_slots=token,asset_indices=order.assets,
            order_sequence=order.sequence,request_slots=order.request_keys,item_ordinals=order.item_ordinals)

    def market_resolution(self,sampler,effect,condition,columns,asset_tokens):
        """整市场原订单字符串序关闭反馈，最后首次Resolved；共同失败则整批不发。"""
        from lifecycle import map_market_resolved_order,map_resolved_position
        from economic import map_economic
        from queues import _take
        roots=self.account_slots.numel()
        width=len(effect.orders.events)+1
        if self.resolution_columns is not None and self.resolution_columns is not columns:
            raise ValueError('判定关闭反馈须承接同一完整来源身份字典')
        working=replace(self,resolution_columns=columns,condition_slots=condition)
        lives=[event.effect.closed for event in effect.orders.events]
        active=torch.stack([*lives,effect.resolved],1)
        platforms=effect.at[:,None].expand(roots,width)
        emitter,emission=working.emitter.emit(sampler,active,torch.zeros_like(active),platforms,platforms,
            torch.zeros_like(platforms),effect.fault[:,None].expand_as(active))
        categories=torch.tensor([1]*(width-1)+[3],device=effect.at.device).expand(roots,width)
        counts=torch.stack([*(1+event.effect.delta.emitted.to(torch.int64) for event in effect.orders.events),
            torch.ones_like(effect.at)],1)
        ordinals=map_source_ordinals(emission.emitted,categories,counts,effect.fault[:,None].expand_as(active))
        row=columns.fields[effect.reference.clamp(0,columns.fields.size(0)-1)]
        records=[]
        root=torch.arange(roots,device=effect.at.device)
        for column,event in enumerate(effect.orders.events):
            index=root*width+column
            private=_take(emission.private_sequences,index)
            batch=_take(emission.batch_sequences,index)
            working=working.bind_base_order(event.order,asset_tokens,emission.emitted[:,column])
            life=map_market_resolved_order(event.order.sequence,row[:,3],private,ordinals.first_ordinals[:,column],
                working.account_slots,event.order.conditions,working.token_slots,emission.emitted[:,column])
            economic=map_economic(event.effect.delta,working.account_slots,event.order.conditions,working.token_slots,
                event.order.assets,working.asset_roots,private,ordinals.first_ordinals[:,column]+1,effect.at,
                torch.full_like(effect.at,5),event.order.sequence,torch.zeros_like(effect.at),resolution_identity=row[:,3])
            common=(private,batch,emission.received_at[:,column],categories[:,column],effect.at)
            records.append((life,EVIDENCE,*common,emission.emitted[:,column]))
            records.append((economic,ECONOMIC,*common,emission.emitted[:,column]&event.effect.delta.emitted))
        private=_take(emission.private_sequences,root*width+width-1)
        batch=_take(emission.batch_sequences,root*width+width-1)
        resolved=map_resolved_position(working.account_slots,condition,row[:,3],row[:,2],private,
            ordinals.first_ordinals[:,-1],emission.emitted[:,-1])
        records.append((resolved,EVIDENCE,private,batch,emission.received_at[:,-1],categories[:,-1],
            effect.at,emission.emitted[:,-1]))
        return working._finish(emitter,records,emission.root_fault | ordinals.fault)

    def settlement_requests(self,sampler,condition,resolution,policy,outcome,observation,at,active,fault):
        """同次原有序SIM请求观察→独立Settlement反馈；无账户delta。"""
        from lifecycle import map_settlement_request
        emitter,emission=self.emitter.emit(sampler,active,torch.zeros_like(active),
            at,at,torch.zeros_like(at),fault[:,None].expand_as(active))
        from queues import _take
        roots,width=active.shape
        root=torch.arange(roots,device=at.device)
        categories=torch.full_like(at,3)
        ordinals=map_source_ordinals(emission.emitted,categories,torch.ones_like(at),fault[:,None].expand_as(active))
        records=[]
        for column in range(width):
            private=_take(emission.private_sequences,root*width+column)
            batch=_take(emission.batch_sequences,root*width+column)
            record=map_settlement_request(self.account_slots,condition[:,column],resolution[:,column],policy[:,column],
                outcome[:,column],observation[:,column],private,ordinals.first_ordinals[:,column],emission.emitted[:,column])
            records.append((record,EVIDENCE,private,batch,emission.received_at[:,column],
                categories[:,column],at[:,column],emission.emitted[:,column]))
        return self._finish(emitter,records,emission.root_fault | ordinals.fault)

    def pending_base_acceptance(self,sampler,orders,pool,effects,asset_tokens):
        """已完成的单项Base接单来源；完整Pair响应仍须原请求封口协调。

        当前入口只映射本次接受的订单/成交。未接受项须由独立原拒绝producer装配。
        """
        order=orders.read(effects.order.slots)
        bound=self.bind_base_order(order,asset_tokens,effects.order.emitted)
        return bound.acceptance(sampler,effects.prepared,effects.acceptance,pool,
            order.accepted_at,effects.response.received_at,direct_completed=effects.response.emitted)

    def fill_from_origin(self,sampler,orders,pool,frame,asset_tokens,calendars,rebate_rate,at,*,maker_results=()):
        """首Maker反馈或后续唯一最终性义务均取同一实际fill来源，不读取冷结果正文。"""
        from queues import _take
        from fill_pool import _take_record
        from asset_buy import take_ratio
        root=torch.arange(pool.roots,device=at.device)
        safe=frame.slot.clamp(0,pool.capacity-1)
        index=root*pool.capacity+safe
        origin=pool.origins.take(index)
        order=orders.read(origin.order_slots)
        enabled=frame.event.emitted
        bound=self.bind_base_order(order,asset_tokens,enabled)
        same_order=origin.order_sequences.compare(order.sequence)
        same_fill=_take(pool.sequences,index).compare(frame.fill_sequence)
        error=origin.order_sequences.fault | origin.prices.fault | same_order.fault | same_fill.fault
        error=error | ((frame.slot<0)|(frame.slot>=pool.capacity)|~pool.occupied[root,safe]|
            (same_order.order!=0)|(same_fill.order!=0)|(origin.conditions!=order.conditions)|
            (origin.roles<0)|(origin.roles>1)).to(torch.int32)
        maker_index,found,lookup_fault=calendars.maker._lookup(frame.fill_sequence)
        maker=origin.roles==1
        equivalent=take_ratio(calendars.maker.fee_equivalent,
            root*calendars.maker.fill_occupied.size(1)+maker_index)
        error=error | torch.where(maker,lookup_fault | (~found).to(torch.int32) | equivalent.fault,0)
        fault=bound.emitter.fault | pool.fault | torch.where(enabled,error,0)
        bound=replace(bound,emitter=replace(bound.emitter,fault=fault))
        return bound.finality(sampler,_take_record(pool.records,index),frame,origin.prices,rebate_rate,at,
            maker_results=maker_results,liquidity_role=origin.roles,maker_fee_equivalent=equivalent)

    def settlement_success(self,sampler,conditions,resolutions,policy,assets,tokens,delta,at,emitted,fault,*,observation=None):
        """一条实际成功Request及可选多资产经济，原同private/source顺序。"""
        from lifecycle import map_settlement_request
        from economic import map_resolution_redemption
        roots=emitted.numel()
        active=emitted[:,None]
        counts=1+delta.emitted.to(torch.int64)
        categories=torch.full_like(active,3,dtype=torch.int64)
        ordinals=map_source_ordinals(active,categories,counts[:,None],fault[:,None])
        emitter,frame=self.emitter.emit(sampler,active,torch.zeros_like(active),at[:,None],at[:,None],
            torch.zeros_like(at[:,None]),fault[:,None])
        private=frame.private_sequences
        batch=frame.batch_sequences
        received=frame.received_at[:,0]
        first=ordinals.first_ordinals[:,0]
        observation=torch.full_like(conditions,9) if observation is None else observation
        request=map_settlement_request(self.account_slots,conditions,resolutions,policy,
            torch.where(observation==5,2,torch.where(observation==9,2,1)),observation,private,first,frame.emitted[:,0])
        economic=map_resolution_redemption(delta,self.account_slots,conditions,resolutions,policy,assets,tokens,
            self.asset_roots,private,first+1,at)
        records=[(request,EVIDENCE,private,batch,received,torch.full_like(conditions,3),at,emitted),
            (economic,ECONOMIC,private,batch,received,torch.full_like(conditions,3),at,delta.emitted)]
        return self._finish(emitter,records,fault | frame.root_fault | ordinals.fault)

    def _record_bank(self,mapped,kind):
        roots=self.account_slots.numel()
        zero=torch.zeros(roots,dtype=torch.int64,device=self.account_slots.device)
        schema=getattr(mapped,'payload_schema',11 if kind==ECONOMIC else 8)
        incoming=EvidenceBank(mapped.logical_keys,mapped.economic_keys if kind==ECONOMIC else zero[:,None],
            mapped.semantic,getattr(mapped,'cancellation_fields',None) if getattr(mapped,'cancellation_fields',None) is not None else zero[:,None],torch.full_like(zero,kind),mapped.source_order,zero,mapped.fault,
            torch.full_like(zero,schema),frozenset([schema]))
        return incoming

    def settlement_cost(self,sampler,condition,resolution,policy,identity,recognized,paid,has_correction,correction,
            economic_kind,sequence,balances,outcomes,transported_outcomes,settlement_liability,taker_liability,at,active,fault,*,economic_present,source_ordinal,record_ordinal):
        """实际Cost反馈及完整V2 delta，同private/time，原ordinal递增。"""
        from settlement_cash import map_cost
        return self._settlement_cash(sampler,condition,resolution,policy,identity,economic_kind,sequence,
            balances,outcomes,transported_outcomes,settlement_liability,taker_liability,economic_present,source_ordinal,record_ordinal,at,active,fault,
            lambda private,ordinal,emitted:map_cost(self.account_slots,condition,resolution,policy,identity,
                recognized,paid,has_correction,correction,private,ordinal,emitted))

    def external_cash_feedback(self,sampler,identity,amount,attribution,has_correction,correction,
            sequence,balances,outcomes,transported_outcomes,settlement_liability,taker_liability,at,active,fault,*,economic_present,source_ordinal,record_ordinal):
        from settlement_cash import map_external_cash
        absent=torch.full_like(identity,-1)
        return self._settlement_cash(sampler,absent,absent,absent,identity,torch.full_like(identity,2),sequence,
            balances,outcomes,transported_outcomes,settlement_liability,taker_liability,economic_present,source_ordinal,record_ordinal,at,active,fault,
            lambda private,ordinal,emitted:map_external_cash(self.account_slots,identity,amount,attribution,
                has_correction,correction,private,ordinal,emitted))

    def _settlement_cash(self,sampler,condition,resolution,policy,identity,kind,sequence,
            balances,outcomes,transported_outcomes,settlement_liability,taker_liability,economic_present,source_ordinal,record_ordinal,at,active,fault,make_lifecycle):
        from settlement_cash import map_settlement_cash_economic
        if (economic_present.shape!=active.shape or economic_present.dtype!=torch.bool
                or economic_present.device!=active.device):
            raise TypeError('Economic存在位须来自原Option(delta)，为同设备bool[root]')
        if (source_ordinal.shape!=active.shape or source_ordinal.dtype!=torch.int64
                or source_ordinal.device!=active.device or not isinstance(record_ordinal,ExactInt)
                or record_ordinal.sign.shape!=active.shape or record_ordinal.words.device!=active.device):
            raise TypeError('反馈片段需原自然批source ordinal及精确u64 record ordinal起点')
        # 原自然批可能先有Request或其它反馈；两种坐标分别沿其实际来源传入。
        upper=record_ordinal.compare(record_ordinal.constant((1<<64)-1))
        last=record_ordinal+ExactInt.from_i64_tensor(economic_present.to(torch.int64))
        last_upper=last.compare(last.constant((1<<64)-1))
        coordinate_fault=upper.fault|last_upper.fault|record_ordinal.fault|last.fault
        coordinate_fault|=((record_ordinal.sign<0)|(upper.order>0)|(last_upper.order>0)|
            (source_ordinal<0)|(source_ordinal>0xffffffff)|
            (source_ordinal+economic_present.to(torch.int64)>0xffffffff)).to(torch.int32)
        fault=fault|torch.where(active,coordinate_fault,0)
        categories=torch.full_like(active[:,None],3,dtype=torch.int64)
        emitter,frame=self.emitter.emit(sampler,active[:,None],torch.zeros_like(active[:,None]),
            at[:,None],at[:,None],torch.zeros_like(at[:,None]),fault[:,None])
        ordinals=map_source_ordinals(frame.emitted,categories,(1+economic_present.to(torch.int64))[:,None],fault[:,None])
        private,batch=frame.private_sequences,frame.batch_sequences
        first=source_ordinal+ordinals.first_ordinals[:,0]
        life=make_lifecycle(private,first,frame.emitted[:,0])
        economic=map_settlement_cash_economic(self.account_slots,condition,at,kind,resolution,policy,identity,
            sequence,balances,outcomes,transported_outcomes,settlement_liability,taker_liability,
            private,first+1,frame.emitted[:,0]&economic_present)
        common=(private,batch,frame.received_at[:,0],categories[:,0],at)
        return self._finish(emitter,[(life,EVIDENCE,*common,active),
            (economic,ECONOMIC,*common,active&economic_present)],fault | frame.root_fault | ordinals.fault |
                torch.where(active&economic_present,economic.fault,0),record_ordinal_base=record_ordinal)

    def _finish(self,emitter,records,source_fault,*,record_ordinal_base=None):
        from queues import ExactInt,ExactRational
        def cat_integer(values):
            width=max(value.words.size(1) for value in values)
            words=torch.stack([value.with_capacity(width).words for value in values],dim=1).flatten(0,1)
            signs=torch.stack([value.sign for value in values],dim=1).flatten()
            faults=torch.stack([value.fault for value in values],dim=1).flatten()
            return ExactInt(words,signs,faults)
        def cat_ratio(values):
            return ExactRational(cat_integer([value.numerator for value in values]),
                cat_integer([value.denominator for value in values]))
        original=self
        banks=[]
        active=[]
        private=[]
        batch=[]
        received=[]
        for mapped,kind,sequence,batch_sequence,received_at,category,platform_at,wanted in records:
            banks.append(self._record_bank(mapped,kind))
            active.append(mapped.emitted)
            private.append(sequence)
            batch.append(batch_sequence)
            received.append(received_at)
        incoming=EvidenceBank.concatenate(banks)
        roots=self.account_slots.numel()
        normalizer=Normalizer.empty(incoming,roots) if self.normalizer is None else self.normalizer.append_bank(incoming)
        offset=normalizer.bank.kinds.numel()-incoming.kinds.numel()
        rows=offset+torch.arange(roots,device=self.account_slots.device)[:,None]+roots*torch.arange(len(records),device=self.account_slots.device)[None,:]
        enabled=torch.stack(active,dim=1)
        working=replace(self,emitter=emitter,normalizer=normalizer)
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
        if record_ordinal_base is not None:
            ordinals=ordinals+_take(record_ordinal_base,torch.arange(roots,device=rows.device).repeat_interleave(width))
        # 共享原经济列随规范序移动，Ledger不从semantic token反解或读回CPU。
        economic_sequences=[]
        economic_balances=[]
        economic_outcomes=[]
        economic_masks=[]
        economic_assets=[]
        for mapped,kind,private_sequence,_,_,_,_,_ in records:
            zero=ExactRational.ratio(private_sequence.constant(0),private_sequence.constant(1))
            economic_sequences.append(mapped.sequence if kind==ECONOMIC else private_sequence.constant(0))
            economic_balances.append(mapped.balances if kind==ECONOMIC else (zero,)*8)
            economic_outcomes.append(mapped.outcome if kind==ECONOMIC else zero)
            economic_masks.append(torch.full_like(self.account_slots,kind==ECONOMIC,dtype=torch.bool))
            asset=getattr(mapped,'asset_indices',None)
            economic_assets.append(self.asset_indices if asset is None else asset)
        balance_fields=[cat_ratio([record[field] for record in economic_balances]) for field in range(8)]
        # 先[root,record]后字段8；规范row位置可直接gather共享精确列。
        balance_width=max(value.numerator.words.size(1) for value in balance_fields)
        denominator_width=max(value.denominator.words.size(1) for value in balance_fields)
        def fields_integer(values,width):
            return ExactInt(torch.stack([value.with_capacity(width).words for value in values],dim=1).flatten(0,1),
                torch.stack([value.sign for value in values],dim=1).flatten(),
                torch.stack([value.fault for value in values],dim=1).flatten())
        packed_balance=ExactRational(fields_integer([value.numerator for value in balance_fields],balance_width),
            fields_integer([value.denominator for value in balance_fields],denominator_width))
        def take_ratio(value,index):
            return ExactRational(_take(value.numerator,index),_take(value.denominator,index))
        from fields import RecordList
        from transport import append_record_lists,take_record_list
        vectors=None
        for col,(mapped,kind,*_) in enumerate(records):
            value=getattr(mapped,'outcome_list',None) if kind==ECONOMIC else None
            if value is None:
                value=RecordList((economic_assets[col],economic_outcomes[col]),
                    (economic_masks[col] & (economic_outcomes[col].numerator.sign!=0))[:,None])
            vectors=value if vectors is None else append_record_lists(vectors,value,roots,col,1)
        vectors=take_record_list(vectors,locations)
        economic_sequence=_take(cat_integer(economic_sequences),locations)
        economic_balance=take_ratio(packed_balance,(locations[:,None]*8+torch.arange(8,device=rows.device)).flatten())
        economic_outcome=take_ratio(cat_ratio(economic_outcomes),locations)
        economic_mask=torch.stack(economic_masks,dim=1).gather(1,order)
        economic_mask=economic_mask & normalized.emitted & (normalized.outcomes==OUT_FACT)
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
            categories,platforms,requests,items,self.context_slots[:,None].expand_as(rows),self.run_slots[:,None].expand_as(rows),
            economic_sequence,economic_balance,economic_outcome,torch.stack(economic_assets,1).gather(1,order),economic_mask,vectors)

    def cancellation(self,sampler,targets,at,direct_received,method,completed):
        """已处理完整单资产请求的真实取消效果，保原双重顺序。

        SIM完成所有target之后提供原冻结人口，包含NoEffect。private按各正取消
        CallerCanceled→最终Direct分配；mapper source ordinal按全部Target→Completed
        →各CallerCanceled/Economic分配。未完成请求须在SIM保留，不能提前调用。
        """
        from queues import _take,ExactInt
        roots=self.account_slots.numel()
        width=len(targets)+1
        zero=torch.zeros_like(at)
        for value in [at,direct_received,method]:
            if value.shape!=(roots,) or value.dtype!=torch.int64 or value.device!=self.account_slots.device:
                raise TypeError('取消完成时点/method需同设备int64[roots]')
        if completed.shape!=(roots,) or completed.dtype!=torch.bool or completed.device!=at.device:
            raise TypeError('完整取消请求完成需同设备bool[roots]')
        lives=[target.active&(target.outcome==0) for target in targets]
        active=torch.stack([*lives,completed],1)
        error=torch.zeros_like(at,dtype=torch.int32)
        for target in targets:error=error|torch.where(target.active,target.fault|target.delta.fault,0)
        error=error|((method<0)&completed).to(torch.int32)
        direct=torch.zeros_like(active);direct[:,-1]=True
        platforms=at[:,None].expand(roots,width)
        received=torch.zeros_like(platforms);received[:,-1]=direct_received
        emitter,emission=self.emitter.emit(sampler,active,direct,platforms,platforms,received,error[:,None].expand_as(active))
        categories=torch.tensor([1]*len(targets)+[0],device=at.device).expand(roots,width)
        direct_count=sum((target.active.to(torch.int64) for target in targets),torch.zeros_like(at))+1
        counts=torch.stack([*[1+target.delta.emitted.to(torch.int64) for target in targets],direct_count],1)
        ordinals=map_source_ordinals(emission.emitted,categories,counts,error[:,None].expand_as(active))
        def sequence(column):
            positions=torch.arange(roots,device=at.device)*width+column
            return _take(emission.private_sequences,positions),_take(emission.batch_sequences,positions),emission.received_at[:,column]
        private,batch,time=sequence(width-1)
        records=[]
        source_ordinal=ordinals.first_ordinals[:,-1]
        for target in targets:
            wanted=target.active&emission.emitted[:,-1]
            mapped=map_cancellation(target.order_sequence,target.target_ordinal,target.input_item_refs,target.outcome,
                private,source_ordinal,self.account_slots,self.request_slots,wanted)
            records.append((mapped,EVIDENCE,private,batch,time,zero,at,wanted))
            source_ordinal=source_ordinal+target.active.to(torch.int64)
        total=lambda outcome:ExactInt.from_i64_tensor(sum(((target.active&(target.outcome==outcome)).to(torch.int64)
            for target in targets),torch.zeros_like(at)))
        mapped=map_cancellation_completed(total(0),total(1),total(2),private,source_ordinal,
            self.account_slots,self.request_slots,emission.emitted[:,-1])
        records.append((mapped,EVIDENCE,private,batch,time,zero,at,emission.emitted[:,-1]))
        for column,target in enumerate(targets):
            private,batch,time=sequence(column)
            mapped=map_order_canceled(target.order_sequence,private,ordinals.first_ordinals[:,column],
                self.account_slots,self.condition_slots,self.token_slots,emission.emitted[:,column])
            records.append((mapped,EVIDENCE,private,batch,time,torch.ones_like(at),at,emission.emitted[:,column]))
            mapped=map_economic(target.delta,self.account_slots,self.condition_slots,self.token_slots,
                self.asset_indices,self.asset_roots,private,ordinals.first_ordinals[:,column]+1,at,
                torch.full_like(at,2),target.order_sequence,zero)
            records.append((mapped,ECONOMIC,private,batch,time,torch.ones_like(at),at,target.delta.emitted&emission.emitted[:,column]))
        candidate,batch=self._finish(emitter,records,emission.root_fault|ordinals.fault)
        return candidate,replace(batch,cancellation_method=torch.where(batch.source_categories==0,
            method[:,None],torch.full_like(batch.source_categories,-1)))

    def acceptance(self,sampler,prepared,effects,pool,at,direct_received,*,direct_completed=None):
        """已真实接纳Base产生Order/逐fill/直接响应；其它结果须对应producer。"""
        from queues import _take,ExactRational
        from fill_pool import _take_record
        from asset_buy import take_ratio
        roots=self.account_slots.numel()
        width=len(effects.fills)+2
        direct_active=effects.submission_delta.emitted | effects.rejected
        if direct_completed is not None:
            direct_active=direct_active & direct_completed
        active=torch.stack([effects.submission_delta.emitted,*[frame.event.emitted for frame in effects.fills],
            direct_active],dim=1)
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

    def finality(self,sampler,record,frame,price,rebate_rate,at,*,finalized=(),payments=(),kind=None,
                 maker_results=(),liquidity_role=None,maker_fee_equivalent=None):
        from queues import _take,ExactRational
        roots=self.account_slots.numel()
        population=tuple((result,False) for result in finalized)+tuple((result,True) for result in payments)
        width=2+len(maker_results)+len(population)
        active=torch.stack([frame.event.emitted,frame.event.rebate_delta.emitted,
            *[result.emitted for result in maker_results],
            *[result.emitted for result,_ in population]],dim=1)
        source_fault=torch.stack([frame.event.fault,frame.event.fault,
            *[result.fault for result in maker_results],*[result.fault for result,_ in population]],dim=1)
        platform=at[:,None].expand(roots,width)
        direct=torch.zeros_like(active)
        emitter,emission=self.emitter.emit(sampler,active,direct,platform,platform,platform,source_fault)
        counts=torch.stack([1+(frame.event.economic_delta.emitted | frame.event.failure_delta.emitted).to(torch.int64),
            1+frame.event.rebate_delta.emitted.to(torch.int64),
            *[1+result.account_delta.emitted.to(torch.int64) for result in maker_results],
            *[1+result.account_delta.emitted.to(torch.int64) for result,_ in population]],dim=1)
        categories=torch.tensor([2,3]+[3]*(len(maker_results)+len(population)),device=at.device).expand(roots,width)
        ordinals=map_source_ordinals(emission.emitted,categories,counts,source_fault)
        def sequence(column):
            positions=torch.arange(roots,device=at.device)*width+column
            return _take(emission.private_sequences,positions),_take(emission.batch_sequences,positions),emission.received_at[:,column]
        private,batch,time=sequence(0)
        zero=ExactRational.ratio(record.base.constant(0),record.base.constant(1))
        role=torch.zeros_like(at) if liquidity_role is None else liquidity_role
        equivalent=zero if maker_fee_equivalent is None else maker_fee_equivalent
        read_kind=torch.where(role==0,2,1 if maker_fee_equivalent is None else 0)
        mapped=map_trade(record,frame,self.order_sequence,price,self.account_slots,self.condition_slots,self.token_slots,
            private,ordinals.first_ordinals[:,0],liquidity_role=role,maker_fee_equivalent=equivalent,
            maker_fee_read_kind=read_kind)
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
        for column,result in enumerate(maker_results,start=2):
            private,batch,time=sequence(column)
            records.extend(self._maker_records(result,at,private,batch,time,ordinals.first_ordinals[:,column]))
        for column,(result,is_payment) in enumerate(population,start=2+len(maker_results)):
            private,batch,time=sequence(column)
            records.extend(self._rebate_records(result,is_payment,kind,at,private,batch,time,ordinals.first_ordinals[:,column]))
        return self._finish(emitter,records,emission.root_fault | ordinals.fault)

    def _maker_records(self,result,at,private,batch,received,ordinal):
        from fields import RecordList
        from lifecycle import map_maker_finalized
        roots,capacity=result.member_mask.shape
        contributions=RecordList((result.fill_sequences,result.condition.repeat_interleave(capacity),
            torch.zeros(roots*capacity,dtype=torch.int64,device=at.device),result.days.flatten(),result.contributions),
            result.member_mask)
        mapped=map_maker_finalized(result.condition,result.status,contributions,private,ordinal,self.account_slots,result.emitted)
        records=[(mapped,EVIDENCE,private,batch,received,torch.full_like(at,3),at,result.emitted)]
        mapped=map_economic(result.account_delta,self.account_slots,result.condition,self.token_slots,
            self.asset_indices,self.asset_roots,private,ordinal+1,at,torch.full_like(at,9),private.constant(0),torch.zeros_like(at))
        records.append((mapped,ECONOMIC,private,batch,received,torch.full_like(at,3),at,result.account_delta.emitted))
        return records

    def _rebate_records(self,result,is_payment,kind,at,private,batch,received,ordinal):
        from fields import IntegerList
        from lifecycle import map_day_finalized,map_rebate_payment
        from economic import map_rebate_economic
        if kind is None:
            raise TypeError('实际日结果必须绑定原Maker/Taker种类')
        if is_payment:
            mapped=map_rebate_payment(kind,result.day,result.payable,private,ordinal,self.account_slots,result.emitted)
        else:
            refs=IntegerList(result.contribution_refs,result.reference_mask)
            mapped=map_day_finalized(kind,result.day,result.accrued,result.payable,result.locked,
                refs,private,ordinal,self.account_slots,result.emitted)
        records=[(mapped,EVIDENCE,private,batch,received,torch.full_like(at,3),at,result.emitted)]
        mapped=map_rebate_economic(result.account_delta,self.account_slots,self.asset_roots,private,ordinal+1,
            at,kind,result.day,torch.full_like(result.emitted,is_payment))
        records.append((mapped,ECONOMIC,private,batch,received,torch.full_like(at,3),at,result.account_delta.emitted))
        return records

    def rebate_days(self,sampler,finalized,payments,kind,at):
        """已真实消费的日结果先发完再付款；每反馈独立Evidence+可选Economic。"""
        from queues import _take
        roots=self.account_slots.numel()
        population=tuple((result,False) for result in finalized)+tuple((result,True) for result in payments)
        if not population:
            raise ValueError('日批需要物理候选人口；未发出行用emitted表示')
        width=len(population)
        active=torch.stack([result.emitted for result,_ in population],dim=1)
        fault=torch.stack([result.fault for result,_ in population],dim=1)
        times=at[:,None].expand(roots,width)
        emitter,emission=self.emitter.emit(sampler,active,torch.zeros_like(active),times,times,times,fault)
        counts=torch.stack([1+result.account_delta.emitted.to(torch.int64) for result,_ in population],dim=1)
        categories=torch.full_like(times,3)
        ordinals=map_source_ordinals(emission.emitted,categories,counts,fault)
        records=[]
        for column,(result,is_payment) in enumerate(population):
            index=torch.arange(roots,device=at.device)*width+column
            private=_take(emission.private_sequences,index)
            batch=_take(emission.batch_sequences,index)
            received=emission.received_at[:,column]
            ordinal=ordinals.first_ordinals[:,column]
            records.extend(self._rebate_records(result,is_payment,kind,at,private,batch,received,ordinal))
        return self._finish(emitter,records,emission.root_fault | ordinals.fault)

    def rebate_clock(self,sampler,frame,at):
        """原返佣协调候选完整一批：市场结果→全部Day→全部Payment。"""
        from queues import _take
        population=tuple((result,None,False) for result in frame.market_results)+tuple(
            (result,kind,False) for kind,result in frame.phases.finalized)+tuple(
            (result,kind,True) for kind,result in frame.phases.payments)
        roots=self.account_slots.numel()
        if not population:
            raise ValueError('时钟帧须有物理候选人口，未发出项保留emitted=false')
        width=len(population)
        active=torch.stack([result.emitted for result,_,_ in population],dim=1)
        fault=torch.stack([result.fault for result,_,_ in population],dim=1)
        times=at[:,None].expand(roots,width)
        emitter,emission=self.emitter.emit(sampler,active,torch.zeros_like(active),times,times,times,fault)
        counts=torch.stack([1+result.account_delta.emitted.to(torch.int64) for result,_,_ in population],dim=1)
        ordinals=map_source_ordinals(emission.emitted,torch.full_like(times,3),counts,fault)
        records=[]
        for column,(result,kind,is_payment) in enumerate(population):
            positions=torch.arange(roots,device=at.device)*width+column
            private=_take(emission.private_sequences,positions)
            batch=_take(emission.batch_sequences,positions)
            received=emission.received_at[:,column]
            if kind is None:
                records.extend(self._maker_records(result,at,private,batch,received,ordinals.first_ordinals[:,column]))
            else:
                records.extend(self._rebate_records(result,is_payment,torch.full_like(at,kind),
                    at,private,batch,received,ordinals.first_ordinals[:,column]))
        return self._finish(emitter,records,emission.root_fault | ordinals.fault | frame.fault)
