"""按完整公共组接纳的Native history16及窗口路径；纯计算后由协调器提交。"""
from dataclasses import dataclass
import json
import sqlite3
import tempfile
from pathlib import Path
from contextlib import contextmanager
import torch
from prepare import _numbers
from public_state import _take
from input_events import _extreme
from input_trades import _sum


def _join(values, dim, shapes):
    numbers = _numbers()
    width = max(value.words.shape[1] for value in values)
    return numbers.ExactInt(
        torch.cat([torch.nn.functional.pad(v.words,(0,width-v.words.shape[1])).reshape(*shape,width)
                   for v,shape in zip(values,shapes)],dim=dim).reshape(-1,width),
        torch.cat([v.sign.reshape(shape) for v,shape in zip(values,shapes)],dim=dim).reshape(-1),
        torch.cat([v.fault.reshape(shape) for v,shape in zip(values,shapes)],dim=dim).reshape(-1))


class SourceIdentityRegistry:
    """跨材料块复用的冷身份表；完整原DirectObservationAnchor向量决定等价性。"""
    def __init__(self, *, directory=None):
        # 运行所属临时目录；训练环境TMPDIR指向数据盘。不跨运行复用或恢复。
        self._directory = tempfile.TemporaryDirectory(prefix='source-identities-',dir=directory)
        self.path = Path(self._directory.name)/'identities.sqlite3'
        self._connection = self._connect()
        self._connection.execute('CREATE TABLE identities (id INTEGER PRIMARY KEY, key TEXT NOT NULL UNIQUE)')
        self.markets = {}

    def _connect(self):
        connection = sqlite3.connect(self.path,isolation_level=None,cached_statements=4)
        connection.execute('PRAGMA cache_size=-2048')
        return connection

    @contextmanager
    def binding(self):
        """当前块冷绑定的一个事务；不将键列表留到下一块。"""
        self._connection.execute('BEGIN')
        try:
            yield self
        except BaseException:
            self._connection.rollback()
            raise
        else:
            self._connection.commit()

    @property
    def identity_count(self):
        return self._connection.execute('SELECT count(*) FROM identities').fetchone()[0]

    def close(self):
        connection=getattr(self,'_connection',None)
        if connection is not None:
            connection.close();self._connection=None
        directory=getattr(self,'_directory',None)
        if directory is not None:directory.cleanup()

    def __del__(self):
        self.close()

    def market_id(self, market):
        key = json.dumps(market,sort_keys=True,separators=(',',':'))
        if key not in self.markets: self.markets[key] = len(self.markets)+1
        return self.markets[key]

    def register(self, anchors, members, path):
        identity = [(path,a['position'],a['group_id'],a['direct']['source_message_id'],
                     a['direct']['source_event_at_utc_ms'],a['direct']['received_at_utc_ms'])
                    for a in (anchors[index] for index in members)]
        if not identity: return 0
        key = json.dumps(identity,sort_keys=True,separators=(',',':'))
        self._connection.execute('INSERT INTO identities(key) VALUES(?) ON CONFLICT(key) DO NOTHING',(key,))
        return self._connection.execute('SELECT id FROM identities WHERE key=?',(key,)).fetchone()[0]


@dataclass(frozen=True)
class SourceHistoryMemory:
    values: object             # 精确(value*2e28)，[root,6,16]。
    identities: torch.Tensor   # 完整原身份向量的冷表坐标，不按值或时间去重。
    times: torch.Tensor        # 原实际交付cutoff，不是下一网格时刻。
    counts: torch.Tensor
    path_count: torch.Tensor
    path_first: object
    path_last: object
    path_min: object
    path_max: object
    path_squares: object
    last_prefix: torch.Tensor
    last_cutoff: torch.Tensor
    market_ids: torch.Tensor
    root_started_at: torch.Tensor

    def rows(self, indices):
        from input_100ms import _memory_rows
        return _memory_rows(self,indices,self.counts.size(0))

    def append(self, other):
        """物理新增空行；精确值按原字宽合并，不重建已有历史。"""
        from dataclasses import fields
        numbers=_numbers()
        def join(left,right):
            if isinstance(left,torch.Tensor):return torch.cat((left,right))
            width=max(left.words.size(1),right.words.size(1))
            return numbers.ExactInt(torch.cat((torch.nn.functional.pad(left.words,(0,width-left.words.size(1))),
                torch.nn.functional.pad(right.words,(0,width-right.words.size(1))))),
                torch.cat((left.sign,right.sign)),torch.cat((left.fault,right.fault)))
        return type(self)(**{f.name:join(getattr(self,f.name),getattr(other,f.name)) for f in fields(self)})


@dataclass(frozen=True)
class NativeSourceHistory:
    history: torch.Tensor      # [root,24]，含固定为空的Binance一路。
    path: torch.Tensor         # [root,30]，PM A/B、spot、TWAP30/60。
    memory: SourceHistoryMemory
    fault: torch.Tensor


class SourceHistoryInputs:
    def __init__(self, shared, market, references, columns, *, identity_registry=None):
        self.shared,self.market,self.references = shared,market,references
        self.registry = identity_registry if identity_registry is not None else SourceIdentityRegistry()
        device = shared.slots.device
        self.market_ids = torch.tensor([self.registry.market_id(market) for market in columns['markets']],
                                       dtype=torch.int64,device=device)
        with self.registry.binding():
            self.bba_identity = torch.tensor([self.registry.register(shared.anchors,columns['anchor_sets'][row[6]],
                'best_bid_ask') for row in columns['bba_values']],dtype=torch.int64,device=device)
            self.reference_identity = torch.tensor([self.registry.register(shared.anchors,columns['anchor_sets'][row[1]],
                ('chainlink_spot' if row[3]==0 else 'chainlink_twap_thirty' if row[3]==30 else 'chainlink_twap_sixty'))
                for row in columns['reference_values']],dtype=torch.int64,device=device)
        numbers = _numbers()
        def units():
            source = shared.decimal_columns
            factor = source.coefficient.constant(1)
            for exponent in range(1,29):
                factor = factor.constant(10**exponent).select(28-source.scale==exponent,factor)
            return source.coefficient*factor
        self.units = numbers.run_exact_batch(units)

    def empty(self, roots, started_at):
        device = self.shared.slots.device
        if started_at.shape!=(roots,) or started_at.dtype!=torch.int64 or started_at.device!=device:
            raise ValueError('根起点须同设备int64[root]')
        zeros = torch.zeros((roots,6),dtype=torch.int64,device=device)
        value = _numbers().ExactInt.from_i64_tensor(zeros)
        ring = _numbers().ExactInt.from_i64_tensor(torch.zeros((roots,6,16),dtype=torch.int64,device=device))
        return SourceHistoryMemory(ring,zeros[:,:,None].expand(-1,-1,16).clone(),
            zeros[:,:,None].expand(-1,-1,16).clone(),zeros,zeros,
            value,value,value,value,value,torch.full_like(started_at,-1),started_at.clone(),
            torch.zeros_like(started_at),started_at.clone())

    def consume(self, prefixes, delivered_cutoffs, market_indices, started_at, memory,*,applied_prefix=None,enabled=None):
        """一次批量接纳连续完整组；不得省略无字段变化的组。

        delivered_cutoffs由公共交付协调器给出，含实际delivery delay及跨块前沿。
        prefixes是共享块坐标；跨块前缀/资产坐标必须由装配器沿原身份迁移，身份表复用。
        本函数没有设备业务标量回传；任何fault时协调器不能提交候选记忆。
        """
        roots,groups = len(market_indices),len(prefixes)
        device = self.shared.slots.device
        for value,shape in ((prefixes,(groups,)),(delivered_cutoffs,(groups,)),
                            (market_indices,(roots,)),(started_at,(roots,))):
            if value.shape!=shape or value.dtype!=torch.int64 or value.device!=device:
                raise ValueError('需同设备int64完整组/根参数')
        if groups==0: return memory,torch.zeros(roots,dtype=torch.int32,device=device)
        if memory.counts.shape!=(roots,6): raise ValueError('根历史形状不一致')
        valid_market = (market_indices>=0) & (market_indices<self.market.market_count)
        if not self.market.market_count:
            return memory,torch.ones(roots,dtype=torch.int32,device=device)
        safe = market_indices.clamp(0,self.market.market_count-1)
        market_frame = self.market.frame(prefixes)
        bba = self.shared.observation(prefixes).bba
        _,bba_ids,_,_ = self.shared.bba.query(prefixes)
        _,reference_ids,_,_ = self.references.global_references.query(prefixes)
        reference = self.references.frame(prefixes).global_values
        assets = self.market.asset_pairs[safe]
        windows = self.market.times[safe]
        boundary = _numbers().ExactInt.from_i64_tensor(windows[:,0])
        # T-30差用精确整数，int64起止极值不在设备减法中溢出。
        threshold = boundary-boundary.constant(30000)
        eligible_root = _numbers().ExactInt.from_i64_tensor(started_at).compare(threshold)
        clocks = delivered_cutoffs[:,None].expand(-1,roots)
        time = _numbers().ExactInt.from_i64_tensor(clocks)
        start = _take(threshold,torch.arange(roots,device=device).repeat(groups))
        end = _numbers().ExactInt.from_i64_tensor(windows[:,1][None].expand(groups,-1))
        after_start,before_end = time.compare(start),time.compare(end)
        consecutive = ((prefixes>=0) & (prefixes<self.shared.group_count)).all()
        consecutive &= ((prefixes[1:]-prefixes[:-1])==1).all()
        monotonic = (delivered_cutoffs[1:]>=delivered_cutoffs[:-1]).all()
        requested=torch.ones((groups,roots),dtype=torch.bool,device=device)
        if applied_prefix is not None:
            requested=(prefixes[:,None]>memory.last_prefix[None])&(prefixes[:,None]<=applied_prefix[None])
        if enabled is not None:requested &= enabled[None]
        has_groups=requested.any(0)
        group_first=requested.to(torch.int64).argmax(0)
        group_last=torch.where(requested,torch.arange(groups,device=device)[:,None],-1).max(0).values.clamp_min(0)
        valid_root = valid_market & consecutive & monotonic & (prefixes[group_first]==memory.last_prefix+1)
        valid_root &= delivered_cutoffs[group_first]>=memory.last_cutoff
        valid_root &= (memory.market_ids==0) | (memory.market_ids==self.market_ids[safe])
        valid_root &= started_at==memory.root_started_at
        fault = torch.where(has_groups,(~valid_root).to(torch.int32) | eligible_root.fault,0)
        fault |= _numbers().combine_fault_rows((after_start.fault|before_end.fault).reshape(groups,roots).T)
        valid_root &= has_groups
        eligible = (requested & valid_root[None] & (eligible_root.order<=0)[None]
                    & (after_start.order.reshape(groups,roots)>=0) & (before_end.order.reshape(groups,roots)<0)
                    & market_frame.admitted[:,safe])
        numbers = _numbers()
        def compute():
            candidates,ids,presents,errors = [],[],[],[]
            for route in range(6):
                if route<2 and self.shared.assets:
                    asset = assets[:,route];valid_asset = asset>=0
                    field = bba.values[:,asset.clamp_min(0)]
                    selected = bba_ids[:,asset.clamp_min(0)]
                    identity = self.bba_identity[selected]
                    value = _take(self.units,field[:,:,1].reshape(-1))+_take(self.units,field[:,:,3].reshape(-1))
                    present = eligible & valid_asset[None] & field[:,:,0].bool() & field[:,:,2].bool() & (field[:,:,7]<=1)
                    error = bba.fault[:,asset.clamp_min(0)]
                elif route>=3:
                    field = reference.values[:,route-3]
                    identity = self.reference_identity[reference_ids[:,route-3]][:,None].expand(-1,roots)
                    value = _take(self.units,field[:,0,None].expand(-1,roots).reshape(-1))
                    value = value*value.constant(2)
                    present = eligible & reference.present[:,route-3,None] & (field[:,2,None]<=1)
                    error = reference.fault[:,route-3,None].expand(-1,roots)
                else:
                    identity = torch.zeros((groups,roots),dtype=torch.int64,device=device)
                    value = numbers.ExactInt.from_i64_tensor(identity)
                    present = torch.zeros_like(identity,dtype=torch.bool);error = identity.to(torch.int32)
                present &= identity>0
                order = torch.arange(groups*roots,device=device).reshape(groups,roots).T.reshape(-1)
                candidates.append(_take(value,order));ids.append(identity.T);presents.append(present.T)
                errors.append(torch.where(present,error,0).T)
            values = _join(candidates,1,[(roots,1,groups)]*6)
            identities = torch.stack(ids,dim=1);present = torch.stack(presents,dim=1)
            flat = roots*6
            index = torch.arange(groups,device=device)[None,None].expand(roots,6,-1)
            last_valid = torch.cummax(torch.where(present,index+1,0),dim=2).values
            previous_valid = torch.cat((last_valid.new_zeros((roots,6,1)),last_valid[:,:,:-1]),dim=2)
            last_index = (memory.counts-1).clamp_min(0)
            prior_identity = memory.identities.gather(2,last_index[:,:,None]).squeeze(2)
            identity_bank = torch.cat((prior_identity[:,:,None],identities),dim=2)
            accepted = present & (identities!=identity_bank.gather(2,previous_valid))
            count = accepted.sum(dim=2)
            order = torch.argsort((~accepted).to(torch.int8),dim=2,stable=True)
            base = torch.arange(flat,device=device).reshape(roots,6,1)*groups
            new_values = _take(values,(base+order).reshape(-1))
            new_times = delivered_cutoffs[None,None,:].expand(roots,6,-1).gather(2,order)
            new_ids = identities.gather(2,order)
            new_present = index<count[:,:,None]
            old_present = torch.arange(16,device=device)[None,None]<memory.counts[:,:,None]
            combined_present = torch.cat((old_present,new_present),dim=2)
            merge_order = torch.argsort((~combined_present).to(torch.int8),dim=2,stable=True)
            total = memory.counts+count
            keep = total.clamp_max(16)
            ring_position = (total-16).clamp_min(0)[:,:,None]+torch.arange(16,device=device)[None,None]
            ring_position = ring_position.clamp_max(groups+15)
            selected = merge_order.gather(2,ring_position)
            merged_values = _join([memory.values,new_values],2,[(roots,6,16),(roots,6,groups)])
            ring_values = _take(merged_values,(torch.arange(flat,device=device).reshape(roots,6,1)*(16+groups)+selected).reshape(-1))
            ring_ids = torch.cat((memory.identities,new_ids),dim=2).gather(2,selected)
            ring_times = torch.cat((memory.times,new_times),dim=2).gather(2,selected)
            active = torch.arange(16,device=device)[None,None]<keep[:,:,None]
            ring_values = ring_values.select(active.reshape(-1),ring_values.constant(0))
            ring_ids = torch.where(active,ring_ids,0);ring_times = torch.where(active,ring_times,0)
            first = _take(new_values,torch.arange(flat,device=device)*groups)
            last = _take(new_values,torch.arange(flat,device=device)*groups+(count.reshape(-1)-1).clamp_min(0))
            minimum,_,ef = _extreme(new_values,new_present.reshape(flat,groups),maximum=False)
            maximum,_,eg = _extreme(new_values,new_present.reshape(flat,groups),maximum=True)
            right = torch.arange(flat*groups,device=device).reshape(flat,groups)[:,1:]
            differences = _take(new_values,right.reshape(-1))-_take(new_values,(right-1).reshape(-1))
            if groups>1:
                squares = _sum(differences*differences,new_present.reshape(flat,groups)[:,1:])
            else: squares = first.constant(0)
            cross = first-memory.path_last
            cross = (cross*cross).select(((memory.path_count>0)&(count>0)).reshape(-1),cross.constant(0))
            has_new = (count>0).reshape(-1);was_empty = (memory.path_count==0).reshape(-1)
            low = memory.path_min.compare(minimum);high = memory.path_max.compare(maximum)
            path_first = first.select(has_new & was_empty,memory.path_first)
            path_last = last.select(has_new,memory.path_last)
            path_min = minimum.select(has_new & (was_empty | (low.order>0)),memory.path_min)
            path_max = maximum.select(has_new & (was_empty | (high.order<0)),memory.path_max)
            result = SourceHistoryMemory(ring_values,ring_ids,ring_times,keep,memory.path_count+count,
                path_first,path_last,path_min,path_max,memory.path_squares+squares+cross,
                torch.where(valid_root,prefixes[group_last],memory.last_prefix),
                torch.where(valid_root,delivered_cutoffs[group_last],memory.last_cutoff),
                torch.where(valid_root,self.market_ids[safe],memory.market_ids),memory.root_started_at)
            error = fault | numbers.combine_fault_rows(torch.stack(errors,dim=1).reshape(roots,-1))
            error |= numbers.combine_fault_rows((ef|eg|low.fault|high.fault).reshape(roots,6))
            return result,error
        return numbers.run_exact_batch(compute,initial_word_capacity=self.units.words.shape[1])

    def freeze(self, memory):
        """冻结24/30字段，返回清空路径但保留history的候选记忆；不消费账户/GRU。"""
        numbers = _numbers();roots = len(memory.counts);device = memory.counts.device
        def compute():
            flat = roots*6;base = torch.arange(flat,device=device)*16
            count = memory.counts.reshape(-1)
            last = _take(memory.values,base+(count-1).clamp_min(0))
            previous = _take(memory.values,base+(count-2).clamp_min(0))
            right = torch.arange(flat*16,device=device).reshape(flat,16)[:,1:]
            delta = _take(memory.values,right.reshape(-1))-_take(memory.values,(right-1).reshape(-1))
            squares = _sum(delta*delta,torch.arange(1,16,device=device)[None]<count[:,None])
            n = numbers.ExactInt.from_i64_tensor(count)
            history = torch.zeros((flat,4),device=device)
            history[:,0],error = n.project(n.constant(16))
            history[:,1],e = (last-previous).project(last.constant(2*10**28),operation=1);error |= e
            denominator = n.constant((2*10**28)**2)*(n-n.constant(1)).select(count>1,n.constant(1))
            history[:,2],e = squares.project(denominator,operation=2);error |= e
            times = memory.times.reshape(flat,16)
            elapsed = numbers.ExactInt.from_i64_tensor(times.gather(1,(count-1).clamp_min(0)[:,None]))
            elapsed = elapsed-numbers.ExactInt.from_i64_tensor(times[:,0])
            history[:,3],e = elapsed.project(elapsed.constant(1000),operation=1);error |= e
            history[:,1:] = torch.where((count>1)[:,None],history[:,1:],0.0)
            path_n = numbers.ExactInt.from_i64_tensor(memory.path_count)
            present = memory.path_count.reshape(-1)>0
            path = torch.zeros((flat,6),device=device)
            path[:,0],e = path_n.project(path_n.constant(1),operation=1);error |= e
            pm = (torch.arange(6,device=device)[None].expand(roots,-1).reshape(-1)<2)
            for position,value in enumerate((memory.path_first,memory.path_last,memory.path_min,memory.path_max),1):
                transformed,e = value.project(value.constant(2*10**28),operation=1);error |= e
                phi,e = (value*value.constant(2)-value.constant(2*10**28)).project(value.constant(2*10**28));error |= e
                path[:,position] = torch.where(present,torch.where(pm,phi,transformed),0.0)
            denominator = path_n.constant((2*10**28)**2)*(path_n-path_n.constant(1)).select(memory.path_count.reshape(-1)>1,path_n.constant(1))
            path[:,5],e = memory.path_squares.project(denominator,operation=2);error |= e
            path[:,5] = torch.where(memory.path_count.reshape(-1)>1,path[:,5],0.0)
            zero = path_n.constant(0)
            cleared = SourceHistoryMemory(memory.values,memory.identities,memory.times,memory.counts,
                torch.zeros_like(memory.path_count),zero,zero,zero,zero,zero,memory.last_prefix,memory.last_cutoff,memory.market_ids,memory.root_started_at)
            paths = path.reshape(roots,6,6)
            return NativeSourceHistory(history.reshape(roots,24),torch.cat((paths[:,:2],paths[:,3:]),dim=1).reshape(roots,30),
                cleared,numbers.combine_fault_rows(error.reshape(roots,6)))
        return numbers.run_exact_batch(compute,initial_word_capacity=memory.values.words.shape[1])
