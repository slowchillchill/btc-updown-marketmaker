"""原公共PM步骤的设备数量/选择/ready/tick状态；hidden及SIM状态各由原模块拥有。"""
from dataclasses import dataclass
import torch
from prepare import _numbers, decimal_columns


def _take(value, indices):
    cls = _numbers().ExactInt
    return cls(value.words[indices], value.sign[indices], value.fault[indices])


class PrefixColumn:
    def __init__(self, rows, targets, device):
        rows = torch.tensor(rows, dtype=torch.int64).reshape(-1, 4).pin_memory().to(device, non_blocking=True)
        self._install_rows(rows,targets)

    def _install_rows(self, rows, targets):
        ordered = rows[torch.argsort(rows[:, 0], stable=True)].contiguous()
        self.padded = torch.cat((ordered.new_zeros((1, 4)), ordered))
        self.rows = self.padded[1:]  # 与sentinel共享存储；热查询不复制整段历史。
        counts = torch.bincount(self.rows[:, 0], minlength=targets)
        self.offsets = torch.cat((counts.new_zeros(1), counts.cumsum(0))).contiguous()

    @classmethod
    def from_device_rows(cls,rows,targets):
        result=cls.__new__(cls)
        result._install_rows(rows,targets)
        return result

    @classmethod
    def from_prepared(cls, padded, offsets):
        """冷安装已经验收并预排的二进制列；不再次排序或计算offset。"""
        if (padded.ndim!=2 or padded.size(1)!=4 or padded.size(0)<1
                or padded.dtype!=torch.int64 or offsets.ndim!=1 or offsets.numel()<1
                or offsets.dtype!=torch.int64 or padded.device!=offsets.device):
            raise ValueError('预排公共列须同设备int64 sentinel行与offset列')
        result=cls.__new__(cls)
        result.padded,result.rows,result.offsets=padded,padded[1:],offsets
        return result

    def query(self, prefixes):
        selected, fault = _numbers().public_extension().prefix_lookup(self.rows, self.offsets, prefixes)
        found = selected >= 0
        # 冷列为空也有同设备占位，不能使缺值伪装成已知零。
        rows = self.padded[selected + 1]
        return rows[:, :, 1], rows[:, :, 3], found, fault


@dataclass(frozen=True)
class PublicBookFrame:
    quantity: object
    present: torch.Tensor
    initialized: torch.Tensor
    ready: torch.Tensor
    tick_ids: torch.Tensor
    tick_present: torch.Tensor
    anchor_ids: torch.Tensor
    anchor_present: torch.Tensor
    book_anchor_ids: torch.Tensor
    book_anchor_present: torch.Tensor
    quality: torch.Tensor
    prefix_fault: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class ObservedPmField:
    values: torch.Tensor
    present: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class PublicPmObservation:
    bba: ObservedPmField
    trade: ObservedPmField
    tick: ObservedPmField
    prefix_fault: torch.Tensor


class PublicPmState:
    """一次冷加载共享外生列；查询只消费设备公共组前缀，不读设备选择回CPU。

    slots包含整块全部精确价位。未来槽位必须present=false；Native须先压紧
    当前正量档位才能进入模型，不能把这些共享槽位直接当模型padding。
    本入口包含PM盘口、直接BBA/trade/tick；生命周期/参考/Binance仍由后续公共后端衔接。
    """
    def __init__(self, columns, *, device='cuda', prepared=None):
        if columns['version'] not in ('public_pm_device_columns_v1','public_pm_device_columns_v2'):
            raise ValueError('设备列版本错误')
        self.input_stream_id = columns['input_stream_id']
        self.assets = columns['assets']  # 只在冷身份边界使用。
        self.markets = columns['markets']
        self.market_assets = columns['market_assets']
        assets, slots, groups = len(self.assets), len(columns['slots']), len(columns['groups'])
        self.group_count = groups
        for asset, side, price in columns['slots']:
            if not (0 <= asset < assets and side in (0, 1) and 0 <= price < len(columns['decimals'])):
                raise ValueError('公共设备价位身份或精确值引用越界')
        self.slots = (torch.tensor(columns['slots'], dtype=torch.int64, device=device).reshape(-1, 3)
            if prepared is None else prepared.tensor('slots'))
        self.groups = (torch.tensor(columns['groups'], dtype=torch.int64, device=device).reshape(-1, 3)
            if prepared is None else prepared.tensor('groups'))
        # 原组时间可回退；宿主按原组顺序累计可见时间，不改变原事实或重排。
        self.causal_visible_at = (torch.cummax(self.groups[:,2],dim=0).values.contiguous()
            if prepared is None else prepared.tensor('causal_visible_at'))
        if prepared is None:
            raw, offsets = bytearray(), [0]
            for text in columns['decimals']:
                raw.extend(text.encode('ascii')); offsets.append(len(raw))
            byte_column = torch.tensor(list(raw), dtype=torch.uint8).pin_memory().to(device, non_blocking=True)
            offset_column = torch.tensor(offsets, dtype=torch.int64).pin_memory().to(device, non_blocking=True)
            self.decimal_columns = decimal_columns(byte_column, offset_column,
                torch.ones(len(offsets)-1, dtype=torch.bool, device=device))
            self.decimals = self.decimal_columns.rational()
        else:
            self.decimal_columns,self.decimals=prepared.decimal_columns,prepared.decimals
        self.decimal_fault = self.decimals.fault  # 不可变字典的合并错误只冷计算一次。
        self.anchors = columns['anchors']
        self.anchor_times = (torch.tensor([[0, 0, 0]] + [
            [int(a['direct']['source_event_at_utc_ms'] is not None),
             a['direct']['source_event_at_utc_ms'] or 0, a['direct']['received_at_utc_ms']]
            for a in self.anchors], dtype=torch.int64, device=device)
            if prepared is None else prepared.tensor('anchor_times'))
        for name in ('quantities', 'selections', 'resets', 'initialized', 'ready', 'ticks',
                     'level_anchors', 'book_quality', 'book_anchors'):
            target_count = slots if name in ('quantities', 'selections', 'level_anchors') else assets
            previous = {}
            for target, sequence, group, value in columns[name]:
                if not (0 <= target < target_count and 0 <= sequence < columns['original_pm_steps']
                        and 0 <= group < groups):
                    raise ValueError('公共设备列位置越界')
                prior = previous.get(target, (-1, -1))
                if sequence < prior[0] or group < prior[1]:
                    raise ValueError('公共设备列原顺序倒退')
                previous[target] = (sequence, group)
                if name in ('quantities', 'ticks') and not 0 <= value < len(columns['decimals']):
                    raise ValueError('公共设备精确值引用越界')
                if name in ('selections', 'initialized', 'ready') and value not in (0, 1):
                    raise ValueError('公共设备存在标志错误')
                if name in ('level_anchors','book_anchors') and not 0 <= value < len(self.anchors):
                    raise ValueError('公共档位直接锚点引用越界')
                if name == 'book_quality' and not 0 <= value < 6:
                    raise ValueError('公共盘口质量错误')
            setattr(self, name, PrefixColumn(columns[name], target_count, device) if prepared is None else prepared.prefix(name))
        self.anchor_sets = columns['anchor_sets']
        for members in self.anchor_sets:
            if any(not 0 <= member < len(self.anchors) for member in members):
                raise ValueError('观察字段直接来源集合引用越界')
        self.unique_anchor_ids = (torch.tensor([members[0] if len(members) == 1 else -1
            for members in self.anchor_sets],dtype=torch.int64,device=device)
            if prepared is None else prepared.tensor('unique_anchor_ids'))
        for name, table_name, width, decimal_indices, presence_indices, anchor_index in (
                ('bba','bba_values',8,(1,3,5),(0,2,4),6),
                ('trades','trade_values',7,(0,1,4),(3,),5),
                ('observed_ticks','observed_tick_values',4,(1,),(0,),2)):
            table = columns[table_name]
            for values in table:
                if len(values) != width or not 0 <= values[-1] < 6:
                    raise ValueError('公共观察字段布局或质量错误')
                if any(not 0 <= values[i] < len(columns['decimals']) for i in decimal_indices):
                    raise ValueError('公共观察字段精确值引用越界')
                if any(values[i] not in (0,1) for i in presence_indices):
                    raise ValueError('公共观察字段缺失位错误')
                if not 0 <= values[anchor_index] < len(self.anchor_sets):
                    raise ValueError('公共观察字段来源集合引用越界')
                if name == 'trades' and values[2] not in (0,1):
                    raise ValueError('公共成交方向错误')
            previous = {}
            for target, sequence, group, value in columns[name]:
                if not (0 <= target < assets and 0 <= sequence < columns['field_updates']
                        and 0 <= group < groups and 0 <= value < len(table)):
                    raise ValueError('公共观察字段原位置或载荷引用越界')
                prior = previous.get(target,(-1,-1))
                if sequence < prior[0] or group < prior[1]:
                    raise ValueError('公共观察字段原顺序倒退')
                previous[target] = (sequence,group)
            setattr(self,name,PrefixColumn(columns[name],assets,device) if prepared is None else prepared.prefix(name))
            setattr(self,table_name,torch.tensor(table,dtype=torch.int64,device=device).reshape(-1,width) if prepared is None else prepared.tensor(table_name))
        from valuation_state import PublicValuationState
        self.valuation = PublicValuationState(self,columns,prepared=prepared)
        if self.valuation.complete:
            from valuation_state import PREFIXES
            for name in PREFIXES: setattr(self,name,getattr(self.valuation,name))

    def prefix_at(self, observation_ms):
        """UTC观察选择完整合法前缀；后组必须同时等到全部前组可见。"""
        if observation_ms.dtype != torch.int64 or observation_ms.ndim != 1 or observation_ms.device != self.groups.device:
            raise ValueError('需同设备int64观察UTC批次')
        return torch.searchsorted(self.causal_visible_at,observation_ms,right=True)-1

    def frame(self, prefixes):
        qseq, qid, qfound, qfault = self.quantities.query(prefixes)
        sseq, selected, sfound, sfault = self.selections.query(prefixes)
        reset, _, reset_found, reset_fault = self.resets.query(prefixes)
        iseq, initialized, ifound, ifault = self.initialized.query(prefixes)
        rseq, ready, rfound, rfault = self.ready.query(prefixes)
        tseq, tick, tfound, tfault = self.ticks.query(prefixes)
        aseq, anchor, afound, afault = self.level_anchors.query(prefixes)
        bseq, book_anchor, bfound, bfault = self.book_anchors.query(prefixes)
        _, quality, quality_found, quality_fault = self.book_quality.query(prefixes)
        asset = self.slots[:, 0]
        reset = torch.where(reset_found, reset, -1)
        recent_quantity = qfound & (qseq >= reset[:, asset])
        present = sfound & (sseq >= reset[:, asset]) & selected.bool()
        # 无数量事实的槽位保留0占位与recent_quantity=false，不生成经济事实。
        qid = torch.where(recent_quantity, qid, 0)
        numbers = _numbers()
        quantity = numbers.ExactRational(_take(self.decimals.numerator, qid.reshape(-1)),
                                          _take(self.decimals.denominator, qid.reshape(-1)))
        asset_fault = reset_fault | ifault | rfault | tfault | quality_fault | bfault
        fault = qfault | sfault | afault | asset_fault[:, asset]
        fault = fault | quantity.fault.reshape(qid.shape)
        fault = fault | self.decimal_fault[self.slots[:, 2]][None]
        fault = fault | self.decimal_fault[tick][:, asset]
        fault = fault | (quantity.numerator.sign.reshape(qid.shape) < 0).to(torch.int32)
        prefix_fault = ((prefixes < -1) | (prefixes >= self.group_count)).to(torch.int32)
        fault = fault | prefix_fault[:, None]
        return PublicBookFrame(quantity, present,
            ifound & initialized.bool(),
            rfound & ready.bool() & (rseq >= reset), tick, tfound,
            anchor, afound & (aseq >= reset[:, asset]),
            book_anchor,bfound & (bseq >= reset),
            torch.where(quality_found, quality, 3), prefix_fault, fault)

    def observation(self, prefixes):
        """原末端公共所选字段；未过滤质量或用L2生成BBA，不产生模型窗口。"""
        invalid_prefix = ((prefixes < -1) | (prefixes >= self.group_count)).to(torch.int32)
        fields = []
        for name, table_name, decimal_indices in (
                ('bba','bba_values',(1,3,5)), ('trades','trade_values',(0,1,4)),
                ('observed_ticks','observed_tick_values',(1,))):
            _, ids, found, fault = getattr(self,name).query(prefixes)
            values = getattr(self,table_name)[ids]
            fault = fault | invalid_prefix[:,None]
            for index in decimal_indices:
                fault = fault | self.decimal_fault[values[:,:,index]]
            present = found & values[:,:,0].bool() if name == 'observed_ticks' else found
            fields.append(ObservedPmField(values,present,fault))
        return PublicPmObservation(*fields,invalid_prefix)

    def valuation_bid(self, prefixes, *, frame=None):
        """公共Core同前缀估值；不改变原到达BBA或Native盘口特征。"""
        return self.valuation.query(prefixes,frame=frame)
