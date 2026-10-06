"""共享事实列的合法前缀区间查询；不包含模型特征或业务状态归约。"""
from dataclasses import dataclass
import torch


@dataclass(frozen=True)
class FactWindow:
    facts: torch.Tensor
    fact_present: torch.Tensor
    sources: torch.Tensor
    source_present: torch.Tensor
    fault: torch.Tensor


class PublicFactWindow:
    def __init__(self, columns, *, device='cuda', initial_capacity=128):
        if initial_capacity < 1:
            raise ValueError('事实区间资源容量须为正')
        self.group_count = len(columns['groups'])
        self.types = tuple(columns['fact_types'])
        if len(set(self.types)) != len(self.types):
            raise ValueError('公共事实类型字典重复')
        self.fact_capacity = self.source_capacity = initial_capacity
        expected_source = 0
        previous_group = -1
        for group, kind, start, end in columns['facts']:
            if not (0 <= group < self.group_count and group >= previous_group
                    and 0 <= kind < len(self.types) and start == expected_source
                    and start <= end <= len(columns['fact_sources'])):
                raise ValueError('公共事实区间/来源坐标无效')
            for source in columns['fact_sources'][start:end]:
                if source[:2] != [group,kind] or source[2] not in (0,1):
                    raise ValueError('公共事实直接来源关联或存在位错误')
            expected_source, previous_group = end, group
        if expected_source != len(columns['fact_sources']):
            raise ValueError('公共事实直接来源未完整覆盖')
        def load(rows, width):
            values = torch.tensor(rows,dtype=torch.int64).reshape(-1,width).pin_memory().to(device,non_blocking=True)
            padded = torch.cat((values.new_zeros((1,width)),values))
            return padded, padded[1:]
        self.fact_padded, self.facts = load(columns['facts'],4)
        self.source_padded, self.sources = load(columns['fact_sources'],5)
        self.fact_groups = self.facts[:,0].contiguous()
        self.source_groups = self.sources[:,0].contiguous()

    def query(self, previous_prefix, current_prefix):
        """(previous,current]区间；资源扩池只影响墙钟，绝不截断事实。"""
        if (previous_prefix.ndim != 1 or current_prefix.shape != previous_prefix.shape
                or previous_prefix.dtype != torch.int64 or current_prefix.dtype != torch.int64
                or previous_prefix.device != self.facts.device or current_prefix.device != self.facts.device):
            raise ValueError('需同设备int64合法前缀批次')
        valid = ((previous_prefix >= -1) & (current_prefix < self.group_count)
                 & (current_prefix >= previous_prefix))
        safe_previous = previous_prefix.clamp(-1,max(-1,self.group_count-1))
        safe_current = current_prefix.clamp(-1,max(-1,self.group_count-1))
        spans = []
        for groups in (self.fact_groups,self.source_groups):
            start = torch.searchsorted(groups,safe_previous,right=True)
            end = torch.searchsorted(groups,safe_current,right=True)
            spans.append((start,(end-start).clamp_min(0)))
        # 唯一回传为本批两种资源总需求，不回传事实值或模型业务分支。
        required = torch.stack([count.max() for _,count in spans]).cpu().tolist()
        while self.fact_capacity < required[0]: self.fact_capacity *= 2
        while self.source_capacity < required[1]: self.source_capacity *= 2
        results = []
        for padded,(start,count),capacity in zip((self.fact_padded,self.source_padded),spans,
                                                (self.fact_capacity,self.source_capacity)):
            offsets = torch.arange(capacity,device=start.device)[None]
            present = (offsets < count[:,None]) & valid[:,None]
            indices = torch.where(present,start[:,None]+offsets+1,0)
            results.extend((padded[indices],present))
        return FactWindow(*results,(~valid).to(torch.int32))


class PublicTradeWindow:
    """原成交事实的区间，不以逻辑public最新成交替换缺量/未知方向事实。"""
    def __init__(self, columns, *, device='cuda', initial_capacity=128):
        if initial_capacity < 1:
            raise ValueError('原成交区间容量须为正')
        self.capacity = initial_capacity
        self.group_count = len(columns['groups'])
        self.asset_count = len(columns['assets'])
        previous = -1
        for group,asset,price,qp,q,side,fp,fee,sp,_,_ in columns['raw_trades']:
            if not (previous <= group < self.group_count and group >= 0
                    and -1 <= asset < self.asset_count and side in (0,1,2,3)
                    and qp in (0,1) and fp in (0,1) and sp in (0,1)
                    and all(0 <= value < len(columns['decimals']) for value in (price,q,fee))):
                raise ValueError('原成交列坐标/精确值引用错误')
            if asset == -1 and (qp != 1 or side not in (0,1) or fp != 0):
                raise ValueError('Binance原成交事实字段不一致')
            previous = group
        rows = torch.tensor(columns['raw_trades'],dtype=torch.int64).reshape(-1,11).pin_memory().to(device,non_blocking=True)
        self.padded = torch.cat((rows.new_zeros((1,11)),rows))
        self.rows = self.padded[1:]
        self.groups = self.rows[:,0].contiguous()

    def query(self, previous_prefix, current_prefix):
        if (previous_prefix.ndim != 1 or current_prefix.shape != previous_prefix.shape
                or previous_prefix.dtype != torch.int64 or current_prefix.dtype != torch.int64
                or previous_prefix.device != self.rows.device or current_prefix.device != self.rows.device):
            raise ValueError('需同设备int64成交区间前缀')
        valid = ((previous_prefix >= -1) & (current_prefix >= previous_prefix)
                 & (current_prefix < self.group_count))
        previous = previous_prefix.clamp(-1,max(-1,self.group_count-1))
        current = current_prefix.clamp(-1,max(-1,self.group_count-1))
        start = torch.searchsorted(self.groups,previous,right=True)
        end = torch.searchsorted(self.groups,current,right=True)
        count = (end-start).clamp_min(0)
        required = int(count.max().cpu())  # 仅一次资源需求，不回传成交值或方向。
        while self.capacity < required: self.capacity *= 2
        offset = torch.arange(self.capacity,device=current.device)[None]
        present = (offset < count[:,None]) & valid[:,None]
        indices = torch.where(present,start[:,None]+offset+1,0)
        return self.padded[indices],present,(~valid).to(torch.int32)
