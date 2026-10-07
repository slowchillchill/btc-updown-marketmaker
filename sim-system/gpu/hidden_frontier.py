"""原reader验收完整桶的共享列与每根运行前沿。

材料预读不推进运行游标。按原顺序成功应用全部参考和步骤后才关闭该桶；
这里只保存运行位置，不代替参考/行情处理器，也不把公开观察当闭桶证明。
"""
from dataclasses import dataclass, replace
import torch
from queues import ExactInt, _take
from scheduling import ScheduledTime


@dataclass(frozen=True)
class HiddenBucketColumns:
    source: object  # 已经原材料reader逐值核验的HiddenPmBlock；完整引用仍由它持有。
    at: torch.Tensor
    step_offsets: torch.Tensor
    reference_offsets: torch.Tensor
    index_ends: object  # 原u64索引位置，不能用有符号设备坐标替代业务位置。
    end_of_input: bool  # 原最后桶closure；不是本设备页末。
    bucket_base: int = 0  # 同一不可变材料的稳定完整桶坐标。

    @classmethod
    def from_block(cls, block):
        if getattr(block,'prepared_page',None) is not None:
            return block.prepared_page.bucket_columns(block)
        steps, references = [0], [0]
        for value in block.buckets:
            steps.append(steps[-1]+len(value['steps']))
            references.append(references[-1]+len(value['references']))
        device = block.level_assets.device
        return cls(block, torch.tensor([value['bucket']['bucket_time_utc_ms'] for value in block.buckets],
                       dtype=torch.int64, device=device),
                   torch.tensor(steps, dtype=torch.int64, device=device),
                   torch.tensor(references, dtype=torch.int64, device=device),
                   ExactInt.from_values([value['bucket']['index_end'] for value in block.buckets], device=device),
                   not block.buckets or block.buckets[-1]['bucket']['closure'] == 'EndOfInput')


@dataclass(frozen=True)
class HiddenBucketFrontier:
    columns: HiddenBucketColumns
    next_bucket: torch.Tensor  # 原只读页内物理桶坐标；不是闭桶u64索引位置。
    next_step: torch.Tensor
    next_reference: torch.Tensor
    applying: torch.Tensor
    closed_at: torch.Tensor
    has_closed: torch.Tensor
    closed_bucket: torch.Tensor  # 同源完整桶的稳定坐标；换页不重解释旧坐标。
    index_position: object
    fault: torch.Tensor

    @classmethod
    def open(cls, columns, roots):
        device = columns.at.device
        zero = torch.zeros(roots, dtype=torch.int64, device=device)
        absent = torch.zeros(roots, dtype=torch.bool, device=device)
        return cls(columns,zero,zero,zero,absent,zero,absent,torch.full_like(zero,-1),
                   ExactInt.from_i64_tensor(zero),torch.zeros_like(zero,dtype=torch.int32))

    def scheduled(self):
        count = self.columns.at.numel()
        present = self.next_bucket < count
        at = self.columns.at[self.next_bucket.clamp(0,count-1)] if count else torch.zeros_like(self.closed_at)
        eof = ~present & self.columns.end_of_input
        return ScheduledTime(at,present,self.fault),eof

    def handoff(self, columns):
        """退出图后的物理页边界验收；不处理业务或给未完成桶签发关闭。

        仅在所有根已完整消费本页后安装同源相邻完整桶页。此冷检查读取
        页耗尽/位置凭据，不代替设备调度；其它SIM、NOE和公开状态由宿主保留。
        """
        from queues import Numbers
        old=self.columns
        source=getattr(old.source,'catalog',None)
        if (source is None or source is not getattr(columns.source,'catalog',None)
                or old.end_of_input or columns.bucket_base!=old.bucket_base+old.at.numel()):
            raise ValueError('hidden换页须保持原共享bank和相邻完整桶范围')
        page=columns.source.prepared_page
        first=_take(source.index_starts,torch.full_like(self.next_bucket,page.begin))
        match=Numbers.run_exact_batch(lambda:self.index_position.compare(first))
        ready=(self.next_bucket==old.at.numel()) & ~self.applying & (self.fault==0)
        ready &= (match.order==0) & (match.fault==0)
        if not ready.all().cpu().item():
            raise ValueError('hidden当前页仍有未提交桶/错误根，不能换页')
        zero=torch.zeros_like(self.next_bucket)
        return replace(self,columns=columns,next_bucket=zero,next_step=zero.clone(),next_reference=zero.clone())

    def begin(self, choice):
        source,_ = self.scheduled()
        active = choice.enabled(4) & source.present & (choice.at == source.at) & ~self.applying
        slot = self.next_bucket.clamp(0,self.columns.at.numel())
        return replace(self,applying=self.applying | active,
                       next_step=torch.where(active,self.columns.step_offsets[slot],self.next_step),
                       next_reference=torch.where(active,self.columns.reference_offsets[slot],self.next_reference))

    def reference_enabled(self, index):
        end = self.columns.reference_offsets[(self.next_bucket+1).clamp_max(self.columns.at.numel())]
        return self.applying & (self.fault == 0) & (self.next_reference == index) & (self.next_reference < end)

    def step_enabled(self, index):
        slot = self.next_bucket.clamp(0,self.columns.at.numel())
        references_done = self.next_reference == self.columns.reference_offsets[(slot+1).clamp_max(self.columns.at.numel())]
        end = self.columns.step_offsets[(slot+1).clamp_max(self.columns.at.numel())]
        return self.applying & references_done & (self.fault == 0) & (self.next_step == index) & (self.next_step < end)

    def reference_applied(self, index, committed, fault):
        enabled = self.reference_enabled(index)
        return replace(self,next_reference=self.next_reference+(enabled & committed & (fault == 0)).to(torch.int64),
                       fault=self.fault | torch.where(enabled,fault,0))

    def step_applied(self, index, committed, fault):
        enabled = self.step_enabled(index)
        return replace(self,next_step=self.next_step+(enabled & committed & (fault == 0)).to(torch.int64),
                       fault=self.fault | torch.where(enabled,fault,0))

    def finish(self, fault):
        count = self.columns.at.numel()
        if not count:
            return replace(self,fault=self.fault | fault),torch.zeros_like(self.applying)
        slot = self.next_bucket.clamp(0,count-1)
        complete = self.applying & (self.next_step == self.columns.step_offsets[slot+1])
        complete = complete & (self.next_reference == self.columns.reference_offsets[slot+1])
        fault = self.fault | fault
        complete = complete & (fault == 0)
        position = _take(self.columns.index_ends,slot).select(complete,self.index_position)
        return replace(self,next_bucket=self.next_bucket+complete.to(torch.int64),
                       applying=self.applying & ~complete,closed_at=torch.where(complete,self.columns.at[slot],self.closed_at),
                       has_closed=self.has_closed | complete,closed_bucket=torch.where(complete,slot+self.columns.bucket_base,self.closed_bucket),
                       index_position=position,fault=fault),complete
