"""原BatchMapper来源序：direct→order→trade→settlement，与private发射序独立。"""
from dataclasses import dataclass
import torch


@dataclass(frozen=True)
class MappedSourceOrdinals:
    first_ordinals: torch.Tensor
    record_counts: torch.Tensor
    emitted: torch.Tensor
    fault: torch.Tensor


def map_source_ordinals(active,categories,record_counts,source_fault):
    """输入为原发射顺序[root,draft]；人口由该载荷真实记录/经济delta决定。

    categories沿原映射遍历0=direct/1=order/2=trade/3=settlement。一个反馈可有多条
    NOE，局部record ordinal加first得到独立source ordinal，不重编号private。
    """
    if active.ndim!=2 or active.dtype!=torch.bool:
        raise TypeError('NOE反馈人口须bool[root,draft]')
    for value in [categories,record_counts,source_fault]:
        if value.shape!=active.shape or value.device!=active.device:
            raise ValueError('NOE来源列须同设备同人口')
    if categories.dtype!=torch.int64 or record_counts.dtype!=torch.int64 or source_fault.dtype!=torch.int32:
        raise TypeError('NOE类别/人口须int64，故障须int32')
    roots,width=active.shape
    if width > ((1<<63)-1)//0xffffffff:
        raise ValueError('NOE物理人口超过可分配源序列宽度')
    local=source_fault | ((categories<0)|(categories>3)|(record_counts<0)|
        (record_counts>0xffffffff)).to(torch.int32)
    counts=torch.where(active,record_counts.clamp(0,0xffffffff),0)
    order=categories.clamp(0,3).argsort(dim=1,stable=True)
    ordered=counts.gather(1,order)
    first=ordered.cumsum(dim=1)-ordered
    ordinals=torch.zeros_like(first).scatter(1,order,first)
    bits=torch.arange(32,dtype=torch.int64,device=active.device)
    flags=(((torch.where(active,local,0).to(torch.int64)[:,:,None]>>bits)&1)!=0).any(dim=1)
    fault=(flags.to(torch.int64)<<bits).sum(dim=1).to(torch.int32)
    # 原最后一个source ordinal可为u32MAX，人口因而可为2^32。
    fault=fault | (counts.sum(dim=1)>(1<<32)).to(torch.int32)
    return MappedSourceOrdinals(ordinals,counts,active & (counts>0) & (fault==0)[:,None],fault)
