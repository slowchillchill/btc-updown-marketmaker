"""真实compact设备页的公开组消费位置；组界、下一组和EOF都沿同一材料。"""
from dataclasses import dataclass,replace
import torch


@dataclass(frozen=True)
class PublicGroupNext:
    at: torch.Tensor
    present: torch.Tensor  # 完整下一组的实际设备页已安装。
    eof: torch.Tensor
    position: torch.Tensor
    fault: torch.Tensor


class PublicGroupSource:
    """共享PreparedPublicPage，物理分页不改变完整原group或输入身份。"""
    def __init__(self,prepared,roots,*,columns_binary=None,page_groups=256):
        if type(page_groups) is not int or page_groups<1:raise ValueError('物理组页容量须为正整数')
        self.page_capacity=page_groups
        self.prepared=prepared
        self.position=torch.zeros(roots,dtype=torch.int64,device=prepared.device)
        self.page_begin=self.page_end=0
        self.page_groups=None
        self.fault=torch.zeros(roots,dtype=torch.int32,device=prepared.device)
        prepared.ensure_group_lineage(columns_binary)
        # 新来源推进所需旁证沿原reader补取，不能据cutoff补造。
        progress=prepared.columns.get('source_progress_changes')
        if progress is None:
            raise ValueError('原compact未导出组来源进度')
        if len(progress)!=len(prepared.columns['groups']):
            raise ValueError('原来源进度与完整组人口不符')

    def load_next(self,groups=None):
        """冷资源边界加载真实完整组片；未加载不冒充EOF或group可消费。"""
        groups=self.page_capacity if groups is None else groups
        if type(groups) is not int or groups<1:raise ValueError('物理组页容量须为正整数')
        begin=int(self.position.min().cpu())
        end=min(begin+groups,len(self.prepared.columns['groups']))
        self.page_groups=torch.as_tensor(self.prepared.columns['groups'][begin:end].copy(),
            dtype=torch.int64,device=self.prepared.device)
        self.page_begin,self.page_end=begin,end
        self.page_at=torch.cummax(self.page_groups[:,2],dim=0).values
        if begin:self.page_at=torch.maximum(self.page_at,self.prepared.tensor('causal_visible_at')[begin-1])

    def next(self):
        count=len(self.prepared.columns['groups'])
        eof=(self.position==count)&self.prepared.normal_end
        present=(self.position>=self.page_begin)&(self.position<self.page_end)&~eof
        times=self.prepared.tensor('causal_visible_at')
        at=times[self.position.clamp(0,count-1)] if count else torch.zeros_like(self.position)
        if self.page_end>self.page_begin:
            offset=(self.position-self.page_begin).clamp(0,self.page_end-self.page_begin-1)
            at=torch.where(present,self.page_at[offset],at)
        # 已加载时读取实际页header；页缺时next.at只证明该组之前的空档。
        return PublicGroupNext(at,present,eof,self.position.clone(),self.fault)

    def consume(self,enabled):
        next_group=self.next()
        committed=enabled&next_group.present&(self.fault==0)
        self.position=self.position+committed.to(torch.int64)
        return committed

    def prefix_at(self,pm,at):
        """查询只能包含实际已消费组，同刻预取组不会进入Ck。"""
        return torch.minimum(pm.prefix_at(at),self.position-1)

    def snapshot(self):
        return self.position.clone(),self.prepared.columns['input_stream_id'],self.prepared.source_manifest_sha256
