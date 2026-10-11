"""原RequestContext的运行所属冷目录；只绑定已经接纳的模型任务。"""
from dataclasses import dataclass
import torch


class ActorRequestDirectory:
    """稳定字符串槽和追加bytes；增长只搬已有列，不重新编码所有旧请求。"""
    def __init__(self,device):
        self.keys=[];self.slots={};self.size=0
        self.bytes=torch.empty(256,dtype=torch.uint8,device=device)
        self.offsets=torch.empty(16,dtype=torch.int64,device=device)
        self.offsets[0]=0

    def append(self,request_id):
        if request_id in self.slots:return self.slots[request_id]
        value=request_id.encode('utf-8');slot=len(self.keys);end=self.size+len(value)
        if end>self.bytes.numel():
            grown=torch.empty(max(end,self.bytes.numel()*2),dtype=self.bytes.dtype,device=self.bytes.device)
            grown[:self.size].copy_(self.bytes[:self.size]);self.bytes=grown
        if slot+2>self.offsets.numel():
            grown=torch.empty(max(slot+2,self.offsets.numel()*2),dtype=self.offsets.dtype,device=self.offsets.device)
            grown[:slot+1].copy_(self.offsets[:slot+1]);self.offsets=grown
        self.bytes[self.size:end]=torch.tensor(list(value),dtype=torch.uint8,device=self.bytes.device)
        self.offsets[slot+1]=end
        self.keys.append(request_id);self.slots[request_id]=slot;self.size=end
        return slot

    def columns(self):
        return self.bytes[:self.size],self.offsets[:len(self.keys)+1]

    def identity_order(self,request_slots):
        """原取消人口的完整字符串排序；不存在的未来请求不参与。"""
        rows=request_slots.detach().cpu().tolist()
        keys={slot for row in rows for slot in row if slot>=0}
        ranks={slot:rank for rank,slot in enumerate(sorted(keys,key=self.keys.__getitem__))}
        return torch.tensor([[ranks[slot] if slot>=0 else -1 for slot in row] for row in rows],
            dtype=torch.int64,device=request_slots.device)


@dataclass(frozen=True)
class ActorTaskRequest:
    contexts: tuple  # 每根原RequestContext；该任务未接纳的根为None，不编码正文。
    slots: torch.Tensor

    def control(self,execute_at):
        from action_selection import ActionControl
        # 两腿是原(request,ordinal0/1)的稳定字典坐标，不是SIM输入提交序。
        legs=self.slots[:,None]*2+torch.arange(2,device=self.slots.device)[None,:]
        legs=torch.where(self.slots[:,None]>=0,legs,-1)
        return ActionControl(self.slots,self.slots,legs,execute_at)


class ActorTaskRequests:
    """根身份与本根真实接纳次序组成RequestContext；任务负责携带到首次发送。"""
    def __init__(self,directory,root_names):
        self.directory=directory;self.root_names=tuple(root_names)
        self.next_task=[0]*len(root_names)

    def bind(self,accepted):
        contexts=[];slots=[]
        for root,on in enumerate(accepted.detach().cpu().tolist()):
            if not on:
                contexts.append(None);slots.append(-1);continue
            context={'global_request_id':f'{self.root_names[root]}:actor:{self.next_task[root]}',
                'caller_correlation_id':None}
            self.next_task[root]+=1
            contexts.append(context);slots.append(self.directory.append(context['global_request_id']))
        return ActorTaskRequest(tuple(contexts),torch.tensor(slots,dtype=torch.int64,device=accepted.device))
