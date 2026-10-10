"""同根100ms观察与模型事件装配；公共层不导入模型或替代SIM业务处理器。"""
from dataclasses import dataclass
import torch


@dataclass(frozen=True)
class ContinuousAdvance:
    complete: torch.Tensor
    blocked: torch.Tensor
    fault: torch.Tensor
    events: int
    stage_result: object = None


class ContinuousActorRun:
    """按设备时间列选择观察/开始/完成/发送，保留尚未完成事件供资源续接。

    freeze接实际当前公开账户并返回模型自有冻结材料；models拥有模型队列、
    hidden及权重版本。receive只续原waiting。显式horizon保持开放，不排空未来。
    """
    def __init__(self,clock,first_observation,models,freeze,receive):
        self.clock=clock;self.next_observation=first_observation.clone()
        self.models=models;self.freeze=freeze;self.receive=receive

    def advance_before(self,horizon,enabled,*,progress=None):
        blocked=torch.zeros_like(enabled);fault=torch.zeros_like(horizon,dtype=torch.int32)
        count=0;stage=None;blocked_stage=None
        while True:
            event=self.models.next_event()
            observe=(~event.present)|(self.next_observation<=event.at)
            at=torch.where(observe,self.next_observation,event.at)
            kind=torch.where(observe,torch.zeros_like(event.kind),event.kind)
            due=enabled&~blocked&(at<horizon)
            if not bool(due.any().cpu()):
                boundary=enabled&~blocked
                stage=self.clock.advance_before(horizon,boundary,progress=progress)
                fault |= stage.fault
                complete=boundary&stage.complete&(fault==0)
                return ContinuousAdvance(complete,blocked| (boundary&~complete),fault,count,blocked_stage if blocked_stage is not None else stage)
            # 与原SIM调用相同，只冷读实际已选处理器类别；根/时间选择留在设备。
            selected_kind=int(kind[due].min().cpu())
            active=due&(kind==selected_kind)
            if selected_kind==3:
                complete,error,send_advance=self.models.send(self.clock,active,progress=progress)
                fault |= error
                if send_advance is not None:blocked_stage=send_advance
                blocked |= active&~complete
            else:
                stage=self.clock.advance_before(at,active,progress=progress)
                fault |= stage.fault
                if bool((active & ~stage.complete).any().cpu()):blocked_stage=stage
                ready=active&stage.complete&(fault==0)
                blocked |= active&~ready
                if selected_kind==0 and bool(ready.any().cpu()):
                    delivery=self.clock.receive_first(enabled=ready)
                    self.clock.public,received=self.receive(self.clock.public,delivery,ready)
                    ready &= received
                    blocked |= active&~ready
                    if bool(ready.any().cpu()):
                        frozen,accepted=self.freeze(self.clock,at,ready)
                        self.models.enqueue(frozen,accepted)
                        self.next_observation += (accepted.to(torch.int64)*100)
                        blocked |= ready&~accepted
                elif selected_kind==1:
                    accepted,error=self.models.start(ready)
                    fault |= error
                    blocked |= ready&~accepted
                elif selected_kind==2:
                    complete=self.models.finish(ready)
                    blocked |= ready&~complete
            self.models.retire_consumed_markets(self.next_observation-100)
            count+=1
            if progress:progress('continuous_event',events=count,kind=selected_kind,
                at=at.detach().cpu().tolist(),active=active.detach().cpu().tolist(),blocked=blocked.detach().cpu().tolist())
