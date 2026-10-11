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
    def __init__(self,clock,first_observation,models,freeze,receive,*,trading_until=None,settlement=None,inputs=None):
        self.clock=clock;self.next_observation=first_observation.clone()
        self.models=models;self.freeze=freeze;self.receive=receive
        self.trading_until=trading_until;self.settlement=settlement
        self.economic_resume=torch.zeros_like(first_observation,dtype=torch.bool)
        owner=inputs if inputs is not None else getattr(freeze,'__self__',None)
        self.input_owner=owner if hasattr(owner,'bind_source') else None
        if self.input_owner is not None:self.input_owner.bind_source(clock.source)
        if self.input_owner is not None:self.models.bind_inputs(self.input_owner)

    def advance_before(self,horizon,enabled,*,progress=None):
        blocked=torch.zeros_like(enabled);fault=torch.zeros_like(horizon,dtype=torch.int32)
        count=0;stage=None;blocked_stage=None
        while True:
            event=self.models.next_event()
            observing=torch.ones_like(enabled) if self.trading_until is None else self.next_observation<self.trading_until
            if self.input_owner is not None and not bool(observing.any().cpu()):self.input_owner.stop_observations()
            observe=observing&((~event.present)|(self.next_observation<=event.at))
            at=torch.where(observe,self.next_observation,event.at)
            kind=torch.where(observe,torch.zeros_like(event.kind),event.kind)
            present=observe|event.present
            if self.trading_until is not None:
                economic=self.clock.next_event(enabled&~observing&~blocked)
                resume=self.economic_resume&enabled&~blocked
                economic_at=torch.where(resume,self.clock.gates.public_visible_at,economic.at)
                economic_present=economic.present|resume
                take=~observing&economic_present&(~present|(economic_at<at))
                at=torch.where(take,economic_at,at);kind=torch.where(take,4,kind)
                present |= take;fault |= economic.fault
            due=enabled&~blocked&present&(at<horizon)&(fault==0)
            if not bool(due.any().cpu()):
                boundary=enabled&~blocked
                stage=self.clock.advance_before(horizon,boundary,progress=progress)
                fault |= stage.fault
                complete=boundary&stage.complete&(fault==0)
                self.models.retire_consumed_markets(torch.where(observing,self.next_observation-100,self.clock.gates.public_visible_at))
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
                stage=self.clock.advance_through(at,active,progress=progress) if selected_kind==4 else self.clock.advance_before(at,active,progress=progress)
                fault |= stage.fault
                if bool((active & ~stage.complete).any().cpu()):blocked_stage=stage
                ready=active&stage.complete&(fault==0)
                blocked |= active&~ready
                if selected_kind==0 and bool(ready.any().cpu()):
                    delivery=self.clock.receive_first(enabled=ready)
                    self.clock.public,received=self.receive(self.clock.public,delivery,ready)
                    ready &= received
                    blocked |= active&~ready
                    if self.settlement is not None and bool(ready.any().cpu()):
                        complete,error=self.settlement.advance(self.clock,ready)
                        fault |= error;blocked |= ready&~complete;ready &= complete
                    if bool(ready.any().cpu()):
                        if self.input_owner is not None:
                            self.input_owner.prepare_rows(self.clock,at);self.models.sync_inputs()
                        frozen,accepted=self.freeze(self.clock,at,ready)
                        if self.input_owner is not None:self.models.sync_inputs()
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
                elif selected_kind==4 and bool(ready.any().cpu()):
                    delivery=self.clock.receive_first(enabled=ready)
                    self.clock.public,received=self.receive(self.clock.public,delivery,ready)
                    self.economic_resume=torch.where(ready,~received,self.economic_resume)
                    blocked |= ready&~received
                    if self.settlement is not None and bool(received.any().cpu()):
                        complete,error=self.settlement.advance(self.clock,received)
                        fault |= error;blocked |= received&~complete
            self.models.retire_consumed_markets(torch.where(observing,self.next_observation-100,self.clock.gates.public_visible_at))
            count+=1
            if progress:progress('continuous_event',events=count,kind=selected_kind,
                at=at.detach().cpu().tolist(),active=active.detach().cpu().tolist(),blocked=blocked.detach().cpu().tolist())
