"""原SIM下一义务/完整hidden桶/待处理请求的设备选择。

只读既有模块状态，不把预取游标当已应用前沿。宿主提供合法推进上界与下一
完整源桶/EOF证明；相同毫秒先义务、再hidden、最后请求。公开观察不是闭桶证明。
"""
from dataclasses import dataclass
import torch


@dataclass(frozen=True)
class ScheduledTime:
    at: torch.Tensor
    present: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def absent(cls, reference):
        return cls(torch.zeros_like(reference),torch.zeros_like(reference,dtype=torch.bool),
                   torch.zeros_like(reference,dtype=torch.int32))


@dataclass(frozen=True)
class SimDriveChoice:
    at: torch.Tensor
    phase: torch.Tensor  # Settlement/GTD/Finality/Rebate/Hidden/Mutation原阶段序。
    present: torch.Tensor
    fault: torch.Tensor
    requests: object
    fills: object
    calendars: object

    def enabled(self, phase):
        return self.present & (self.phase==phase) & (self.fault==0)


def next_sim_drive(requests, fills, calendars, hidden, hidden_eof, advance_until, enabled,
                   *, settlement=None, gtd=None):
    """选择每根唯一原阶段；同阶段人口仍由其原(time,seq,ordinal)入口选择。

    settlement/GTD由所属实际义务模块供给，None仅表示本装配无此类义务。
    此函数不安装时钟、应用桶、推进receive门或替代缺失的结算/GTD后端。
    """
    roots=requests.roots
    device=requests.keys.device
    def column(value,dtype):
        if value.shape!=(roots,) or value.dtype!=dtype or value.device!=device:
            raise TypeError('SIM推进时刻/人口须为同设备根列')
    column(advance_until,torch.int64)
    column(enabled,torch.bool)
    column(hidden_eof,torch.bool)
    settlement=ScheduledTime.absent(advance_until) if settlement is None else settlement
    gtd=ScheduledTime.absent(advance_until) if gtd is None else gtd
    for source in (settlement,gtd,hidden):
        column(source.at,torch.int64)
        column(source.present,torch.bool)
        column(source.fault,torch.int32)
    finality_at,_,finality_present=fills.next_obligation()
    rebate_at,_,rebate_present=calendars.next_clock()
    pending=requests.next_pending()
    times=torch.stack((settlement.at,gtd.at,finality_at,rebate_at,hidden.at,pending.effective_at),1)
    population=torch.stack((settlement.present,gtd.present,finality_present,rebate_present,hidden.present,pending.exists),1)
    masked=torch.where(population,times,(1 << 63)-1)
    at=masked.min(1).values
    # INT64_MAX是真实合法时刻；明确人口防空sentinel抢先，并列保持原阶段顺序。
    same=population & (times==at[:,None])
    phase=same.to(torch.int64).argmax(1)
    fault=requests.fault | fills.fault | calendars.maker.fault | calendars.taker.fault
    fault=fault | settlement.fault | gtd.fault | hidden.fault
    source_known=hidden.present | hidden_eof
    present=population.any(1) & source_known & enabled & (at<=advance_until) & (fault==0)
    return SimDriveChoice(at,phase,present,fault,requests,fills,calendars)
