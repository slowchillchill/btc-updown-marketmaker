"""原Reward合同的精确模拟时间面积与末端奖励；不写Ledger或学习器。"""
from dataclasses import dataclass,replace
import torch
from .gpu_exact import ExactInt,ExactRational


def _owned(value):
    def integer(column):return ExactInt(column.words.clone(),column.sign.clone(),column.fault.clone())
    return ExactRational(integer(value.numerator),integer(value.denominator))


def _select(value,mask,old):
    return ExactRational(value.numerator.select(mask,old.numerator),value.denominator.select(mask,old.denominator))


def _check_time(at,reference):
    if at.shape!=reference.shape or at.ndim!=1 or at.dtype!=torch.int64 or at.device!=reference.device:
        raise TypeError('区间时间须同根同设备int64实际模拟UTC毫秒')


def _check_mask(mask,reference):
    if mask.shape!=reference.shape or mask.dtype!=torch.bool or mask.device!=reference.device:
        raise TypeError('人口/known须同根同设备bool')


def _check_amount(amount,reference):
    for integer in (amount.numerator,amount.denominator):
        if integer.sign.shape!=reference.shape or integer.words.device!=reference.device:
            raise TypeError('精确金额须同根同设备')


def _check_portfolio(portfolio,reference):
    if portfolio.known.shape!=(reference.numel(),4) or portfolio.known.device!=reference.device or portfolio.known.dtype!=torch.bool:
        raise TypeError('需要同根公共WCM读面')
    _check_amount(portfolio.amounts[0],reference)


@dataclass(frozen=True)
class IntervalReward:
    exact: ExactRational
    value: torch.Tensor
    dt_seconds: torch.Tensor
    present: torch.Tensor
    known: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class RewardEndpoint:
    at_ms: torch.Tensor
    nav: ExactRational
    area_pusd_ms: ExactRational
    nav_known: torch.Tensor
    area_known: torch.Tensor
    fault: torch.Tensor


def interval_between(left,right,a_pusd,present):
    """同一原累计记忆的两个真实公开端点；不重新积分或修改端点。"""
    _check_time(right.at_ms,left.at_ms);_check_mask(present,left.at_ms)
    _check_amount(a_pusd,left.at_ms)
    dt=ExactInt.from_i64_tensor(right.at_ms)-ExactInt.from_i64_tensor(left.at_ms)
    exact=ExactRational(dt.constant(10000),dt.constant(2000))*(right.nav-left.nav-
        a_pusd*(right.area_pusd_ms-left.area_pusd_ms)*ExactRational(dt.constant(1),dt.constant(6_000_000)))
    value,error=exact.project();seconds,time_error=dt.project(dt.constant(1000))
    fault=left.fault|right.fault|torch.where(present,dt.fault|exact.fault|error|time_error|a_pusd.fault|
        (dt.sign<0).to(torch.int32)|(a_pusd.numerator.sign<=0).to(torch.int32),0)
    known=present&left.nav_known&right.nav_known&left.area_known&right.area_known&(fault==0)
    return IntervalReward(exact,value,seconds,present.clone(),known,fault)


@dataclass(frozen=True)
class RewardIntervalMemory:
    opened_at_ms: torch.Tensor
    last_at_ms: torch.Tensor
    initial_nav: ExactRational
    initial_nav_known: torch.Tensor
    exposure: ExactRational
    exposure_known: torch.Tensor
    area_pusd_ms: ExactRational
    area_known: torch.Tensor
    fault: torch.Tensor

    @classmethod
    def open(cls,at_ms,portfolio,exposure):
        """实际区间起点拥有少量跨调用值；不是活动账户checkpoint。"""
        _check_time(at_ms,exposure.known);_check_mask(exposure.known,at_ms)
        _check_portfolio(portfolio,at_ms);_check_amount(exposure.amount,at_ms)
        zero=ExactInt.from_i64_tensor(torch.zeros_like(at_ms))
        return cls(at_ms.clone(),at_ms.clone(),_owned(portfolio.amounts[0]),portfolio.known[:,0].clone(),
            _owned(exposure.amount),exposure.known.clone(),ExactRational(zero,zero.constant(1)),
            torch.ones_like(exposure.known),portfolio.fault.clone()|exposure.fault)

    def snapshot(self):
        """图外发布跨调用的小状态；下一capture不能覆盖已保留的起点/面积/E。"""
        return replace(self,**{name:(_owned(value) if isinstance(value,ExactRational) else value.clone())
            for name,value in ((name,getattr(self,name)) for name in self.__dataclass_fields__)})

    def _integrate(self,at_ms,enabled,incoming_fault):
        _check_time(at_ms,self.last_at_ms);_check_mask(enabled,at_ms)
        elapsed=ExactInt.from_i64_tensor(at_ms)-ExactInt.from_i64_tensor(self.last_at_ms)
        fault=self.fault|torch.where(enabled,elapsed.fault|incoming_fault|(elapsed.sign<0).to(torch.int32),0)
        commit=enabled&(fault==0)
        duration=elapsed.select(commit,elapsed.constant(0))
        area=self.area_pusd_ms+self.exposure*ExactRational(duration,duration.constant(1))
        fault=fault|torch.where(commit,area.fault,0);commit=commit&(fault==0)
        known=self.area_known&((duration.sign==0)|self.exposure_known)
        return replace(self,last_at_ms=torch.where(commit,at_ms,self.last_at_ms),
            area_pusd_ms=_select(area,commit,self.area_pusd_ms),
            area_known=torch.where(commit,known,self.area_known),fault=fault),commit

    def observe(self,at_ms,exposure,enabled):
        """实际公开变化先积分旧E，再接新E；迟到事实从实际可见时刻起作用。

        enabled来自本次实际公开提交。候选和Numbers fits必须共同接纳；资源退出
        不安装本候选，沿原memory/同输入增长重试。日期/PPO切片不调用open重置。
        """
        _check_amount(exposure.amount,self.last_at_ms);_check_mask(exposure.known,self.last_at_ms)
        integrated,commit=self._integrate(at_ms,enabled,exposure.fault)
        return replace(integrated,exposure=_select(exposure.amount,commit,self.exposure),
            exposure_known=torch.where(commit,exposure.known,self.exposure_known))

    def endpoint(self,at_ms,portfolio,enabled):
        """冻结同前缀端点前积分至真实Ck；只拥有当前未消费批需要的事实。"""
        _check_portfolio(portfolio,self.last_at_ms)
        integrated,_=self._integrate(at_ms,enabled,portfolio.fault)
        endpoint=RewardEndpoint(at_ms.clone(),_owned(portfolio.amounts[0]),_owned(integrated.area_pusd_ms),
            portfolio.known[:,0].clone(),integrated.area_known.clone(),integrated.fault.clone()|portfolio.fault)
        return integrated,endpoint

    def close(self,at_ms,portfolio,a_pusd,enabled):
        """纯计算区间训练分及可选后继memory；调用者决定是否安装后继。

        计算本身不归属学习样本；静默尾段仍积分。

        C0=2000固定。a是显式正pUSD参数，无正式默认。先精确结合ΔN与
        a*面积/(100pUSD*60s)，再一次末端投影；不裁剪或分别标准化两项。
        Unknown真实区间保持present，不静默删样本。未来collector和optimizer
        须消费known/fault及共同资源门，本组件不实现更新屏障或消费一次。
        """
        _check_portfolio(portfolio,self.last_at_ms);_check_amount(a_pusd,self.last_at_ms)
        integrated,_=self._integrate(at_ms,enabled,self.fault)
        dt=ExactInt.from_i64_tensor(at_ms)-ExactInt.from_i64_tensor(self.opened_at_ms)
        zero=dt.constant(0);one=dt.constant(1)
        normalization=ExactRational(dt.constant(10000),dt.constant(2000))
        area_scale=ExactRational(one,dt.constant(6_000_000))  # pUSD·毫秒→原ΔX。
        exact=normalization*(portfolio.amounts[0]-self.initial_nav-a_pusd*integrated.area_pusd_ms*area_scale)
        value,error=exact.project()
        dt_seconds,time_error=dt.project(dt.constant(1000))
        fault=integrated.fault|torch.where(enabled,portfolio.fault|a_pusd.fault|exact.fault|error|time_error|
            (a_pusd.numerator.sign<=0).to(torch.int32),0)
        known=enabled&self.initial_nav_known&portfolio.known[:,0]&integrated.area_known&(fault==0)
        commit=enabled&(fault==0)
        next_memory=replace(integrated,opened_at_ms=torch.where(commit,at_ms,self.opened_at_ms),
            initial_nav=_select(portfolio.amounts[0],commit,self.initial_nav),
            initial_nav_known=torch.where(commit,portfolio.known[:,0],self.initial_nav_known),
            area_pusd_ms=_select(ExactRational(zero,one),commit,integrated.area_pusd_ms),
            area_known=torch.where(commit,torch.ones_like(known),integrated.area_known),fault=fault)
        return next_memory,IntervalReward(exact,value,dt_seconds,enabled.clone(),known,fault)
