"""原账户/预留/生命周期市场图并集的人口，随成功事务保留。

目录坐标来自共享固定condition字典；不是可见市场或已成交市场的证明。
人口只增不因归零/关闭清除。中途前缀须安装实际原人口，不能猜为Known空集。
"""
from dataclasses import dataclass, replace
import torch


@dataclass(frozen=True)
class ProjectedMarkets:
    conditions: torch.Tensor  # [markets]，稳定原condition字典坐标。
    present: torch.Tensor     # [roots,markets]，三种原市场图的并集。
    known: torch.Tensor
    required_conditions: torch.Tensor  # [roots]，缺物理目录时请求扩展，不提交事务。
    fault: torch.Tensor

    @classmethod
    def open(cls, conditions, present, known):
        roots, count = present.shape
        device = conditions.device
        if (conditions.shape != (count,) or conditions.dtype != torch.int64
                or present.dtype != torch.bool or present.device != device
                or known.shape != (roots,) or known.dtype != torch.bool or known.device != device):
            raise TypeError('市场人口需要同设备固定condition目录、存在位和原前缀已知位')
        duplicates = conditions[:, None] == conditions[None, :]
        duplicates &= ~torch.eye(count, dtype=torch.bool, device=device)
        fault = torch.zeros(roots, dtype=torch.int32, device=device)
        fault |= ((conditions < 0).any() | duplicates.any()).to(torch.int32)
        return cls(conditions, present.clone(), known.clone(), torch.full((roots,), -1, device=device), fault)

    def admit(self, condition, enabled):
        roots = self.present.size(0)
        if (condition.shape != (roots,) or condition.dtype != torch.int64
                or condition.device != self.present.device or enabled.shape != (roots,)
                or enabled.dtype != torch.bool or enabled.device != self.present.device):
            raise TypeError('实际市场写入需要同设备condition[roots]和事务人口')
        same = condition[:, None] == self.conditions[None, :]
        found = same.any(1)
        fault = self.fault | (enabled & (condition < 0)).to(torch.int32)
        ready = ~enabled | (found & (fault == 0))
        return replace(self, present=self.present | (same & enabled[:, None] & ready[:, None]),
            required_conditions=torch.where(self.required_conditions >= 0, self.required_conditions,
                torch.where(enabled & ~found, condition, -1)), fault=fault), ready

    def extended(self, conditions):
        """共享块目录新增身份后迁移人口；已存在身份必须保留，未见市场仍为False。"""
        if conditions.ndim != 1 or conditions.dtype != torch.int64 or conditions.device != self.conditions.device:
            raise TypeError('增长目录需要同设备int64 condition列')
        same = self.conditions[:, None] == conditions[None, :]
        present = (self.present[:, :, None] & same[None, :, :]).any(1)
        candidate = type(self).open(conditions, present, self.known)
        lost = (~same.any(1)).any().to(torch.int32)
        return replace(candidate, fault=self.fault | candidate.fault | lost)

    def coordinated(self, original, committed):
        return replace(self, present=torch.where(committed[:, None], self.present, original.present),
            known=torch.where(committed, self.known, original.known))
