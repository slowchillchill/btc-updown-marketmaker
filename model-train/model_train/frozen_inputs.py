"""同cutoff公共市场/账户编码的自有模型输入；不保留活动Ledger引用。"""
from dataclasses import dataclass
import torch


@dataclass(frozen=True)
class FrozenPolicyInputs100ms:
    fast: torch.Tensor
    slow: torch.Tensor
    book_levels: torch.Tensor
    book_mask: torch.Tensor
    role: torch.Tensor
    identities: torch.Tensor  # [rows,3]：trajectory/account/condition的无损字典坐标。
    cutoff: torch.Tensor
    provided: torch.Tensor
    fault: torch.Tensor
    resources_fit: torch.Tensor

    @classmethod
    def publish(cls, market, account, identities, exact_fits):
        """图外发布：即时编码后只拥有模型实际消费值，不复制账本历史。

        调用方负责将同stream的市场和账户候选放在共同资源门内。Unknown金额
        仍按Native三槽编码；provided仅表示字段能力，并非金额/历史已知证明。
        冻结不绑定策略版本；版本在模型真正开始计算时绑定。
        """
        rows=market.fast.shape[0];device=market.fast.device
        if (account.slow.shape!=(rows,433) or account.provided.shape!=(rows,433)
                or identities.shape!=(rows,3) or identities.dtype!=torch.int64
                or identities.device!=device or exact_fits.shape!=()
                or exact_fits.dtype!=torch.bool or exact_fits.device!=device):
            raise TypeError('需同设备完整Native账户、身份坐标与共同精确容量结果')
        fault=market.fault|account.fault
        fault|=(account.cutoff!=market.endpoint_memory.previous_observation_ms).to(torch.int32)
        fault|=(identities<0).any(1).to(torch.int32)
        return cls(market.fast.clone(),account.slow.clone(),market.book_levels.clone(),
            market.book_mask.clone(),market.role.clone(),identities.clone(),account.cutoff.clone(),
            account.provided.clone(),fault.clone(),(market.resources_fit&exact_fits).clone())

    def rows(self, indices):
        return type(self)(**{name:value[indices] for name,value in vars(self).items()})

    def model_inputs(self):
        return self.fast,self.slow,self.book_levels,self.book_mask


@dataclass(frozen=True)
class FrozenPolicyEvaluation:
    state: torch.Tensor
    value: torch.Tensor
    hidden: torch.Tensor
    policy_version: torch.Tensor
    ready: torch.Tensor
    fault: torch.Tensor
