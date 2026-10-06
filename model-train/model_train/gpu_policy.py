"""native_inputs_100ms_v1的设备模型与条件概率；不生成交易合法域或经济状态。"""
from dataclasses import dataclass

import torch
from torch import Tensor, nn

from .book import BookEncoder, BOOK_SUMMARY
from .components import AlignedLayerNorm, BareGRU, ResidualGRU
from .graphs import ActionDecoder, action_stages


@dataclass(frozen=True)
class ModelShape100ms:
    layers: int
    hidden: int
    baseline: bool = False


class State100ms(nn.Module):
    def __init__(self, shape: ModelShape100ms):
        super().__init__()
        if shape.layers <= 0 or shape.hidden <= 0 or shape.hidden % 2:
            raise ValueError('模型层数须为正、hidden须为正偶数')
        if shape.baseline and shape.hidden != 256:
            raise ValueError('原BareGRU基线固定hidden=256')
        self.core = (BareGRU(626 + BOOK_SUMMARY, 433, shape.layers) if shape.baseline
                     else ResidualGRU(626 + BOOK_SUMMARY, 433, shape.hidden, shape.layers))
        self.book = BookEncoder()

    def forward(self, fast, slow, book_levels, book_mask, hidden):
        return self.core(torch.cat((fast, self.book(book_levels, book_mask)), dim=-1), slow, hidden)


class Policy100ms(nn.Module):
    """共享状态z，Actor保持原参数头，Critic独立且不回喂Actor。随机种子由批准profile提供。"""
    input_version = 'native_inputs_100ms_v1'

    def __init__(self, shape: ModelShape100ms):
        super().__init__()
        self.shape = shape
        self.state = State100ms(shape)
        self.action = ActionDecoder(shape.hidden)
        self.critic = nn.Sequential(AlignedLayerNorm(shape.hidden),
                                    nn.Linear(shape.hidden, shape.hidden // 2), nn.SiLU(),
                                    nn.Linear(shape.hidden // 2, 1))

    def stages(self):
        return action_stages(self.action)

    def state_and_value(self, fast, slow, book_levels, book_mask, hidden):
        z, next_hidden = self.state(fast, slow, book_levels, book_mask, hidden)
        return z, next_hidden, self.critic(z).squeeze(-1)

    def unroll(self, inputs, hidden, advance_mask):
        """固定时间轴推进全部层；advance_mask只表示实际状态行，ContextOnly须为真。

        root/market/冻结观察绑定由采集器提供。缺行保持原hidden，不伪造状态行；
        角色的策略/价值loss mask由学习器单独持有，不能用它切断ContextOnly梯度。
        """
        fast, slow, books, book_mask = inputs
        states, values = [], []
        for step in range(fast.shape[0]):
            z, candidate, value = self.state_and_value(fast[step], slow[step], books[step], book_mask[step], hidden)
            active = advance_mask[step]
            hidden = torch.where(active[None, :, None], candidate, hidden)
            states.append(torch.where(active[:, None], z, 0.0))
            values.append(torch.where(active, value, 0.0))
        return torch.stack(states), torch.stack(values), hidden

    def learning_unroll(self, burn_inputs, burn_mask, learning_inputs, learning_mask, hidden):
        """每次当前参数学习重算burn-in；burn-in no_grad，学习起点全部层detach。"""
        if burn_inputs[0].shape[0]:
            with torch.no_grad():
                _, _, hidden = self.unroll(burn_inputs, hidden, burn_mask)
        hidden = hidden.detach()
        return self.unroll(learning_inputs, hidden, learning_mask)


@dataclass(frozen=True)
class ConditionalScore:
    choice: Tensor
    log_prob: Tensor
    entropy: Tensor
    fault: Tensor


def score_condition(logits, support, choice, active, support_fault):
    """设备按原全局索引重算一个条件分布。fault须交公共协调器，不能充作正常选择。

    原完整支持由公共GPU业务模块给出。本函数只消费mask，不按浮点金额重建或缩小支持。
    inactive行不生成动作；唯一合法项恰为0 log_prob/entropy并保持零梯度。
    """
    count = support.sum(dim=-1)
    invalid_scores = ((~torch.isfinite(logits)) & support).any(dim=-1)
    fault = support_fault | ((count == 0) & active).to(torch.int32)
    fault = fault | (invalid_scores & active).to(torch.int32) * 2
    inside = (choice >= 0) & (choice < logits.shape[-1])
    safe_choice = choice.clamp(0, logits.shape[-1] - 1)
    chosen_legal = support.gather(1, safe_choice[:, None]).squeeze(1) & inside
    fault = fault | ((~chosen_legal) & active).to(torch.int32) * 4
    # 无效行只为数值计算设占位，最终fault保留；它不能提交业务结果。
    valid_row = active & (fault == 0)
    fallback = torch.arange(logits.shape[-1], device=logits.device)[None] == 0
    numerical_support = torch.where(valid_row[:, None], support, fallback)
    numerical_logits = torch.where(valid_row[:, None], logits, 0.0)
    masked = numerical_logits.masked_fill(~numerical_support, -torch.inf)
    distribution = torch.distributions.Categorical(logits=masked, validate_args=False)
    ell = distribution.log_prob(safe_choice)
    entropy = distribution.entropy()
    ell = torch.where(count == 1, ell * 0.0, ell)
    entropy = torch.where(count == 1, entropy * 0.0, entropy)
    return ConditionalScore(torch.where(valid_row, choice, -1),
                            torch.where(valid_row, ell, 0.0), torch.where(valid_row, entropy, 0.0), fault)


def deterministic_condition(logits, support, active, support_fault):
    """同分沿原全局顺序取首项，不读取设备choice回CPU。"""
    choice = logits.masked_fill(~support, -torch.inf).argmax(dim=-1)
    return score_condition(logits, support, choice, active, support_fault)
