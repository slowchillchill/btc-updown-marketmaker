"""显式逐根业务/熵随机游标；纯候选不修改活动随机流。"""
from dataclasses import dataclass
import torch


@dataclass(frozen=True)
class ActionRandom:
    seed: torch.Tensor  # [R] int64位模式；profile提供，无正式默认。
    counter: torch.Tensor  # [R,4] int64容器中的u32，offset低/高、subsequence低/高。

    def snapshot(self):return ActionRandom(self.seed.clone(),self.counter.clone())

    def fault(self):
        return ((self.counter<0)|(self.counter>0xffffffff)).any(1).to(torch.int32)

    def uniform(self,enabled):
        from prepare import _numbers
        return _numbers().public_extension().policy_uniform(self.seed,self.counter,enabled)

    def advanced(self,used):
        carry=used.to(torch.int64);words=[]
        for index in range(4):
            total=self.counter[:,index]+carry
            words.append(total&0xffffffff);carry=total>>32
        return ActionRandom(self.seed,torch.stack(words,1))


def draw_condition(logits,support,uniform):
    """全局支持的一次逆CDF选择，只有真实多项条件提交游标推进。

    uniform是Philox 32位词的严格(0,1)中点，不依赖全局Torch RNG。
    概率从当前参数产生，singleton沿全局唯一索引选择，不抽取第二条路径。
    """
    fallback=torch.arange(logits.shape[1],device=logits.device)[None]==0
    numerical=torch.where(support.any(1)[:,None],support,fallback)
    probabilities=torch.softmax(logits.masked_fill(~numerical,-torch.inf),1).double()
    cumulative=probabilities.cumsum(1);cumulative=cumulative/cumulative[:,-1:]
    selected=(cumulative>=uniform[:,None])&numerical
    return selected.to(torch.int32).argmax(1)
