"""阶段 #6 两图；张量合同见 #5 §14.2.1，特征布局见 #6。"""
from dataclasses import dataclass
from collections.abc import Callable, Sequence

import torch
from torch import Tensor, nn
from .components import ActorScores, BareGRU, FeatureEncoder, ResidualGRU


@dataclass(frozen=True)
class Configuration:
    name: str
    layers: int
    hidden: int
    seed: int
    baseline: bool = False


CONFIGURATIONS = (
    Configuration("BASELINE-1x256", 1, 256, 741001, True),
    Configuration("BASELINE-2x256", 2, 256, 741002, True),
    Configuration("MAIN-1x256", 1, 256, 741003),
    Configuration("MAIN-30x1024", 30, 1024, 741004),
)
STATE_INPUTS = ("fast_features", "slow_features", "h_prev")
STATE_OUTPUTS = ("z_t", "h_next")
ACTION_INPUTS = ("z_t", "rule_features", "prefix_features", "cancel_features",
                 "cancel_mask", "cancel_reduced", "cancel_exists")
ACTION_OUTPUTS = ("family_logits", "quantity_branch_logits", "buy_variant_logits",
                  "price_logits", "post_only_logits", "expiry_logits",
                  "cancel_embeddings", "cancel_logits")


class ActionDecoder(nn.Module):
    def __init__(self, hidden: int):
        super().__init__()
        # 构造次序属于初始化合同；各支路独立权重，对象两遍共享同一编码器。
        self.rule = FeatureEncoder(30, hidden)
        self.prefix = FeatureEncoder(60, hidden)
        self.object_encoder = FeatureEncoder(129, hidden)
        self.summary = FeatureEncoder(3 * hidden // 2, hidden)
        self.actor = ActorScores(hidden)

    def forward(self, z_t: Tensor, rule_features: Tensor, prefix_features: Tensor,
                cancel_features: Tensor, cancel_mask: Tensor,
                cancel_reduced: Tensor, cancel_exists: Tensor) -> tuple[Tensor, ...]:
        objects = self.object_encoder(cancel_features)
        objects = torch.where(cancel_mask.unsqueeze(-1), objects, 0.0)
        summary = torch.where(cancel_exists.unsqueeze(-1), self.summary(cancel_reduced), 0.0)
        scores = self.actor(z_t, self.rule(rule_features), self.prefix(prefix_features), summary, objects)
        return (scores["family"], scores["quantity"], scores["variant"], scores["price"],
                scores["post_only"], scores["expiry"], objects,
                torch.where(cancel_mask, scores["cancel"].squeeze(-1), 0.0))


class TwoGraphs(nn.Module):
    def __init__(self, config: Configuration):
        super().__init__()
        with torch.random.fork_rng(devices=[]):
            torch.manual_seed(config.seed)
            self.state = (BareGRU(836, 412, config.layers) if config.baseline
                          else ResidualGRU(836, 412, config.hidden, config.layers))
            self.action = ActionDecoder(config.hidden)


def reduce_objects(blocks: Sequence[tuple[Tensor, Tensor]]) -> tuple[Tensor, Tensor]:
    """宿主可微归约。块按全局顺序；计数为整数，并列 max 只回传首项。"""
    first = blocks[0][0]
    reduced, exists = [], []
    for row in range(first.shape[0]):
        objects = torch.cat([values[row][mask[row]] for values, mask in blocks], dim=0).double()
        count = objects.shape[0]
        exists.append(count != 0)
        if count:
            total = torch.zeros_like(objects[0])
            for item in objects.unbind(0):
                total = total + item
            reduced.append(torch.cat((total, total / count, objects.max(dim=0).values)).float())
        else:
            reduced.append(first.new_zeros(3 * first.shape[-1]))
    return torch.stack(reduced), torch.tensor(exists, dtype=torch.bool, device=first.device)


def object_blocks(features: Tensor, mask: Tensor, lengths: Sequence[int], *, empty: bool):
    """宿主已知空集合用K=1；非空逻辑块补到256，不读取设备mask判空。"""
    offset = 0
    for length in lengths:
        f = features[:, offset:offset + length]
        m = mask[:, offset:offset + length]
        slots = 1 if empty else 256
        padding = slots - length
        f = torch.cat((f, f.new_zeros((f.shape[0], padding, f.shape[2]))), dim=1)
        m = torch.cat((m, m.new_zeros((m.shape[0], padding))), dim=1)
        yield f, m, length
        offset += length


def encode_all(call: Callable, z: Tensor, rule: Tensor, features: Tensor,
               mask: Tensor, lengths: Sequence[int], *, empty: bool) -> tuple[Tensor, Tensor]:
    prefix = z.new_zeros((z.shape[0], 60))
    prefix[:, 7] = 1  # Family，其他尚未选择。
    reduced = z.new_zeros((z.shape[0], 3 * z.shape[1] // 2))
    exists = torch.zeros(z.shape[0], dtype=torch.bool, device=z.device)
    blocks = [(call(z, rule, prefix, f, m, reduced, exists)[6][:, :length], m[:, :length])
              for f, m, length in object_blocks(features, mask, lengths, empty=empty)]
    return reduce_objects(blocks)


def score_all(call: Callable, z: Tensor, rule: Tensor, prefix: Tensor,
              features: Tensor, mask: Tensor, lengths: Sequence[int],
              reduced: Tensor, exists: Tensor, *, empty: bool) -> list[Tensor]:
    """选族后的完整对象切片；返回各行全部真实分数供一次全局选择/归一化。"""
    outputs = [(call(z, rule, prefix, f, m, reduced, exists)[7][:, :length], m[:, :length])
               for f, m, length in object_blocks(features, mask, lengths, empty=empty)]
    return [torch.cat([v[row][m[row]] for v, m in outputs]) for row in range(z.shape[0])]


def active_rows(call: Callable, rows: Tensor, *inputs: Tensor):
    """只评分宿主传入的活动行；保留原行映射，无活动行不调用图。"""
    if rows.numel() == 0:
        return rows, None
    return rows, call(*(value.index_select(0, rows) for value in inputs))
