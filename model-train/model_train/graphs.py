"""native_inputs_v6 / native_actions_v2：状态与动作职责，八张物理图。"""
from dataclasses import dataclass
from collections.abc import Callable, Sequence

import torch
from torch import Tensor, nn
from .components import ActorScores, BareGRU, FeatureEncoder, ResidualGRU
from .book import BookEncoder, BOOK_SUMMARY


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
STATE_INPUTS = ("fast_features", "slow_features", "book_levels", "book_mask", "h_prev")
STATE_OUTPUTS = ("z_t", "h_next")
ACTION_INPUTS = ("z_t", "rule_features", "prefix_features", "cancel_features",
                 "cancel_mask", "cancel_reduced", "cancel_exists", "price_fee_features")
ACTION_OUTPUTS = ("family_logits", "quantity_branch_logits", "buy_variant_logits",
                  "price_logits", "cancel_embeddings", "cancel_logits")


class ObjectFeatureEncoder(FeatureEncoder):
    def forward(self, features: Tensor) -> Tensor:
        # 保持 PyTorch Linear 的二维 addmm 路径，导出为带偏置 Gemm。
        # 仅展平独立对象前导维；参数、float32 运算、逐对象数学与梯度保持。
        shape = features.shape
        encoded = super().forward(features.reshape(-1, shape[-1]))
        return encoded.reshape(*shape[:-1], encoded.shape[-1])


class ActionDecoder(nn.Module):
    def __init__(self, hidden: int):
        super().__init__()
        # 构造次序属于初始化合同；各支路独立权重，对象两遍共享同一编码器。
        self.rule = FeatureEncoder(30, hidden, aligned_linear=True)
        self.prefix = FeatureEncoder(36, hidden, aligned_linear=True)
        self.object_encoder = ObjectFeatureEncoder(184, hidden)
        self.summary = FeatureEncoder(3 * hidden // 2, hidden)
        self.actor = ActorScores(hidden)

    def forward(self, z_t: Tensor, rule_features: Tensor, prefix_features: Tensor,
                cancel_features: Tensor, cancel_mask: Tensor,
                cancel_reduced: Tensor, cancel_exists: Tensor,
                price_fee_features: Tensor | None = None) -> tuple[Tensor, ...]:
        objects = self.object_encoder(cancel_features)
        objects = torch.where(cancel_mask.unsqueeze(-1), objects, 0.0)
        summary = torch.where(cancel_exists.unsqueeze(-1), self.summary(cancel_reduced), 0.0)
        scores = self.actor(z_t, self.rule(rule_features), self.prefix(prefix_features), summary, objects, price_fee_features)
        return (scores["family"], scores["quantity"], scores["variant"], scores["price"], objects,
                torch.where(cancel_mask, scores["cancel"].squeeze(-1), 0.0))


STAGE_INTERFACES = {
    "action_objects": (("cancel_features", "cancel_mask"), ("cancel_embeddings",)),
    "action_family": (("z_t", "rule_features", "cancel_reduced", "cancel_exists"), ("family_logits", "rule_context")),
    "action_quantity": (("z_t", "rule_context", "prefix_features"), ("quantity_branch_logits",)),
    "action_variant": (("z_t", "rule_context", "prefix_features"), ("buy_variant_logits",)),
    "action_price": (("z_t", "rule_context", "prefix_features", "price_indices", "price_fee_features"), ("price_logits",)),
    "action_cancel_context": (("z_t", "rule_context", "prefix_features"), ("decoder_context",)),
    "action_cancel": (("decoder_context", "cancel_embeddings", "cancel_mask"), ("cancel_logits",)),
}


class ObjectGraph(nn.Module):
    def __init__(self, action):
        super().__init__()
        self.encoder = action.object_encoder

    def forward(self, cancel_features, cancel_mask):
        return torch.where(cancel_mask.unsqueeze(-1), self.encoder(cancel_features), 0.0)


class FamilyGraph(nn.Module):
    def __init__(self, action):
        super().__init__()
        self.rule, self.summary, self.head = action.rule, action.summary, action.actor.family

    def forward(self, z_t, rule_features, cancel_reduced, cancel_exists):
        rule = self.rule(rule_features)
        summary = torch.where(cancel_exists.unsqueeze(-1), self.summary(cancel_reduced), 0.0)
        return self.head(torch.cat((z_t, rule, summary), dim=-1)), rule


class ContextGraph(nn.Module):
    def __init__(self, action):
        super().__init__()
        self.prefix, self.decoder = action.prefix, action.actor.decoder

    def forward(self, z_t, rule_context, prefix_features):
        return self.decoder(torch.cat((z_t, rule_context, self.prefix(prefix_features)), dim=-1))


class ParameterGraph(ContextGraph):
    def __init__(self, action, head):
        super().__init__(action)
        self.head = getattr(action.actor, head)

    def forward(self, z_t, rule_context, prefix_features):
        return self.head(super().forward(z_t, rule_context, prefix_features))


class PriceGraph(ContextGraph):
    def __init__(self, action):
        super().__init__(action)
        self.head, self.fee = action.actor.price, action.actor.fee_weight

    def forward(self, z_t, rule_context, prefix_features, price_indices, price_fee_features):
        context = super().forward(z_t, rule_context, prefix_features)
        # 必须在乘法前取行；P 是当前精确合法区间长度。
        weights = self.head.weight.index_select(0, price_indices)
        bias = self.head.bias.index_select(0, price_indices)
        return torch.nn.functional.linear(context, weights, bias) + self.fee(context) * price_fee_features


class CancelGraph(nn.Module):
    def __init__(self, action):
        super().__init__()
        self.head = action.actor.cancel

    def forward(self, decoder_context, cancel_embeddings, cancel_mask):
        context = decoder_context.unsqueeze(1).expand(-1, cancel_embeddings.shape[1], -1)
        return torch.where(cancel_mask, self.head(torch.cat((context, cancel_embeddings), dim=-1)).squeeze(-1), 0.0)


def action_stages(action):
    """包装共享实际参数，不初始化新的头；包装只按调用持有。"""
    return {"action_objects": ObjectGraph(action), "action_family": FamilyGraph(action),
            "action_quantity": ParameterGraph(action, "quantity"),
            "action_variant": ParameterGraph(action, "variant"),
            "action_price": PriceGraph(action), "action_cancel_context": ContextGraph(action),
            "action_cancel": CancelGraph(action)}


class BookState(nn.Module):
    """一帧完整六侧编码后仅推进一次原 GRU，参数图继续复用 z_t。"""
    def __init__(self, config: Configuration):
        super().__init__()
        self.layers, self.hidden_size = config.layers, config.hidden
        # v5 注册顺序：原递推核心（扩展市场宽度）、PM/BN盘口编码器、原参数图。
        self.core = (BareGRU(596 + BOOK_SUMMARY, 433, config.layers) if config.baseline
                     else ResidualGRU(596 + BOOK_SUMMARY, 433, config.hidden, config.layers))
        self.book = BookEncoder()

    def forward(self, fast: Tensor, slow: Tensor, book_levels: Tensor,
                book_mask: Tensor, h_prev: Tensor) -> tuple[Tensor, Tensor]:
        return self.core(torch.cat((fast, self.book(book_levels, book_mask)), dim=-1), slow, h_prev)


class TwoGraphs(nn.Module):
    def __init__(self, config: Configuration):
        super().__init__()
        with torch.random.fork_rng(devices=[]):
            torch.manual_seed(config.seed)
            self.state = BookState(config)
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
    prefix = z.new_zeros((z.shape[0], 36))
    prefix[:, 7] = 1  # Family，其他尚未选择。
    reduced = z.new_zeros((z.shape[0], 3 * z.shape[1] // 2))
    exists = torch.zeros(z.shape[0], dtype=torch.bool, device=z.device)
    blocks = [(call(z, rule, prefix, f, m, reduced, exists)[4][:, :length], m[:, :length])
              for f, m, length in object_blocks(features, mask, lengths, empty=empty)]
    return reduce_objects(blocks)


def score_all(call: Callable, z: Tensor, rule: Tensor, prefix: Tensor,
              features: Tensor, mask: Tensor, lengths: Sequence[int],
              reduced: Tensor, exists: Tensor, *, empty: bool) -> list[Tensor]:
    """选族后的完整对象切片；返回各行全部真实分数供一次全局选择/归一化。"""
    outputs = [(call(z, rule, prefix, f, m, reduced, exists)[5][:, :length], m[:, :length])
               for f, m, length in object_blocks(features, mask, lengths, empty=empty)]
    return [torch.cat([v[row][m[row]] for v, m in outputs]) for row in range(z.shape[0])]


def active_rows(call: Callable, rows: Tensor, *inputs: Tensor):
    """只评分宿主传入的活动行；保留原行映射，无活动行不调用图。"""
    if rows.numel() == 0:
        return rows, None
    return rows, call(*(value.index_select(0, rows) for value in inputs))
