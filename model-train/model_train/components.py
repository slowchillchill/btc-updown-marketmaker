"""参数化特征、状态与 Actor 分数；输入均为调用方提供的张量。"""

import torch
from torch import Tensor, nn


def fixed_sum_last(features: Tensor) -> Tensor:
    """仅展开静态特征轴；相邻两项相加，奇数尾项保留，B/K 不参与归约。"""
    width = features.shape[-1]
    while width > 1:
        paired = width // 2 * 2
        summed = features[..., :paired:2] + features[..., 1:paired:2]
        features = torch.cat((summed, features[..., paired:]), dim=-1) if width % 2 else summed
        width = (width + 1) // 2
    return features


class AlignedLayerNorm(nn.LayerNorm):
    """原中心化方差定义与参数；两个末轴均值使用同一固定加法次序。"""
    def forward(self, features: Tensor) -> Tensor:
        width = self.normalized_shape[0]
        mean = fixed_sum_last(features) / width
        centered = features - mean
        variance = fixed_sum_last(centered * centered) / width
        normalized = centered * torch.rsqrt(variance + self.eps)
        return normalized * self.weight + self.bias


class AlignedLinear(nn.Linear):
    """原仿射参数；按 16 个输出通道分块，末轴使用共同固定加法树。"""

    def forward(self, features: Tensor) -> Tensor:
        blocks = [
            fixed_sum_last(features.unsqueeze(-2) * self.weight[start : start + 16]).squeeze(-1)
            for start in range(0, self.out_features, 16)
        ]
        output = torch.cat(blocks, dim=-1)
        return output if self.bias is None else output + self.bias


class FeatureEncoder(nn.Sequential):
    """将显式指定宽度的特征编码为 H/2 维表示。"""

    def __init__(self, feature_size: int, hidden_size: int, *, aligned_linear: bool = False):
        linear = AlignedLinear if aligned_linear else nn.Linear
        super().__init__(
            AlignedLayerNorm(feature_size),
            linear(feature_size, hidden_size // 2),
            nn.SiLU(),
            linear(hidden_size // 2, hidden_size // 2),
        )


class ResidualGRU(nn.Module):
    """快慢双支路融合及逐层残差 GRU，显式传入和返回全部层状态。"""

    def __init__(
        self, fast_size: int, slow_size: int, hidden_size: int, num_layers: int
    ):
        super().__init__()
        self.fast_encoder = FeatureEncoder(fast_size, hidden_size)
        self.slow_encoder = FeatureEncoder(slow_size, hidden_size)
        self.gate = nn.Linear(hidden_size, hidden_size)
        self.value = nn.Linear(hidden_size, hidden_size)
        self.fusion_norm = AlignedLayerNorm(hidden_size)
        self.layer_norms = nn.ModuleList(
            AlignedLayerNorm(hidden_size) for _ in range(num_layers)
        )
        self.grus = nn.ModuleList(
            nn.GRU(hidden_size, hidden_size) for _ in range(num_layers)
        )

    def forward(
        self, fast: Tensor, slow: Tensor, h_prev: Tensor
    ) -> tuple[Tensor, Tensor]:
        """输入 [B,F_fast]、[B,F_slow]、[L,B,H]，返回 z_t 与 h_next。"""
        features = torch.cat((self.fast_encoder(fast), self.slow_encoder(slow)), dim=-1)
        gate = torch.sigmoid(self.gate(features))
        z = self.fusion_norm(gate * features + (1 - gate) * self.value(features))
        next_states = []
        for index, (norm, gru) in enumerate(zip(self.layer_norms, self.grus)):
            update, state = gru(norm(z).unsqueeze(0), h_prev[index : index + 1])
            z = z + update.squeeze(0)
            next_states.append(state)
        return z, torch.cat(next_states, dim=0)


class BareGRU(nn.Module):
    """固定 H=256 的裸 GRU 基线，不含融合门或纵向残差。"""

    def __init__(self, fast_size: int, slow_size: int, num_layers: int):
        super().__init__()
        self.encoder = nn.Sequential(
            AlignedLayerNorm(fast_size + slow_size),
            nn.Linear(fast_size + slow_size, 256),
            nn.SiLU(),
        )
        self.gru = nn.GRU(256, 256, num_layers=num_layers)

    def forward(
        self, fast: Tensor, slow: Tensor, h_prev: Tensor
    ) -> tuple[Tensor, Tensor]:
        """输入 [B,F_fast]、[B,F_slow]、[L,B,256]，返回顶层输出与全部状态。"""
        features = self.encoder(torch.cat((fast, slow), dim=-1))
        output, h_next = self.gru(features.unsqueeze(0), h_prev)
        return output.squeeze(0), h_next


class ScalarLinear(nn.Linear):
    """同一标量仿射参数；显式末轴求和使相同对象行共享同一计算表达。"""
    def __init__(self, in_features: int):
        super().__init__(in_features, 1)

    def forward(self, features: Tensor) -> Tensor:
        return fixed_sum_last(features * self.weight) + self.bias


class ActorScores(nn.Module):
    """对已编码上下文计算分数；同一参数头可用于任一腿的前缀。"""

    def __init__(self, hidden_size: int):
        super().__init__()
        self.family = nn.Sequential(
            AlignedLayerNorm(2 * hidden_size),
            nn.Linear(2 * hidden_size, hidden_size // 2),
            nn.SiLU(),
            nn.Linear(hidden_size // 2, 6),
        )
        self.decoder = nn.Sequential(
            AlignedLayerNorm(2 * hidden_size),
            AlignedLinear(2 * hidden_size, hidden_size // 2),
            nn.SiLU(),
        )
        self.quantity = nn.Linear(hidden_size // 2, 16)
        self.variant = nn.Linear(hidden_size // 2, 2)
        self.price = nn.Linear(hidden_size // 2, 999)
        self.cancel = nn.Sequential(
            AlignedLayerNorm(hidden_size),
            AlignedLinear(hidden_size, hidden_size // 2),
            nn.SiLU(),
            ScalarLinear(hidden_size // 2),
        )
        self.fee_weight = nn.Linear(hidden_size // 2, 1, bias=False)

    def forward(
        self,
        z_t: Tensor,
        rule_context: Tensor,
        prefix_context: Tensor,
        cancel_summary: Tensor,
        objects: Tensor,
        price_fee_features: Tensor | None = None,
    ) -> dict[str, Tensor]:
        """z_t 为 [B,H]，上下文为 [B,H/2]，objects 为 [B,N,H/2]。"""
        family = self.family(torch.cat((z_t, rule_context, cancel_summary), dim=-1))
        decoder = self.decoder(torch.cat((z_t, rule_context, prefix_context), dim=-1))
        object_decoder = decoder.unsqueeze(1).expand(-1, objects.shape[1], -1)
        return {
            "family": family,
            "quantity": self.quantity(decoder),
            "variant": self.variant(decoder),
            "price": self.price(decoder) + (self.fee_weight(decoder) * price_fee_features
                                             if price_fee_features is not None else 0.0),
            "cancel": self.cancel(torch.cat((object_decoder, objects), dim=-1)),
        }
