"""参数化特征、状态与 Actor 分数；输入均为调用方提供的张量。"""

import torch
from torch import Tensor, nn


class FeatureEncoder(nn.Sequential):
    """将显式指定宽度的特征编码为 H/2 维表示。"""

    def __init__(self, feature_size: int, hidden_size: int):
        super().__init__(
            nn.LayerNorm(feature_size),
            nn.Linear(feature_size, hidden_size // 2),
            nn.SiLU(),
            nn.Linear(hidden_size // 2, hidden_size // 2),
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
        self.fusion_norm = nn.LayerNorm(hidden_size)
        self.layer_norms = nn.ModuleList(
            nn.LayerNorm(hidden_size) for _ in range(num_layers)
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
            nn.LayerNorm(fast_size + slow_size),
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


class ActorScores(nn.Module):
    """对已编码上下文计算分数；同一参数头可用于任一腿的前缀。"""

    def __init__(self, hidden_size: int):
        super().__init__()
        self.family = nn.Sequential(
            nn.LayerNorm(2 * hidden_size),
            nn.Linear(2 * hidden_size, hidden_size // 2),
            nn.SiLU(),
            nn.Linear(hidden_size // 2, 6),
        )
        self.decoder = nn.Sequential(
            nn.LayerNorm(2 * hidden_size),
            nn.Linear(2 * hidden_size, hidden_size // 2),
            nn.SiLU(),
        )
        self.quantity = nn.Linear(hidden_size // 2, 2)
        self.variant = nn.Linear(hidden_size // 2, 4)
        self.price = nn.Linear(hidden_size // 2, 9999)
        self.post_only = nn.Linear(hidden_size // 2, 2)
        self.expiry = nn.Linear(hidden_size // 2, 3)
        self.cancel = nn.Sequential(
            nn.LayerNorm(hidden_size),
            nn.Linear(hidden_size, hidden_size // 2),
            nn.SiLU(),
            nn.Linear(hidden_size // 2, 1),
        )

    def forward(
        self,
        z_t: Tensor,
        rule_context: Tensor,
        prefix_context: Tensor,
        cancel_summary: Tensor,
        objects: Tensor,
    ) -> dict[str, Tensor]:
        """z_t 为 [B,H]，上下文为 [B,H/2]，objects 为 [B,N,H/2]。"""
        family = self.family(torch.cat((z_t, rule_context, cancel_summary), dim=-1))
        decoder = self.decoder(torch.cat((z_t, rule_context, prefix_context), dim=-1))
        object_decoder = decoder.unsqueeze(1).expand(-1, objects.shape[1], -1)
        return {
            "family": family,
            "quantity": self.quantity(decoder),
            "variant": self.variant(decoder),
            "price": self.price(decoder),
            "post_only": self.post_only(decoder),
            "expiry": self.expiry(decoder),
            "cancel": self.cancel(torch.cat((object_decoder, objects), dim=-1)),
        }
