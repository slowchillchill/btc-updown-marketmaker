"""原生412/129输入的组件消费；此处不构造完整ModelFrame或动作支持集。"""
from dataclasses import dataclass
from collections.abc import Callable

import torch
from torch import Tensor
from .graphs import TwoGraphs, object_blocks, reduce_objects


def hidden_key(value: dict) -> tuple:
    account = value["account_id"]
    return (value["trajectory_id"], tuple(sorted(account.items())), value["condition_id"])


@dataclass
class ComponentGroup:
    slow_embeddings: list[Tensor]
    cancel_embeddings: Tensor
    cancel_reduced: Tensor
    cancel_exists: Tensor


class NativeComponentConsumer:
    """Rust决定根操作与帧顺序；Python只执行，不推导当前/旧市场资格。"""
    def __init__(self, graphs: TwoGraphs):
        self.graphs = graphs
        self.hidden: dict[tuple, Tensor] = {}

    def _operations(self, operations: list[dict]):
        parameter = next(self.graphs.parameters())
        for operation in operations:
            kind, value = next(iter(operation.items()))
            if kind == "ResetRootHidden":
                root = (value["trajectory_id"], tuple(sorted(value["account_id"].items())))
                self.hidden = {key: state for key, state in self.hidden.items() if key[:2] != root}
            elif kind == "InitializeZero":
                key = hidden_key(value["hidden_state_key"])
                self.hidden[key] = parameter.new_zeros((len(self.graphs.state.grus), 1,
                                                       self.graphs.state.grus[0].hidden_size))
            elif kind == "Retire":
                del self.hidden[hidden_key(value["hidden_state_key"])]
            else:
                raise ValueError(f"unknown native hidden operation: {kind}")

    def consume(self, group: dict, *, lengths: list[int] | None = None,
                on_frame: Callable[[dict, Tensor, Tensor], Tensor] | None = None) -> ComponentGroup:
        """on_frame在真实完整帧宿主中承接状态推进；组件验收无需填造fast特征。

        每个Rust帧调用一次；参数头直接使用返回对象，不重新消费组或推进隐藏值。
        """
        parameter = next(self.graphs.parameters())
        self._operations(group["pre_frame_hidden_ops"])
        slow = []
        for row in group["rows"]:
            features = parameter.new_tensor(row["slow"]).reshape(1, 412)
            encoded = self.graphs.state.slow_encoder(features)
            slow.append(encoded)
            if on_frame is not None:
                key = hidden_key(row["hidden_state_key"])
                self.hidden[key] = on_frame(row, encoded, self.hidden[key])
        features = parameter.new_tensor(group["cancel"]).reshape(1, -1, 129)
        count = len(group["cancel"])
        mask = torch.ones((1, count), dtype=torch.bool, device=parameter.device)
        if lengths is None:
            lengths = [min(256, count - offset) for offset in range(0, count, 256)] or [0]
        if sum(lengths) != count or any(length < 0 or length > 256 for length in lengths):
            raise ValueError("blocks must partition the complete native object population")
        blocks = [(self.graphs.action.object_encoder(f)[:, :length], m[:, :length])
                  for f, m, length in object_blocks(features, mask, lengths, empty=count == 0)]
        reduced, exists = reduce_objects(blocks)
        objects = torch.cat([value for value, _ in blocks], dim=1)
        self._operations(group["post_frame_hidden_ops"])
        return ComponentGroup(slow, objects, reduced, exists)
