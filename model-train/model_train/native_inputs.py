"""原生836/412状态输入与30/60/129参数组件消费；不推导动作支持集。"""
from dataclasses import dataclass

import torch
from torch import Tensor
from .graphs import TwoGraphs, object_blocks, reduce_objects


def hidden_key(value: dict) -> tuple:
    account = value["account_id"]
    return (value["trajectory_id"], tuple(sorted(account.items())), value["condition_id"])


@dataclass
class ComponentGroup:
    states: list[Tensor]
    rules: list[Tensor]
    roles: list[str]
    cancel_features: Tensor
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
                state = self.graphs.state
                layers = len(state.grus) if hasattr(state, "grus") else state.gru.num_layers
                hidden = state.grus[0].hidden_size if hasattr(state, "grus") else state.gru.hidden_size
                self.hidden[key] = parameter.new_zeros((layers, 1, hidden))
            elif kind == "Retire":
                del self.hidden[hidden_key(value["hidden_state_key"])]
            else:
                raise ValueError(f"unknown native hidden operation: {kind}")

    def consume(self, group: dict, *, lengths: list[int] | None = None) -> ComponentGroup:
        """整组逐帧一次状态前向；返回冻结 z_t，供后续参数组件调用。"""
        parameter = next(self.graphs.parameters())
        self._operations(group["pre_frame_hidden_ops"])
        states, rules, roles = [], [], []
        for row in group["rows"]:
            key = hidden_key(row["hidden_state_key"])
            fast = parameter.new_tensor(row["fast"]).reshape(1, 836)
            slow = parameter.new_tensor(row["slow"]).reshape(1, 412)
            z, next_hidden = self.graphs.state(fast, slow, self.hidden[key])
            self.hidden[key] = next_hidden
            states.append(z)
            rules.append(parameter.new_tensor(row["rule"]).reshape(1, 30))
            roles.append(row["role"])
        features, objects, reduced, exists = self.objects(group["cancel"], lengths=lengths)
        self._operations(group["post_frame_hidden_ops"])
        return ComponentGroup(states, rules, roles, features, objects, reduced, exists)

    def objects(self, values: list[list[float]], *, lengths: list[int] | None = None):
        """完整对象人口组件可独立消费，不把缺少前序hidden的截点冒称连续帧。"""
        parameter = next(self.graphs.parameters())
        features = parameter.new_tensor(values).reshape(1, -1, 129)
        count = len(values)
        mask = torch.ones((1, count), dtype=torch.bool, device=parameter.device)
        if lengths is None:
            lengths = [min(256, count - offset) for offset in range(0, count, 256)] or [0]
        if sum(lengths) != count or any(length < 0 or length > 256 for length in lengths):
            raise ValueError("blocks must partition the complete native object population")
        blocks = [(self.graphs.action.object_encoder(f)[:, :length], m[:, :length])
                  for f, m, length in object_blocks(features, mask, lengths, empty=count == 0)]
        reduced, exists = reduce_objects(blocks)
        objects = torch.cat([value for value, _ in blocks], dim=1)
        return features, objects, reduced, exists

    def parameters(self, group: ComponentGroup, row: int, prefix: list[float],
                   *, lengths: list[int] | None = None) -> list[tuple[Tensor, ...]]:
        """实际参数图消费 Rust 精确状态投影；复用该帧 z/rule 和全局对象归约。"""
        if group.roles[row] != "DecisionRequired":
            raise ValueError("ContextOnly has no parameter invocation")
        features = group.cancel_features
        count = features.shape[1]
        lengths = lengths if lengths is not None else [min(256, count - i)
                    for i in range(0, count, 256)] or [0]
        mask = torch.ones((1, count), dtype=torch.bool, device=features.device)
        prefix = features.new_tensor(prefix).reshape(1, 60)
        return [self.graphs.action(group.states[row], group.rules[row], prefix, f, m,
                                  group.cancel_reduced, group.cancel_exists)
                for f, m, _ in object_blocks(features, mask, lengths, empty=count == 0)]

    def cancel_scores(self, group: ComponentGroup, row: int, prefix: list[float],
                      *, lengths: list[int] | None = None) -> list[dict]:
        """原图 raw score；Rust 按原对象族/身份选择，全局归约沿完整人口复用。"""
        count = group.cancel_features.shape[1]
        lengths = lengths if lengths is not None else [min(256, count - i)
                   for i in range(0, count, 256)] or [0]
        outputs = self.parameters(group, row, prefix, lengths=lengths)
        return [{"scores": value[7][0].detach().cpu().tolist(), "valid_len": length}
                for value, length in zip(outputs, lengths)]
