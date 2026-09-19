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
    cancel_embeddings: Tensor | None
    cancel_reduced: Tensor | None
    cancel_exists: Tensor | None


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
        return self.consume_batch([group], lengths=[lengths])[0]

    def consume_batch(self, groups: list[dict], *, lengths=None, prepare_parameters=None) -> list[ComponentGroup]:
        """独立根同一帧序位合批，压缩无帧行；逐根保留原帧序和隐藏操作。"""
        parameter = next(self.graphs.parameters())
        states = [[] for _ in groups]
        for group in groups:
            self._operations(group["pre_frame_hidden_ops"])
        for frame in range(max((len(g["rows"]) for g in groups), default=0)):
            active = [(i, g["rows"][frame]) for i, g in enumerate(groups) if frame < len(g["rows"])]
            keys = [hidden_key(row["hidden_state_key"]) for _, row in active]
            fast = parameter.new_tensor([row["fast"] for _, row in active])
            slow = parameter.new_tensor([row["slow"] for _, row in active])
            z, next_hidden = self.graphs.state(fast, slow, torch.cat([self.hidden[k] for k in keys], dim=1))
            for batch, ((index, _), key) in enumerate(zip(active, keys)):
                # 独立存储避免小的活跃行通过view保留整个历史批次。
                self.hidden[key] = next_hidden[:, batch:batch + 1].clone()
                states[index].append(z[batch:batch + 1].clone())
        results = []
        for i, group in enumerate(groups):
            has_decision = any(row["role"] == "DecisionRequired" for row in group["rows"])
            if has_decision and (prepare_parameters is None or prepare_parameters[i]):
                features, objects, reduced, exists = self.objects(group["cancel"], lengths=None if lengths is None else lengths[i])
            else:
                # 无参数消费时只保留原特征；不执行对象编码或伪造一个参数行。
                features = parameter.new_tensor(group["cancel"]).reshape(1, -1, 129)
                objects = reduced = exists = None
            self._operations(group["post_frame_hidden_ops"])
            results.append(ComponentGroup(states[i],
                [parameter.new_tensor(row["rule"]).reshape(1, 30) for row in group["rows"]],
                [row["role"] for row in group["rows"]], features, objects, reduced, exists))
        return results

    def parameters_batch(self, entries):
        """活动Actor参数步真实B批次；完成行由调用者压缩，输出按原行回填。"""
        if not entries:
            return []
        features = entries[0][0].cancel_features
        counts = [group.cancel_features.shape[1] for group, _, _ in entries]
        results = [[] for _ in entries]
        for offset in range(0, max(max(counts), 1), 256):
            # 空对象行仍参与非撤单头，掩码不会形成候选。
            slots = min(256, max(max(counts) - offset, 1))
            objects = features.new_zeros((len(entries), slots, 129))
            mask = torch.zeros((len(entries), slots), dtype=torch.bool, device=features.device)
            for i, (group, row, _) in enumerate(entries):
                if group.roles[row] != "DecisionRequired":
                    raise ValueError("ContextOnly has no parameter invocation")
                valid = max(0, min(slots, counts[i] - offset))
                objects[i, :valid] = group.cancel_features[0, offset:offset + valid]
                mask[i, :valid] = True
            outputs = self.graphs.action(
                torch.cat([group.states[row] for group, row, _ in entries]),
                torch.cat([group.rules[row] for group, row, _ in entries]),
                features.new_tensor([actor.prefix_features() for _, _, actor in entries]),
                objects, mask,
                torch.cat([group.cancel_reduced for group, _, _ in entries]),
                torch.cat([group.cancel_exists for group, _, _ in entries]),
                features.new_tensor([actor.price_fee_features() for _, _, actor in entries]))
            for i in range(len(entries)):
                results[i].append(tuple(output[i:i + 1] for output in outputs))
        return results

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
                   *, lengths: list[int] | None = None, price_fee_features=None) -> list[tuple[Tensor, ...]]:
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
                                  group.cancel_reduced, group.cancel_exists,
                                  features.new_tensor(price_fee_features).reshape(1,9999) if price_fee_features is not None else features.new_zeros((1,9999)))
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
