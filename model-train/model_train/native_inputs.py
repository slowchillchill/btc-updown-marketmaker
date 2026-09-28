"""原生596/433与六侧全量盘口状态输入与30/36/184参数组件消费；不推导动作支持集。"""
from dataclasses import dataclass, field

import torch
from torch import Tensor
from .graphs import TwoGraphs, object_blocks, reduce_objects, action_stages
from .book import PackedBookInputs


def hidden_key(value: dict) -> tuple:
    account = value["account_id"]
    return (value["trajectory_id"], tuple(sorted(account.items())), value["condition_id"])


@dataclass
class ComponentGroup:
    states: list[Tensor]
    rules: list[Tensor]
    roles: list[str]
    cancel_features: Tensor
    cancel_reduced: Tensor | None
    cancel_exists: Tensor | None
    cancel_embeddings: Tensor | None = None
    rule_contexts: dict = field(default_factory=dict)


class NativeComponentConsumer:
    """Rust决定根操作与帧顺序；Python只执行，不推导当前/旧市场资格。"""
    def __init__(self, graphs: TwoGraphs):
        self.graphs = graphs
        self.hidden: dict[tuple, Tensor] = {}
        self.book_inputs = []
        self.stages = action_stages(graphs.action)

    def _operations(self, operations: list[dict]):
        parameter = next(self.graphs.parameters())
        for operation in operations:
            kind, value = next(iter(operation.items()))
            if kind == "ResetRootHidden":
                self.book_inputs.clear()
                root = (value["trajectory_id"], tuple(sorted(value["account_id"].items())))
                self.hidden = {key: state for key, state in self.hidden.items() if key[:2] != root}
            elif kind == "InitializeZero":
                key = hidden_key(value["hidden_state_key"])
                state = self.graphs.state
                layers, hidden = state.layers, state.hidden_size
                self.hidden[key] = parameter.new_zeros((layers, 1, hidden))
            elif kind == "Retire":
                self.book_inputs.clear()
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
        frames = max((len(g["rows"]) for g in groups), default=0)
        self.book_inputs = self.book_inputs[:frames]
        self.book_inputs.extend(PackedBookInputs() for _ in range(frames - len(self.book_inputs)))
        for frame in range(frames):
            active = [(i, g["rows"][frame]) for i, g in enumerate(groups) if frame < len(g["rows"])]
            keys = [hidden_key(row["hidden_state_key"]) for _, row in active]
            fast = parameter.new_tensor([row["fast"] for _, row in active])
            slow = parameter.new_tensor([row["slow"] for _, row in active])
            book, mask = self.book_inputs[frame].pack([row["book_levels"] for _, row in active], keys, like=parameter)
            z, next_hidden = self.graphs.state(fast, slow, book, mask, torch.cat([self.hidden[k] for k in keys], dim=1))
            for batch, ((index, _), key) in enumerate(zip(active, keys)):
                # 独立存储避免小的活跃行通过view保留整个历史批次。
                self.hidden[key] = next_hidden[:, batch:batch + 1].clone()
                states[index].append(z[batch:batch + 1].clone())
        results = []
        for i, group in enumerate(groups):
            has_decision = any(row["role"] == "DecisionRequired" for row in group["rows"])
            if has_decision and (prepare_parameters is None or prepare_parameters[i]):
                features, reduced, exists, embeddings = self.objects(group["cancel"], lengths=None if lengths is None else lengths[i])
            else:
                # 无参数消费时只保留原特征；不执行对象编码或伪造一个参数行。
                features = parameter.new_tensor(group["cancel"]).reshape(1, -1, 184)
                reduced = exists = embeddings = None
            self._operations(group["post_frame_hidden_ops"])
            results.append(ComponentGroup(states[i],
                [parameter.new_tensor(row["rule"]).reshape(1, 30) for row in group["rows"]],
                [row["role"] for row in group["rows"]], features, reduced, exists, embeddings))
        return results

    def parameters_batch(self, entries):
        """按真实阶段及相同合法价格区间分组，稳定回填原活动行。"""
        if not entries:
            return []
        buckets = {}
        results = [None] * len(entries)
        for i, (group, row, actor) in enumerate(entries):
            support = actor.support()
            if sum(support) == 1:
                values = [group.states[row].new_empty((1,0)) for _ in range(6)]
                values[actor.head_index()] = group.states[row].new_zeros((1,len(support)))
                results[i] = [tuple(values)]
                continue
            indices = tuple(actor.price_indices()) if actor.head_index() == 3 else ()
            buckets.setdefault((actor.head_index(), indices), []).append(i)
        for (head, indices), positions in buckets.items():
            selected = [entries[i] for i in positions]
            if head == 5:
                for i, (group, row, actor) in zip(positions, selected):
                    results[i] = self.parameters(group, row, actor.prefix_features())
                continue
            groups = [entry[0] for entry in selected]
            states = torch.cat([g.states[r] for g, r, _ in selected])
            if head == 0:
                output, contexts = self.stages["action_family"](states,
                    torch.cat([g.rules[r] for g, r, _ in selected]),
                    torch.cat([g.cancel_reduced for g in groups]),
                    torch.cat([g.cancel_exists for g in groups]))
                for b, (g, r, _) in enumerate(selected):
                    g.rule_contexts[r] = contexts[b:b+1]
            else:
                contexts = torch.cat([self._rule(g, r) for g, r, _ in selected])
                prefix = states.new_tensor([a.prefix_features() for _, _, a in selected])
                if head == 3:
                    axis = torch.tensor(indices, dtype=torch.int64, device=states.device)
                    fees = states.new_tensor([a.price_fee_features() for _, _, a in selected])
                    local = self.stages["action_price"](states, contexts, prefix, axis, fees)
                    output = local.new_zeros((len(selected), 999)).index_copy(1, axis, local)
                else:
                    output = self.stages["action_quantity" if head == 1 else "action_variant"](states, contexts, prefix)
            for b, i in enumerate(positions):
                values = [output.new_empty((1, 0)) for _ in range(6)]
                values[head] = output[b:b+1]
                results[i] = [tuple(values)]
        return results

    def _rule(self, group, row):
        if row not in group.rule_contexts:
            # 单项族跳过评分时没有任何后续参数；定向前缀验证可直接编码规则。
            group.rule_contexts[row] = self.graphs.action.rule(group.rules[row])
        return group.rule_contexts[row]

    def objects(self, values: list[list[float]], *, lengths: list[int] | None = None):
        """完整对象人口组件可独立消费，不把缺少前序hidden的截点冒称连续帧。"""
        parameter = next(self.graphs.parameters())
        features = parameter.new_tensor(values).reshape(1, -1, 184)
        count = len(values)
        mask = torch.ones((1, count), dtype=torch.bool, device=parameter.device)
        if lengths is None:
            lengths = [min(256, count - offset) for offset in range(0, count, 256)] or [0]
        elif sum(lengths) != count or any(length < 0 or length > 256 for length in lengths):
            raise ValueError("blocks must partition the complete native object population")
        blocks = [(self.stages["action_objects"](f, m)[:, :length], m[:, :length])
                  for f, m, length in object_blocks(features, mask, lengths, empty=count == 0)]
        reduced, exists = reduce_objects(blocks)
        return features, reduced, exists, torch.cat([v for v, _ in blocks], dim=1)

    def parameters(self, group: ComponentGroup, row: int, prefix: list[float],
                   *, lengths: list[int] | None = None, price_fee_features=None,
                   price_indices=None) -> list[tuple[Tensor, ...]]:
        """评分当前阶段；价格局部位置在此映回原 k−1 轴。"""
        if group.roles[row] != "DecisionRequired":
            raise ValueError("ContextOnly has no parameter invocation")
        z = group.states[row]
        encoded = z.new_tensor(prefix).reshape(1, 36)
        stage = prefix[7:12].index(1)
        head = (0, 1, 2, 3, 5)[stage]
        if head == 0:
            score, rule = self.stages["action_family"](z, group.rules[row], group.cancel_reduced, group.cancel_exists)
            group.rule_contexts[row] = rule
            outputs = [score]
        else:
            rule = self._rule(group, row)
            if head == 5:
                count = group.cancel_features.shape[1]
                lengths = lengths or [min(256, count-i) for i in range(0, count, 256)] or [0]
                if sum(lengths) != count or any(n < 0 or n > 256 for n in lengths):
                    raise ValueError("blocks must partition the complete native object population")
                context = self.stages["action_cancel_context"](z, rule, encoded)
                outputs, offset = [], 0
                for length in lengths:
                    values = group.cancel_embeddings[:, offset:offset+length]
                    mask = torch.ones((1, length), dtype=torch.bool, device=z.device)
                    outputs.append(self.stages["action_cancel"](context, values, mask))
                    offset += length
            elif head == 3:
                indices = torch.tensor(price_indices if price_indices is not None else range(999), dtype=torch.int64, device=z.device)
                fees = (z.new_tensor(price_fee_features).reshape(1, len(indices))
                        if price_fee_features is not None else z.new_zeros((1,len(indices))))
                local = self.stages["action_price"](z, rule, encoded, indices, fees)
                outputs = [local.new_zeros((1,999)).index_copy(1, indices, local)]
            else:
                outputs = [self.stages["action_quantity" if head == 1 else "action_variant"](z, rule, encoded)]
        result = []
        for score in outputs:
            values = [score.new_empty((1, 0)) for _ in range(6)]
            values[head] = score
            result.append(tuple(values))
        return result

    def cancel_scores(self, group: ComponentGroup, row: int, prefix: list[float],
                      *, lengths: list[int] | None = None) -> list[dict]:
        """原图 raw score；Rust 按原对象族/身份选择，全局归约沿完整人口复用。"""
        outputs = self.parameters(group, row, prefix, lengths=lengths)
        count = group.cancel_features.shape[1]
        lengths = lengths if lengths is not None else [min(256, count - i)
                   for i in range(0, count, 256)] or [0]
        return [{"scores": value[5][0].detach().cpu().tolist(), "valid_len": length}
                for value, length in zip(outputs, lengths)]
