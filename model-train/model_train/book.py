"""v5 六侧全量盘口：逐档关联编码与图内 float32 动态配对归约。"""
import torch
from torch import Tensor, nn

BOOK_SIDES = 6
BOOK_WIDTH = 9
BOOK_EMBEDDING = 32
BOOK_SUMMARY = BOOK_SIDES * 3 * BOOK_EMBEDDING


class BookLinear(nn.Linear):
    """相同仿射及配对树；输出通道连续，使每次切片复制完整连续块。"""
    def forward(self, features: Tensor) -> Tensor:
        # 首层直接计算相邻输入对，省掉完整广播乘积后的两次大切片。
        paired = self.in_features // 2 * 2
        values = (features[..., :paired:2].unsqueeze(-1) * self.weight[:, :paired:2].transpose(0, 1)
                  + features[..., 1:paired:2].unsqueeze(-1) * self.weight[:, 1:paired:2].transpose(0, 1))
        if self.in_features % 2:
            tail = features[..., paired:].unsqueeze(-1) * self.weight[:, paired:].transpose(0, 1)
            values = torch.cat((values, tail), dim=-2)
        width = (self.in_features + 1) // 2
        while width > 1:
            paired = width // 2 * 2
            summed = values[..., :paired:2, :] + values[..., 1:paired:2, :]
            values = torch.cat((summed, values[..., paired:, :]), dim=-2) if width % 2 else summed
            width = (width + 1) // 2
        output = values.squeeze(-2)
        return output if self.bias is None else output + self.bias


def pairwise_sum(values: Tensor) -> Tensor:
    """沿档位轴相邻相加，奇数尾项原样保留；补齐零不改变有效配对次序。"""
    if torch.onnx.is_in_onnx_export():
        # 导出边界将这个标记降为标准 ONNX Loop；生产图不需要自定义内核。
        return torch.onnx.ops.symbolic(
            'native_inputs::PairwiseSum', (values,), dtype=values.dtype,
            shape=(values.shape[0], values.shape[1], values.shape[3]), version=1)
    while values.shape[2] > 1:
        paired = values.shape[2] // 2 * 2
        sums = values[:, :, :paired:2] + values[:, :, 1:paired:2]
        values = torch.cat((sums, values[:, :, paired:]), dim=2)
    return values.squeeze(2)


def reduce_levels(values: Tensor, mask: Tensor) -> Tensor:
    """sum/mean/max 各32维；计数为整数，max并列只向首项回传梯度。"""
    count = mask.to(torch.int64).sum(dim=2, keepdim=True)
    total = pairwise_sum(torch.where(mask.unsqueeze(-1), values, 0.0))
    maximum = torch.where(mask.unsqueeze(-1), values, -torch.inf).max(dim=2).values
    maximum = torch.where(count > 0, maximum, 0.0)
    return torch.cat((total, total / count.clamp_min(1), maximum), dim=-1)


class BookEncoder(nn.Module):
    def __init__(self):
        super().__init__()
        self.pm = nn.Sequential(BookLinear(9, 32), nn.SiLU(), BookLinear(32, 32))
        self.binance = nn.Sequential(BookLinear(9, 32), nn.SiLU(), BookLinear(32, 32))

    def forward(self, levels: Tensor, mask: Tensor) -> Tensor:
        values = torch.cat((self.encode_valid(self.pm, levels[:, :4], mask[:, :4]),
                            self.encode_valid(self.binance, levels[:, 4:], mask[:, 4:])), dim=1)
        return reduce_levels(values, mask).flatten(1)

    @staticmethod
    def encode_valid(encoder: nn.Module, levels: Tensor, mask: Tensor) -> Tensor:
        """仅编码有效行；原位恢复后归约，保持六侧顺序及固定配对加法树。"""
        flat = levels.flatten(0, 2)
        indices = mask.flatten().nonzero().squeeze(1)
        encoded = encoder(flat.index_select(0, indices))
        restored = encoded.new_zeros((flat.shape[0], BOOK_EMBEDDING)).index_copy(0, indices, encoded)
        return restored.reshape(*levels.shape[:3], BOOK_EMBEDDING)


def pad_books(rows, *, like: Tensor) -> tuple[Tensor, Tensor]:
    """仅在独立根合批边界派生补齐；原生行保存六个变长数组。"""
    depth = max((len(side) for row in rows for side in row), default=0)
    levels = like.new_zeros((len(rows), 6, max(depth, 1), 9))
    mask = torch.zeros(levels.shape[:3], dtype=torch.bool, device=like.device)
    for batch, row in enumerate(rows):
        for side, values in enumerate(row):
            if values:
                levels[batch, side, :len(values)] = like.new_tensor(values)
                mask[batch, side, :len(values)] = True
    return levels, mask


class PackedBookInputs:
    """只保留不可变原生侧与已发布张量；更新时复制，支持较早前向延后反传。"""
    def __init__(self):
        self.keys = None
        self.sides = None
        self.levels = None
        self.mask = None

    def pack(self, rows, keys, *, like: Tensor) -> tuple[Tensor, Tensor]:
        if not all(hasattr(side, "same_storage") for row in rows for side in row):
            # JSON／普通数组仍走完整打包；不以可变对象地址推断其内容。
            self.__init__()
            return pad_books(rows, like=like)
        depth = max((len(side) for row in rows for side in row), default=0)
        shape = (len(rows), 6, max(depth, 1), 9)
        rebuild = (self.keys != keys or self.levels is None or tuple(self.levels.shape) != shape
                   or self.levels.dtype != like.dtype or self.levels.device != like.device)
        changes = [(b, s) for b, row in enumerate(rows) for s, side in enumerate(row)
                   if rebuild or not side.same_storage(self.sides[b][s])]
        if not changes and not rebuild:
            return self.levels, self.mask
        if rebuild:
            levels = like.new_zeros(shape)
            mask = torch.zeros(shape[:3], dtype=torch.bool, device=like.device)
        else:
            # no_grad 也可能有 detach 别名；从不写入已发布版本。
            levels = self.levels.clone()
            mask = (self.mask.clone() if any(len(rows[b][s]) != len(self.sides[b][s]) for b, s in changes)
                    else self.mask)
        for batch, side in changes:
            source = rows[batch][side]
            levels[batch, side].zero_()
            if rebuild or len(source) != len(self.sides[batch][side]):
                mask[batch, side].fill_(False)
                mask[batch, side, :len(source)] = True
            if source:
                levels[batch, side, :len(source)] = like.new_tensor(source.values())
        self.keys = list(keys)
        self.sides = [tuple(row) for row in rows]
        self.levels, self.mask = levels, mask
        return levels, mask


def lower_pairwise_sum(model):
    """将导出标记展开为 opset18 的标准 Loop/Slice/Add/Concat，保留动态 B/N。"""
    import onnx
    from onnx import helper as h, TensorProto as T
    nodes = []
    for index, node in enumerate(model.graph.node):
        if node.domain != 'native_inputs' or node.op_type != 'PairwiseSum':
            nodes.append(node)
            continue
        prefix = f'book_sum_{index}_'
        def name(n): return prefix + n
        def const(n, values):
            return h.make_node('Constant', [], [name(n)], value=h.make_tensor(name(n), T.INT64, [len(values)], values))
        def op(kind, inputs, outputs, **kwargs):
            return h.make_node(kind, [name(n) for n in inputs], [name(n) for n in outputs], **kwargs)
        constants = [const('axis', [2]), const('zero', [0]), const('one', [1]), const('two', [2]), const('end', [2**63-1])]
        nodes.extend(constants)
        nodes.extend([
            h.make_node('Shape', [node.input[0]], [name('initial_shape')]),
            op('Gather', ['initial_shape','axis'], ['initial_n'], axis=0),
            op('Greater', ['initial_n','one'], ['initial_cond_vector']),
            op('Squeeze', ['initial_cond_vector','zero'], ['initial_cond']),
        ])
        body = [
            op('Shape', ['values'], ['shape']), op('Gather', ['shape','axis'], ['n'], axis=0),
            op('Div', ['n','two'], ['half']), op('Mul', ['half','two'], ['paired']),
            op('Slice', ['values','zero','paired','axis','two'], ['even']),
            op('Slice', ['values','one','paired','axis','two'], ['odd']),
            op('Add', ['even','odd'], ['sums']),
            op('Slice', ['values','paired','end','axis','one'], ['tail']),
            op('Concat', ['sums','tail'], ['next_values'], axis=2),
            op('Shape', ['next_values'], ['next_shape']),
            op('Gather', ['next_shape','axis'], ['next_n'], axis=0),
            op('Greater', ['next_n','one'], ['next_cond_vector']),
            op('Squeeze', ['next_cond_vector','zero'], ['next_cond']),
        ]
        graph = h.make_graph(body, name('loop_body'), [
            h.make_tensor_value_info(name('iteration'), T.INT64, []),
            h.make_tensor_value_info(name('cond'), T.BOOL, []),
            h.make_tensor_value_info(name('values'), T.FLOAT, ['B',6,'N',32]),
        ], [h.make_tensor_value_info(name('next_cond'), T.BOOL, []),
            h.make_tensor_value_info(name('next_values'), T.FLOAT, ['B',6,'next_N',32])])
        nodes.extend([
            h.make_node('Loop', ['',name('initial_cond'),node.input[0]], [name('result')], body=graph),
            h.make_node('Squeeze', [name('result'),name('axis')], list(node.output)),
        ])
    del model.graph.node[:]
    model.graph.node.extend(nodes)
    imports = [op for op in model.opset_import if op.domain != 'native_inputs']
    del model.opset_import[:]
    model.opset_import.extend(imports)
    onnx.checker.check_model(model)
    return model
