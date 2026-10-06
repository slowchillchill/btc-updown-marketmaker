"""原公共PM步骤的设备数量/选择/ready/tick状态；hidden及SIM状态各由原模块拥有。"""
from dataclasses import dataclass
import torch
from prepare import _numbers, decimal_columns


def _take(value, indices):
    cls = _numbers().ExactInt
    return cls(value.words[indices], value.sign[indices], value.fault[indices])


class PrefixColumn:
    def __init__(self, rows, targets, device):
        rows = torch.tensor(rows, dtype=torch.int64).reshape(-1, 4).pin_memory().to(device, non_blocking=True)
        self.rows = rows[torch.argsort(rows[:, 0], stable=True)].contiguous()
        counts = torch.bincount(self.rows[:, 0], minlength=targets)
        self.offsets = torch.cat((counts.new_zeros(1), counts.cumsum(0))).contiguous()

    def query(self, prefixes):
        selected, fault = _numbers().public_extension().prefix_lookup(self.rows, self.offsets, prefixes)
        found = selected >= 0
        # 冷列为空也有同设备占位，不能使缺值伪装成已知零。
        padded = torch.cat((self.rows.new_zeros((1, 4)), self.rows))
        rows = padded[selected + 1]
        return rows[:, :, 1], rows[:, :, 3], found, fault


@dataclass(frozen=True)
class PublicBookFrame:
    quantity: object
    present: torch.Tensor
    initialized: torch.Tensor
    ready: torch.Tensor
    tick_ids: torch.Tensor
    tick_present: torch.Tensor
    fault: torch.Tensor


class PublicPmState:
    """一次冷加载共享外生列；查询只消费设备公共组前缀，不读设备选择回CPU。

    slots包含整块全部精确价位。未来槽位必须present=false，模型使用其mask；
    不把这个数量切片冒充包含quality/BBA/trade/生命周期的完整公共GPU状态。
    """
    def __init__(self, columns, *, device='cuda'):
        if columns['version'] != 'public_pm_device_columns_v1':
            raise ValueError('设备列版本错误')
        self.input_stream_id = columns['input_stream_id']
        self.assets = columns['assets']  # 只在冷身份边界使用。
        assets, slots, groups = len(self.assets), len(columns['slots']), len(columns['groups'])
        self.group_count = groups
        self.slots = torch.tensor(columns['slots'], dtype=torch.int64, device=device).reshape(-1, 3)
        self.groups = torch.tensor(columns['groups'], dtype=torch.int64, device=device).reshape(-1, 3)
        raw, offsets = bytearray(), [0]
        for text in columns['decimals']:
            raw.extend(text.encode('ascii')); offsets.append(len(raw))
        byte_column = torch.tensor(list(raw), dtype=torch.uint8).pin_memory().to(device, non_blocking=True)
        offset_column = torch.tensor(offsets, dtype=torch.int64).pin_memory().to(device, non_blocking=True)
        self.decimals = decimal_columns(byte_column, offset_column,
            torch.ones(len(offsets)-1, dtype=torch.bool, device=device)).rational()
        for name in ('quantities', 'selections', 'resets', 'initialized', 'ready', 'ticks'):
            target_count = slots if name in ('quantities', 'selections') else assets
            previous = {}
            for target, sequence, group, value in columns[name]:
                if not (0 <= target < target_count and 0 <= sequence < columns['original_pm_steps']
                        and 0 <= group < groups):
                    raise ValueError('公共设备列位置越界')
                prior = previous.get(target, (-1, -1))
                if sequence < prior[0] or group < prior[1]:
                    raise ValueError('公共设备列原顺序倒退')
                previous[target] = (sequence, group)
                if name in ('quantities', 'ticks') and not 0 <= value < len(columns['decimals']):
                    raise ValueError('公共设备精确值引用越界')
                if name in ('selections', 'initialized', 'ready') and value not in (0, 1):
                    raise ValueError('公共设备存在标志错误')
            setattr(self, name, PrefixColumn(columns[name], target_count, device))

    def frame(self, prefixes):
        qseq, qid, qfound, qfault = self.quantities.query(prefixes)
        sseq, selected, sfound, sfault = self.selections.query(prefixes)
        reset, _, reset_found, reset_fault = self.resets.query(prefixes)
        iseq, initialized, ifound, ifault = self.initialized.query(prefixes)
        rseq, ready, rfound, rfault = self.ready.query(prefixes)
        tseq, tick, tfound, tfault = self.ticks.query(prefixes)
        asset = self.slots[:, 0]
        reset = torch.where(reset_found, reset, -1)
        recent_quantity = qfound & (qseq >= reset[:, asset])
        present = sfound & (sseq >= reset[:, asset]) & selected.bool()
        # 无数量事实的槽位保留0占位与recent_quantity=false，不生成经济事实。
        qid = torch.where(recent_quantity, qid, 0)
        numbers = _numbers()
        quantity = numbers.ExactRational(_take(self.decimals.numerator, qid.reshape(-1)),
                                          _take(self.decimals.denominator, qid.reshape(-1)))
        asset_fault = reset_fault | ifault | rfault | tfault
        fault = qfault | sfault | asset_fault[:, asset]
        fault = fault | quantity.fault.reshape(qid.shape)
        fault = fault | ((prefixes < -1) | (prefixes >= self.group_count))[:, None].to(torch.int32)
        return PublicBookFrame(quantity, present & recent_quantity,
            ifound & initialized.bool(),
            rfound & ready.bool() & (rseq >= reset), tick, tfound, fault)
