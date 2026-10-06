"""原SIM动态实体身份的设备字节序；固定账户/Trade命名空间由根目录绑定。

当前仅生成原source_trade_id同前缀后的u64十进制排序字段，不创建新身份或hash。
全u64保持无损；该格式不能用于任意LIVE外部字符串或跨命名空间比较。
"""
from dataclasses import dataclass
import torch


@dataclass(frozen=True)
class SimTradeOrderKey:
    bytes: torch.Tensor
    fault: torch.Tensor


def sim_trade_order_key(sequence):
    # sequence为公共ExactInt，与其它数值共用原扩池/设备stream。
    from queues import Numbers
    keys,fault=Numbers.public_extension().sim_trade_key(sequence.words,sequence.sign)
    return SimTradeOrderKey(keys,fault | sequence.fault)
