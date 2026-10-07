"""原Native整窗逐笔77字段；只消费原规范成交事实和合法前缀。"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from public_state import _take
from input_events import _extreme, time_envelope


@dataclass(frozen=True)
class NativeTradeWindow:
    values: torch.Tensor  # 原fast[258:335]：UP28、DOWN28、Binance21。
    fault: torch.Tensor
    required: torch.Tensor
    enough: torch.Tensor


def _sum(value, present):
    numbers = _numbers()
    roots,width = present.shape
    value = value.select(present.reshape(-1),value.constant(0))
    while width > 1:
        indices = torch.arange(roots*width,device=present.device).reshape(roots,width)
        left,right = indices[:,0::2],indices[:,1::2]
        a = _take(value,left.reshape(-1))
        if left.shape[1] != right.shape[1]:
            # 最后一个有效元素只加零，不重复消费。
            right = torch.cat((right,left[:,-1:]),dim=1)
            valid_right = torch.ones_like(right,dtype=torch.bool)
            valid_right[:,-1] = False
            b = _take(value,right.reshape(-1))
            b = b.select(valid_right.reshape(-1),b.constant(0))
        else:
            b = _take(value,right.reshape(-1))
        value = a+b
        width = left.shape[1]
    return value


class TradeInputs:
    def __init__(self, state, public_window):
        self.state, self.window = state, public_window
        numbers = _numbers()
        # 原SourceDecimal96、scale<=28的公共分母；不会给经济状态规定固定域。
        def units():
            source = state.decimal_columns
            factor = source.coefficient.constant(1)
            for exponent in range(1,29):
                factor = factor.constant(10**exponent).select(28-source.scale == exponent,factor)
            return source.coefficient*factor
        self.units = numbers.run_exact_batch(units)

    def encode(self, previous_prefix, current_prefix, asset_indices, observation_ms):
        roots = current_prefix.numel()
        if (asset_indices.shape != (roots,2) or asset_indices.dtype != torch.int64
                or asset_indices.device != current_prefix.device or observation_ms.shape != (roots,)
                or observation_ms.dtype != torch.int64 or observation_ms.device != current_prefix.device):
            raise ValueError('需同设备int64市场资产/观察时间')
        window = self.window.query(previous_prefix,current_prefix)
        rows,present,prefix_fault = window.rows,window.present,window.fault
        valid_assets = (asset_indices >= 0) & (asset_indices < self.window.asset_count)
        base_fault = prefix_fault | (~valid_assets.all(dim=1)).to(torch.int32)
        numbers = _numbers()
        width = rows.shape[1]
        def compute():
            result = torch.zeros((roots,77),device=current_prefix.device)
            fault = base_fault
            p = _take(self.units,rows[:,:,2].reshape(-1))
            q = _take(self.units,rows[:,:,4].reshape(-1))
            fee = _take(self.units,rows[:,:,7].reshape(-1))
            ordinal = torch.arange(width,device=current_prefix.device)[None].expand(roots,-1)
            for leg in range(3):
                mask = present & ((rows[:,:,1] == asset_indices[:,leg,None]) & valid_assets[:,leg,None]
                                  if leg < 2 else rows[:,:,1] == -1)
                n = mask.sum(dim=1)
                count = numbers.ExactInt.from_i64_tensor(n)
                denominator = numbers.ExactInt.from_i64_tensor(n.clamp_min(1))
                start = leg*28 if leg < 2 else 56
                out = result[:,start:start+(28 if leg < 2 else 21)]
                def store(index,value,denominator_value=None,*,pm=False,known=None):
                    nonlocal fault
                    denominator_value = value.constant(1) if denominator_value is None else denominator_value
                    if pm:
                        value = value.constant(2)*value-denominator_value
                    projected,error = value.project(denominator_value,operation=0 if pm else 1)
                    out[:,index] = torch.where((n>0) if known is None else known,projected,0.0)
                    fault = fault | error
                store(0,count)
                first = torch.where(mask,ordinal,width).min(dim=1).values.clamp_max(width-1)
                last = torch.where(mask,ordinal,-1).max(dim=1).values.clamp_min(0)
                batch = torch.arange(roots,device=current_prefix.device)*width
                low,_,lf = _extreme(p,mask,maximum=False)
                high,_,hf = _extreme(p,mask,maximum=True)
                fault = fault | lf | hf
                price_sum = _sum(p,mask)
                prices = (_take(p,batch+first),_take(p,batch+last),low,high,price_sum)
                for member,value in enumerate(prices):
                    divisor = value.constant(10**28)*(denominator if member==4 else value.constant(1))
                    store(1+member,value,divisor,pm=leg<2)
                known_q = mask & rows[:,:,3].bool()
                qty = _sum(q,known_q)
                amount = _sum(p*q,known_q)
                store(6,qty,qty.constant(10**28))
                if leg < 2:
                    fraction,pf = numbers.ExactInt.from_i64_tensor(known_q.sum(dim=1)).project(denominator)
                    out[:,7] = torch.where(n>0,fraction,0.0); fault = fault | pf
                    store(8,amount,amount.constant(10**56))
                    for side in range(4):
                        side_count = (mask & (rows[:,:,5]==side)).sum(dim=1)
                        store(9+side,numbers.ExactInt.from_i64_tensor(side_count))
                    for side in range(2):
                        side_mask = mask & (rows[:,:,5]==side)
                        side_known = side_mask & rows[:,:,3].bool()
                        sq = _sum(q,side_known)
                        store(13+side,sq,sq.constant(10**28))
                        a = numbers.ExactInt.from_i64_tensor(side_known.sum(dim=1))
                        b = numbers.ExactInt.from_i64_tensor(side_mask.sum(dim=1).clamp_min(1))
                        value,error = a.project(b)
                        out[:,15+side] = value; fault = fault | error
                    fee_mask = mask & rows[:,:,6].bool()
                    known_fee = fee_mask.sum(dim=1)
                    total_fee = _sum(fee,fee_mask)
                    divisor = total_fee.constant(10**28)*numbers.ExactInt.from_i64_tensor(known_fee.clamp_min(1))
                    store(17,total_fee,divisor,known=known_fee>0)
                    value,error = numbers.ExactInt.from_i64_tensor(known_fee).project(denominator)
                    out[:,18] = torch.where(n>0,value,0.0); fault = fault | error
                    time_start = 19
                else:
                    store(7,amount,amount.constant(10**56))
                    for side in range(2):
                        side_mask = mask & (rows[:,:,5]==side)
                        store(8+side,numbers.ExactInt.from_i64_tensor(side_mask.sum(dim=1)))
                        sq = _sum(q,side_mask)
                        store(10+side,sq,sq.constant(10**28))
                    time_start = 12
                envelope,ef = time_envelope(rows[:,:,9],rows[:,:,10],mask & rows[:,:,8].bool(),mask,observation_ms)
                out[:,time_start:] = envelope
                fault = fault | ef
            return NativeTradeWindow(result,fault,window.required,window.enough)
        return numbers.run_exact_batch(compute,initial_word_capacity=self.units.words.shape[1])
