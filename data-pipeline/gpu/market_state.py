"""原公开市场字段的设备前缀状态；冷registry不代表当前可见身份或交易资格。"""
from dataclasses import dataclass
import torch
from public_state import PrefixColumn, ObservedPmField


@dataclass(frozen=True)
class PublicMarketFrame:
    admitted: torch.Tensor
    lifecycle: ObservedPmField
    trading: ObservedPmField
    terminal: ObservedPmField
    prefix_fault: torch.Tensor


class PublicMarketState:
    def __init__(self, pm_state, columns):
        self.shared = pm_state
        device = pm_state.slots.device
        self.market_count = len(columns['markets'])
        self.lifecycle_states = columns['lifecycle_states']
        # 原Native只编码outcome 0/1；完整其他身份仍留在公共registry，不改变经济合法域。
        pairs = [[-1,-1] for _ in range(self.market_count)]
        for market,asset,outcome in columns['market_assets']:
            if not (0 <= market < self.market_count and 0 <= asset < len(pm_state.assets) and outcome >= 0):
                raise ValueError('公共市场原资产/outcome坐标错误')
            if outcome < 2 and pairs[market][outcome] == -1: pairs[market][outcome] = asset
        self.asset_pairs = torch.tensor(pairs,dtype=torch.int64,device=device).reshape(-1,2)
        self.times = torch.tensor([[m['window']['start_at_utc_ms'],m['window']['end_at_utc_ms']]
                                   for m in columns['markets']],dtype=torch.int64,device=device).reshape(-1,2)
        def load(name,width):
            values = torch.tensor(columns[name],dtype=torch.int64,device=device).reshape(-1,width)
            setattr(self,name,values)
            return values
        life = load('lifecycle_values',4)
        trading = load('trading_values',8)
        terminal = load('terminal_values',5)
        for p,s,anchor,q in columns['lifecycle_values']:
            if p not in (0,1) or not 0 <= q < 6 or not 0 <= anchor < len(pm_state.anchor_sets) or (p and not 0 <= s < len(self.lifecycle_states)):
                raise ValueError('公开生命周期载荷坐标错误')
        for row in columns['trading_values']:
            if any(v not in (0,1) for v in row[:6]) or not 0 <= row[6] < len(pm_state.anchor_sets) or not 0 <= row[7] < 6:
                raise ValueError('公开交易开关载荷错误')
        for p,winner,payout,anchor,q in columns['terminal_values']:
            if (p not in (0,1) or (p and not 0 <= winner < len(pm_state.assets))
                    or not 0 <= payout < len(columns['terminal_payouts'])
                    or not 0 <= anchor < len(pm_state.anchor_sets) or not 0 <= q < 6):
                raise ValueError('公开终态载荷坐标错误')
        maximum = max(1,max(map(len,columns['terminal_payouts']),default=0))
        payouts = torch.zeros((len(columns['terminal_payouts']),maximum,2),dtype=torch.int64,device=device)
        payouts[:,:,0] = -1
        for index,rows in enumerate(columns['terminal_payouts']):
            for asset,decimal in rows:
                if not (0 <= asset < len(pm_state.assets) and 0 <= decimal < pm_state.decimal_fault.numel()):
                    raise ValueError('公开终态payout身份/精确值错误')
            if rows: payouts[index,:len(rows)] = torch.tensor(rows,dtype=torch.int64,device=device)
        self.payouts = payouts
        for name,value_count in (('market_admitted',2),('market_lifecycle',life.shape[0]),
                                 ('market_trading',trading.shape[0]),('market_terminal',terminal.shape[0])):
            previous = {}
            for target,sequence,group,value in columns[name]:
                if not (0 <= target < self.market_count and 0 <= sequence < columns['field_updates']
                        and 0 <= group < pm_state.group_count and 0 <= value < value_count):
                    raise ValueError('公开市场字段位置错误')
                prior = previous.get(target,(-1,-1))
                if sequence < prior[0] or group < prior[1]:
                    raise ValueError('公开市场字段原顺序倒退')
                previous[target] = (sequence,group)
            setattr(self,name,PrefixColumn(columns[name],self.market_count,device))

    def frame(self,prefixes):
        invalid = ((prefixes < -1) | (prefixes >= self.shared.group_count)).to(torch.int32)
        _,admitted,found,af = self.market_admitted.query(prefixes)
        fields = []
        for name,table in (('market_lifecycle',self.lifecycle_values),('market_trading',self.trading_values),
                           ('market_terminal',self.terminal_values)):
            _,ids,present,fault = getattr(self,name).query(prefixes)
            fields.append(ObservedPmField(table[ids],present,fault | invalid[:,None] | af))
        return PublicMarketFrame(found & admitted.bool(),*fields,invalid)
