"""原公共参考选择的共享设备前缀状态；旧材料显式缺少准备能力。"""
from dataclasses import dataclass
import torch
from public_state import PrefixColumn, ObservedPmField


@dataclass(frozen=True)
class PublicReferenceFrame:
    global_values: ObservedPmField
    rule: ObservedPmField
    official: ObservedPmField
    temporary: ObservedPmField
    prefix_fault: torch.Tensor


class PublicReferenceState:
    def __init__(self, market_state, columns, *, prepared=None):
        self.market = market_state
        self.shared = market_state.shared
        self.preparation_complete = columns['reference_preparation_complete']
        device = self.shared.slots.device
        for name,width in (('reference_values',4),('reference_rule_values',3),
                           ('reference_official_values',4),('reference_temporary_values',4)):
            setattr(self,name,torch.tensor(columns[name],dtype=torch.int64,device=device).reshape(-1,width) if prepared is None else prepared.tensor(name))
        for name,targets,values in (('global_references',3,self.reference_values),
                ('reference_rules',market_state.market_count,self.reference_rule_values),
                ('reference_official',market_state.market_count,self.reference_official_values),
                ('reference_temporary',market_state.market_count,self.reference_temporary_values)):
            previous = {}
            for target,sequence,group,value in columns[name]:
                if not (0 <= target < targets and 0 <= sequence < columns['field_updates']
                        and 0 <= group < self.shared.group_count and 0 <= value < len(values)):
                    raise ValueError('公共参考列引用越界')
                prior = previous.get(target,(-1,-1))
                if sequence < prior[0] or group < prior[1]: raise ValueError('公共参考原顺序倒退')
                previous[target] = (sequence,group)
            setattr(self,name,PrefixColumn(columns[name],targets,device) if prepared is None else prepared.prefix(name))
        for price,anchor,q,window in columns['reference_values']:
            if not (0 <= price < self.shared.decimal_fault.numel() and 0 <= anchor < len(self.shared.anchor_sets)
                    and 0 <= q < 6 and window in (0,30,60)): raise ValueError('公共全局参考值错误')
        for rule,anchor,q in columns['reference_rule_values']:
            if not (0 <= rule <= 3 and 0 <= anchor < len(self.shared.anchor_sets) and 0 <= q < 6):
                raise ValueError('公共参考规则错误')
        for name in ('reference_official_values','reference_temporary_values'):
            for present,price,anchor,last in columns[name]:
                if not (present in (0,1) and 0 <= price < self.shared.decimal_fault.numel()
                        and 0 <= anchor < len(self.shared.anchor_sets)
                        and (0 <= last < 6 if name=='reference_official_values' else last in (0,30,60))):
                    raise ValueError('公共市场参考值错误')

    def frame(self,prefixes):
        fault = ((prefixes < -1) | (prefixes >= self.shared.group_count)).to(torch.int32)
        # 无能力标记不能冒充已经观察到“无临时参考”；须沿原输入重新准备。
        if not self.preparation_complete: fault = fault | 1
        fields = []
        for name,values in (('global_references',self.reference_values),('reference_rules',self.reference_rule_values),
                            ('reference_official',self.reference_official_values),('reference_temporary',self.reference_temporary_values)):
            _,ids,present,error = getattr(self,name).query(prefixes)
            fields.append(ObservedPmField(values[ids],present,error | fault[:,None]))
        return PublicReferenceFrame(*fields,fault)
