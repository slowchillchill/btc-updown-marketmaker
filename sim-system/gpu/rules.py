"""原封存PublicTradingRuleContext的设备规则，SIM拥有真实成本/执行规则。"""
import torch
from prepare import _numbers, decimal_columns


class PublicRuleGpu:
    def __init__(self, context, *, device='cuda'):
        # 此context由SIM原封存加载器验证后投影；不接受镜像环境或GPU样板默认值。
        if context['version']!='public_trading_rule_context_v1':
            raise ValueError('原公共规则版本错误')
        self.context = context
        self.snapshot_file_sha256 = context['snapshot_file_sha256']
        tokens = [context[name] for name in ('limit_min_quantity_shares','market_buy_min_spend_pusd','fee_rate')]
        raw,offsets = bytearray(),[0]
        for token in tokens:
            raw.extend(token.encode('ascii'));offsets.append(len(raw))
        self.values = decimal_columns(torch.tensor(list(raw),dtype=torch.uint8,device=device),
            torch.tensor(offsets,dtype=torch.int64,device=device),torch.ones(3,dtype=torch.bool,device=device)).rational()
        numbers = _numbers()
        self.delay_ms = numbers.ExactInt.from_ints([context['taker_delay_ms']],device=device)
        self.exponent = numbers.ExactInt.from_ints([context['fee_exponent']],device=device)
        def project():
            tail = torch.zeros(16,device=device)
            fault = torch.zeros(1,dtype=torch.int32,device=device)
            for member,slot in ((0,0),(1,2),(2,10)):
                n = numbers.ExactInt(self.values.numerator.words[member:member+1],self.values.numerator.sign[member:member+1],
                                     self.values.numerator.fault[member:member+1])
                d = numbers.ExactInt(self.values.denominator.words[member:member+1],self.values.denominator.sign[member:member+1],
                                     self.values.denominator.fault[member:member+1])
                value,error = n.project(d,operation=1)
                tail[slot]=value[0];tail[slot+1]=1.0;fault |= error
            for value,denominator,slot in ((self.delay_ms,1000,6),(self.exponent,1,12)):
                projected,error = value.project(value.constant(denominator),operation=1)
                tail[slot]=projected[0];tail[slot+1]=1.0;fault |= error
            for name,slot in (('taker_delay_enabled',4),('fees_enabled',8),('fee_taker_only',14)):
                if type(context[name]) is not bool: raise ValueError('原公共规则开关类型错误')
                tail[slot]=float(context[name]);tail[slot+1]=1.0
            return tail,fault
        self.tail,self.fault = numbers.run_exact_batch(project)
