"""原实际Binance深度快照/绝对增量与成交的共享GPU前缀状态。"""
import torch
from public_state import PrefixColumn, PublicBookFrame, ObservedPmField, _take
from prepare import _numbers


class PublicBinanceState:
    def __init__(self, shared, columns):
        self.shared = shared
        self.assets = columns['binance_symbols']  # 实际symbol身份，不是PM资产。
        self.slots = torch.tensor(columns['binance_slots'],dtype=torch.int64,device=shared.slots.device).reshape(-1,3)
        self.group_count = shared.group_count
        self.decimal_columns,self.decimals,self.decimal_fault = shared.decimal_columns,shared.decimals,shared.decimal_fault
        self.anchors,self.anchor_times = shared.anchors,shared.anchor_times
        self.unique_anchor_ids = shared.unique_anchor_ids
        for symbol,side,price in columns['binance_slots']:
            if not (0 <= symbol < len(self.assets) and side in (0,1) and 0 <= price < self.decimal_fault.numel()):
                raise ValueError('Binance原slot坐标错误')
        for name,width in (('binance_depth_values',3),('binance_trade_values',5)):
            setattr(self,name,torch.tensor(columns[name],dtype=torch.int64,device=self.slots.device).reshape(-1,width))
        for known,anchor,q in columns['binance_depth_values']:
            if not (known in (0,1) and 0 <= anchor < len(shared.anchor_sets) and 0 <= q < 6):
                raise ValueError('Binance深度载荷错误')
        for price,quantity,maker,anchor,q in columns['binance_trade_values']:
            if not (0 <= price < self.decimal_fault.numel() and 0 <= quantity < self.decimal_fault.numel()
                    and maker in (0,1) and 0 <= anchor < len(shared.anchor_sets) and 0 <= q < 6):
                raise ValueError('Binance成交载荷错误')
        for name,targets,count in (('binance_quantities',len(self.slots),self.decimal_fault.numel()),
                                  ('binance_resets',len(self.assets),1),
                                  ('binance_depth',len(self.assets),len(self.binance_depth_values)),
                                  ('binance_trades',len(self.assets),len(self.binance_trade_values))):
            previous = {}
            for target,sequence,group,value in columns[name]:
                if not (0 <= target < targets and 0 <= sequence < columns['field_updates']
                        and 0 <= group < self.group_count and 0 <= value < count):
                    raise ValueError('Binance原前缀列坐标错误')
                prior = previous.get(target,(-1,-1))
                if sequence < prior[0] or group < prior[1]: raise ValueError('Binance原前缀顺序倒退')
                previous[target] = (sequence,group)
            setattr(self,name,PrefixColumn(columns[name],targets,self.slots.device))

    def observation(self,prefixes):
        _,ids,present,fault = self.binance_trades.query(prefixes)
        invalid = ((prefixes < -1) | (prefixes >= self.group_count)).to(torch.int32)
        return ObservedPmField(self.binance_trade_values[ids],present,fault | invalid[:,None])

    def frame(self,prefixes):
        qseq,qid,qfound,qfault = self.binance_quantities.query(prefixes)
        reset,_,reset_found,rf = self.binance_resets.query(prefixes)
        _,depth,found,df = self.binance_depth.query(prefixes)
        depth = self.binance_depth_values[depth]
        root_fault = ((prefixes < -1) | (prefixes >= self.group_count)).to(torch.int32)
        recent = qfound & (qseq >= torch.where(reset_found,reset,-1)[:,self.slots[:,0]])
        qid = torch.where(recent,qid,0)
        numbers = _numbers()
        quantity = numbers.ExactRational(_take(self.decimals.numerator,qid.reshape(-1)),
                                         _take(self.decimals.denominator,qid.reshape(-1)))
        fault = qfault | (rf | df)[:,self.slots[:,0]] | root_fault[:,None]
        fault = fault | quantity.fault.reshape(qid.shape) | self.decimal_fault[self.slots[:,2]][None]
        fault = fault | (quantity.numerator.sign.reshape(qid.shape)<0).to(torch.int32)
        known = found & depth[:,:,0].bool()
        empty_slot_ids = torch.zeros_like(qid)
        empty_slots = torch.zeros_like(qid,dtype=torch.bool)
        empty_assets = torch.zeros_like(found)
        # 原Binance只有整本锚点，原Native逐档T5/侧E9均空；不复制整本时间为逐档时间。
        return PublicBookFrame(quantity,recent,known,known,torch.zeros_like(depth[:,:,0]),empty_assets,
            empty_slot_ids,empty_slots,torch.zeros_like(depth[:,:,0]),empty_assets,
            depth[:,:,2],root_fault,fault)
