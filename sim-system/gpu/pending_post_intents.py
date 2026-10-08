"""原PostOrderItemV1到处理时SDK terms的批控制面；设备选择pending与准入。

仅一次整批搬运编码所需坐标/tick，Rust纯函数复用CPU原SDK规则。该边界不
在CPU推进逐条件业务，也不把观察时网格或参考成交结果作为处理时输入。
"""
from dataclasses import dataclass
from fractions import Fraction
import json
import torch
from queues import Numbers, ExactInt, ExactRational, _take
from hidden_market import rational_take, micro_units
from pending_base_execution import FrozenBaseTerms


REJECTED_REASONS = ('EndAtReached', 'ExplicitTradingStateVeto', 'MarketNotInScenario',
    'AssetNotInMarket', 'OrderEncodingRejected', 'BelowMinimumShares',
    'BelowMarketBuyMinimumPrincipal', 'PostOnlyWouldTake',
    'BelowMarketableLimitBuyMinimumPrincipal', 'GtdExpirationTooSoon',
    'InsufficientAvailablePusd', 'FokNotFullyFilled', 'FakNoMatch')


def _integer_rows(words, signs):
    return [sign*sum((word & 0xffffffff) << (32*i) for i,word in enumerate(row))
        for row,sign in zip(words,signs,strict=True)]


def _decimal(value):
    """将实际有限十进制tick无损交给Rust Decimal；不能通过float往返。"""
    n,d=value.numerator,value.denominator
    scale=0
    while d>1 and scale<=28:
        common=__import__('math').gcd(d,10)
        if common==1:
            raise ValueError('原SDK Decimal不能表达该tick')
        n*=10//common; d//=common; scale+=1
    if d!=1 or scale>28:
        raise ValueError('原SDK Decimal tick超出原支持域')
    text=str(abs(n)).rjust(scale+1,'0')
    return ('-' if n<0 else '')+(text if scale==0 else text[:-scale]+'.'+text[-scale:])


@dataclass(frozen=True)
class PendingPostIntents:
    items: tuple  # 每根不可变原PostOrderItemV1 JSON字符串目录，不造另一意图合同。
    assets: torch.Tensor  # [root,item]原token绑定；-1为原未知资产。
    conditions: torch.Tensor  # 原市场目录坐标；-1为原市场不存在。
    bid_rows: torch.Tensor
    market_end: torch.Tensor
    fault: torch.Tensor

    def __post_init__(self):
        object.__setattr__(self,'items',tuple(tuple(row) for row in self.items))
        for name in ('assets','conditions','bid_rows','market_end','fault'):
            object.__setattr__(self,name,getattr(self,name).clone())

    def encode(self, pending, hidden, controls, rule, codec, enabled):
        roots,width=self.assets.shape
        axis=torch.arange(roots,device=self.assets.device)
        refs=pending.item_ref
        valid=(refs>=0)&(refs<width)
        safe=refs.clamp(0,width-1)
        assets=self.assets[axis,safe]; conditions=self.conditions[axis,safe]
        bid=self.bid_rows[axis,safe]
        asset=assets.clamp(0,hidden.initialized.numel()-1)
        in_asset=(assets>=0)&(assets<hidden.initialized.numel())&(hidden.asset_markets[asset]==conditions)
        in_market=(conditions>=0)&(conditions<controls.trading.size(1))
        market=conditions.clamp(0,controls.trading.size(1)-1)
        trading=controls.trading[axis,market]
        all_assets=hidden.asset_markets[:,None]==conditions[None,:]
        initialized=(~all_assets|hidden.initialized[:,None]).all(0)
        ended=(pending.effective_at>=self.market_end[axis,safe])|(all_assets&hidden.matching_closed[:,None]).any(0)
        veto=(trading[:,0]==0)|(trading[:,1]==1)|(trading[:,2]==0)
        # 原buy_admission顺序：市场/end/veto/资产/preload/quality/tick。
        disposition=torch.full_like(refs,-1)
        disposition=torch.where(hidden.quality[asset]!=0,-2,disposition)
        disposition=torch.where(~initialized,-2,disposition)
        disposition=torch.where(~in_asset,3,disposition)
        disposition=torch.where(veto,1,disposition)
        disposition=torch.where(ended,0,disposition)
        disposition=torch.where(~in_market,2,disposition)
        active=pending.exists & enabled
        ready=active&valid&(disposition==-1)
        unknown_reason=torch.where(~initialized,-1,hidden.quality[asset])
        fault=self.fault|pending.fault|controls.fault
        fault |= torch.where(active,(~valid).to(torch.int32),0)
        fault |= torch.where(ready,hidden.fault[asset]|(~hidden.tick_present[asset]).to(torch.int32),0)
        tick=rational_take(hidden.tick,asset)
        # Entire encoding population crosses once; CPU performs only the original pure SDK call.
        packed=torch.cat((refs[:,None],ready[:,None].to(torch.int64),tick.numerator.sign[:,None],
            tick.numerator.words.to(torch.int64),tick.denominator.sign[:,None],
            tick.denominator.words.to(torch.int64)),dim=1).cpu().tolist()
        nwidth=tick.numerator.words.size(1)
        selected=[i for i,row in enumerate(packed) if row[1]]
        inputs=[];ticks=[]
        for i in selected:
            row=packed[i]
            n=_integer_rows([row[3:3+nwidth]],[row[2]])[0]
            d=_integer_rows([row[4+nwidth:]],[row[3+nwidth]])[0]
            inputs.append(json.loads(self.items[i][row[0]]))
            ticks.append(_decimal(Fraction(n,d)))
        encoded=json.loads(codec(json.dumps({'entries':[{'item':item,'tick':tick,'normalized_gtd':None}
            for item,tick in zip(inputs,ticks,strict=True)],
            'minimum_shares':rule.context['limit_min_quantity_shares']})))
        base=[0]*roots; quote=[0]*roots; prices=[Fraction(0)]*roots
        kinds=[0]*roots; post_only=[False]*roots; reasons=[-1]*roots
        for i,result in zip(selected,encoded,strict=True):
            if 'Err' in result:
                reasons[i]=REJECTED_REASONS.index(result['Err']); continue
            (kind,terms),=result['Ok'].items()
            if kind=='Gtc':
                base[i]=terms['accepted_base_units'];quote[i]=terms['accepted_quote_units']
                prices[i]=Fraction(terms['limit_price_pusd_per_share']);post_only[i]=terms['post_only']
            elif kind=='Fok':
                base[i]=terms['sdk_taker_amount_audit_base_units'];quote[i]=terms['accepted_quote_budget_units']
                prices[i]=Fraction(terms['worst_price_pusd_per_share']);kinds[i]=1
            else:
                raise ValueError('此pending生产入口仅绑定原GTC/FOK请求')
        tensor=lambda values,dtype=torch.int64:torch.tensor(values,dtype=dtype,device=refs.device)
        numbers=lambda values:ExactInt.from_values(values,device=refs.device)
        reason=torch.where(ready,tensor(reasons),disposition)
        if rule.limit_minimum is None:
            raise ValueError('接单须保留同封存快照的marketable LIMIT最低本金')
        minimum=Numbers.run_exact_batch(lambda:_take(micro_units(rule.limit_minimum),torch.zeros_like(refs)))
        return FrozenBaseTerms(refs,numbers(base),numbers(quote),
            ExactRational.ratio(numbers([p.numerator for p in prices]),numbers([p.denominator for p in prices])),
            assets,bid,fault,conditions,tensor(kinds),reason,tensor(post_only,torch.bool),minimum,unknown_reason)
