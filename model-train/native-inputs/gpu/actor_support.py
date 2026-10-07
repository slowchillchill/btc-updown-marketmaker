"""原NativeActor六族合法前缀的设备实现。只求支持/候选，不采样、不预留或提交。"""
from dataclasses import dataclass,replace
import torch
from prepare import _numbers
from asset_buy import take_ratio
from buy_cost import limit_buy_cost


def _take(value,index):
    numbers=_numbers()
    return numbers.ExactInt(value.words[index],value.sign[index],value.fault[index])


def _minimum(a,b): return a.select(a.compare(b).order<=0,b)
def _maximum(a,b): return a.select(a.compare(b).order>=0,b)
def _ceil(a,b):
    quotient,remainder=a.divrem(b)
    return (quotient+quotient.constant(1)).select(remainder.sign!=0,quotient)


@dataclass(frozen=True)
class ActorPrefix:
    family: torch.Tensor
    stage: torch.Tensor  # 0Family/1Quantity/2VariantUp/3VariantDown/4PriceUp/5PriceDown/6Cancel/7Done。
    single_side: torch.Tensor
    root_upper: object
    lo: object
    hi: object
    quantity: object
    variants: torch.Tensor
    prices: torch.Tensor  # 真实price_index k；未选择=-1，不是末端价格float。
    cancel_index: torch.Tensor
    fault: torch.Tensor


@dataclass(frozen=True)
class ActorSupport:
    family: torch.Tensor
    quantity: torch.Tensor
    variant: torch.Tensor
    price: torch.Tensor
    cancel: torch.Tensor
    fault: torch.Tensor


class DeviceNativeActor:
    def __init__(self,context):
        self.context=context;self.rows=context.cutoff.numel();self.device=context.cutoff.device
        self.encoding_limit=((1<<96)-1)//10000  # 原SDK编码域，不裁剪现金root。
        self.exponent=context.rules.context['fee_exponent']  # 共享封存配置，非设备业务值。

    def initial(self):
        c=self.context;zero=c.cash_micro.constant(0);unset=torch.full_like(c.cutoff,-1)
        stage=torch.where(c.decision_required,0,7)
        return ActorPrefix(unset,stage,unset,zero,zero,zero,zero,
            unset[:,None].expand(-1,2).clone(),unset[:,None].expand(-1,2).clone(),unset,c.fault.clone())

    def prefix_features(self,state):
        """同一合法选择前缀的36列模型投影及原精确fault；不推进动作。"""
        from actor_features import prefix_features
        return prefix_features(self,state)

    def _side(self,state):
        return torch.where(state.stage==3,1,torch.where(state.stage==5,1,0))

    def _unit(self,side):
        return _take(self.context.tick_units,torch.arange(self.rows,device=self.device)*2+side)

    def _rule(self,index):
        return take_ratio(self.context.rules.values,torch.full_like(self.context.cutoff,index))

    def _minimum_limit(self):
        value=take_ratio(self.context.rules.limit_minimum,torch.zeros_like(self.context.cutoff))
        return (value.numerator*value.numerator.constant(1000000)).divrem(value.denominator)[0]

    def _minimum_market(self):
        value=self._rule(1)
        return (value.numerator*value.numerator.constant(1000000)).divrem(value.denominator)[0]

    def _repair(self):
        c=self.context;cap=c.cap_centishares
        up=_take(cap,torch.arange(self.rows,device=self.device)*2)
        down=_take(cap,torch.arange(self.rows,device=self.device)*2+1)
        # 原repair按Up/Down：正gap才读取该侧承诺；首个正百份cap直接返回。
        up_used=c.gap_positive[:,0]&c.gap_known[:,0]
        up_found=up_used&c.cap_known[:,0]&(up.sign>0)
        down_used=~up_found&c.gap_positive[:,1]&c.gap_known[:,1]
        fault=(~c.gap_known[:,0]|(up_used&~c.cap_known[:,0])
            |(~up_found&(~c.gap_known[:,1]|(down_used&~c.cap_known[:,1])))).to(torch.int32)
        up=up.select(up_used&c.cap_known[:,0],up.constant(0))
        down=down.select(down_used&c.cap_known[:,1],down.constant(0))
        selected=up.select(up_found,down)
        selected=_numbers().ExactInt(selected.words,selected.sign,selected.fault|fault)
        return torch.where(up_found,0,1),selected

    def _upper(self,family):
        side,cap=self._repair();single=self._unit(side)
        paired=self._unit(torch.zeros_like(side))+self._unit(torch.ones_like(side))
        denominator=single.select(family==1,paired)
        upper=self.context.cash_micro.divrem(denominator)[0]
        upper=_minimum(upper,cap).select(family==1,upper)
        return upper.select(self.context.buy_enabled,upper.constant(0))

    def _cost(self,n,side,k,variant):
        numbers=_numbers();unit=self._unit(side);price=numbers.ExactRational.ratio(k*unit,k.constant(10000))
        cost=limit_buy_cost(n,price,self._rule(2),self.exponent,
            post_only=torch.zeros_like(variant,dtype=torch.bool),minimum_principal_micro=self._minimum_limit())
        return cost.principal_micro+(cost.fee_1e5_units*cost.fee_1e5_units.constant(10)).select(variant==0,cost.fee_1e5_units.constant(0))

    def _min_cost(self,state,side,variants=None):
        n=state.quantity.select(state.quantity.sign>0,state.quantity.constant(1));unit=n*self._unit(side)
        chosen=state.variants if variants is None else variants
        variant=chosen.gather(1,side[:,None]).squeeze(1)
        fixed=state.prices.gather(1,side[:,None]).squeeze(1)
        maximum=_minimum(unit.constant(999),unit.constant(10000).divrem(self._unit(side))[0]-unit.constant(1))
        best=unit.constant(0);exists=torch.zeros_like(side,dtype=torch.bool);fault=unit.fault.clone()
        for candidate in (0,1):
            minimum=unit.constant(1) if candidate==0 else _maximum(_ceil(self._minimum_market(),unit),unit.constant(1))
            k=_numbers().ExactInt.from_i64_tensor(fixed.clamp_min(1)).select(fixed>=0,minimum)
            low=k.compare(minimum);high=k.compare(maximum)
            fits=((variant<0)|(variant==candidate))&(low.order>=0)&(high.order<=0)
            # 无完成性的FOK最低价可能超出合法价格域；不把它当真实成本调用。
            cost=self._cost(n,side,_minimum(_maximum(k,k.constant(1)),maximum),torch.full_like(side,candidate))
            best=cost.select(fits&(~exists|(cost.compare(best).order<0)),best)
            exists|=fits;fault|=low.fault|high.fault|torch.where(fits,cost.fault,0)
        return best,exists,fault

    def _fixed_completion(self,state,variants):
        zero=state.quantity.constant(0);total=zero;known=torch.ones_like(state.stage,dtype=torch.bool);fault=state.fault.clone()
        for i in (0,1):
            side=torch.full_like(state.stage,i);cost,exists,error=self._min_cost(state,side,variants)
            member=(state.family==2)|((state.family==1)&(state.single_side==i))
            total=total+cost.select(member,zero);known&=~member|exists;fault|=torch.where(member,error,0)
        bound=state.quantity.compare(state.quantity.constant(self.encoding_limit))
        upper=state.quantity.compare(self._upper(state.family));cash=total.compare(self.context.cash_micro)
        return (known&(state.quantity.sign>0)&(bound.order<=0)&(upper.order<=0)&(cash.order<=0)
            &self.context.buy_enabled),fault|bound.fault|upper.fault|cash.fault

    def _segments(self,state):
        size=state.hi-state.lo+state.lo.constant(1);begins=[];ends=[];masks=[]
        effective_upper=_minimum(state.root_upper,state.root_upper.constant(self.encoding_limit))
        for j in range(16):
            lo=state.lo+(size*size.constant(j)).divrem(size.constant(16))[0]
            end=state.lo+(size*size.constant(j+1)).divrem(size.constant(16))[0]
            hi=end-end.constant(1)
            masks.append((lo.compare(end).order<0)&(lo.compare(effective_upper).order<=0)
                &(hi.sign>0)&self.context.buy_enabled)
            begins.append(lo);ends.append(hi)
        return begins,ends,torch.stack(masks,1)

    def _price_range(self,state):
        side=self._side(state);other=1-side;other_cost,other_exists,fault=self._min_cost(state,other)
        member=state.family==2
        remaining=self.context.cash_micro-other_cost.select(member,other_cost.constant(0))
        n=state.quantity.select(state.quantity.sign>0,state.quantity.constant(1));unit=n*self._unit(side)
        max_grid=_minimum(unit.constant(999),unit.constant(10000).divrem(self._unit(side))[0]-unit.constant(1))
        maximum=_minimum(max_grid,remaining.divrem(unit)[0]);maximum=_maximum(maximum,maximum.constant(0))
        variant=state.variants.gather(1,side[:,None]).squeeze(1)
        minimum=_maximum(_ceil(self._minimum_market(),unit),unit.constant(1)).select(variant==1,unit.constant(1))
        # max<=999；固定10轮包含全部原价格轴，不逐档枚举成本。
        lo=unit.constant(0);hi=maximum
        for _ in range(10):
            k=lo+_ceil(hi-lo,lo.constant(2));safe_k=k.select(k.sign>0,k.constant(1))
            cost=self._cost(n,side,safe_k,torch.zeros_like(side));test=cost.compare(remaining)
            accept=(test.order<=0)&(k.sign>0)
            hi=(k-k.constant(1)).select(~accept&(hi.compare(lo).order>0),hi)
            lo=k.select(accept,lo);fault|=test.fault
        upper=maximum.select(variant==1,lo)
        present=(~member|other_exists)&(remaining.sign>=0)&(upper.compare(minimum).order>=0)
        return minimum,upper,present,fault|remaining.fault|upper.fault|minimum.fault

    def support(self,state):
        c=self.context;side,cap=self._repair();single=self._upper(torch.ones_like(state.stage))
        paired=self._upper(torch.full_like(state.stage,2));bound=single.constant(self.encoding_limit)
        family=torch.stack((torch.ones_like(state.stage,dtype=torch.bool),
            (_minimum(single,bound).sign>0)&(cap.sign>0),_minimum(paired,bound).sign>0,
            *((c.cancels.present&(c.cancels.kinds==kind)).any(1) for kind in (0,1,2))),1)
        family&=c.decision_required[:,None]
        _,_,quantity=self._segments(state);variants=[];fault=state.fault|single.fault|paired.fault
        current=self._side(state)
        for candidate in (0,1):
            chosen=state.variants.scatter(1,current[:,None],torch.full_like(current[:,None],candidate))
            fits,error=self._fixed_completion(state,chosen);variants.append(fits);fault|=error
        minimum,maximum,present,error=self._price_range(state)
        fault|=torch.where((state.stage==4)|(state.stage==5),error,0)
        coordinates=torch.arange(1,1000,device=self.device)[None,:].expand(self.rows,-1)
        prices=[]
        # 原price_index最多999，末端比较保持精确；没有设备数值回CPU。
        for value in (_minimum(minimum,minimum.constant(1000)),maximum):
            words=value.words.to(torch.int64)&0xffffffff
            prices.append(words[:,0]+(words[:,1]<<32))
        price=(coordinates>=prices[0][:,None])&(coordinates<=prices[1][:,None])&present[:,None]
        cancel=c.cancels.present&(c.cancels.kinds==(state.family-3)[:,None])&c.decision_required[:,None]
        return ActorSupport(family,quantity,torch.stack(variants,1),price,cancel,fault)

    def choose(self,state,choice):
        if choice.shape!=(self.rows,) or choice.dtype!=torch.int64 or choice.device!=self.device:
            raise TypeError('逐根选择需要同设备int64坐标')
        support=self.support(state);valid=torch.zeros_like(choice,dtype=torch.bool)
        for stages,mask in (((0,),support.family),((1,),support.quantity),((2,3),support.variant),
                            ((4,5),support.price),((6,),support.cancel)):
            width=mask.shape[1]
            if width:
                in_range=(choice>=0)&(choice<width)
                selected=mask.gather(1,choice.clamp(0,width-1)[:,None]).squeeze(1)
                stage_mask=torch.zeros_like(valid)
                for stage in stages: stage_mask|=state.stage==stage
                valid|=stage_mask&in_range&selected
        valid|=state.stage==7
        fault=state.fault|support.fault|(~valid).to(torch.int32)
        commit=valid&(fault==0)&(state.stage!=7)
        family=torch.where(commit&(state.stage==0),choice,state.family)
        single,_=self._repair();single=torch.where(commit&(state.stage==0),single,state.single_side)
        upper=self._upper(family);one=upper.constant(1)
        lo=one.select(commit&(state.stage==0)&((family==1)|(family==2)),state.lo)
        hi=upper.select(commit&(state.stage==0)&((family==1)|(family==2)),state.hi)
        starts,ends,_=self._segments(state)
        for j in range(16):
            selected=commit&(state.stage==1)&(choice==j)
            lo=starts[j].select(selected,lo);hi=ends[j].select(selected,hi)
        quantity=lo.select(lo.compare(hi).order==0,state.quantity)
        stage=state.stage.clone();stage=torch.where(commit&(state.stage==0),torch.where(family==0,7,torch.where(family<=2,1,6)),stage)
        settled=commit&((state.stage==0)|(state.stage==1))&(lo.compare(hi).order==0)&((family==1)|(family==2))
        stage=torch.where(settled,torch.where((family==1)&(single==1),3,2),stage)
        stage=torch.where(commit&(state.stage==2),torch.where(family==2,3,4),stage)
        stage=torch.where(commit&(state.stage==3),torch.where(family==2,4,5),stage)
        stage=torch.where(commit&(state.stage==4),torch.where(family==2,5,7),stage)
        stage=torch.where(commit&((state.stage==5)|(state.stage==6)),7,stage)
        variants=state.variants.clone();prices=state.prices.clone()
        for side in (0,1):
            variants[:,side]=torch.where(commit&(state.stage==2+side),choice,variants[:,side])
            prices[:,side]=torch.where(commit&(state.stage==4+side),choice+1,prices[:,side])
        cancel=torch.where(commit&(state.stage==6),choice,state.cancel_index)
        return ActorPrefix(family,stage,single,upper.select(commit&(state.stage==0),state.root_upper),
            lo,hi,quantity,variants,prices,cancel,fault)
