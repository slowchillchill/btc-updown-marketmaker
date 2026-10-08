"""同冻结精确Actor输入；只拥有后续支持/意图真正使用的值，不保留活动Ledger。"""
from dataclasses import dataclass, replace, fields
import torch
from prepare import _numbers
from asset_buy import take_ratio
from input_projection import decimal_value


def _owned(value):
    if value is None:return None
    if isinstance(value,torch.Tensor): return value.clone()
    if hasattr(value,'__dataclass_fields__'):
        return replace(value,**{field.name:_owned(getattr(value,field.name)) for field in fields(value)})
    raise TypeError('Actor自有输入只含实际张量和精确值')


@dataclass(frozen=True)
class FrozenActorContext:
    cutoff: torch.Tensor
    identities: torch.Tensor  # trajectory/account/condition原字典坐标。
    assets: torch.Tensor     # 原公共资产目录，固定Up/Down。
    cash_micro: object       # floor_positive(min(actual,maker)*1e6)。
    cap_centishares: object  # 原input_repair cap的精确百份数，不重复算gap/commitment。
    gap_positive: torch.Tensor
    gap_known: torch.Tensor
    cap_known: torch.Tensor
    tick_units: object       # 原tick*10000，数量n的单档本金为n*tick_units。
    buy_enabled: torch.Tensor
    decision_required: torch.Tensor
    cancels: object          # 全current/old人口及真实成员/形成顺序。
    rules: object            # 原不可变PublicRuleGpu；不复制共享规则。
    fault: torch.Tensor
    order_identities: object | None = None  # 同cutoff原公开订单轴，仅发布正文使用。
    paired_members: torch.Tensor | None = None  # 原Pair目录ordinal0/1，不从布尔集合恢复。
    condition_slots: torch.Tensor | None = None  # 原市场轴→condition身份字典。


def publish_actor_context(account,native,market_inputs,market_freeze,market_indices,
                          condition_slots,identities,cancel_objects):
    """图外成功发布。资源门由现有共同device_exact_batch/市场装箱结果承担。

    identity目录与封存规则共享只读；当前冻结实际数值和撤单人口拥有独立存储。
    这里只为DecisionRequired行建立动作上下文，ContextOnly仍消费当时账户观察。
    """
    numbers=_numbers();shared=market_inputs.shared;market=market_inputs.market
    rules=market_inputs.rules.process_rules;rows=account.cutoff.numel();device=account.cutoff.device
    if rules.limit_minimum is None:
        raise ValueError('Actor需要封存快照真实LIMIT费门槛，旧观察上下文不能猜值')
    if native.inputs.repair is None:
        raise ValueError('Actor需要同冻结input_repair的精确cap和真实承诺输入')
    if (market_indices.shape!=(rows,) or market_indices.dtype!=torch.int64
            or identities.shape!=(rows,3) or identities.dtype!=torch.int64
            or identities.device!=device or market_indices.device!=device
            or condition_slots.shape!=(market.market_count,) or condition_slots.dtype!=torch.int64
            or condition_slots.device!=device or cancel_objects.present.shape[0]!=rows):
        raise TypeError('Actor需要同设备原市场/condition/根身份与完整撤单对象')
    prefix=market_freeze.prefixes;observed=shared.observation(prefix);frame=market.frame(prefix)
    batch=torch.arange(rows,device=device);valid=(market_indices>=0)&(market_indices<market.market_count)
    safe=market_indices.clamp(0,market.market_count-1);assets=market.asset_pairs[safe]
    asset_valid=(assets>=0)&(assets<len(shared.assets));safe_assets=assets.clamp(0,len(shared.assets)-1)
    tick=observed.tick.values[batch[:,None],safe_assets]
    tick_known=observed.tick.present[batch[:,None],safe_assets]&asset_valid&(tick[:,:,3]<=1)
    exact_tick=decimal_value(shared,tick[:,:,1]);unit=(exact_tick.numerator*exact_tick.numerator.constant(10000)).divrem(exact_tick.denominator)
    tick_units,remainder=unit
    supported=(tick_units.compare(tick_units.constant(100)).order==0)|(tick_units.compare(tick_units.constant(10)).order==0)
    funds=account.funds;minimum=funds.actual_available.compare(funds.maker_available)
    available=type(funds.actual_available)(funds.actual_available.numerator.select(minimum.order<=0,funds.maker_available.numerator),
        funds.actual_available.denominator.select(minimum.order<=0,funds.maker_available.denominator))
    cash=(available.numerator*available.numerator.constant(1000000)).divrem(available.denominator)[0]
    cash=cash.select(cash.sign>0,cash.constant(0))
    repair=native.inputs.repair
    cap=(repair.cap_exact.numerator*repair.cap_exact.numerator.constant(100)).divrem(repair.cap_exact.denominator)[0]
    trading=frame.trading.values[batch,safe];tp=frame.trading.present[batch,safe]
    terminal=frame.terminal.present[batch,safe]&frame.terminal.values[batch,safe,0].bool()
    enabled=~(tp&trading[:,0].bool()&~trading[:,1].bool())
    enabled&=~(tp&trading[:,2].bool()&trading[:,3].bool())
    enabled&=~(tp&trading[:,4].bool()&~trading[:,5].bool())&~terminal
    decision=market_freeze.role==1
    binding=(native.scopes.present[:,0]&(native.scopes.conditions[:,0]==condition_slots[safe,None])).any(1)
    fault=account.fault|native.inputs.fault|market_freeze.fault|cancel_objects.fault|rules.fault
    fault|=minimum.fault|cash.fault
    fault|=numbers.combine_fault_rows(tick_units.fault.reshape(rows,2)|remainder.fault.reshape(rows,2))
    fault|=numbers.combine_fault_rows(cap.fault.reshape(rows,2))
    fault|=(decision&(~valid|~binding|~frame.admitted[batch,safe])).to(torch.int32)
    valid_tick=supported.reshape(rows,2)&(remainder.sign.reshape(rows,2)==0)
    fault|=(decision&(tick_known&~valid_tick).any(1)).to(torch.int32)
    fault|=(decision&enabled&~tick_known.all(1)).to(torch.int32)
    fault|=(account.cutoff!=market_freeze.endpoint_memory.previous_observation_ms).to(torch.int32)
    fault|=(native.inputs.cutoff!=account.cutoff).to(torch.int32)
    fault|=(identities[:,2]!=condition_slots[safe]).to(torch.int32)
    # 未被买入消费的缺tick使用算术暂存占位，不能产生动作资格或抹去实际fault。
    tick_units=tick_units.select((tick_known&valid_tick).flatten(),tick_units.constant(100))
    return FrozenActorContext(account.cutoff.clone(),identities.clone(),assets.clone(),_owned(cash),_owned(cap),
        (repair.gap_exact.numerator.sign.reshape(rows,2)>0).clone(),repair.gap_known.clone(),repair.cap_known.clone(),
        _owned(tick_units),(enabled&decision).clone(),decision.clone(),_owned(cancel_objects),rules,fault.clone(),
        None if account.intent_orders is None else _owned(account.intent_orders.identities),
        None if account.cancel_directory is None else account.cancel_directory.paired_members.clone(),
        condition_slots.clone())
