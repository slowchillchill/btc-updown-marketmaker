"""原共享公开账户冻结到Native资金/订单/请求字段。

provided标明本装配实际写入的槽位；未提供的经济/结算字段不能以零冒充完整slow433。
完整根还须接同一冻结前缀的持仓、承诺、经济、估值和结算读面。
"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from input_orders import order_inputs, write_order_scope_fields


@dataclass(frozen=True)
class FrozenAccountInputs:
    slow: torch.Tensor
    provided: torch.Tensor
    order_fields: object
    cutoff: torch.Tensor
    fault: torch.Tensor


def frozen_account_inputs(account, scope_members, scope_known, scope_applicable, *, scope_conditions=None, scope_present=None, asset_outcomes=None, executed_prefix_known=None, success_prefix_known=None, rebate_prefix_known=None,settlement_prefix_known=None,ledger_prefix_known=None,economic_liability_prefix_known=None,economic_prefix_known=None,row_role=None):
    """消费一次真实冻结值；当前/旧scope由根权威选择器传入，不按订单猜scope。"""
    numbers = _numbers()
    roots = account.cutoff.numel()
    device = account.cutoff.device
    orders = order_inputs(account.orders, account.order_sources, account.cutoff, scope_members, scope_known)
    slow = torch.zeros((roots, 433), dtype=torch.float32, device=device)
    provided = torch.zeros_like(slow, dtype=torch.bool)
    slow = write_order_scope_fields(slow, orders, scope_applicable)
    provided[:, 382:412] = True
    fault = account.fault | orders.fault
    if scope_conditions is not None:
        from input_reservations import reservation_inputs, write_reservation_fields
        reservations = reservation_inputs(account.ledger.reservations, scope_conditions, scope_present, scope_applicable)
        slow = write_reservation_fields(slow, reservations)
        provided[:, :54] = True
        fault |= reservations.fault
        if economic_prefix_known is not None:
            if success_prefix_known is None or settlement_prefix_known is None:
                raise ValueError('完整市场经济输入须显式安装成功和结算前缀')
            from input_economics import economic_inputs,write_economic_fields
            economic=economic_inputs(account.ledger,scope_conditions,scope_present,scope_applicable,
                economic_prefix_known=economic_prefix_known,success_prefix_known=success_prefix_known,
                settlement_prefix_known=settlement_prefix_known)
            slow=write_economic_fields(slow,economic)
            provided[:,284:293]=True
            provided[:,299:308]=True
            provided[:,332:382]=True
            fault|=economic.fault
        if account.ledger.settlement_costs is not None:
            from input_settlement_fee import settlement_fee_inputs
            fees,fee_fault=settlement_fee_inputs(account.ledger,scope_conditions,scope_present,scope_applicable)
            slow[:,296:299]=fees[:,0]
            slow[:,311:314]=fees[:,1]
            provided[:,296:299]=True
            provided[:,311:314]=True
            fault |= fee_fault
        if economic_liability_prefix_known is not None:
            from input_taker_fee import taker_fee_inputs,write_taker_fee_fields
            fees=taker_fee_inputs(account.ledger,scope_conditions,scope_present,scope_applicable,
                prefix_known=economic_liability_prefix_known)
            slow=write_taker_fee_fields(slow,fees)
            provided[:,293:296]=True
            provided[:,308:311]=True
            fault|=fees.fault
        if ledger_prefix_known is not None:
            from input_market_account import market_account_inputs,write_market_account_fields
            markets=market_account_inputs(account.ledger,scope_conditions,scope_present,scope_applicable,
                prefix_known=ledger_prefix_known)
            slow=write_market_account_fields(slow,markets)
            provided[:,81:93]=True
            provided[:,95:98]=True
            fault|=markets.fault
        if rebate_prefix_known is not None:
            from input_rebates import rebate_inputs,write_rebate_fields
            rebates=rebate_inputs(account.ledger,scope_conditions,scope_present,scope_applicable,
                prefix_known=rebate_prefix_known)
            slow=write_rebate_fields(slow,rebates)
            provided[:,212:284]=True
            fault|=rebates.fault
        commitments = None
        frozen_commitments = getattr(account,'commitments',None)
        if frozen_commitments is not None and frozen_commitments.condition_slots is not None:
            from input_commitments import commitment_inputs,write_commitment_fields
            commitments = commitment_inputs(frozen_commitments,scope_conditions,scope_present,scope_applicable)
            slow = write_commitment_fields(slow,commitments)
            provided[:,98:110] = True
            fault |= commitments.fault
        if executed_prefix_known is not None:
            from input_executed import executed_inputs, write_executed_fields
            executed = executed_inputs(account.ledger.trade_success,scope_conditions,scope_present,scope_applicable,
                prefix_known=executed_prefix_known)
            slow = write_executed_fields(slow,executed)
            provided[:,176:212] = True
            fault |= executed.fault
        if asset_outcomes is not None:
            if executed_prefix_known is not None:
                from input_repair import repair_inputs,write_repair_fields
                repair=repair_inputs(account.ledger,scope_conditions,scope_present,scope_applicable,
                    asset_outcomes,prefix_known=executed_prefix_known,
                    settlement_prefix_known=settlement_prefix_known,commitments=commitments)
                slow=write_repair_fields(slow,repair)
                provided[:,164:176]=True
                fault|=repair.fault
            from input_positions import position_inputs, write_position_fields
            positions = position_inputs(account.ledger,scope_conditions,scope_present,scope_applicable,asset_outcomes)
            slow = write_position_fields(slow,positions)
            provided[:,54:81] = True
            fault |= positions.fault
            if success_prefix_known is not None:
                from input_success import success_inputs,write_success_fields
                success = success_inputs(account.ledger.success_pools,scope_conditions,scope_present,scope_applicable,
                    asset_outcomes,prefix_known=success_prefix_known,resolutions=getattr(account.ledger,'resolutions',None),
                    settlement_prefix_known=settlement_prefix_known)
                slow = write_success_fields(slow,success)
                provided[:,122:164] = True
                fault |= success.fault
    elif scope_present is not None or asset_outcomes is not None or rebate_prefix_known is not None or settlement_prefix_known is not None or ledger_prefix_known is not None or economic_liability_prefix_known is not None or economic_prefix_known is not None:
        raise ValueError('scope存在位不能脱离原condition人口')
    if row_role is not None:
        if row_role.shape!=(roots,) or row_role.dtype!=torch.int64 or row_role.device!=device:
            raise TypeError('状态行角色需实际同设备int64[roots]，ContextOnly0/DecisionRequired1')
        slow[:,93]=(row_role==1).float()
        slow[:,94]=(row_role==0).float()
        provided[:,93:95]=(row_role>=0)[:,None]
        fault|=((row_role< -1)|(row_role>1)).to(torch.int32)
    for start, value in [(110, account.funds.actual_available), (113, account.funds.actual_reserved),
                         (116, account.funds.maker_available), (119, account.funds.maker_reserved)]:
        projected, error = value.project(operation=1)
        slow[:, start:start+3] = torch.stack((projected, torch.ones_like(projected), torch.ones_like(projected)), 1)
        provided[:, start:start+3] = True
        fault |= error
    count = account.pending_noe_count
    projected, error = count.project(count.constant(1), operation=1)
    slow[:, 412:415] = torch.stack((projected, torch.ones_like(projected), torch.ones_like(projected)), 1)
    provided[:, 412:415] = True
    fault |= error
    requests = account.cancellations
    if requests is not None:
        pending = requests.pending_count
        value, error = pending.project(pending.constant(1), operation=1)
        slow[:, 415:418] = torch.stack((value, torch.ones_like(value), torch.ones_like(value)), 1)
        fault |= error | requests.fault
        counts = requests.response_counts
        value, error = counts.project(counts.constant(1), operation=1)
        enabled = requests.counts_applicable[:, None].expand(roots, 3)
        rows = torch.stack((torch.where(enabled, value.reshape(roots, 3), 0.0),
                            enabled.float(), enabled.float()), -1)
        slow[:, 418:427] = rows.reshape(roots, 9)
        fault |= numbers.combine_fault_rows(torch.where(enabled, error.reshape(roots, 3), 0))
        present = requests.has_latest
        for offset, left, right in [(427, account.cutoff, requests.responded_at),
                                    (428, account.cutoff, requests.received_at),
                                    (429, requests.received_at, requests.responded_at)]:
            delta = numbers.ExactInt.from_i64_tensor(left) - numbers.ExactInt.from_i64_tensor(right)
            value, error = delta.project(delta.constant(1000), operation=1)
            slow[:, offset] = torch.where(present, value, 0.0)
            fault |= torch.where(present, error, 0)
        slow[:, 430:433] = present[:, None].expand(roots, 3).float()
        # 原Known(None)请求最新响应为NA六项，而订单模板无来源仍有适用位。
        provided[:, 415:433] = True
    return FrozenAccountInputs(slow, provided, orders, account.cutoff.clone(), fault)


@dataclass(frozen=True)
class NativeAccountAtPrefix:
    inputs: FrozenAccountInputs
    scopes: object


def account_inputs_at_prefix(account, market, condition_slots, prefixes, root_started_at,
                             order_conditions, order_condition_known, asset_outcomes, *, executed_prefix_known=None, success_prefix_known=None, rebate_prefix_known=None,settlement_prefix_known=None,ledger_prefix_known=None,economic_liability_prefix_known=None,economic_prefix_known=None,row_role=None):
    """同一实际cutoff：设备选原current/old，再装配已交付账户字段。

    market只含当前完整公共前缀；order_conditions沿已冻结原意图/订单关联，
    不能读取SIM内部状态。尚未实现的字段仍以provided=False明确保留缺口。
    """
    from input_scopes import account_scopes
    projected = account.ledger.projected_markets
    if projected is None:
        raise ValueError('当前根未安装原Ledger投影市场人口')
    scopes = account_scopes(market, condition_slots, prefixes, projected, account.cutoff, root_started_at)
    roots, count = account.orders.present.shape
    device = account.cutoff.device
    if (order_conditions.shape != (roots,count) or order_conditions.dtype != torch.int64
            or order_conditions.device != device or order_condition_known.shape != (roots,count)
            or order_condition_known.dtype != torch.bool or order_condition_known.device != device):
        raise TypeError('订单作用域需要同冻结订单坐标的原condition及known列')
    members = ((order_conditions[:,None,:,None] == scopes.conditions[:,:,None,:])
               & scopes.present[:,:,None,:]).any(3) & order_condition_known[:,None,:]
    # 原未知未闭合订单condition令非空scope订单读面Unavailable，不能忽略。
    unknown_order = (account.orders.unfinished & ~order_condition_known).any(1)
    population_known = torch.stack((torch.ones_like(scopes.population_known),scopes.population_known),1)
    scope_known = population_known & ~(unknown_order[:,None] & scopes.present.any(2))
    inputs = frozen_account_inputs(account,members,scope_known,scopes.applicable,
        scope_conditions=scopes.conditions,scope_present=scopes.present,asset_outcomes=asset_outcomes,
        executed_prefix_known=executed_prefix_known,success_prefix_known=success_prefix_known,rebate_prefix_known=rebate_prefix_known,
        settlement_prefix_known=settlement_prefix_known,ledger_prefix_known=ledger_prefix_known,
        economic_liability_prefix_known=economic_liability_prefix_known,economic_prefix_known=economic_prefix_known,row_role=row_role)
    slow = inputs.slow.clone()
    # 没有完整old人口时，已有成员之和不能冒充整个old作用域；保留适用Unknown。
    for first,last in [(0,27),(54,66),(81,87),(98,104),(122,128),(134,140),(146,155),(176,194),(212,248)]:
        width = last-first
        value = slow[:,first:first+2*width].reshape(roots,2,width//3,3)
        unknown = scopes.applicable & ~population_known
        value = torch.where(unknown[:,:,None,None],torch.tensor([0.,0.,1.],device=device),value)
        slow[:,first:first+2*width] = value.reshape(roots,2*width)
    # 这两scope相隔15个槽位，不能套连续current/old字段段。
    if economic_liability_prefix_known is not None:
        unknown_old=scopes.applicable[:,1]&~population_known[:,1]
        slow[:,308:311]=torch.where(unknown_old[:,None],torch.tensor([0.,0.,1.],device=device),slow[:,308:311])
    if account.ledger.settlement_costs is not None:
        unknown_old=scopes.applicable[:,1]&~population_known[:,1]
        slow[:,311:314]=torch.where(unknown_old[:,None],torch.tensor([0.,0.,1.],device=device),slow[:,311:314])
    if economic_prefix_known is not None:
        unknown_old=scopes.applicable[:,1]&~population_known[:,1]
        for first,last in [(299,308),(344,356),(364,382)]:
            value=slow[:,first:last].reshape(roots,-1,3)
            slow[:,first:last]=torch.where(unknown_old[:,None,None],
                torch.tensor([0.,0.,1.],device=device),value).flatten(1)
    return NativeAccountAtPrefix(FrozenAccountInputs(slow,inputs.provided,inputs.order_fields,
        inputs.cutoff,inputs.fault | scopes.fault),scopes)
