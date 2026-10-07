"""原公开订单冻结来源到Native时间字段；不接纳反馈或推进Ledger。

调用方提供同一次ActionBefore的cutoff、未闭合人口及当前/旧市场作用域。
形成时间只来自OrderLifecycle Accepted，不从Control或SIM内部状态补造。
"""
from dataclasses import dataclass
import torch
from prepare import _numbers
from asset_buy import take_ratio
from success_pool import select
from trade_success import sum_rows


@dataclass(frozen=True)
class FrozenOrderInputs:
    formed_time: torch.Tensor       # [roots,orders,6]，cancel每腿base+35。
    latest_direct_time: torch.Tensor  # [roots,orders,6]，cancel每腿base+41。
    ages: torch.Tensor              # [roots,scopes,2,3]，最大/最小age。
    active: torch.Tensor            # [roots,scopes,3]，原未闭合订单剩余量和。
    cancellable_count: torch.Tensor
    order_count: torch.Tensor
    fault: torch.Tensor


def feedback_time(source, now, applicable):
    """原cancel模板六项：三种秒差S、双存在位和适用位。

    适用订单缺来源时保留[0,0,0,0,0,1]；不是已知零时间。
    原i64时点先转精确整数再作差，避免两个合法时点相减溢出。
    """
    numbers = _numbers()
    roots, width = source.present.shape
    shape = (roots, width)
    device = source.present.device
    if (now.shape != (roots,) or now.dtype != torch.int64 or now.device != device
            or applicable.shape != shape or applicable.dtype != torch.bool
            or applicable.device != device):
        raise TypeError('订单时间需要同设备cutoff[roots]及bool[roots,orders]适用人口')
    present = source.present & applicable
    cutoff = now[:, None].expand(shape).reshape(-1)
    source_at = source.platform_at.reshape(-1)
    received_at = source.received_at.reshape(-1)
    features = []
    errors = torch.zeros(shape, dtype=torch.int32, device=device)
    for left, right in ((cutoff, source_at), (cutoff, received_at), (received_at, source_at)):
        delta = numbers.ExactInt.from_i64_tensor(left) - numbers.ExactInt.from_i64_tensor(right)
        value, error = delta.project(delta.constant(1000), operation=1)
        features.append(torch.where(present, value.reshape(shape), 0.0))
        errors |= torch.where(present, error.reshape(shape), 0)
    result = torch.stack((*features, present.float(), present.float(), applicable.float()), -1)
    return result, numbers.combine_fault_rows(errors)


def order_inputs(orders, sources, cutoff, scope_members, scope_known):
    """原slow382/385及394/397的age；只用action_snapshot未闭合订单。

    scope_members为按公开condition匹配的[roots,scopes,orders]。非空市场scope
    若有未闭合订单的condition未知，调用方按原配方令scope_known=False。
    空订单集合为NotApplicable；任一所选订单缺formed则整个age为Unavailable。
    """
    numbers = _numbers()
    roots, count = orders.present.shape
    device = orders.present.device
    if (scope_members.ndim != 3 or scope_members.shape[0] != roots
            or scope_members.shape[2] != count or scope_members.dtype != torch.bool
            or scope_members.device != device or scope_known.shape != scope_members.shape[:2]
            or scope_known.dtype != torch.bool or scope_known.device != device):
        raise TypeError('订单age需要同设备bool作用域人口及关联可用性')
    formed, formed_fault = feedback_time(sources.formed, cutoff, orders.present)
    direct, direct_fault = feedback_time(sources.latest_direct, cutoff, orders.present)
    selected = scope_members & orders.unfinished[:, None, :]
    population = selected.any(2)
    known = scope_known & population & ~(selected & ~sources.formed.present[:, None, :]).any(2)
    applicable = population | ~scope_known
    if count:
        source_at = sources.formed.platform_at[:, None, :].expand_as(scope_members)
        # 最大age对应最早formed，最小age对应最晚formed。哨兵仅为物理归约，
        # known/适用人口决定是否输出，合法i64端点仍完整参加精确差值。
        earliest = torch.where(selected, source_at, torch.iinfo(torch.int64).max).amin(2)
        latest = torch.where(selected, source_at, torch.iinfo(torch.int64).min).amax(2)
    else:
        earliest = latest = torch.zeros_like(scope_known, dtype=torch.int64)
    ages = []
    fault = orders.fault | sources.fault | formed_fault | direct_fault
    for start in (earliest, latest):
        now = cutoff[:, None].expand_as(start)
        delta = numbers.ExactInt.from_i64_tensor(now.reshape(-1)) - numbers.ExactInt.from_i64_tensor(start.reshape(-1))
        value, error = delta.project(delta.constant(1000), operation=1)
        ages.append(torch.stack((torch.where(known, value.reshape(start.shape), 0.0),
                                 known.float(), applicable.float()), -1))
        fault |= numbers.combine_fault_rows(torch.where(known, error.reshape(start.shape), 0))
    def scalar(value, known):
        projected, error = value.project(operation=1)
        fault_rows = numbers.combine_fault_rows(torch.where(known, error.reshape(known.shape), 0))
        triple = torch.stack((torch.where(known, projected.reshape(known.shape), 0.0),
                              known.float(), torch.ones_like(known, dtype=torch.float32)), -1)
        return triple, fault_rows
    scopes = scope_known.size(1)
    if count:
        index = (torch.arange(roots, device=device)[:, None, None]*count +
                 torch.arange(count, device=device)[None, None, :]).expand(roots, scopes, count).flatten()
        active_values = take_ratio(orders.active, index)
        zero = numbers.ExactRational(active_values.numerator.constant(0), active_values.denominator.constant(1))
        active_population = selected & orders.active_applicable[:, None, :] & orders.active_known[:, None, :]
        active_total = sum_rows(select(active_values, active_population.flatten(), zero), roots*scopes, count)
    else:
        zero = numbers.ExactInt.from_i64_tensor(torch.zeros(roots*scopes, dtype=torch.int64, device=device))
        active_total = numbers.ExactRational(zero, zero.constant(1))
    active_known = scope_known & ~(selected & (~orders.active_applicability_known[:, None, :] |
        (orders.active_applicable[:, None, :] & ~orders.active_known[:, None, :]))).any(2)
    active, error = scalar(active_total, active_known)
    fault |= error
    cancel_known = scope_known & ~(selected & ~orders.cancellable_known[:, None, :]).any(2)
    cancel_population = (selected & orders.cancellable[:, None, :] & orders.cancellable_known[:, None, :]).to(torch.int64).sum(2)
    cancel_integer = numbers.ExactInt.from_i64_tensor(cancel_population.flatten())
    cancellable, error = scalar(numbers.ExactRational(cancel_integer, cancel_integer.constant(1)), cancel_known)
    fault |= error
    count_integer = numbers.ExactInt.from_i64_tensor(selected.to(torch.int64).sum(2).flatten())
    order_count, error = scalar(numbers.ExactRational(count_integer, count_integer.constant(1)), scope_known)
    fault |= error
    return FrozenOrderInputs(formed, direct, torch.stack(ages, 2), active, cancellable, order_count, fault)


def write_order_scope_fields(slow, inputs, scope_applicable):
    """写原current/old订单块；其它账户字段须由实际读面装配，不能以零补全。

    第0/1个scope必须由Native根的当前/旧市场选择器提供。当前市场不存在时
    scope_applicable[:,0]=False；旧市场空集合仍适用。不从订单猜当前市场。
    """
    roots = slow.size(0)
    if (slow.shape != (roots, 433) or slow.dtype != torch.float32
            or inputs.ages.shape != (roots, 2, 2, 3)
            or scope_applicable.shape != (roots, 2) or scope_applicable.dtype != torch.bool
            or scope_applicable.device != slow.device or inputs.ages.device != slow.device):
        raise TypeError('原slow433订单块需要同设备current/old双scope及float32输入')
    result = slow.clone()
    for group in range(2):
        use = scope_applicable[:, group, None]
        for base, value in ((382, inputs.ages[:, group, 0]), (385, inputs.ages[:, group, 1]),
                            (388, inputs.active[:, group]), (391, inputs.cancellable_count[:, group])):
            result[:, base+group*12:base+group*12+3] = torch.where(use, value, 0.0)
        result[:, 406+group*3:409+group*3] = torch.where(use, inputs.order_count[:, group], 0.0)
    return result
