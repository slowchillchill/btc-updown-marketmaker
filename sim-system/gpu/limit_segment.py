"""原maximum_base_for_limit_order_segment_v1的精确设备候选。

舍入差可能不单调；按原有限模糊区间的floor-sum人口寻找末个可行量。
不缩小原量域，不枚举每一份数量，不提交队列/账户。二分轮数只由输入物理字容量决定。
"""
from dataclasses import dataclass
from queues import ExactInt


@dataclass(frozen=True)
class LimitSegmentResult:
    base: object
    quote: object
    present: object
    fault: object


def _minimum(a, b):
    relation = a.compare(b)
    value = a.select(relation.order <= 0, b)
    return ExactInt(value.words, value.sign, value.fault | relation.fault)


def _search(lower, upper, predicate, iterations, *, last=False):
    """固定资源形状的设备二分；inactive行停在原值，不回传业务条件。"""
    one = lower.constant(1)
    two = lower.constant(2)
    fault = lower.fault | upper.fault
    for _ in range(iterations):
        relation = lower.compare(upper)
        active = relation.order < 0
        difference = upper-lower
        midpoint = lower+(difference+one if last else difference).divrem(two)[0]
        yes, error = predicate(midpoint)
        fault = fault | relation.fault | midpoint.fault | error
        if last:
            lower = midpoint.select(active & yes, lower)
            upper = (midpoint-one).select(active & ~yes, upper)
        else:
            upper = midpoint.select(active & yes, upper)
            lower = (midpoint+one).select(active & ~yes, lower)
    fault = fault | (lower.compare(upper).order != 0).to(fault.dtype)
    return lower, fault


def maximum_limit_segment(external_base, external_quote, consumed, maximum,
                          order_base, order_quote, matched, used):
    """全部ExactInt同设备[batch]。返回None由present=false表达，fault不能当None。"""
    inputs = (external_base, external_quote, consumed, maximum, order_base, order_quote, matched, used)
    iterations = 32*max(value.words.shape[1] for value in inputs)+1
    zero, one = external_base.constant(0), external_base.constant(1)
    fault = zero.fault
    for value in inputs:
        fault = fault | value.fault | (value.sign < 0).to(fault.dtype)
    fault = fault | ((external_base.sign <= 0) | (order_base.sign <= 0)).to(fault.dtype)
    external_remaining = external_base-consumed
    order_remaining = order_base-matched
    for relation in (consumed.compare(external_base), matched.compare(order_base),
                     maximum.compare(external_remaining), maximum.compare(order_remaining)):
        fault = fault | relation.fault | (relation.order > 0).to(fault.dtype)
    eb = external_base.select(external_base.sign > 0, one)
    ob = order_base.select(order_base.sign > 0, one)
    eq = external_quote.select(external_quote.sign > 0, one)
    before, external_remainder = (consumed*external_quote).divrem(eb)
    allocated, order_remainder = (matched*order_quote).divrem(ob)
    credit = allocated-used
    fault = fault | credit.fault | (credit.sign < 0).to(fault.dtype)
    missing = eb-external_remainder
    quotient, remainder = missing.divrem(eq)
    lower = quotient+one.select(remainder.sign != 0, zero)
    lower = one.select(lower.sign <= 0, lower)
    lower_comparison = lower.compare(maximum)
    eligible = (external_quote.sign > 0) & (order_quote.sign > 0) & (maximum.sign > 0) & (lower_comparison.order <= 0)
    fault = fault | lower_comparison.fault

    def delta(amount):
        external_after = ((consumed+amount)*external_quote).divrem(eb)[0]
        order_after = ((matched+amount)*order_quote).divrem(ob)[0]
        return external_after-before, order_after-used

    full_quote, full_allowance = delta(maximum)
    feasible_full = full_quote.compare(full_allowance)
    fault = fault | full_quote.fault | full_allowance.fault | feasible_full.fault
    full = eligible & (full_quote.sign > 0) & (feasible_full.order <= 0)
    needs_search = eligible & ~full & (full_quote.sign > 0)
    denominator = eb*ob
    offset = external_remainder*ob-order_remainder*eb-credit*denominator
    slope = external_quote*ob-order_quote*eb
    fault = fault | denominator.fault | offset.fault | slope.fault
    safe_lower = lower.select(needs_search, one)
    safe_upper = maximum.select(needs_search, one)

    def numerator(amount):
        return offset+slope*amount

    def nonnegative(amount):
        value = numerator(amount)
        return value.sign >= 0, value.fault

    def at_least_one(amount):
        value = numerator(amount)
        relation = value.compare(denominator)
        return relation.order >= 0, value.fault | relation.fault

    def below_one(amount):
        result, error = at_least_one(amount)
        return ~result, error

    first_nonnegative, error1 = _search(safe_lower, safe_upper, nonnegative, iterations)
    first_one, error2 = _search(safe_lower, safe_upper, at_least_one, iterations)
    first_below, error3 = _search(safe_lower, safe_upper, below_one, iterations)
    at_upper = numerator(safe_upper).compare(denominator)
    at_offset = offset.compare(denominator)
    positive = slope.sign > 0
    negative = slope.sign < 0
    equal = slope.sign == 0
    ambiguous_lower = first_nonnegative.select(positive, first_below.select(negative, safe_lower))
    ambiguous_upper = safe_upper.select(at_upper.order < 0, first_one-one)
    ambiguous_upper = ambiguous_upper.select(positive, safe_upper)
    range_relation = ambiguous_lower.compare(ambiguous_upper)
    exists = ((positive & (range_relation.order <= 0) & ((at_upper.order < 0) | (first_one.compare(safe_lower).order != 0)))
              | (negative & (at_upper.order < 0))
              | (equal & (offset.sign >= 0) & (at_offset.order < 0))) & needs_search
    fault = fault | (error1 | error2 | error3 | at_upper.fault | at_offset.fault | range_relation.fault) * needs_search.to(fault.dtype)
    count_lower = ambiguous_lower.select(exists, one)
    count_upper = ambiguous_upper.select(exists, one)

    def zero_count(low, high):
        end = high+one
        external_sum = end.floor_sum(eb, external_quote, external_remainder)-low.floor_sum(eb, external_quote, external_remainder)
        order_sum = end.floor_sum(ob, order_quote, order_remainder)-low.floor_sum(ob, order_quote, order_remainder)
        length = end-low
        sum_h = external_sum-order_sum-length*credit
        relation = sum_h.compare(length)
        error = sum_h.fault | length.fault | relation.fault
        error = error | ((sum_h.sign < 0) | (relation.order > 0)).to(error.dtype)
        return length-sum_h, error

    population, count_error = zero_count(count_lower, count_upper)
    has_population = exists & (population.sign > 0)
    fault = fault | count_error * exists.to(fault.dtype)

    def suffix_has_population(midpoint):
        count, error = zero_count(midpoint, count_upper)
        return count.sign > 0, error * has_population.to(error.dtype)

    # 所有无搜索行用同一合法单点，避免假区间产生错误或无意义大容量请求。
    search_lower = count_lower.select(has_population, one)
    search_upper = count_upper.select(has_population, one)
    selected, search_error = _search(search_lower, search_upper, suffix_has_population, iterations, last=True)
    fault = fault | search_error * has_population.to(fault.dtype)
    fallback = exists & ~has_population & positive & (ambiguous_lower.compare(safe_lower).order > 0)
    constant_negative = needs_search & equal & (offset.sign < 0)
    amount = selected.select(has_population, (ambiguous_lower-one).select(fallback, safe_upper.select(constant_negative, zero)))
    amount = maximum.select(full, amount)
    present = eligible & (full | has_population | fallback | constant_negative) & (fault == 0)
    amount = amount.select(present, zero)
    quote, _ = delta(amount)
    fault = fault | quote.fault | amount.fault
    present = present & (fault == 0)
    return LimitSegmentResult(amount.select(present, zero), quote.select(present, zero), present, fault)
