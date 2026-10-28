"""Fixed #377 analytic-moment-bound-v3 arithmetic, on finite CPU data only.

This is a comparison consumer, not an optimizer or an alternative gradient gate.
Importing the module does not import torch.  Upward rounding is local to the
fixed nonnegative expressions; no post-state is an input to the bound.
"""
import math


ANALYTIC_VERSION = "b-analytic-moment-bound-v3"
U = 2.0 ** -24
TAU = 2.0 ** -126
F32_MAX = float.fromhex("0x1.fffffep+127")


class NotApplicable(ValueError):
    """The adopted finite coverage set cannot be established (Unknown)."""


def finite(torch, value):
    if not bool(torch.isfinite(value).all()):
        raise NotApplicable("nonfinite analytic input/intermediate/bound")
    return value


def _nonnegative(torch, value):
    value = torch.as_tensor(value, dtype=torch.float64, device="cpu")
    finite(torch, value)
    if bool((value < 0).any()):
        raise NotApplicable("negative operand in fixed nonnegative expression")
    return value


def add_up(torch, left, right):
    left, right = _nonnegative(torch, left), _nonnegative(torch, right)
    result = finite(torch, left + right)
    # Exact zero is known only when both nonnegative inputs are zero.
    return finite(torch, torch.where((left == 0) & (right == 0), result,
                                    torch.nextafter(result, torch.full_like(result, math.inf))))


def mul_up(torch, left, right):
    left, right = _nonnegative(torch, left), _nonnegative(torch, right)
    result = finite(torch, left * right)
    return finite(torch, torch.where((left == 0) | (right == 0), torch.zeros_like(result),
                                    torch.nextafter(result, torch.full_like(result, math.inf))))


def one_minus_up(value):
    if type(value) is not float or not 0 <= value < 1:
        raise NotApplicable("beta must be a Python float in [0,1)")
    return math.nextafter(1.0 - value, math.inf)


def gamma_up(n):
    # n*u and 1-n*u are exact here (n=5 or 6, binary powers).
    return math.nextafter((n * U) / (1.0 - n * U), math.inf)


def power_up(torch, base, exponent):
    value = _nonnegative(torch, 1.0)
    for _ in range(exponent):
        value = mul_up(torch, value, base)
    return value


def u32(torch, value):
    value = _nonnegative(torch, value)
    rounded = value.to(dtype=torch.float32)
    result = torch.nextafter(rounded, torch.full_like(rounded, math.inf)).double()
    return finite(torch, result)


def gradient_envelope(torch, reference):
    """Enclose the *unchanged* double/reference-directed old gradient gate."""
    reference = reference.double().abs()
    a, r = u32(torch, 1e-7), u32(torch, 1e-3)
    threshold = u32(torch, a + u32(torch, r * reference))
    # threshold is exactly FP32 representable; final neighbor covers subtract.
    return u32(torch, threshold)


def _fp32(torch, value):
    if (not isinstance(value, torch.Tensor) or value.device.type != "cpu"
            or value.dtype != torch.float32 or value.layout != torch.strided
            or value.is_complex()):
        raise NotApplicable("expected real dense CPU FP32 parameter/gradient/moment")
    return finite(torch, value.double())


def rho(torch, m, v, g, beta2):
    m, v, g = _fp32(torch, m), _fp32(torch, v), _fp32(torch, g)
    if m.shape != v.shape or m.shape != g.shape or bool((v < 0).any()):
        raise NotApplicable("moment/gradient shape or negative second moment")
    am, av, ag = m.abs(), v.abs(), g.abs()
    # Sufficient finite coverage of both permitted lerp forms and square branch.
    # Failure is Unknown, never an infinite envelope or an optimizer change.
    square = mul_up(torch, ag, ag)
    if bool((add_up(torch, am, ag) > F32_MAX).any()) or bool((square > F32_MAX).any()):
        raise NotApplicable("FP32 intermediate overflow cannot be excluded")
    b2v = mul_up(torch, beta2, av)
    weighted_square = mul_up(torch, one_minus_up(beta2), square)
    if bool((add_up(torch, b2v, weighted_square) > F32_MAX).any()):
        raise NotApplicable("FP32 second-moment addition overflow cannot be excluded")
    rm = add_up(torch, mul_up(torch, 2 * gamma_up(6), add_up(torch, am, ag)),
                mul_up(torch, mul_up(torch, 7.0, power_up(torch, 1 + U, 6)), TAU))
    tiny = add_up(torch, mul_up(torch, TAU, add_up(torch, add_up(torch, av,
                                                             mul_up(torch, 2.0, ag)), 5.0)),
                  mul_up(torch, TAU, TAU))
    rv = add_up(torch, mul_up(torch, gamma_up(5), add_up(torch, b2v, weighted_square)),
                mul_up(torch, power_up(torch, 1 + U, 5), tiny))
    return rm, rv


def advance(torch, pre_r, pre_c, gradient_r, gradient_c, previous, betas, maximize=False):
    """Return M/V/G using only pre-state and actual full gradients.

    Caller first applies the original gradient/parameter/diagnostic gates and
    handles None/missing-state separately.  Arrays are one named-parameter chunk.
    """
    if type(betas) not in (tuple, list) or len(betas) != 2:
        raise NotApplicable("two Python-float betas required")
    b1, b2 = betas
    one_minus_up(b1)
    one_minus_up(b2)
    gr, gc = _fp32(torch, gradient_r), _fp32(torch, gradient_c)
    if maximize:
        gradient_r, gradient_c = -gradient_r, -gradient_c
        gr, gc = -gr, -gc
    if gr.shape != gc.shape:
        raise NotApplicable("gradient shape mismatch")
    rm_r, rv_r = rho(torch, pre_r[0], pre_r[1], gradient_r, b2)
    rm_c, rv_c = rho(torch, pre_c[0], pre_c[1], gradient_c, b2)
    g = gradient_envelope(torch, gr)
    pm, pv = previous
    m = add_up(torch, add_up(torch, mul_up(torch, b1, pm), mul_up(torch, one_minus_up(b1), g)),
               add_up(torch, rm_r, rm_c))
    square_difference = add_up(torch, mul_up(torch, mul_up(torch, 2.0, gr.abs()), g),
                               mul_up(torch, g, g))
    v = add_up(torch, add_up(torch, mul_up(torch, b2, pv),
                                     mul_up(torch, one_minus_up(b2), square_difference)),
               add_up(torch, rv_r, rv_c))
    return m, v, g


def difference_within(torch, reference, candidate, upper):
    """Real FP32 difference <= binary64 bound, including equality.

    Most elements are decided by neighboring binary64 differences.  Only an
    ambiguous equality boundary uses the two scalars' exact dyadic integer
    ratios. This is a scalar comparison fallback, not an arithmetic interpreter.
    """
    r, c = _fp32(torch, reference).reshape(-1), _fp32(torch, candidate).reshape(-1)
    upper = _nonnegative(torch, upper).expand_as(reference).reshape(-1)
    difference = (c - r).abs()
    low = torch.nextafter(difference, torch.full_like(difference, -math.inf))
    high = torch.nextafter(difference, torch.full_like(difference, math.inf))
    passed = high <= upper
    equal = r == c
    passed |= equal
    ambiguous = (~equal) & (low <= upper) & (~passed)
    for index in torch.nonzero(ambiguous, as_tuple=False).flatten().tolist():
        rn, rd = float(r[index]).as_integer_ratio()
        cn, cd = float(c[index]).as_integer_ratio()
        un, ud = float(upper[index]).as_integer_ratio()
        passed[index] = abs(cn * rd - rn * cd) * ud <= un * cd * rd
    return bool(passed.all()), {"elements": r.numel(),
                                "max_abs": float(difference.max()) if r.numel() else 0.0,
                                "max_bound": float(upper.max()) if r.numel() else 0.0,
                                "boundary_scalar_checks": int(ambiguous.sum())}
