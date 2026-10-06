#include "../../contracts/gpu/exact.cuh"
using namespace public_gpu;

namespace {
struct Integer {
  uint32_t* words;
  int sign;
};

// 每线程独占临时arena。原量最多w字：offset至多3w+2，搜索n至多w+1，
// floor-sum的n*(n-1)*quotient至多3w+3字。6w+8留出加法和Euclid临时空间。
// 128个值槽上界覆盖所有同时存活的原表达式；循环每轮回收临时槽，不限制业务量。
struct Arena {
  uint32_t* storage;
  uint32_t* floor_work;
  int64_t cap;
  int64_t used = 0;
  int32_t fault = 0;

  __device__ Integer fresh() {
    if (used >= 128) { fault |= 1; return {storage + 127 * cap, 0}; }
    uint32_t* result = storage + used++ * cap;
    zero(result, cap);
    return {result, 0};
  }
  __device__ Integer normalized(Integer value) {
    if (!length(value.words, cap)) value.sign = 0;
    return value;
  }
  __device__ Integer input(const uint32_t* words, int64_t width, int sign) {
    Integer result = fresh();
    copy(result.words, cap, words, width);
    result.sign = sign;
    if (sign < -1 || sign > 1 || (length(result.words, cap) && sign == 0)) fault |= 1;
    return normalized(result);
  }
  __device__ Integer number(uint32_t value) {
    Integer result = fresh();
    result.words[0] = value;
    result.sign = value ? 1 : 0;
    return result;
  }
  __device__ Integer clone(Integer value) {
    Integer result = fresh();
    copy(result.words, cap, value.words, cap);
    result.sign = value.sign;
    return result;
  }
  __device__ void assign(Integer& target, Integer source) {
    copy(target.words, cap, source.words, cap);
    target.sign = source.sign;
  }
  __device__ Integer neg(Integer value) { value.sign = -value.sign; return value; }
  __device__ int cmp(Integer a, Integer b) {
    if (a.sign != b.sign) return a.sign < b.sign ? -1 : 1;
    return a.sign * compare(a.words, cap, b.words, cap);
  }
  __device__ Integer plus(Integer a, Integer b) {
    Integer result = fresh();
    if (a.sign == b.sign) {
      uint64_t carry = 0;
      for (int64_t j = 0; j < cap; ++j) {
        uint64_t value = uint64_t(a.words[j]) + b.words[j] + carry;
        result.words[j] = uint32_t(value);
        carry = value >> 32;
      }
      if (carry) fault |= 1;
      result.sign = a.sign;
    } else if (compare(a.words, cap, b.words, cap) >= 0) {
      subtract(result.words, cap, a.words, cap, b.words, cap); result.sign = a.sign;
    } else {
      subtract(result.words, cap, b.words, cap, a.words, cap); result.sign = b.sign;
    }
    return normalized(result);
  }
  __device__ Integer minus(Integer a, Integer b) { return plus(a, neg(b)); }
  __device__ Integer times(Integer a, Integer b) {
    Integer result = fresh();
    int64_t na = length(a.words, cap), nb = length(b.words, cap);
    if (na + nb > cap) { fault |= 1; return result; }
    multiply(result.words, cap, a.words, na, b.words, nb);
    result.sign = a.sign * b.sign;
    return normalized(result);
  }
  __device__ Integer quotient(Integer a, Integer b, Integer* remainder = nullptr) {
    Integer q = fresh(), r = fresh();
    if (!divide(q.words, cap, r.words, cap, a.words, cap, b.words, cap)) fault |= 1;
    q.sign = a.sign * b.sign;
    r.sign = a.sign;
    if (remainder) *remainder = normalized(r);
    return normalized(q);
  }
  __device__ Integer floor_sum(Integer n, Integer m, Integer a, Integer b) {
    Integer result = fresh();
    int64_t required = 0;
    if (n.sign < 0 || m.sign <= 0 || a.sign < 0 || b.sign < 0 ||
        !floor_sum_value(n.words, cap, m.words, cap, a.words, cap, b.words, cap,
                         result.words, cap, floor_work, cap, required) || required > cap) fault |= 1;
    result.sign = 1;
    return normalized(result);
  }
};

__device__ Integer numerator(Arena& arena, Integer offset, Integer slope, Integer amount) {
  return arena.plus(offset, arena.times(slope, amount));
}

// 原first_true的终点保底语义；所有循环仅在设备读取实际数值。
__device__ Integer first_true(Arena& a, Integer lower, Integer upper,
                              Integer offset, Integer slope, Integer denominator, int predicate) {
  Integer left = a.clone(lower), right = a.clone(upper), one = a.number(1), two = a.number(2);
  int64_t mark = a.used;
  while (!a.fault && a.cmp(left, right) < 0) {
    Integer midpoint = a.plus(left, a.quotient(a.minus(right, left), two));
    Integer value = numerator(a, offset, slope, midpoint);
    bool yes = predicate == 0 ? value.sign >= 0 :
        predicate == 1 ? a.cmp(value, denominator) >= 0 : a.cmp(value, denominator) < 0;
    if (yes) a.assign(right, midpoint);
    else a.assign(left, a.plus(midpoint, one));
    a.used = mark;
  }
  return left;
}

// 固定尾端的两个floor-sum只求一次；sum_h越界是fault，不能clip成空支持。
__device__ Integer zero_count(Arena& a, Integer low, Integer end, Integer external_end,
    Integer order_end, Integer eb, Integer eq, Integer er, Integer ob, Integer oq,
    Integer remainder, Integer credit) {
  Integer external = a.minus(external_end, a.floor_sum(low, eb, eq, er));
  Integer order = a.minus(order_end, a.floor_sum(low, ob, oq, remainder));
  Integer size = a.minus(end, low);
  Integer sum_h = a.minus(a.minus(external, order), a.times(size, credit));
  if (sum_h.sign < 0 || a.cmp(sum_h, size) > 0) a.fault |= 1;
  return a.minus(size, sum_h);
}
}

__global__ void limit_segment_kernel(const uint32_t* inputs, const int8_t* signs,
    int64_t width, uint32_t* bases, uint32_t* quotes, bool* present,
    int32_t* faults, int64_t* required, uint32_t* work, int64_t cap, int64_t rows) {
  int64_t row = int64_t(blockIdx.x) * blockDim.x + threadIdx.x;
  if (row >= rows) return;
  Arena a{work + row * 138 * cap, work + row * 138 * cap + 128 * cap, cap};
  Integer values[8];
  for (int i = 0; i < 8; ++i) {
    values[i] = a.input(inputs + (row * 8 + i) * width, width, signs[row * 8 + i]);
    if (values[i].sign < 0) a.fault |= 1;
  }
  Integer eb = values[0], eq = values[1], consumed = values[2], maximum = values[3];
  Integer ob = values[4], oq = values[5], matched = values[6], used = values[7];
  Integer one = a.number(1), two = a.number(2), amount = a.number(0), quote = a.number(0);
  if (eb.sign <= 0 || ob.sign <= 0 || a.cmp(consumed, eb) > 0 || a.cmp(matched, ob) > 0 ||
      a.cmp(maximum, a.minus(eb, consumed)) > 0 || a.cmp(maximum, a.minus(ob, matched)) > 0) a.fault |= 1;
  if (!a.fault && eq.sign > 0 && oq.sign > 0 && maximum.sign > 0) {
    Integer er, order_rem;
    Integer before = a.quotient(a.times(consumed, eq), eb, &er);
    Integer allocated = a.quotient(a.times(matched, oq), ob, &order_rem);
    Integer credit = a.minus(allocated, used);
    if (credit.sign < 0) a.fault |= 1;
    Integer residue;
    Integer lower = a.quotient(a.minus(eb, er), eq, &residue);
    if (residue.sign) lower = a.plus(lower, one);
    if (a.cmp(lower, one) < 0) lower = a.clone(one);
    Integer full_quote = a.minus(a.quotient(a.times(a.plus(consumed, maximum), eq), eb), before);
    Integer full_allowance = a.minus(a.quotient(a.times(a.plus(matched, maximum), oq), ob), used);
    if (!a.fault && a.cmp(lower, maximum) <= 0 && full_quote.sign > 0) {
      if (a.cmp(full_quote, full_allowance) <= 0) {
        a.assign(amount, maximum);
      } else {
        Integer denominator = a.times(eb, ob);
        Integer offset = a.minus(a.minus(a.times(er, ob), a.times(order_rem, eb)), a.times(credit, denominator));
        Integer slope = a.minus(a.times(eq, ob), a.times(oq, eb));
        Integer ambiguous_lower = a.clone(lower), ambiguous_upper = a.clone(maximum);
        bool exists = true;
        if (slope.sign > 0) {
          ambiguous_lower = first_true(a, lower, maximum, offset, slope, denominator, 0);
          if (a.cmp(numerator(a, offset, slope, maximum), denominator) >= 0) {
            Integer first_one = first_true(a, lower, maximum, offset, slope, denominator, 1);
            if (a.cmp(first_one, lower) == 0) exists = false;
            else ambiguous_upper = a.minus(first_one, one);
          }
          exists = exists && a.cmp(ambiguous_lower, ambiguous_upper) <= 0;
        } else if (slope.sign < 0) {
          if (a.cmp(numerator(a, offset, slope, maximum), denominator) >= 0) exists = false;
          else ambiguous_lower = first_true(a, lower, maximum, offset, slope, denominator, 2);
        } else if (offset.sign < 0) {
          a.assign(amount, maximum); exists = false;
        } else if (a.cmp(offset, denominator) >= 0) exists = false;
        if (exists && !a.fault) {
          Integer end = a.plus(ambiguous_upper, one);
          Integer external_end = a.floor_sum(end, eb, eq, er);
          Integer order_end = a.floor_sum(end, ob, oq, order_rem);
          int64_t mark = a.used;
          Integer population = zero_count(a, ambiguous_lower, end, external_end, order_end, eb, eq, er, ob, oq, order_rem, credit);
          bool has_population = population.sign > 0;
          a.used = mark;
          if (has_population && !a.fault) {
            Integer left = a.clone(ambiguous_lower), right = a.clone(ambiguous_upper);
            mark = a.used;
            while (!a.fault && a.cmp(left, right) < 0) {
              Integer midpoint = a.plus(left, a.quotient(a.plus(a.minus(right, left), one), two));
              Integer count = zero_count(a, midpoint, end, external_end, order_end, eb, eq, er, ob, oq, order_rem, credit);
              if (count.sign > 0) a.assign(left, midpoint);
              else a.assign(right, a.minus(midpoint, one));
              a.used = mark;
            }
            a.assign(amount, left);
          } else if (!a.fault && slope.sign > 0 && a.cmp(ambiguous_lower, lower) > 0) {
            a.assign(amount, a.minus(ambiguous_lower, one));
          }
        }
      }
    }
    if (!a.fault && amount.sign > 0) {
      a.assign(quote, a.minus(a.quotient(a.times(a.plus(consumed, amount), eq), eb), before));
    }
  }
  int64_t nb = length(amount.words, cap), nq = length(quote.words, cap);
  required[row] = nb > nq ? nb : nq;
  if (required[row] > width) a.fault |= 2;
  faults[row] = a.fault;
  present[row] = !a.fault && amount.sign > 0 && quote.sign > 0;
  zero(bases + row * width, width);
  zero(quotes + row * width, width);
  if (present[row]) {
    copy(bases + row * width, width, amount.words, nb);
    copy(quotes + row * width, width, quote.words, nq);
  }
}

extern "C" void sim_gpu_limit_segment(const uint32_t* inputs, const int8_t* signs,
    int64_t width, uint32_t* bases, uint32_t* quotes, bool* present, int32_t* faults,
    int64_t* required, uint32_t* work, int64_t cap, int64_t rows, cudaStream_t stream) {
  limit_segment_kernel<<<(rows+127)/128,128,0,stream>>>(inputs,signs,width,bases,quotes,present,faults,required,work,cap,rows);
}
