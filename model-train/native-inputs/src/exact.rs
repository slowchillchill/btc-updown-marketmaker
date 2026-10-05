//! 原精确有理数求和后投影；区间端点同舍入才确定自然对数的 f32。
use contracts::{ExactDecimal, ExactRationalV1, economics::EconomicRational as R};
use num_bigint::{BigInt, BigUint};
use num_integer::Integer;
use num_traits::{One, Signed, ToPrimitive, Zero};
use rust_decimal::Decimal;
use std::sync::OnceLock;

type SharedProjection = std::sync::Arc<OnceLock<f32>>;

/// 一次原生组构造内共享纯投影；精确毫秒差和规范有理数分别作为键。
/// 不保存账户/市场业务状态；字段的单位、缺失位和来源时点仍由调用方保留。
#[derive(Default)]
pub(crate) struct InputProjection {
    times: std::sync::Mutex<std::collections::HashMap<i128, SharedProjection>>,
    values: std::sync::Mutex<std::collections::HashMap<(BigInt, BigInt), SharedProjection>>,
}
impl InputProjection {
    pub(crate) fn project(&self, milliseconds: i128) -> f32 {
        let value = self
            .times
            .lock()
            .unwrap()
            .entry(milliseconds)
            .or_default()
            .clone();
        // 只同步同键的首次计算；不同精确值仍可并行，不持有整张表的锁计算。
        *value.get_or_init(|| signed_log_f32(&ratio(milliseconds, 1000)))
    }
    pub(crate) fn log(&self, exact: &R) -> f32 {
        if exact.is_zero() {
            return 0.;
        }
        let key = (exact.numerator().clone(), exact.denominator().clone());
        let value = self.values.lock().unwrap().entry(key).or_default().clone();
        *value.get_or_init(|| signed_log_f32(exact))
    }
}

pub(crate) fn ratio(n: impl Into<BigInt>, d: impl Into<BigInt>) -> R {
    R::ratio(n.into(), d.into())
}
pub(crate) fn decimal(d: &ExactDecimal) -> R {
    R::from_decimal(Decimal::from_str_exact(&d.0).expect("公共精确金额"))
}
pub(crate) fn rational(r: &ExactRationalV1) -> R {
    R::try_from(r).expect("公共整数比")
}
pub(crate) fn integer(n: impl Into<BigInt>) -> R {
    ratio(n, 1)
}

/// 一次最近偶数舍入，指数与余数比较全部在整数域。
pub fn round_f32(value: &R) -> f32 {
    let n = value.numerator();
    let d = value.denominator().magnitude();
    let sign = if n.is_negative() { 1u32 << 31 } else { 0 };
    let n = n.magnitude();
    if n.is_zero() {
        return f32::from_bits(sign);
    }
    let mut e = i128::from(n.bits()) - i128::from(d.bits());
    let lower = if e >= 0 {
        n < &(d << usize::try_from(e).unwrap())
    } else {
        &(n << usize::try_from(-e).unwrap()) < d
    };
    if lower {
        e -= 1;
    }
    if e > 127 {
        return f32::from_bits(sign | 0x7f800000);
    }
    if e < -150 {
        return f32::from_bits(sign);
    }
    let shift = (e - 23).max(-149);
    let (a, b) = if shift >= 0 {
        (n.clone(), d << usize::try_from(shift).unwrap())
    } else {
        (n << usize::try_from(-shift).unwrap(), d.clone())
    };
    let (mut q, r) = a.div_rem(&b);
    if (&r << 1) > b || ((&r << 1) == b && q.is_odd()) {
        q += BigUint::one();
    }
    let mut q = q.to_u32().unwrap();
    if q == 0 {
        return f32::from_bits(sign);
    }
    if shift == -149 && q < (1 << 23) {
        return f32::from_bits(sign | q);
    }
    let mut exponent = shift + 23;
    if q == (1 << 24) {
        q >>= 1;
        exponent += 1;
    }
    if exponent > 127 {
        return f32::from_bits(sign | 0x7f800000);
    }
    f32::from_bits(sign | ((exponent + 127) as u32) << 23 | (q - (1 << 23)))
}
fn log_interval(t: &R, terms: &BigInt) -> (R, R) {
    let square = t.mul(t);
    let mut power = t.clone();
    let mut sum = R::zero();
    let mut j = BigInt::zero();
    while &j < terms {
        sum = sum.add(&power.divided_by(&integer(2 * &j + 1)));
        power = power.mul(&square);
        j += 1;
    }
    let lower = sum.mul(&integer(2));
    let remainder = power
        .mul(&integer(2))
        .divided_by(&integer(2 * terms + 1).mul(&integer(1).sub(&square)));
    (lower.clone(), lower.add(&remainder))
}
pub fn signed_log_f32(value: &R) -> f32 {
    #[cfg(test)]
    if let Some(result) =
        projection_diagnostic::project("signed_log_f32", value, || signed_log_f32_exact(value))
    {
        return result;
    }
    signed_log_f32_exact(value)
}

fn signed_log_f32_exact(value: &R) -> f32 {
    if value.is_zero() {
        return 0.0;
    }
    let n = value.numerator();
    let b = value.denominator();
    let a = n.abs() + b;
    let mut k = a.bits() - b.bits();
    if a < (b << usize::try_from(k).unwrap()) {
        k -= 1;
    }
    let scaled = b << usize::try_from(k).unwrap();
    let t = ratio(&a - &scaled, &a + &scaled);
    let mut terms = BigInt::from(8);
    loop {
        let (ml, mh) = log_interval(&t, &terms);
        // 相同有理区间只初始化一次；精度加倍仍走原算法，不改近似或舍入规则。
        static LN2_INITIAL: OnceLock<(R, R)> = OnceLock::new();
        let (low, high) = if k == 0 {
            (ml, mh)
        } else {
            let refined;
            let (ll, lh) = if terms == BigInt::from(8) {
                LN2_INITIAL.get_or_init(|| log_interval(&ratio(1, 3), &BigInt::from(8)))
            } else {
                refined = log_interval(&ratio(1, 3), &terms);
                &refined
            };
            (ml.add(&ll.mul(&integer(k))), mh.add(&lh.mul(&integer(k))))
        };
        let (low, high) = (round_f32(&low), round_f32(&high));
        if low.to_bits() == high.to_bits() {
            return if n.is_negative() { -low } else { low };
        }
        terms *= 2;
    }
}

/// 非负精确平方均值的 sqrt/log1p 投影；有理区间端点同舍入才返回。
pub(crate) fn sqrt_log_f32(value: &R) -> f32 {
    #[cfg(test)]
    if let Some(result) =
        projection_diagnostic::project("sqrt_log_f32", value, || sqrt_log_f32_exact(value))
    {
        return result;
    }
    sqrt_log_f32_exact(value)
}

fn sqrt_log_f32_exact(value: &R) -> f32 {
    if value.is_zero() {
        return 0.;
    }
    let n = value.numerator().magnitude();
    let d = value.denominator().magnitude();
    let mut bits = 32usize;
    loop {
        let scale = BigUint::one() << bits;
        let q = ((n << (2 * bits)) / d).sqrt();
        let low = ratio(BigInt::from(q.clone()), BigInt::from(scale.clone()));
        let high = ratio(BigInt::from(q + BigUint::one()), BigInt::from(scale));
        // 这是原精确sqrt区间内部的端点，不是模型消费的末端log输入。
        let (a, b) = (signed_log_f32_exact(&low), signed_log_f32_exact(&high));
        if a.to_bits() == b.to_bits() {
            return a;
        }
        bits *= 2;
    }
}

/// 只由专用ignored单测试激活。进程级状态覆盖Rayon工作线程；生产构建没有此模块。
#[cfg(test)]
pub(crate) mod projection_diagnostic {
    use super::R;
    use serde_json::{Value, json};
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::Mutex;

    #[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
    struct Key(String, String, String);
    struct Entry {
        reference_bits: u32,
        contexts: BTreeSet<String>,
        calls: usize,
    }
    struct State {
        policy: String,
        context: String,
        replacements: Option<BTreeMap<Key, u32>>,
        captured: BTreeMap<Key, Entry>,
        missing: BTreeSet<Key>,
    }
    static STATE: Mutex<Option<State>> = Mutex::new(None);

    pub(crate) struct Session;
    impl Drop for Session {
        fn drop(&mut self) {
            *STATE.lock().unwrap() = None;
        }
    }
    pub(crate) fn begin(policy: &str, table: Option<&Value>) -> Session {
        let replacements = table.map(|table| {
            assert_eq!(table["schema"], "projection_replacements_v1");
            assert_eq!(table["policy"], policy);
            let mut output = BTreeMap::new();
            for row in table["rows"].as_array().unwrap() {
                let operation = row["operation"].as_str().unwrap();
                assert!(matches!(operation, "signed_log_f32" | "sqrt_log_f32"));
                let input: contracts::ExactRationalV1 =
                    serde_json::from_value(row["input"].clone()).unwrap();
                let normalized = R::try_from(&input).unwrap().public();
                assert_eq!(normalized, input, "替换表必须保存规范有理数");
                let key = Key(operation.into(), input.numerator, input.denominator);
                let bits = u32::try_from(row["bits_u32"].as_u64().unwrap()).unwrap();
                assert!(
                    f32::from_bits(bits).is_finite(),
                    "组件诊断不接受非有限候选输入"
                );
                assert!(output.insert(key, bits).is_none(), "重复投影键");
            }
            output
        });
        let mut state = STATE.lock().unwrap();
        assert!(state.is_none(), "专用诊断不能嵌套或与另一诊断并发");
        *state = Some(State {
            policy: policy.into(),
            context: "initialization".into(),
            replacements,
            captured: BTreeMap::new(),
            missing: BTreeSet::new(),
        });
        Session
    }

    pub(crate) fn context(value: impl Into<String>) {
        STATE.lock().unwrap().as_mut().unwrap().context = value.into();
    }

    pub(super) fn project(operation: &str, value: &R, exact: impl FnOnce() -> f32) -> Option<f32> {
        let key = {
            let state = STATE.lock().unwrap();
            state.as_ref()?;
            Key(
                operation.into(),
                value.numerator().to_string(),
                value.denominator().to_string(),
            )
        };
        let existing = STATE
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .captured
            .get(&key)
            .map(|entry| entry.reference_bits);
        // 参考求值不持全局锁；不同Rayon任务仍可独立使用原精确实现。
        let reference_bits = existing.unwrap_or_else(|| exact().to_bits());
        let mut guard = STATE.lock().unwrap();
        let state = guard.as_mut().unwrap();
        let context = state.context.clone();
        let entry = state.captured.entry(key.clone()).or_insert_with(|| Entry {
            reference_bits,
            contexts: BTreeSet::new(),
            calls: 0,
        });
        assert_eq!(entry.reference_bits, reference_bits);
        entry.calls += 1;
        entry.contexts.insert(context);
        let bits = match &state.replacements {
            None => reference_bits,
            Some(table) => match table.get(&key) {
                Some(bits) => *bits,
                None => {
                    // 仅用于发现新的Actor路径；finish把整轮标为discovery_only，不能冒充完成比较。
                    state.missing.insert(key);
                    reference_bits
                }
            },
        };
        Some(f32::from_bits(bits))
    }

    pub(crate) fn snapshot() -> Value {
        let guard = STATE.lock().unwrap();
        let state = guard.as_ref().unwrap();
        let rows: Vec<_> = state
            .captured
            .iter()
            .map(|(key, entry)| {
                json!({
                    "operation": key.0, "input": {"numerator":key.1,"denominator":key.2},
                    "bits_u32":entry.reference_bits, "contexts":entry.contexts, "calls":entry.calls,
                    "missing_replacement":state.missing.contains(key),
                })
            })
            .collect();
        json!({"schema":"projection_capture_v1","policy":state.policy,
            "status":if state.missing.is_empty() {"complete"} else {"discovery_only"},
            "missing_replacements":state.missing.len(),"rows":rows,
            "scope":"公共组件实际末端投影；缺项精确值仅供发现路径，整轮不得作为候选比较完成"})
    }
}

#[cfg(test)]
mod projection_tests {
    use super::*;
    use rayon::prelude::*;

    #[test]
    fn shared_time_projection_preserves_signed_wide_differences_and_original_bits() {
        let projection = InputProjection::default();
        // 同低64位但不同精确差值不能串用；负来源年龄不能截为零。
        let differences = [
            0,
            1,
            -1,
            17,
            -17,
            999,
            1000,
            1001,
            i128::from(u64::MAX),
            i128::from(u64::MAX) + 2,
            -i128::from(u64::MAX),
        ];
        let expected: Vec<_> = differences
            .iter()
            .map(|&d| signed_log_f32(&ratio(d, 1000)).to_bits())
            .collect();
        for _ in 0..3 {
            let actual: Vec<_> = differences
                .par_iter()
                .map(|&d| projection.project(d).to_bits())
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn shared_value_projection_keeps_denominators_signs_and_nonterminating_values() {
        let projection = InputProjection::default();
        let values = [
            ratio(0, 1),
            ratio(1, 2),
            ratio(2, 4),
            ratio(-1, 2),
            ratio(1, 3),
            ratio(25001, 50003),
            ratio(1000, 1),
        ];
        let expected: Vec<_> = values.iter().map(|v| signed_log_f32(v).to_bits()).collect();
        for _ in 0..3 {
            let actual: Vec<_> = values
                .par_iter()
                .map(|v| projection.log(v).to_bits())
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    #[ignore = "显式导出已有精确归约与投影的 CPU 参考，供有限 GPU 数值诊断"]
    fn export_gpu_exact_numeric_reference_v1() {
        use serde_json::{Value, json};

        fn public_input(n: impl ToString, d: impl ToString) -> ExactRationalV1 {
            ExactRationalV1 {
                numerator: n.to_string(),
                denominator: d.to_string(),
            }
        }
        fn projection(kind: &str, value: &R, known_bits: Option<u32>) -> Value {
            let bits = match kind {
                "round_f32" => round_f32(value),
                "signed_log_f32" => signed_log_f32(value),
                "sqrt_log_f32" => sqrt_log_f32(value),
                _ => unreachable!(),
            }
            .to_bits();
            if let Some(expected) = known_bits {
                assert_eq!(bits, expected, "{kind}: {:?}", value.public());
            }
            json!({"operation": kind, "input": value.public(), "bits_u32": bits,
                "bits_hex": format!("0x{bits:08x}"), "existing_reference_bits": known_bits})
        }

        // ledger 的既有归一化案例；由公共解析入口执行符号归一化和 gcd 约分。
        let normalized: Vec<_> = [
            ("2", "4"),
            ("01", "02"),
            ("-1", "-2"),
            ("1", "-2"),
            ("0", "-7"),
        ]
        .into_iter()
        .map(|(n, d)| {
            let input = public_input(n, d);
            let value = R::try_from(&input).unwrap();
            json!({"input": input, "output": value.public()})
        })
        .collect();
        let half = R::try_from(&public_input(2, 4)).unwrap();
        let negative = R::try_from(&public_input(1, -2)).unwrap();
        let cancellation = negative.add(&half);
        assert_eq!(cancellation, R::zero());
        let comparisons: Vec<_> = [
            (&negative, R::zero()),
            (&half, ratio(1, 2)),
            (&half, R::zero()),
        ]
        .into_iter()
        .map(|(a, b)| {
            json!({"left": a.public(), "right": b.public(),
                "ordering": format!("{:?}", a.cmp(&b))})
        })
        .collect();

        // 两个有限技术构造，区别于上面的既有案例；只检验跨 limb 约分和有符号除法。
        let factor = BigInt::one() << 140usize;
        let wide_input = public_input(BigInt::from(-7) * &factor, BigInt::from(15) * &factor);
        let wide_reduced = R::try_from(&wide_input).unwrap();
        let signed_divisor = R::try_from(&public_input(25001, 50003)).unwrap();
        let signed_quotient = wide_reduced.divided_by(&signed_divisor);
        let technical_probes = json!({
            "source": "two_finite_technical_constructions; not_existing_business_fixtures",
            "cross_limb_normalization": {"input": wide_input, "output": wide_reduced.public()},
            "signed_division": {"left": wide_reduced.public(), "right": signed_divisor.public(),
                "quotient": signed_quotient.public()}
        });

        // market_tests 的真实中点序列；逐步调用生产四则，不另写分数公式。
        let midpoints = [ratio(2, 10), ratio(4, 10), ratio(3, 10)];
        let mut sum = R::zero();
        let mut rms_steps = Vec::new();
        for pair in midpoints.windows(2) {
            let difference = pair[1].sub(&pair[0]);
            let square = difference.mul(&difference);
            let previous_sum = sum.clone();
            sum = sum.add(&square);
            rms_steps.push(
                json!({"previous": pair[0].public(), "current": pair[1].public(),
                "difference": difference.public(), "square": square.public(),
                "previous_sum": previous_sum.public(), "sum": sum.public()}),
            );
        }
        let count = integer(midpoints.len() - 1);
        let mean = sum.divided_by(&count);
        assert_eq!(mean, ratio(25, 1000));

        // tests/rounding.rs 的六个既有最近偶数舍入案例，保留原参考位模式。
        let rounded: Vec<_> = [
            (ratio(16777217, 16777216), 1f32.to_bits()),
            (ratio(16777219, 16777216), 1f32.to_bits() + 2),
            (ratio(-16777217, 16777216), (-1f32).to_bits()),
            (ratio(1, BigInt::one() << 150), 0),
            (ratio(3, BigInt::one() << 150), 2),
            (integer(16777217), 16777216f32.to_bits()),
        ]
        .into_iter()
        .map(|(value, bits)| projection("round_f32", &value, Some(bits)))
        .collect();

        // 只保存后续 log/sqrt 的 CPU 参考；不表示本轮 GPU 已实现这些能力。
        let mut logs = Vec::new();
        for (n, d, bits) in [
            (0, 1, 0),
            (1, 1, 0x3f317218),
            (100, 1, 0x4093af11),
            (7, 10, 0x3f07d741),
            (3, 10, 0x3e86549c),
            (25001, 50003, 0x3ecf9840),
            (25002, 50003, 0x3ecf99ff),
        ] {
            logs.push(projection("signed_log_f32", &ratio(n, d), Some(bits)));
            if n != 0 {
                logs.push(projection(
                    "signed_log_f32",
                    &ratio(-n, d),
                    Some(bits | 0x80000000),
                ));
            }
        }
        for (n, d, bits) in [
            (
                "171828199048127284869960653697337016552858003929760823281171",
                "100000000000000000000000000000000000000000000000000000000000",
                0x3f800000,
            ),
            (
                "1718281990481272848699606536973370165528580039297608232811711",
                "1000000000000000000000000000000000000000000000000000000000000",
                0x3f800001,
            ),
        ] {
            logs.push(projection(
                "signed_log_f32",
                &R::try_from(&public_input(n, d)).unwrap(),
                Some(bits),
            ));
        }
        let mut roots: Vec<_> = [
            (5, 100, 0x3e4ea56b),
            (1, 100, 0x3dc331fc),
            (7, 3, 0x3f6d5fa5),
        ]
        .into_iter()
        .map(|(n, d, bits)| projection("sqrt_log_f32", &ratio(n, d), Some(bits)))
        .collect();
        roots.push(projection("sqrt_log_f32", &mean, None));
        let report = json!({
            "schema": "gpu_exact_numeric_reference_v1",
            "scope": "finite_cpu_reference_cases_and_two_technical_probes; no_gpu_execution_or_production_width_bound",
            "sources": {
                "rational": "contracts/src/economics.rs::EconomicRational",
                "normalization_cases": "ledger/src/economics.rs::rational_tests::integer_ratios_normalize_for_arithmetic_and_persistence",
                "round_and_log_cases": "model-train/native-inputs/tests/rounding.rs",
                "rms_and_sqrt_cases": "model-train/native-inputs/src/market_tests.rs::changing_real_midpoints_feed_exact_increment_rms",
                "producer": "model-train/native-inputs/src/exact.rs::projection_tests::export_gpu_exact_numeric_reference_v1"
            },
            "rational_reduction": {"normalization": normalized, "comparisons": comparisons,
                "technical_probes": technical_probes,
                "cancellation": {"left": negative.public(), "right": half.public(), "sum": cancellation.public()},
                "rms": {"steps": rms_steps, "sum": sum.public(), "divisor": count.public(), "mean": mean.public()}},
            "round_f32": rounded,
            "deferred_gpu_scope": {"signed_log_f32": logs, "sqrt_log_f32": roots}
        });
        println!("BTC_GPU_EXACT_NUMERIC_REFERENCE_V1={report}");
    }
}
