//! 公共有限分摊的 CPU 诊断材料；标准输出为 JSON，进度写标准错误。
//! 直接调用新 helper；不接入 Ledger，不把旧无限分母状态原地迁移。
use contracts::{
    economics::EconomicRational as R,
    finite_attribution::{
        ATTRIBUTION_DECIMAL_PLACES_V1, FINITE_ATTRIBUTION_RULE_V1,
        allocate_capacity_scale12_candidate_v1, round_scale12_rne_v1, split_scale12_rne_v1,
    },
};
use num_bigint::BigInt;
use num_traits::Signed;

fn r(n: i64, d: i64) -> R {
    R::ratio(n.into(), d.into())
}
// 只输出 BigInt 的规范十进制整数；无需引入新的 JSON 依赖或通用协议。
fn json(value: &R) -> String {
    format!(
        "{{\"numerator\":\"{}\",\"denominator\":\"{}\"}}",
        value.numerator(),
        value.denominator()
    )
}
fn array(values: &[R]) -> String {
    format!(
        "[{}]",
        values.iter().map(json).collect::<Vec<_>>().join(",")
    )
}
fn main() {
    let rounding = [
        ("zero", r(0, 1)),
        ("positive_half_to_even_zero", r(1, 2_000_000_000_000)),
        ("positive_half_to_even_two", r(3, 2_000_000_000_000)),
        ("positive_half_down_to_even_two", r(5, 2_000_000_000_000)),
        ("negative_half_to_even_zero", r(-1, 2_000_000_000_000)),
        ("negative_half_to_even_two", r(-3, 2_000_000_000_000)),
    ]
    .iter()
    .map(|(id, input)| {
        format!(
            "{{\"id\":\"{id}\",\"input\":{},\"rounded\":{}}}",
            json(input),
            json(&round_scale12_rne_v1(input))
        )
    })
    .collect::<Vec<_>>();
    let fine = r(10_000_000_000_007, 10_000_000_000_000);
    let splits = [
        ("one_third", r(1, 1), r(1, 1), r(3, 1)),
        ("fine_round_up", r(6, 10_000_000_000_000), r(9, 1), r(10, 1)),
        ("fine_remainder", fine.clone(), r(1, 1), r(3, 1)),
        ("zero_part", fine.clone(), r(0, 1), r(1, 1)),
        ("full_part", fine, r(1, 1), r(1, 1)),
        ("zero_total", r(0, 1), r(1, 1), r(3, 1)),
    ]
    .iter()
    .map(|(id, total, part, whole)| {
        let result = split_scale12_rne_v1(total, part, whole).unwrap();
        assert_eq!(result.allocated.add(&result.remaining), *total);
        assert!(result.allocated >= R::zero() && result.remaining >= R::zero());
        format!(
            "{{\"id\":\"{id}\",\"total\":{},\"part\":{},\"whole\":{},\"allocated\":{},\"remaining\":{}}}",
            json(total), json(part), json(whole), json(&result.allocated), json(&result.remaining)
        )
    })
    .collect::<Vec<_>>();
    let unit = 10_000_000_000_000;
    let allocations = [
        ("three_way_tail", r(16, unit), vec![r(10, unit); 3]),
        ("lower_capacity_boundary", r(9, unit), vec![r(6, unit); 2]),
        ("zero_capacity", r(1, 1), vec![r(0, 1), r(2, 1), r(0, 1)]),
        ("zero_total", r(0, 1), vec![r(0, 1); 3]),
    ]
    .iter()
    .map(|(id, total, capacities)| {
        let allocated = allocate_capacity_scale12_candidate_v1(total, capacities).unwrap();
        assert_eq!(
            allocated.iter().fold(R::zero(), |sum, v| sum.add(v)),
            *total
        );
        assert!(
            allocated
                .iter()
                .zip(capacities)
                .all(|(a, c)| a >= &R::zero() && a <= c)
        );
        format!(
            "{{\"id\":\"{id}\",\"total\":{},\"capacities\":{},\"allocated\":{}}}",
            json(total),
            array(capacities),
            array(&allocated)
        )
    })
    .collect::<Vec<_>>();

    // 与旧 32772 位分母构造使用同一成交序列；每轮配平的是 1/501，余项留池。
    const ITERATIONS: u32 = 3654;
    let initial = r(3, 2);
    let added = r(1, 250);
    let part = r(1, 1);
    let whole = r(501, 1);
    let mut cost = initial.clone();
    let mut cumulative = R::zero();
    let mut max_denominator_bits = cost.denominator().bits();
    let mut checkpoints = Vec::new();
    for iteration in 1..=ITERATIONS {
        let split = split_scale12_rne_v1(&cost.add(&added), &part, &whole).unwrap();
        cost = split.remaining;
        cumulative = cumulative.add(&split.allocated);
        max_denominator_bits = max_denominator_bits.max(cost.denominator().bits());
        assert_eq!(
            cost.add(&cumulative),
            initial.add(&added.mul(&r(i64::from(iteration), 1)))
        );
        assert!(cost >= R::zero() && cumulative >= R::zero());
        if [1, 2, 10, 100, 1000, ITERATIONS].contains(&iteration) {
            checkpoints.push(format!(
                "{{\"iteration\":{iteration},\"cost\":{},\"cumulative_allocated\":{}}}",
                json(&cost),
                json(&cumulative)
            ));
        }
        if iteration % 1000 == 0 || iteration == ITERATIONS {
            eprintln!("CPU 有限成本分摊：{iteration}/{ITERATIONS} 轮");
        }
    }
    let exact = r(2, 1).sub(&R::ratio(
        BigInt::from(500u32).pow(ITERATIONS),
        BigInt::from(2u8) * BigInt::from(501u32).pow(ITERATIONS),
    ));
    let difference = cost.sub(&exact);
    let error = R::ratio(
        difference.numerator().abs(),
        difference.denominator().clone(),
    );
    let bound = r(i64::from(ITERATIONS), 2_000_000_000_000);
    assert!(error <= bound);
    assert_eq!(exact.denominator().bits(), 32772);
    println!(
        "{{\"schema\":\"finite_attribution_reference_v1\",\"rule\":\"{}\",\"scale\":{},\
        \"scope\":\"step4_cpu_diagnostic_only;capacity_shares_quantum_is_candidate;no_ledger_migration\",\
        \"rounding\":[{}],\"splits\":[{}],\"capacity_allocations_candidate\":[{}],\
        \"growth\":{{\"iterations\":{},\"initial_cost\":{},\"added_cost_per_iteration\":{},\
        \"part\":{},\"whole\":{},\"final_cost\":{},\"cumulative_allocated\":{},\
        \"original_total_cost\":{},\"max_persisted_denominator_bits\":{},\
        \"denominator_measurement\":\"cost_after_each_split;includes_initial\",\
        \"checkpoints\":[{}],\"closed_form_reference_denominator_bits\":32772,\
        \"error_bound_pusd\":{},\"closed_form_error_within_bound\":true}}}}",
        FINITE_ATTRIBUTION_RULE_V1,
        ATTRIBUTION_DECIMAL_PLACES_V1,
        rounding.join(","),
        splits.join(","),
        allocations.join(","),
        ITERATIONS,
        json(&initial),
        json(&added),
        json(&part),
        json(&whole),
        json(&cost),
        json(&cumulative),
        json(&initial.add(&added.mul(&r(i64::from(ITERATIONS), 1)))),
        max_denominator_bits,
        checkpoints.join(","),
        json(&bound),
    );
}
