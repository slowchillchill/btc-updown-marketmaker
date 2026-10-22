//! 实际公共NOE链输出直接输入正式标签函数；不创建另一套经济/撮合归约。
//! 调用时刻是独立手算标签夹具，非原账户经营时序或模型训练轨迹。
use contracts::economics::EconomicRational as R;
use model_train::learning::{confirmed_k, finite_j, freeze_labels};
use serde_json::{Value, json};
use shared_run::LearningAccountReadV1;
use std::sync::Arc;
fn read(value: &Value) -> LearningAccountReadV1 {
    LearningAccountReadV1 {
        confirmed: Arc::new(serde_json::from_value(value.clone()).unwrap()),
        obligations: vec![],
        pending_noe_record_count: 0,
        qualified_rebates: vec![],
        directed_confirmations: None,
    }
}
fn row(k: &R, v: f64, t: i64) -> Value {
    json!({"k_exact":k.public(),"old_value":v,"old_joint_logp":-0.5,"processing_start_utc_ms":t})
}
fn close(a: &Value, b: f64) {
    assert!((a.as_f64().unwrap() - b).abs() < 1e-12);
}
#[test]
fn original_noe_negative_details_compensation_connect_to_rewards_gae_and_finite_tail() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/confirmed-original-noe-v1.json")).unwrap();
    let before = read(&fixture["negative"]["before_details"]);
    let detailed = read(&fixture["negative"]["detailed"]);
    let compensated = read(&fixture["negative"]["compensated"]);
    assert_eq!(
        confirmed_k(&before).unwrap(),
        confirmed_k(&detailed).unwrap()
    );
    let k = confirmed_k(&before).unwrap();
    let frames = vec![row(&R::zero(), 1.0, 1000), row(&k, 2.0, 101000)];
    let l = freeze_labels(&frames, &detailed, 300.0).unwrap();
    // 原5*.6=3本金及5*.07*.6*.4=.084费，各扣一次；不是NAV差额奖励。
    close(&l[0]["reward"], -0.084);
    close(&l[1]["reward"], -3.0);
    close(&l[0]["critic_target"], -3.084);
    close(&l[1]["critic_target"], -3.0);
    close(&l[0]["advantage"], 0.916 - 5.0 * (-100.0_f64 / 300.0).exp());
    close(&l[1]["advantage"], -5.0);
    let reversed = freeze_labels(&frames, &compensated, 300.0).unwrap();
    close(&reversed[0]["reward"], -0.084);
    close(&reversed[1]["reward"], 0.084);
    close(&reversed[0]["critic_target"], 0.0);
    close(&reversed[1]["critic_target"], 0.084);
    assert!(finite_j(&compensated).unwrap().is_zero());
}
#[test]
fn original_partial_maker_pair_confirmation_and_remaining_principal_connect_to_labels() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/confirmed-original-noe-v1.json")).unwrap();
    let pair = read(&fixture["pair"]);
    let k = confirmed_k(&pair).unwrap();
    assert_eq!(k, R::from_decimal(rust_decimal::Decimal::new(3, 1)));
    let l = freeze_labels(
        &[row(&R::zero(), 1.0, 1000), row(&k, 2.0, 2000)],
        &pair,
        300.0,
    )
    .unwrap();
    // 原分摊公共池Gpair=.3、未判定剩余本金=.5，故有限J=-.2。
    close(&l[0]["reward"], 0.3);
    close(&l[1]["reward"], -0.5);
    close(&l[0]["critic_target"], -0.2);
    close(&l[1]["critic_target"], -0.5);
    close(&l[0]["advantage"], 1.3 - 2.5 * (-1.0_f64 / 300.0).exp());
}

#[test]
fn original_negative_economic_target_and_special_credit_join_before_float_and_h_reversal() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/confirmed-original-noe-v1.json")).unwrap();
    let before = read(&fixture["negative"]["before_details"]);
    let end = read(&fixture["negative"]["detailed"]);
    let k = confirmed_k(&before).unwrap();
    let mut rows = vec![row(&R::zero(), 0.0, 10), row(&k, 0.0, 260)];
    for (r, c) in rows.iter_mut().zip([0, 1]) {
        r["special_credit"] = json!({"clipped_pusd":R::from_decimal(rust_decimal::Decimal::new(c,2)).public(),"necessary_unknown":[]});
    }
    let labels = model_train::learning::freeze_special_labels(
        &rows,
        &end,
        300.0,
        &model_train::special::SpecialPolicyV1::adopted(1),
    )
    .unwrap();
    // 原真实NOE确认费-.084及持仓本金终值-3；信用+.01随后H冲回-.01。
    close(&labels[0]["reward"], -0.074);
    close(&labels[1]["reward"], -3.01);
    close(&labels[1]["critic_target"], -3.01);
    let reward_exact: contracts::economics::ExactRationalV1 =
        serde_json::from_value(labels[1]["reward_exact"].clone()).unwrap();
    assert_eq!(
        R::try_from(&reward_exact).unwrap(),
        R::from_decimal(rust_decimal::Decimal::new(-301, 2))
    );
}
