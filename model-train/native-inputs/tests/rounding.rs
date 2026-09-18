use contracts::{ExactRationalV1, economics::EconomicRational};
use model_native_inputs::exact::{round_f32, signed_log_f32};
use num_bigint::BigInt;
fn r(n: impl ToString, d: impl ToString) -> EconomicRational {
    EconomicRational::try_from(&ExactRationalV1 {
        numerator: n.to_string(),
        denominator: d.to_string(),
    })
    .unwrap()
}
#[test]
fn exact_rounding_ties_subnormal_sign_and_large_integer() {
    assert_eq!(round_f32(&r(16777217, 16777216)).to_bits(), 1f32.to_bits());
    assert_eq!(
        round_f32(&r(16777219, 16777216)).to_bits(),
        1f32.to_bits() + 2
    );
    assert_eq!(
        round_f32(&r(-16777217, 16777216)).to_bits(),
        (-1f32).to_bits()
    );
    assert_eq!(round_f32(&r(1, BigInt::from(1) << 150)).to_bits(), 0);
    assert_eq!(round_f32(&r(3, BigInt::from(1) << 150)).to_bits(), 2);
    assert_eq!(round_f32(&r(16777217, 1)), 16777216.);
}
#[test]
fn interval_log_matches_independent_high_precision_reference_bits() {
    // mpmath 120 decimal digits, then exact midpoint comparison between adjacent f32s.
    // Checked constants include nonterminating rebate fractions and signed values.
    for (n, d, bits) in [
        ("0", "1", 0),
        ("1", "1", 0x3f317218),
        ("100", "1", 0x4093af11),
        ("7", "10", 0x3f07d741),
        ("3", "10", 0x3e86549c),
        ("25001", "50003", 0x3ecf9840),
        ("25002", "50003", 0x3ecf99ff),
    ] {
        assert_eq!(signed_log_f32(&r(n, d)).to_bits(), bits, "{n}/{d}");
        if n != "0" {
            assert_eq!(
                signed_log_f32(&r(format!("-{n}"), d)).to_bits(),
                bits | 0x80000000
            );
        }
    }
}

#[test]
fn logarithm_interval_resolves_both_sides_of_a_float_midpoint() {
    assert_eq!(
        signed_log_f32(&r(
            "171828199048127284869960653697337016552858003929760823281171",
            "100000000000000000000000000000000000000000000000000000000000"
        ))
        .to_bits(),
        0x3f800000
    );
    assert_eq!(
        signed_log_f32(&r(
            "1718281990481272848699606536973370165528580039297608232811711",
            "1000000000000000000000000000000000000000000000000000000000000"
        ))
        .to_bits(),
        0x3f800001
    );
}
