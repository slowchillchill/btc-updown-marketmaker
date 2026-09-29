//! 同一决定的精确选择状态；只编码，不推导可行支持集或选择动作。
use crate::exact::{integer, ratio, round_f32, signed_log_f32};
use contracts::economics::EconomicRational as R;
use num_bigint::{BigInt, BigUint};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    NoTrade,
    SubmitSingleBuy,
    SubmitPairedBuy,
    CancelOrder,
    CancelOrders,
    CancelMarketOrders,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Leg {
    A,
    B,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Family,
    QuantityBranch,
    Variant(Leg),
    Price(Leg),
    CancelObject,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Variant {
    GtcLimit,
    GtdLimit,
    FokMarket,
    FakMarket,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifetime {
    Seconds180,
    Seconds240,
    Seconds300,
}
#[derive(Clone, Debug, Default)]
pub struct LegSelection {
    pub variant: Option<Variant>,
    pub post_only: Option<bool>,
    pub lifetime: Option<Lifetime>,
    pub price_index: Option<BigUint>,
}
#[derive(Clone, Debug)]
pub struct QuantityInterval {
    pub upper: BigUint,
    pub lo: BigUint,
    pub hi: BigUint,
}
#[derive(Clone, Debug)]
pub struct PrefixState {
    pub family: Option<Family>,
    pub stage: Stage,
    pub single_leg: Option<Leg>,
    pub interval: Option<QuantityInterval>,
    pub quantity_hundredths: Option<BigUint>,
    pub legs: [LegSelection; 2],
}
impl Default for PrefixState {
    fn default() -> Self {
        Self {
            family: None,
            stage: Stage::Family,
            single_leg: None,
            interval: None,
            quantity_hundredths: None,
            legs: Default::default(),
        }
    }
}
impl PrefixState {
    /// 宿主到达精确叶子时保留原 n，投影不能反过来改变此值。
    pub fn select_quantity(&mut self, n: BigUint) {
        self.quantity_hundredths = Some(n);
        self.interval = None;
    }
    pub(crate) fn encode(&self, ticks: &[Option<R>; 2]) -> [f32; 36] {
        let mut o = [0.; 36];
        let (stage, leg) = match self.stage {
            Stage::Family => (0, None),
            Stage::QuantityBranch => (1, None),
            Stage::Variant(l) => (2, Some(l)),
            Stage::Price(l) => (3, Some(l)),
            Stage::CancelObject => (4, None),
        };
        o[7 + stage] = 1.;
        let Some(f) = self.family else {
            return o;
        };
        let family = match f {
            Family::NoTrade => 0,
            Family::SubmitSingleBuy => 1,
            Family::SubmitPairedBuy => 2,
            Family::CancelOrder => 3,
            Family::CancelOrders => 4,
            Family::CancelMarketOrders => 5,
        };
        o[family] = 1.;
        o[6] = 1.;
        if !matches!(f, Family::SubmitSingleBuy | Family::SubmitPairedBuy) {
            return o;
        }
        o[20] = 1.;
        if let Some(l) = leg {
            o[12 + usize::from(l == Leg::B)] = 1.;
            o[14] = 1.;
        }
        if let Some(n) = &self.quantity_hundredths {
            o[19] = signed_log_f32(&ratio(BigInt::from(n.clone()), 100));
            o[21] = 1.;
        } else if self.stage == Stage::QuantityBranch
            && let Some(q) = &self.interval
        {
            o[15] = signed_log_f32(&integer(BigInt::from(q.upper.clone())));
            o[16] = round_f32(&ratio(
                BigInt::from(q.lo.clone()),
                BigInt::from(q.upper.clone()),
            ));
            o[17] = round_f32(&ratio(
                BigInt::from(q.hi.clone()),
                BigInt::from(q.upper.clone()),
            ));
            o[18] = 1.;
        }
        for (i, l) in self.legs.iter().enumerate() {
            let enabled = f == Family::SubmitPairedBuy
                || self.single_leg == Some(if i == 0 { Leg::A } else { Leg::B });
            if !enabled {
                continue;
            }
            let b = 22 + 7 * i;
            o[b + 2] = 1.;
            o[b + 5] = 1.;
            if let Some(v) = l.variant {
                let k = match v {
                    Variant::GtcLimit => 0,
                    Variant::FokMarket => 1,
                    _ => unreachable!("native_actions_v2 只选择普通 GTC/FOK"),
                };
                o[b + k] = 1.;
                o[b + 3] = 1.;
            }
            if let Some(k) = &l.price_index {
                let tick = ticks[i].as_ref().expect("已选价格使用同帧已知精确tick");
                o[b + 4] = round_f32(
                    &tick
                        .mul(&integer(BigInt::from(k.clone())))
                        .mul(&integer(2))
                        .sub(&integer(1)),
                );
                o[b + 6] = 1.;
            }
        }
        o
    }
}
