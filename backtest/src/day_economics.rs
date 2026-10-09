//! 日内统计仅采样原 Ledger 与公开估值，不形成新的经济分录。
use contracts::{ExactRationalV1, ExecutionReadValueV1, economics::EconomicRational as R};
use ledger::LedgerEconomicReadV1;
use serde::{Deserialize, Serialize};
use shared_run::AccountEconomicsReadV2;

#[derive(Debug, Serialize)]
pub struct HistoricalDayEconomicsSummaryV1 {
    pub economics_version: String,
    pub samples: u64,
    pub observed_maximum_nav_drawdown_pusd: ExactRationalV1,
    pub maximum_nav_drawdown_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub maximum_nav_drawdown_fraction: ExecutionReadValueV1<ExactRationalV1>,
    pub unknown_nav_samples: u64,
    /// 实际预留（含原在途占用）+ 已扣本金尚未归还的原成交。
    pub maximum_committed_capital_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub committed_capital_pusd_milliseconds: ExecutionReadValueV1<ExactRationalV1>,
    pub average_committed_capital_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub measured_duration_ms: u64,
    pub unknown_capital_duration_ms: u64,
}
#[derive(Serialize, Deserialize)]
pub(crate) struct DayEconomicsStateV1 {
    samples: u64,
    last_at: Option<i64>,
    last_capital: Option<ExactRationalV1>,
    peak_nav: Option<ExactRationalV1>,
    drawdown: ExactRationalV1,
    drawdown_fraction: ExactRationalV1,
    peak_capital: ExactRationalV1,
    capital_integral: ExactRationalV1,
    duration_ms: u64,
    unknown_nav: u64,
    unknown_capital_ms: u64,
}
impl Default for DayEconomicsStateV1 {
    fn default() -> Self {
        Self {
            samples: 0,
            last_at: None,
            last_capital: None,
            peak_nav: None,
            drawdown: R::zero().public(),
            drawdown_fraction: R::zero().public(),
            peak_capital: R::zero().public(),
            capital_integral: R::zero().public(),
            duration_ms: 0,
            unknown_nav: 0,
            unknown_capital_ms: 0,
        }
    }
}
fn rational(value: &ExactRationalV1) -> R {
    R::try_from(value).expect("原经济读取的规范有理数")
}
fn amount(value: &contracts::ExactDecimal) -> R {
    R::from_decimal(value.0.parse().expect("原经济读取的精确金额"))
}
impl DayEconomicsStateV1 {
    pub fn sample(
        &mut self,
        at: i64,
        read: Option<(&LedgerEconomicReadV1<'_>, &AccountEconomicsReadV2)>,
    ) {
        if let Some(previous) = self.last_at {
            let duration = at.saturating_sub(previous).max(0) as u64;
            self.duration_ms += duration;
            if let Some(capital) = &self.last_capital {
                self.capital_integral = rational(&self.capital_integral)
                    .add(&rational(capital).mul(&R::from_decimal(duration.into())))
                    .public();
            } else {
                self.unknown_capital_ms += duration;
            }
        }
        self.last_at = Some(at);
        self.samples += 1;
        let Some((ledger, economics)) = read else {
            self.unknown_nav += 1;
            self.last_capital = None;
            return;
        };
        match &economics.conservative_nav_pusd {
            ExecutionReadValueV1::Known(nav) => {
                let nav = rational(nav);
                let peak = self
                    .peak_nav
                    .as_ref()
                    .map(rational)
                    .unwrap_or_else(|| nav.clone())
                    .max(nav.clone());
                let drawdown = peak.sub(&nav).max(R::zero());
                self.drawdown = rational(&self.drawdown).max(drawdown.clone()).public();
                if peak.is_positive() {
                    self.drawdown_fraction = rational(&self.drawdown_fraction)
                        .max(drawdown.divided_by(&peak))
                        .public();
                }
                self.peak_nav = Some(peak.public());
            }
            _ => self.unknown_nav += 1,
        }
        let mut capital = amount(&economics.cash.actual_reserved_pusd);
        for trade in ledger.successful_trades() {
            capital = capital.add(&rational(&trade.remaining_principal_cost_pusd));
        }
        // 原经济已扣款但终局尚未应用的成交仍占用本金，不与成功成交重复计数。
        for trade in ledger
            .trades()
            .filter(|t| t.economic_applied && !t.success_applied)
        {
            capital = capital.add(&amount(&trade.fact.quote_amount_pusd));
        }
        self.peak_capital = rational(&self.peak_capital).max(capital.clone()).public();
        self.last_capital = Some(capital.public());
    }
    pub fn finish_duration(&mut self, at: i64) {
        if let Some(previous) = self.last_at {
            let duration = at.saturating_sub(previous).max(0) as u64;
            self.duration_ms += duration;
            if let Some(capital) = &self.last_capital {
                self.capital_integral = rational(&self.capital_integral)
                    .add(&rational(capital).mul(&R::from_decimal(duration.into())))
                    .public();
            } else {
                self.unknown_capital_ms += duration;
            }
            self.last_at = Some(at);
        }
    }
    pub fn summary(&self) -> HistoricalDayEconomicsSummaryV1 {
        use ExecutionReadValueV1::{Known, Unavailable};
        let capital_known = self.unknown_capital_ms == 0 && self.last_capital.is_some();
        HistoricalDayEconomicsSummaryV1 {
            economics_version: shared_run::ACCOUNT_ECONOMICS_VERSION.into(),
            samples: self.samples,
            observed_maximum_nav_drawdown_pusd: self.drawdown.clone(),
            maximum_nav_drawdown_pusd: if self.unknown_nav == 0 {
                Known(self.drawdown.clone())
            } else {
                Unavailable
            },
            maximum_nav_drawdown_fraction: if self.unknown_nav == 0 {
                Known(self.drawdown_fraction.clone())
            } else {
                Unavailable
            },
            unknown_nav_samples: self.unknown_nav,
            maximum_committed_capital_pusd: if capital_known {
                Known(self.peak_capital.clone())
            } else {
                Unavailable
            },
            committed_capital_pusd_milliseconds: if capital_known {
                Known(self.capital_integral.clone())
            } else {
                Unavailable
            },
            average_committed_capital_pusd: if capital_known && self.duration_ms > 0 {
                Known(
                    rational(&self.capital_integral)
                        .divided_by(&R::from_decimal(self.duration_ms.into()))
                        .public(),
                )
            } else {
                Unavailable
            },
            measured_duration_ms: self.duration_ms,
            unknown_capital_duration_ms: self.unknown_capital_ms,
        }
    }
}
