//! 独立日账户汇总：相加同口径原经济事实，不拼接账户或复利净值。
use crate::*;
use contracts::{economics::EconomicRational as R, *};
use serde::Serialize;
use std::collections::BTreeMap;
#[derive(Debug, Serialize)]
pub struct HistoricalIndependentDaysSummaryV1 {
    pub comparison_identity_sha256: Sha256Hex,
    pub days: usize,
    pub initial_capital_pusd: ExactRationalV1,
    pub total_confirmed_net_pnl_pusd: ExecutionReadValueV1<ExactRationalV1>,
    /// 原已确认净结果扣除尚未到账的已确认 Maker/Taker 返佣。
    pub total_net_pnl_excluding_unpaid_confirmed_rebates_pusd:
        ExecutionReadValueV1<ExactRationalV1>,
    pub capital_weighted_average_day_confirmed_return: ExecutionReadValueV1<ExactRationalV1>,
    pub worst_daily_public_nav_drawdown_pusd: ExecutionReadValueV1<ExactRationalV1>,
    pub unclosed_commitments: usize,
    pub pending_noe_records: usize,
    pub incomplete_or_exit_days: usize,
}
fn exact(value: &serde_json::Value) -> Result<R, MaterialError> {
    Ok(R::try_from(&serde_json::from_value::<ExactRationalV1>(
        value.clone(),
    )?)?)
}
pub fn summarize_independent_historical_days_v1(
    days: &[HistoricalDayResultV1],
) -> Result<Vec<HistoricalIndependentDaysSummaryV1>, MaterialError> {
    let mut candidates = BTreeMap::<String, Vec<&HistoricalDayCandidateResultV1>>::new();
    for day in days {
        for candidate in &day.candidates {
            candidates
                .entry(candidate.comparison_identity_sha256.0.clone())
                .or_default()
                .push(candidate);
        }
    }
    candidates
        .into_iter()
        .map(|(identity, days)| {
            let mut capital = R::zero();
            let mut pnl = R::zero();
            let mut paid_pnl = R::zero();
            let mut drawdown = R::zero();
            let mut pnl_known = true;
            let mut drawdown_known = true;
            let mut unclosed = 0;
            let mut pending = 0;
            let mut incomplete = 0;
            for day in &days {
                capital = capital.add(&R::from_decimal(day.initial_available_pusd.0.parse()?));
                let report = &day.report;
                if let Some(markets) = report["account_economics_memory"]["markets"].as_array() {
                    for market in markets {
                        let net = exact(&market["confirmed_net_pnl_pusd"])?;
                        pnl = pnl.add(&net);
                        paid_pnl = paid_pnl.add(&net);
                        for rebate in market["rebates"]
                            .as_array()
                            .ok_or("日经济读取缺少返佣事实")?
                        {
                            paid_pnl = paid_pnl.sub(&exact(&rebate["confirmed_unpaid_pusd"])?);
                        }
                    }
                } else {
                    pnl_known = false;
                }
                if let Some(value) =
                    report["day_economics"]["maximum_nav_drawdown_pusd"].get("Known")
                {
                    drawdown = drawdown.max(exact(value)?);
                } else {
                    drawdown_known = false;
                }
                unclosed += report["unclosed_commitments"]
                    .as_u64()
                    .ok_or("日结果缺少未闭合数")? as usize;
                pending += report["pending_noe_records"]
                    .as_u64()
                    .ok_or("日结果缺少未消费NOE数")? as usize;
                incomplete += usize::from(report["end_reason"] != "RangeEnd");
            }
            use ExecutionReadValueV1::{Known, Unavailable};
            Ok(HistoricalIndependentDaysSummaryV1 {
                comparison_identity_sha256: Sha256Hex(identity),
                days: days.len(),
                initial_capital_pusd: capital.public(),
                total_confirmed_net_pnl_pusd: if pnl_known {
                    Known(pnl.public())
                } else {
                    Unavailable
                },
                total_net_pnl_excluding_unpaid_confirmed_rebates_pusd: if pnl_known {
                    Known(paid_pnl.public())
                } else {
                    Unavailable
                },
                capital_weighted_average_day_confirmed_return: if pnl_known && capital.is_positive()
                {
                    Known(pnl.divided_by(&capital).public())
                } else {
                    Unavailable
                },
                worst_daily_public_nav_drawdown_pusd: if drawdown_known {
                    Known(drawdown.public())
                } else {
                    Unavailable
                },
                unclosed_commitments: unclosed,
                pending_noe_records: pending,
                incomplete_or_exit_days: incomplete,
            })
        })
        .collect()
}
