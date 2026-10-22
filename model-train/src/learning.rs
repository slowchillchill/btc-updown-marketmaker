//! 目标只组合公共确认/本金读取；公共再生不重跑账户或SIM。
use contracts::{economics::EconomicRational as R, *};
use ledger::ConfirmedComponentKindV1 as K;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use shared_run::LearningAccountReadV1;
use std::path::PathBuf;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CollectionConfig {
    pub group_id: String,
    pub active_end_utc_ms: i64,
    pub evidence_end_utc_ms: i64,
    pub trajectory_path: PathBuf,
    #[serde(default)]
    pub collection_fault_after_call: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub special: Option<crate::special::SpecialPolicyV1>,
}
pub fn confirmed_k(read: &LearningAccountReadV1) -> Result<R, String> {
    let mut overrides = std::collections::BTreeMap::new();
    for r in &read.qualified_rebates {
        if let ExecutionReadValueV1::Known(v) = &r.result {
            let amount = rust_decimal::Decimal::from_str_exact(&v.day_result.payable_pusd.0)
                .map_err(|e| e.to_string())?;
            overrides.insert(r.identity.as_str(), R::from_decimal(amount));
        }
    }
    let mut result = R::zero();
    for c in read
        .confirmed
        .components
        .iter()
        .filter(|c| !overrides.contains_key(c.identity.as_str()))
    {
        let amount = R::try_from(&c.amount_pusd).map_err(|e| e.to_string())?;
        result = match c.kind {
            K::PairedGross | K::MakerRebate | K::EffectiveTakerRebate => result.add(&amount),
            _ => result.sub(&amount),
        };
    }
    Ok(overrides.values().fold(result, |sum, value| sum.add(value)))
}

pub fn finite_j(read: &LearningAccountReadV1) -> Result<R, String> {
    if !read.confirmed.unknown_negative_cost_identities.is_empty() {
        return Err("有限目标必要负成本/NOE覆盖尚待定".into());
    }
    let mut result = confirmed_k(read)?.sub(
        &R::try_from(&read.confirmed.unresolved_position_principal_pusd)
            .map_err(|e| e.to_string())?,
    );
    for c in &read.obligations {
        match &c.principal_upper_pusd {
            ExecutionReadValueV1::Known(p) => {
                result = result.sub(&R::try_from(p).map_err(|e| e.to_string())?)
            }
            ExecutionReadValueV1::NotApplicable => {}
            _ => return Err("义务无合法本金上界，标签待定".into()),
        }
    }
    Ok(result)
}
fn number(v: &R) -> Result<f64, String> {
    use num_traits::ToPrimitive;
    if v.is_zero() {
        return Ok(0.0);
    }
    // 同时提取两边最高53位，再补指数，避免有限比值的inf/inf。
    let ns = v.numerator().bits().saturating_sub(53) as usize;
    let ds = v.denominator().bits().saturating_sub(53) as usize;
    let n = (v.numerator() >> ns).to_f64().ok_or("分子有效位转换失败")?;
    let d = (v.denominator() >> ds)
        .to_f64()
        .ok_or("分母有效位转换失败")?;
    let exponent = ns as i64 - ds as i64;
    let ratio = n / d;
    let value = if exponent > 1023 {
        ratio * 2.0_f64.powi(1023) * 2.0_f64.powi((exponent - 1023).min(i32::MAX as i64) as i32)
    } else if exponent < -1022 {
        ratio * 2.0_f64.powi(-1022) * 2.0_f64.powi((exponent + 1022).max(i32::MIN as i64) as i32)
    } else {
        ratio * 2.0_f64.powi(exponent as i32)
    };
    if value.is_finite() {
        Ok(value)
    } else {
        Err("神经金额投影超出f64表示，精确经济值保留".into())
    }
}
pub fn freeze_labels(
    rows: &[Value],
    end: &LearningAccountReadV1,
    tau: f64,
) -> Result<Vec<Value>, String> {
    let j = finite_j(end)?;
    let mut labels = Vec::new();
    for row in rows {
        if row["processing_start_utc_ms"].as_i64().is_none() || row["old_value"].as_f64().is_none()
        {
            return Err("轨迹缺实际SIM时间或旧价值".into());
        }
    }
    for (i, r) in rows.iter().enumerate() {
        let k = R::try_from(
            &serde_json::from_value::<ExactRationalV1>(r["k_exact"].clone())
                .map_err(|e| e.to_string())?,
        )
        .map_err(|e| e.to_string())?;
        let next = if let Some(n) = rows.get(i + 1) {
            R::try_from(
                &serde_json::from_value::<ExactRationalV1>(n["k_exact"].clone())
                    .map_err(|e| e.to_string())?,
            )
            .map_err(|e| e.to_string())?
        } else {
            j.clone()
        };
        labels.push(json!({"reward":number(&next.sub(&k))?,"critic_target":number(&j.sub(&k))?,"k_exact":k.public(),"j_end_exact":j.public(),"old_value":r["old_value"],"old_joint_logp":r["old_joint_logp"],"processing_start_utc_ms":r["processing_start_utc_ms"]}));
    }
    let mut following_adv = 0.0;
    for i in (0..labels.len()).rev() {
        let old = labels[i]["old_value"].as_f64().ok_or("旧Critic缺失")?;
        let next = labels.get(i + 1);
        let delta = labels[i]["reward"].as_f64().unwrap()
            + next.and_then(|n| n["old_value"].as_f64()).unwrap_or(0.0)
            - old;
        let lambda = next
            .map(|n| {
                (-((n["processing_start_utc_ms"]
                    .as_i64()
                    .expect("已验证实际时间")
                    - labels[i]["processing_start_utc_ms"]
                        .as_i64()
                        .expect("已验证实际时间")) as f64
                    / 1000.0)
                    / tau)
                    .exp()
            })
            .unwrap_or(0.0);
        let advantage = delta + lambda * following_adv;
        labels[i]["advantage"] = advantage.into();
        labels[i]["lambda"] = lambda.into();
        following_adv = advantage;
    }
    Ok(labels)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LearningParameters {
    pub learning_rate: f64,
    pub epsilon: f64,
    pub value_weight: f64,
    pub money_scale_pusd: f64,
    pub tau_gae_s: f64,
    pub tbptt_steps: usize,
    pub ppo_epochs: usize,
    pub checkpoint_directory: PathBuf,
    pub trajectory_page_rows: usize,
    pub trajectory_cached_pages: usize,
    pub training_sequence_batch: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub special: Option<crate::special::SpecialPolicyV1>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LearningJobConfig {
    pub consumer: crate::ConsumerRunConfig,
    pub parameters: LearningParameters,
    pub group_id: String,
    pub output_directory: PathBuf,
    pub active_end_utc_ms: i64,
    /// 多日job须逐日明确E，不能把同一绝对时刻应用到后一天。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_end_by_day_utc_ms: Option<Vec<i64>>,
    #[serde(default)]
    pub audit_before_update: bool,
    #[serde(default)]
    pub gradient_checks: bool,
    #[serde(default)]
    pub fault: Option<String>,
    pub attempt_report: PathBuf,
}
fn read_frozen_labels(
    path: &std::path::Path,
) -> Result<std::collections::BTreeMap<String, Vec<Value>>, backtest::MaterialError> {
    let labels: std::collections::BTreeMap<String, Vec<Value>> =
        serde_json::from_slice(&std::fs::read(path)?)?;
    for values in labels.values() {
        for v in values {
            for key in [
                "reward",
                "critic_target",
                "old_value",
                "old_joint_logp",
                "advantage",
                "lambda",
            ] {
                if v[key].as_f64().is_none() {
                    return Err("冻结标签数值字段损坏".into());
                }
            }
        }
    }
    Ok(labels)
}
fn write_new(path: &std::path::Path, value: &Value) -> Result<(), backtest::MaterialError> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)?;
    serde_json::to_writer_pretty(&mut f, value)?;
    f.write_all(b"\n")?;
    f.sync_all()?;
    Ok(())
}
/// 只读取必要私有轨迹；公共输入随后由原Parquet/统计流按引用再生。
#[derive(Deserialize)]
struct StoredTime {
    logical_time_utc_ms: i64,
}
#[derive(Deserialize)]
struct StoredReceipt {
    group: PublicGroupReferenceV1,
}
#[derive(Deserialize)]
struct StoredCutoff {
    decision_time: StoredTime,
    receipt: StoredReceipt,
}
#[derive(Deserialize)]
struct StoredMarketFrame {
    market_cutoff: StoredCutoff,
}
#[derive(Deserialize)]
struct StoredFrame {
    version: String,
    processing_start_utc_ms: i64,
    old_value: f64,
    old_joint_logp: f64,
    k_exact: ExactRationalV1,
    market_frame: StoredMarketFrame,
    #[serde(default)]
    special_credit: Value,
    #[serde(default)]
    economic: Value,
}
struct Trajectory {
    file: std::io::BufReader<std::fs::File>,
    offsets: Vec<u64>,
    meta: Vec<Value>,
    page_rows: usize,
    max_pages: usize,
    pages: std::collections::VecDeque<(usize, Vec<Value>)>,
}
impl Trajectory {
    #[cfg(test)]
    fn open(
        path: &std::path::Path,
        page_rows: usize,
        max_pages: usize,
    ) -> Result<Self, backtest::MaterialError> {
        Self::open_version(
            path,
            page_rows,
            max_pages,
            crate::public_input::TRAJECTORY_VERSION,
        )
    }
    fn open_version(
        path: &std::path::Path,
        page_rows: usize,
        max_pages: usize,
        version: &str,
    ) -> Result<Self, backtest::MaterialError> {
        use std::io::{BufRead, Seek};
        let mut file = std::io::BufReader::new(std::fs::File::open(path)?);
        let mut offsets = Vec::new();
        let mut meta = Vec::new();
        loop {
            let offset = file.stream_position()?;
            let mut line = String::new();
            if file.read_line(&mut line)? == 0 {
                break;
            }
            let value: Value = serde_json::from_str(&line)?;
            if value["record_type"] == "confirmed_economic_update"
                || value["record_type"] == "rules_binding"
            {
                continue;
            }
            let typed: StoredFrame = serde_json::from_value(value)?;
            if typed.version != version {
                return Err("未知轨迹版本".into());
            }
            let _ = &typed.market_frame.market_cutoff.receipt.group;
            R::try_from(&typed.k_exact)?;
            offsets.push(offset);
            meta.push(json!({"ledger_entry_seq":typed.economic["ledger_entry_seq"],"special_credit":typed.special_credit,"k_exact":typed.k_exact,"processing_start_utc_ms":typed.processing_start_utc_ms,"old_value":typed.old_value,"old_joint_logp":typed.old_joint_logp,"cutoff":typed.market_frame.market_cutoff.decision_time.logical_time_utc_ms}));
        }
        Ok(Self {
            file,
            offsets,
            meta,
            page_rows,
            max_pages,
            pages: Default::default(),
        })
    }
    fn get(&mut self, index: usize) -> Result<Value, backtest::MaterialError> {
        use std::io::{BufRead, Seek, SeekFrom};
        let page = index / self.page_rows;
        if let Some(i) = self.pages.iter().position(|(p, _)| *p == page) {
            let cached = self.pages.remove(i).unwrap();
            self.pages.push_back(cached);
        } else {
            let start = page * self.page_rows;
            let mut values = Vec::new();
            for i in start..(start + self.page_rows).min(self.offsets.len()) {
                self.file.seek(SeekFrom::Start(self.offsets[i]))?;
                let mut line = String::new();
                self.file.read_line(&mut line)?;
                values.push(serde_json::from_str(&line)?);
            }
            if self.pages.len() == self.max_pages {
                self.pages.pop_front();
            }
            self.pages.push_back((page, values));
        }
        Ok(self.pages.back().unwrap().1[index % self.page_rows].clone())
    }
    fn positions_at(&self, at: i64) -> std::ops::Range<usize> {
        self.meta
            .partition_point(|r| r["cutoff"].as_i64().unwrap() < at)
            ..self
                .meta
                .partition_point(|r| r["cutoff"].as_i64().unwrap() <= at)
    }
}
/// 与原日供料相同：只有统计配置及公共交付时序相等的账户共享一次再生。
pub(crate) fn visit_day_frames(
    rules: &backtest::PublicExportRulesV1,
    sealed_rules: Option<std::sync::Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>>,
    day: &backtest::HistoricalDayConfigV1,
    queries: &std::collections::BTreeMap<String, std::collections::BTreeSet<i64>>,
    visit: impl FnMut(
        &str,
        i64,
        &PublicGroupReferenceV1,
        &[shared_run::FrozenMarketReadV1],
    ) -> Result<(), backtest::MaterialError>,
) -> Result<usize, backtest::MaterialError> {
    visit_day_frames_controlled(rules, sealed_rules, day, queries, visit, || false, |_| {})
}
pub(crate) fn visit_day_frames_controlled(
    rules: &backtest::PublicExportRulesV1,
    sealed_rules: Option<std::sync::Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>>,
    day: &backtest::HistoricalDayConfigV1,
    queries: &std::collections::BTreeMap<String, std::collections::BTreeSet<i64>>,
    mut visit: impl FnMut(
        &str,
        i64,
        &PublicGroupReferenceV1,
        &[shared_run::FrozenMarketReadV1],
    ) -> Result<(), backtest::MaterialError>,
    should_stop: impl Fn() -> bool + Sync,
    extra_progress: impl Fn(&backtest::ContinuousPublicExportProgressV1) + Sync,
) -> Result<usize, backtest::MaterialError> {
    use backtest::*;
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for (index, candidate) in day.candidates.iter().enumerate() {
        if let Some(group) = groups.iter_mut().find(|group| {
            let previous = &day.candidates[group[0]];
            previous.public_statistics == candidate.public_statistics
                && previous.timing.public_delivery_delay_ms
                    == candidate.timing.public_delivery_delay_ms
        }) {
            group.push(index);
        } else {
            groups.push(vec![index]);
        }
    }
    let mut streams = 0;
    for indices in groups {
        let first = &day.candidates[indices[0]];
        let keys = indices
            .iter()
            .map(|&index| {
                serde_json::from_value::<crate::ConsumerConfig>(
                    day.candidates[index].strategy.effective_parameters["model_consumer"].clone(),
                )
                .map(|config| config.account_key)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let times = keys
            .iter()
            .flat_map(|key| queries[key].iter().copied())
            .collect::<std::collections::BTreeSet<_>>();
        if times.is_empty() {
            continue;
        }
        let progress_every = serde_json::from_value::<crate::ConsumerConfig>(
            first.strategy.effective_parameters["model_consumer"].clone(),
        )?
        .progress_every;
        let config = ContinuousPublicExportConfigV1 {
            rules: rules.clone(),
            preparation: day.preparation.clone(),
            markets: PublicObservationMarketsV1::All,
            statistics: first.public_statistics.clone().ok_or("缺公共统计配置")?,
            public_delivery_delay_ms: first.timing.public_delivery_delay_ms.clone(),
            observe_after_groups: false,
            query_times_utc_ms: times.into_iter().collect(),
            export_new_trades: false,
        };
        streams += 1;
        let sink =
            |at, group: &PublicGroupReferenceV1, frames: Vec<shared_run::FrozenMarketReadV1>| {
                for key in &keys {
                    if queries[key].contains(&at) {
                        visit(key, at, group, &frames)?;
                    }
                }
                Ok(())
            };
        let progress = |p: &ContinuousPublicExportProgressV1| {
            extra_progress(p);
            if p.stage == "public_application"
                && progress_every > 0
                && p.applied_groups.is_multiple_of(progress_every)
            {
                eprintln!(
                    "公共再生: 原输入{}，组{}，耗时{:.3}s",
                    p.processed_inputs, p.applied_groups, p.elapsed_seconds
                );
            }
        };
        let rules = match &sealed_rules {
            Some(rules) => rules.clone(),
            None => sim_system::platform_rules::load_historical_process_snapshot_v3(
                sim_system::platform_rules::HistoricalProcessSnapshotRequestV1 {
                    snapshot_path: config.rules.snapshot_path.clone(),
                    expected_file_sha256: config.rules.expected_file_sha256.clone(),
                    replay_causal_start_utc_ms: day.preparation.scope.received_start_utc_ms,
                },
            )?,
        };
        visit_continuous_public_frames_with_requirements_v1(
            &config,
            rules,
            crate::public_input::OBSERVATION,
            crate::public_input::SOURCE_PROGRESS,
            sink,
            progress,
            &should_stop,
        )?;
    }
    Ok(streams)
}
/// 公共部分只在调用消费者时再生，不写完整行情/观察/特征材料。
fn visit_group_frames(
    consumer: &crate::ConsumerRunConfig,
    rows: &mut std::collections::BTreeMap<String, Trajectory>,
    sealed_rules: std::sync::Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    mut visit: impl FnMut(&str, usize, Value) -> Result<(), backtest::MaterialError>,
) -> Result<(), backtest::MaterialError> {
    for day in &consumer.days {
        let queries = day
            .candidates
            .iter()
            .map(|candidate| {
                let config: crate::ConsumerConfig = serde_json::from_value(
                    candidate.strategy.effective_parameters["model_consumer"].clone(),
                )?;
                let times = rows[&config.account_key]
                    .meta
                    .iter()
                    .map(|r| r["cutoff"].as_i64().expect("合法市场截止"))
                    .collect();
                Ok((config.account_key, times))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>, serde_json::Error>>()?;
        let public_rules = consumer
            .rules
            .frozen()
            .ok_or("公共再生缺原已解析规则")?
            .public_rules();
        visit_day_frames(
            &public_rules,
            Some(sealed_rules.clone()),
            day,
            &queries,
            |key, at, group, frames| {
                for index in rows[key].positions_at(at) {
                    let row = rows.get_mut(key).unwrap().get(index)?;
                    let expected = &row["market_frame"]["market_cutoff"]["receipt"]["group"];
                    if serde_json::to_value(group)? != *expected {
                        return Err("公共再生组与原帧可信前缀不同".into());
                    }
                    let (payload, _) =
                        compose_trajectory_frame(&row, frames, &consumer.worker.encoding, false)?;
                    visit(key, index, payload)?;
                }
                Ok(())
            },
        )?;
    }
    Ok(())
}

struct CollectedTargets {
    consumer: crate::ConsumerRunConfig,
    rows: std::collections::BTreeMap<String, Trajectory>,
    labels: std::collections::BTreeMap<String, Vec<Value>>,
}
/// 完成采集的共用读取入口；N0必须有真实空轨迹，丢失/截断的非空轨迹仍报错。
fn prepare_collected_targets(
    config: &LearningJobConfig,
    collection: &Value,
) -> Result<CollectedTargets, backtest::MaterialError> {
    use std::collections::BTreeMap;
    let effective: crate::ConsumerRunConfig =
        serde_json::from_value(collection["consumer"].clone())?;
    let labels_path = config.output_directory.join("frozen-labels.json");
    let mut rows = BTreeMap::new();
    let mut labels = BTreeMap::new();
    let mut units = BTreeMap::new();
    let labels_exist = labels_path.exists();
    for (di, day) in effective.days.iter().enumerate() {
        for (ai, c) in day.candidates.iter().enumerate() {
            let params: &Value = &c.strategy.effective_parameters["model_consumer"];
            let key = params["account_key"].as_str().ok_or("无账户键")?.to_owned();
            let trajectory = PathBuf::from(
                params["learning"]["trajectory_path"]
                    .as_str()
                    .ok_or("完整采集记录缺私有轨迹路径")?,
            );
            let records = Trajectory::open_version(
                &trajectory,
                config.parameters.trajectory_page_rows,
                config.parameters.trajectory_cached_pages,
                trajectory_version(config.parameters.special.as_ref()),
            )?;
            let expected =
                collection["summary"]["days"][di]["candidates"][ai]["report"]["calculation_count"]
                    .as_u64()
                    .ok_or("完整采集缺实际调用计数")?;
            if records.meta.len() as u64 != expected {
                return Err("完整私有轨迹与实际调用计数不符".into());
            }
            let end:LearningAccountReadV1=serde_json::from_value(collection["summary"]["days"][di]["candidates"][ai]["report"]["learning_account_memory"].clone())?;
            let mut special_h = Value::Null;
            let frozen = if labels_exist {
                Vec::new()
            } else if let Some(policy) = &config.parameters.special {
                let mut state:crate::special::SpecialStateV1 = serde_json::from_value(collection["summary"]["days"][di]["candidates"][ai]["report"]["learning_result"].clone())?;
                let facts = end
                    .directed_confirmations
                    .as_ref()
                    .ok_or("H缺专项定向公共事实")?;
                state.update(facts)?;
                // H前解释未知不进入r_last或完整剩余目标；不将未知raw补零。
                // 真实P_i的必要信用未知仍由freeze_special_labels拒绝。
                let terminal_credit = special_terminal_credit(&state, policy);
                let ids = facts
                    .requests
                    .values()
                    .filter_map(Option::as_ref)
                    .flatten()
                    .map(|t| t.trade_identity.clone())
                    .collect();
                let trace = trajectory
                    .parent()
                    .ok_or("轨迹无父目录")?
                    .join("special-confirmation-changes.jsonl");
                let mut event_count = 0u64;
                {
                    use std::io::{BufRead, Write};
                    let exists = trace.exists();
                    let mut previous = if exists {
                        Some(std::io::BufReader::new(std::fs::File::open(&trace)?))
                    } else {
                        None
                    };
                    let mut file = if exists {
                        None
                    } else {
                        Some(std::io::BufWriter::new(
                            std::fs::OpenOptions::new()
                                .create_new(true)
                                .write(true)
                                .open(&trace)?,
                        ))
                    };
                    ledger::visit_trade_confirmation_changes_v1(&c.ledger_root, &ids, |event| {
                        let invalid = |e: String| ledger::LedgerDurableErrorV1::InvalidStructure(e);
                        if let Some(previous) = &mut previous {
                            let mut line = String::new();
                            previous
                                .read_line(&mut line)
                                .map_err(|e| invalid(e.to_string()))?;
                            let saved: ledger::TradeConfirmationChangeReadV1 =
                                serde_json::from_str(&line)
                                    .map_err(|e| invalid(format!("原专项引用不完整/损坏:{e}")))?;
                            if saved != event {
                                return Err(invalid("原专项事件引用与同一原提交段不一致".into()));
                            }
                        } else if let Some(file) = &mut file {
                            serde_json::to_writer(&mut *file, &event)
                                .map_err(|e| invalid(e.to_string()))?;
                            file.write_all(b"\n").map_err(|e| invalid(e.to_string()))?;
                        }
                        event_count += 1;
                        Ok(())
                    })?;
                    if let Some(file) = &mut file {
                        file.flush()?;
                    }
                    if let Some(previous) = &mut previous {
                        let mut extra = String::new();
                        if previous.read_line(&mut extra)? != 0 {
                            return Err("原专项引用多出未归属事件".into());
                        }
                    }
                }
                special_h = json!({"state":state,"credit":terminal_credit,"confirmation_changes":trace,"event_count":event_count,"original_noe_root":c.noe_root,"original_ledger_root":c.ledger_root,"trajectory":trajectory,"last_actor_index":records.meta.len().checked_sub(1),"last_processing_start_utc_ms":records.meta.last().map(|r|&r["processing_start_utc_ms"]),"last_account_ledger_entry_seq":records.meta.last().map(|r|&r["ledger_entry_seq"]),"evidence_end_utc_ms":params["learning"]["evidence_end_utc_ms"]});
                freeze_special_labels(&records.meta, &end, config.parameters.tau_gae_s, policy)?
            } else {
                freeze_labels(&records.meta, &end, config.parameters.tau_gae_s)?
            };
            let initial:LearningAccountReadV1=serde_json::from_value(collection["summary"]["days"][di]["candidates"][ai]["report"]["learning_account_start"].clone())?;
            let pstart = confirmed_k(&initial)?;
            let j = finite_j(&end)?;
            let first = records
                .meta
                .first()
                .map(|r| {
                    serde_json::from_value::<ExactRationalV1>(r["k_exact"].clone())
                        .map_err(|e| e.to_string())
                        .and_then(|v| R::try_from(&v).map_err(|e| e.to_string()))
                })
                .transpose()?;
            units.insert(key.clone(),json!({"p_start":pstart.public(),"b_pre":first.as_ref().map(|v|v.sub(&pstart).public()),"j_end":j.public(),"j_from_start":j.sub(&pstart).public(),"n0":records.meta.is_empty(),"actor_samples":records.meta.len(),"special_h":special_h}));
            labels.insert(key.clone(), frozen);
            rows.insert(key, records);
        }
    }
    let unit_path = config.output_directory.join("unit-objectives.json");
    if !unit_path.exists() {
        write_new(&unit_path, &json!(units))?;
    }
    if !labels_exist {
        write_new(&labels_path, &json!(labels))?;
    }
    // 初次与恢复都消费同一不可变持久快照，不用重新计算的浮点逐位相等阻断恢复。
    labels = read_frozen_labels(&labels_path)?;
    if labels.len() != rows.len()
        || rows.iter().any(|(key, trajectory)| {
            labels
                .get(key)
                .is_none_or(|v| v.len() != trajectory.meta.len())
        })
    {
        return Err("冻结标签账户/样本数量与完整轨迹不符".into());
    }
    Ok(CollectedTargets {
        consumer: effective,
        rows,
        labels,
    })
}
fn finish_without_actor(
    config: &LearningJobConfig,
    targets: &CollectedTargets,
    first_observation: &crate::first_observation::FirstObservation,
) -> Result<Option<Value>, backtest::MaterialError> {
    if targets.rows.values().any(|r| !r.meta.is_empty()) {
        return Ok(None);
    }
    let result = json!({"first_observation":first_observation.snapshot(),"group":config.group_id,"no_actor_samples":true,"committed":false,"unit_objectives":config.output_directory.join("unit-objectives.json"),"model_work":{"collected":0,"recomputed":0,"optimizer_steps":0,"commits":0}});
    write_new(&config.attempt_report, &result)?;
    Ok(Some(result))
}

pub fn run_learning_job(config: &LearningJobConfig) -> Result<Value, backtest::MaterialError> {
    run_learning_job_with_opener(config, crate::rules::open_consumer_rules)
}
fn run_learning_job_with_opener(
    config: &LearningJobConfig,
    open: impl FnOnce(
        &crate::ConsumerRunConfig,
    ) -> Result<crate::rules::OpenedRulesV1, backtest::MaterialError>,
) -> Result<Value, backtest::MaterialError> {
    let first_observation = std::sync::Arc::new(crate::first_observation::FirstObservation::new(
        std::time::Instant::now(),
    ));
    let mut current = config.clone();
    current.consumer = crate::public_input::bind(&current.consumer);
    let config = &current;
    let started = std::time::Instant::now();
    let opened = open(&config.consumer)?;
    let mode = opened.opened_via;
    let mut result = run_learning_job_opened(config, opened, started, first_observation)?;
    result["rules_opened_via"] = serde_json::to_value(mode)?;
    Ok(result)
}
/// 检查点存放位置不改变完成采集身份；只投影准确字段，原记录保持不变。
fn collection_binding_semantics(binding: &Value) -> Value {
    let mut semantic = binding.clone();
    if let Some(parameters) = semantic
        .get_mut("parameters")
        .and_then(Value::as_object_mut)
    {
        parameters.remove("checkpoint_directory");
    }
    semantic
}
fn run_learning_job_opened(
    original: &LearningJobConfig,
    opened: crate::rules::OpenedRulesV1,
    started: std::time::Instant,
    first_observation: std::sync::Arc<crate::first_observation::FirstObservation>,
) -> Result<Value, backtest::MaterialError> {
    use crate::{run::run_with_worker_observed, worker::ModelWorker};
    use std::{collections::BTreeMap, time::Instant};
    let mut effective_config = original.clone();
    effective_config.consumer = crate::rules::bind_consumer(&original.consumer, &opened.binding);
    let config = &effective_config;
    if config.parameters.trajectory_page_rows == 0
        || config.parameters.trajectory_cached_pages == 0
        || config.parameters.training_sequence_batch == 0
        || config.parameters.tbptt_steps == 0
    {
        return Err("页/批/梯度块配置须为正整数".into());
    }
    if let Some(policy) = &config.parameters.special {
        policy.validate()?;
    }
    let active_ends = match &config.active_end_by_day_utc_ms {
        Some(v) if v.len() == config.consumer.days.len() => v.clone(),
        None if config.consumer.days.len() == 1 => vec![config.active_end_utc_ms],
        _ => return Err("多日学习job须按days顺序逐日显式提供active_end_by_day_utc_ms".into()),
    };
    let mut binding = json!({"consumer":config.consumer,"parameters":config.parameters,"active_end_utc_ms":config.active_end_utc_ms,"group_id":config.group_id});
    if let Some(ends) = &config.active_end_by_day_utc_ms {
        binding["active_end_by_day_utc_ms"] = json!(ends);
    }
    let group = json!({"id":config.group_id,"binding":binding,"target_version":target_version(config.parameters.special.as_ref()),"trajectory_version":trajectory_version(config.parameters.special.as_ref())});
    for (di, day) in config.consumer.days.iter().enumerate() {
        for c in &day.candidates {
            if c.session.caller_role != SimCallerRoleV1::ModelTrain
                || c.timing.decision_mode != backtest::HistoricalDecisionModeV1::Fixed100msFifo
            {
                return Err("学习消费者要求ModelTrain/全部100ms FIFO".into());
            }
            if active_ends[di] > day.preparation.scope.received_end_utc_ms {
                return Err("经营截止E晚于证据截止H".into());
            }
        }
    }
    let mut account_keys = std::collections::BTreeSet::new();
    for c in config.consumer.days.iter().flat_map(|d| &d.candidates) {
        let consumer: crate::ConsumerConfig =
            serde_json::from_value(c.strategy.effective_parameters["model_consumer"].clone())?;
        if !account_keys.insert(consumer.account_key) {
            return Err("同一学习worker的各日账户键必须唯一".into());
        }
    }
    let weights = account_keys
        .iter()
        .map(|key| (key.clone(), 1.0 / account_keys.len() as f64))
        .collect::<BTreeMap<_, _>>();
    std::fs::create_dir_all(&config.output_directory)?;
    let worker = ModelWorker::start(config.consumer.worker.clone())?;
    let outcome = (|| -> Result<Value, backtest::MaterialError> {
        let transaction=worker.request(json!({"phase":"learn_open","config":config.parameters,"group":group,"account_weights":weights}))?;
        if transaction["already_committed"] == true {
            let result = json!({"first_observation":first_observation.snapshot(),"already_committed":true,"group":group,"result":transaction,"model":worker.request(json!({"phase":"metrics"}))?,"elapsed_seconds":started.elapsed().as_secs_f64()});
            write_new(&config.attempt_report, &result)?;
            return Ok(result);
        }
        let complete = config.output_directory.join("completed-collection.json");
        let mut effective = config.consumer.clone();
        let collection_path =
            config
                .output_directory
                .join(if config.fault.as_deref() == Some("collection") {
                    "collection-fault"
                } else {
                    "collection"
                });
        for (di, day) in effective.days.iter_mut().enumerate() {
            if config.fault.as_deref() == Some("collection") {
                day.candidates.truncate(1);
            }
            for (ai, c) in day.candidates.iter_mut().enumerate() {
                let out = collection_path.join(format!("day-{di}-account-{ai}"));
                std::fs::create_dir_all(&out)?;
                let mut consumer: crate::ConsumerConfig = serde_json::from_value(
                    c.strategy.effective_parameters["model_consumer"].clone(),
                )?;
                consumer.learning = Some(CollectionConfig {
                    special: config.parameters.special.clone(),
                    group_id: config.group_id.clone(),
                    active_end_utc_ms: active_ends[di],
                    evidence_end_utc_ms: day.preparation.scope.received_end_utc_ms,
                    trajectory_path: out.join("trajectory.jsonl"),
                    collection_fault_after_call: (config.fault.as_deref() == Some("collection"))
                        .then_some(4),
                });
                c.strategy.effective_parameters["model_consumer"] = serde_json::to_value(consumer)?;
                c.strategy.effective_parameters["purpose"] =
                    json!(if config.parameters.special.is_some() {
                        "D-0375专项ModelTrain采集及外层整组学习消费者"
                    } else {
                        "D-0371 ModelTrain采集及外层整组学习消费者"
                    });
                c.strategy.effective_parameters["learning_group"] = json!(config.group_id);
                c.strategy.effective_parameters["rules_binding"] = json!(opened.binding);
                c.sim_log = out.join("sim.jsonl");
                c.noe_root = out.join("noe");
                c.ledger_root = out.join("ledger");
                c.metadata_path = out.join("run.json");
                c.result_path = out.join("result.json");
                c.research_evidence_path = Some(out.join("decisions.jsonl"));
            }
        }
        let collection = if complete.exists() {
            let value: Value = serde_json::from_slice(&std::fs::read(&complete)?)?;
            if collection_binding_semantics(&value["binding"])
                != collection_binding_semantics(&binding)
            {
                return Err("已完成轨迹与消费配置不同".into());
            }
            value
        } else {
            let outcome = run_with_worker_observed(
                &effective,
                worker.clone(),
                opened.rules.clone(),
                Instant::now(),
                first_observation.clone(),
            );
            let summary = match outcome {
                Ok(s) => s,
                Err(e) => {
                    let result = json!({"first_observation":first_observation.snapshot(),"failed_collection":true,"error":e.to_string(),"group":group,"model":worker.request(json!({"phase":"metrics"}))?,"worker_ipc":worker.io_metrics(),"elapsed_seconds":started.elapsed().as_secs_f64()});
                    write_new(&config.attempt_report, &result)?;
                    return Err(e);
                }
            };
            let value = json!({"binding":binding,"consumer":effective,"summary":summary});
            write_new(&complete, &value)?;
            value
        };
        let CollectedTargets {
            consumer: effective,
            rows,
            labels,
        } = prepare_collected_targets(config, &collection)?;
        // 全N0只交经营目标，不因空组制造学习提交；混合组继续消费实际样本。
        let targets = CollectedTargets {
            consumer: effective,
            rows,
            labels,
        };
        if let Some(result) = finish_without_actor(config, &targets, &first_observation)? {
            return Ok(result);
        }
        let CollectedTargets {
            consumer: effective,
            mut rows,
            labels,
        } = targets;
        let mut audits = Vec::new();
        let mut audit_output = if config.audit_before_update {
            Some(std::io::BufWriter::new(
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(config.attempt_report.with_extension("audit.jsonl"))?,
            ))
        } else {
            None
        };
        if config.audit_before_update {
            worker.request(json!({"phase":"audit_open"}))?;
            visit_group_frames(
                &effective,
                &mut rows,
                opened.rules.clone(),
                |key, index, frame| {
                    let old = frame["old_joint_logp"].clone();
                    let response = worker.request(json!({"phase":"audit_step","frame":frame}))?;
                    let audit =
                        json!({"account":key,"index":index,"old_logp":old,"result":response});
                    if let Some(file) = &mut audit_output {
                        use std::io::Write;
                        serde_json::to_writer(&mut *file, &audit)?;
                        file.write_all(b"\n")?;
                        file.flush()?;
                    }
                    audits.push(audit);
                    Ok(())
                },
            )?;
        }
        let mut references = Value::Null;
        if config.gradient_checks {
            let key = rows
                .iter()
                .find(|(_, trajectory)| !trajectory.meta.is_empty())
                .map(|(key, _)| key.clone())
                .ok_or("无真实调用")?;
            let mut frames = Vec::new();
            visit_group_frames(
                &effective,
                &mut rows,
                opened.rules.clone(),
                |k, _, frame| {
                    if k == key {
                        frames.push(frame);
                    }
                    Ok(())
                },
            )?;
            let bptt=worker.request(json!({"phase":"gradient_reference","kind":"bptt","frames":frames,"labels":labels[&key],"money_scale_pusd":config.parameters.money_scale_pusd,"tbptt_steps":config.parameters.tbptt_steps}))?;
            let critic=worker.request(json!({"phase":"gradient_reference","kind":"critic","frames":frames,"labels":labels[&key],"money_scale_pusd":config.parameters.money_scale_pusd,"tbptt_steps":config.parameters.tbptt_steps}))?;
            references = json!({"bptt":bptt,"critic":critic});
        }
        let mut epochs = Vec::new();
        for epoch in 0..if rows.values().any(|r| !r.meta.is_empty()) {
            config.parameters.ppo_epochs
        } else {
            0
        } {
            worker.request(json!({"phase":"learn_epoch"}))?;
            let mut batch = Vec::new();
            let mut batch_keys = std::collections::BTreeSet::new();
            visit_group_frames(
                &effective,
                &mut rows,
                opened.rules.clone(),
                |key, index, frame| {
                    if batch_keys.contains(key)
                        || batch.len() == config.parameters.training_sequence_batch
                    {
                        worker.request_batch(std::mem::take(&mut batch))?;
                        batch_keys.clear();
                    }
                    batch.push(
                        json!({"phase":"learn_step","frame":frame,"label":labels[key][index]}),
                    );
                    batch_keys.insert(key.to_owned());
                    Ok(())
                },
            )?;
            if !batch.is_empty() {
                worker.request_batch(batch)?;
            }
            let result = worker.request(json!({"phase":"learn_end_epoch"}))?;
            eprintln!("PPO整轮{}完成，一次联合更新", epoch + 1);
            write_new(
                &config
                    .attempt_report
                    .with_extension(format!("epoch-{}.json", epoch + 1)),
                &result,
            )?;
            epochs.push(result);
        }
        // 故障前审计输出先落盘，不能因回执丢失丢掉已做的实际模型工作。
        let evidence = config.attempt_report.with_extension("precommit.json");
        write_new(
            &evidence,
            &json!({"group":group,"audits":audits,"gradient_references":references,"epochs":epochs,"model":worker.request(json!({"phase":"metrics"}))?}),
        )?;
        let committed=worker.request(json!({"phase":"learn_commit","fault":config.fault,"witness":config.attempt_report.with_extension("fault.json")}));
        let result = match committed {
            Ok(c) => {
                json!({"first_observation":first_observation.snapshot(),"committed":c,"group":group,"model":worker.request(json!({"phase":"metrics"}))?,"worker_ipc":worker.io_metrics(),"elapsed_seconds":started.elapsed().as_secs_f64()})
            }
            Err(e) => {
                let result = json!({"first_observation":first_observation.snapshot(),"commit_reply_failed":true,"error":e,"group":group,"elapsed_seconds":started.elapsed().as_secs_f64()});
                write_new(&config.attempt_report, &result)?;
                return Err("学习提交回执失败；按原事务恢复".into());
            }
        };
        write_new(&config.attempt_report, &result)?;
        Ok(result)
    })();
    if let Err(error) = &outcome
        && !config.attempt_report.exists()
    {
        let metrics = worker.request(json!({"phase":"metrics"}));
        let partial = json!({"first_observation":first_observation.snapshot(),"failed_learning":true,"error":error.to_string(),"model":metrics.as_ref().ok(),"metrics_error":metrics.err(),"worker_ipc":worker.io_metrics(),"elapsed_seconds":started.elapsed().as_secs_f64()});
        write_new(&config.attempt_report, &partial)?;
    }
    outcome
}

#[cfg(test)]
mod target_tests {
    use super::*;
    fn read(k: i64) -> LearningAccountReadV1 {
        LearningAccountReadV1 {
            confirmed: std::sync::Arc::new(ledger::ConfirmedEconomicsReadV1 {
                version: "confirmed_economics_v1".into(),
                components: vec![ledger::ConfirmedComponentReadV1 {
                    identity: "fixed-market-paired".into(),
                    kind: K::PairedGross,
                    amount_pusd: R::from_decimal(k.into()).public(),
                }],
                unresolved_position_principal_pusd: R::zero().public(),
                unknown_negative_cost_identities: vec![],
                covered_unapportioned_negative_cost_identities: vec![],
            }),
            obligations: vec![],
            pending_noe_record_count: 0,
            qualified_rebates: Vec::new(),
            directed_confirmations: None,
        }
    }
    #[test]
    fn actual_sim_interval_and_complete_remaining_target_do_not_reset_at_tbptt() {
        let row = |k: i64, v: f64, t: i64| json!({"k_exact":R::from_decimal(k.into()).public(),"old_value":v,"old_joint_logp":-0.5,"processing_start_utc_ms":t});
        let rows = vec![row(5, 1.0, 1000), row(7, 2.0, 101000)];
        let l = freeze_labels(&rows, &read(20), 300.0).unwrap();
        assert_eq!(l[0]["reward"], 2.0);
        assert_eq!(l[1]["reward"], 13.0);
        assert_eq!(l[0]["critic_target"], 15.0);
        assert_eq!(l[1]["critic_target"], 13.0);
        assert!(
            (l[0]["advantage"].as_f64().unwrap() - (3.0 + (-100.0_f64 / 300.0).exp() * 11.0)).abs()
                < 1e-12
        );
        assert_eq!(l[1]["advantage"], 11.0);
        assert_eq!(l[1]["lambda"], 0.0);
        let sum = l.iter().map(|x| x["reward"].as_f64().unwrap()).sum::<f64>();
        assert_eq!(sum, (20.0 - 3.0) - (5.0 - 3.0));
        assert!(freeze_labels(&[], &read(20), 300.0).unwrap().is_empty());
    }
    #[test]
    fn known_principal_is_unique_and_pending_noe_count_is_not_a_blanket_label_gate() {
        let mut r = read(20);
        r.pending_noe_record_count = 2;
        std::sync::Arc::make_mut(&mut r.confirmed).unresolved_position_principal_pusd =
            R::from_decimal(3.into()).public();
        r.obligations.push(shared_run::ObligationPrincipalReadV1 {
            condition_id: ConditionId("one".into()),
            principal_upper_pusd: ExecutionReadValueV1::Known(R::from_decimal(4.into()).public()),
        });
        assert_eq!(finite_j(&r).unwrap(), R::from_decimal(13.into()));
        r.obligations[0].principal_upper_pusd = ExecutionReadValueV1::Unavailable;
        assert!(finite_j(&r).is_err());
        r.obligations.clear();
        std::sync::Arc::make_mut(&mut r.confirmed)
            .unknown_negative_cost_identities
            .push("uncovered-negative".into());
        assert!(finite_j(&r).is_err());
    }
    #[test]
    fn qualified_right_replaces_same_day_and_payment_does_not_reward_twice() {
        use contracts::economics::*;
        let mut r = read(20);
        let identity = "rebate:Maker:0".to_string();
        std::sync::Arc::make_mut(&mut r.confirmed).components.push(
            ledger::ConfirmedComponentReadV1 {
                identity: identity.clone(),
                kind: K::MakerRebate,
                amount_pusd: R::from_decimal(1.into()).public(),
            },
        );
        let mut entitlement = RebateEntitlementReadV1 {
            day_result: RebateDayResultV1 {
                kind: RebateKindV1::Maker,
                attribution_day_start_utc_ms: 0,
                accrued_pusd: ExactDecimal("1.23456".into()),
                payable_pusd: ExactDecimal("1.2000".into()),
                locked_remainder_pusd: ExactDecimal("0.03456".into()),
                contribution_refs: vec!["original-trade".to_string()],
            },
            paid_pusd: ExactDecimal("0".into()),
        };
        r.qualified_rebates.push(shared_run::QualifiedRebateReadV1 {
            identity,
            kind: RebateKindV1::Maker,
            attribution_day_start_utc_ms: 0,
            result: ExecutionReadValueV1::Known(entitlement.clone()),
        });
        let confirmed = confirmed_k(&r).unwrap();
        assert_eq!(
            confirmed,
            R::from_decimal(rust_decimal::Decimal::new(212, 1))
        );
        assert_eq!(confirmed, confirmed_k(&r).unwrap());
        entitlement.paid_pusd = ExactDecimal("1.2000".into());
        r.qualified_rebates[0].result = ExecutionReadValueV1::Known(entitlement.clone());
        assert_eq!(confirmed, confirmed_k(&r).unwrap());
        entitlement.day_result.payable_pusd = ExactDecimal("1.1000".into());
        r.qualified_rebates[0].result = ExecutionReadValueV1::Known(entitlement);
        assert_eq!(
            confirmed_k(&r).unwrap().sub(&confirmed),
            R::from_decimal(rust_decimal::Decimal::new(-1, 1))
        );
    }
}

#[cfg(test)]
mod float_boundary_test {
    use super::*;
    #[test]
    fn finite_long_rational_does_not_become_inf_over_inf() {
        let p = num_bigint::BigInt::from(10u8).pow(800);
        let value = R::ratio(&p + 1, &p * 5 + 7);
        assert!(value.numerator().to_string().len() > 700);
        let projected = number(&value).unwrap();
        assert!(projected.is_finite());
        assert!((projected - 0.2).abs() < 1e-15);
    }
}

#[cfg(test)]
mod trajectory_boundary_test {
    use super::*;
    #[test]
    fn damaged_persisted_frame_returns_an_error_before_replay() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("trajectory.jsonl");
        std::fs::write(
            &p,
            b"{\"version\":\"learning-trajectory-v1\",\"processing_start_utc_ms\":\"damaged\"}\n",
        )
        .unwrap();
        assert!(Trajectory::open(&p, 4, 2).is_err());
    }
}

#[cfg(test)]
mod frozen_snapshot_tests {
    use super::*;
    #[test]
    fn initial_and_resumed_consumer_use_same_immutable_decimal_snapshot() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("labels.json");
        let computed = json!({"account":[{"lambda":(-0.23_f64/300.0).exp(),"advantage":0.22350061882116806,"reward":0.0,"critic_target":0.0,"old_value":-0.1648910939693451,"old_joint_logp":-0.9744950037365924}]});
        write_new(&p, &computed).unwrap();
        let bytes = std::fs::read(&p).unwrap();
        let first = read_frozen_labels(&p).unwrap();
        let resumed = read_frozen_labels(&p).unwrap();
        assert_eq!(first, resumed);
        assert_eq!(bytes, std::fs::read(&p).unwrap());
    }
}

#[cfg(test)]
#[path = "learning_tests.rs"]
mod learning_tests;

#[cfg(test)]
#[path = "rule_startup_tests.rs"]
mod rule_startup_tests;

/// 同一轨迹私有投影与正式公开帧组合；生产重算和诊断共用。
pub(crate) fn compose_trajectory_frame(
    row: &Value,
    frames: &[shared_run::FrozenMarketReadV1],
    encoding: &crate::encoding::EncodingConfig,
    capture_projection: bool,
) -> Result<(Value, Value), backtest::MaterialError> {
    use crate::encoding::*;
    let started = row["processing_start_utc_ms"]
        .as_i64()
        .ok_or("实际开始缺失")?;
    let mut markets = Vec::new();
    let mut projections = Vec::new();
    for (i, id) in row["market_ids"]
        .as_array()
        .ok_or("市场顺序缺失")?
        .iter()
        .enumerate()
    {
        let f = frames
            .iter()
            .find(|f| {
                serde_json::to_value(&f.observation.market.condition_id)
                    .ok()
                    .as_ref()
                    == Some(id)
            })
            .ok_or("公共再生缺原可见市场")?;
        let mut projected = row["market_private"][i].clone();
        projected["public"] = market_projection(f, started);
        markets.push(tokens(&projected, encoding));
        if capture_projection {
            projections.push(projected);
        }
    }
    let mut payload = row.clone();
    payload["global"] = serde_json::to_value(tokens(&row["global_private"], encoding))?;
    payload["markets"] = serde_json::to_value(markets)?;
    Ok((
        payload,
        if capture_projection {
            json!({"global":row["global_private"],"markets":projections})
        } else {
            Value::Null
        },
    ))
}

#[cfg(test)]
#[path = "public_input_tests.rs"]
pub(crate) mod public_input_tests;

pub fn target_version(policy: Option<&crate::special::SpecialPolicyV1>) -> &'static str {
    if policy.is_some() {
        crate::special::TARGET
    } else {
        "confirmed-finite-j-v1"
    }
}
pub fn trajectory_version(policy: Option<&crate::special::SpecialPolicyV1>) -> &'static str {
    if policy.is_some() {
        crate::special::TRAJECTORY
    } else {
        crate::public_input::TRAJECTORY_VERSION
    }
}
fn special_terminal_credit(
    state: &crate::special::SpecialStateV1,
    policy: &crate::special::SpecialPolicyV1,
) -> Value {
    match state.credit(policy) {
        Ok((raw, clipped)) => {
            json!({"raw_before_terminal_pusd":raw.public(),"clipped_before_terminal_pusd":clipped.public(),"terminal_credit_pusd":R::zero().public(),"necessary_unknown":[]})
        }
        Err(_) => {
            json!({"raw_before_terminal_pusd":null,"clipped_before_terminal_pusd":null,"terminal_credit_pusd":R::zero().public(),"necessary_unknown":state.unknown_reasons(),"scope":"仅H前解释未知；真实P_i信用及base J仍须已知，原来源读取失败不忽略"})
        }
    }
}
/// 对同一组已冻结的信用前缀作势差变换，再沿原GAE递推；不生成末端动作。
pub fn freeze_special_labels(
    rows: &[Value],
    end: &LearningAccountReadV1,
    tau: f64,
    policy: &crate::special::SpecialPolicyV1,
) -> Result<Vec<Value>, String> {
    policy.validate()?;
    let mut labels = freeze_labels(rows, end, tau)?;
    let mut credits = Vec::new();
    for row in rows {
        let credit = &row["special_credit"];
        if credit["necessary_unknown"]
            .as_array()
            .is_none_or(|a| !a.is_empty())
        {
            return Err("专项信用前缀必要事实待定".into());
        }
        let c: ExactRationalV1 =
            serde_json::from_value(credit["clipped_pusd"].clone()).map_err(|e| e.to_string())?;
        credits.push(R::try_from(&c).map_err(|e| e.to_string())?);
    }
    let alpha = R::from_decimal(rust_decimal::Decimal::from(policy.alpha));
    let j = finite_j(end)?;
    for i in 0..labels.len() {
        let following = credits.get(i + 1).cloned().unwrap_or_else(R::zero);
        let shaping = following.sub(&credits[i]);
        let k: ExactRationalV1 =
            serde_json::from_value(rows[i]["k_exact"].clone()).map_err(|e| e.to_string())?;
        let k = R::try_from(&k).map_err(|e| e.to_string())?;
        let next_k = if let Some(row) = rows.get(i + 1) {
            let value: ExactRationalV1 =
                serde_json::from_value(row["k_exact"].clone()).map_err(|e| e.to_string())?;
            R::try_from(&value).map_err(|e| e.to_string())?
        } else {
            j.clone()
        };
        let base = number(&next_k.sub(&k))?;
        let reward_exact = next_k.sub(&k).add(&alpha.mul(&shaping));
        let target_exact = j.sub(&k).sub(&alpha.mul(&credits[i]));
        labels[i]["base_reward"] = base.into();
        labels[i]["credit_pusd"] =
            serde_json::to_value(credits[i].public()).map_err(|e| e.to_string())?;
        labels[i]["credit_delta_pusd"] =
            serde_json::to_value(shaping.public()).map_err(|e| e.to_string())?;
        labels[i]["reward"] = number(&reward_exact)?.into();
        labels[i]["reward_exact"] =
            serde_json::to_value(reward_exact.public()).map_err(|e| e.to_string())?;
        labels[i]["critic_target"] = number(&target_exact)?.into();
        labels[i]["critic_target_exact"] =
            serde_json::to_value(target_exact.public()).map_err(|e| e.to_string())?;
    }
    let mut next_adv = 0.0;
    for i in (0..labels.len()).rev() {
        let value = labels[i]["old_value"].as_f64().ok_or("缺旧价值")?;
        let next_value = labels
            .get(i + 1)
            .and_then(|v| v["old_value"].as_f64())
            .unwrap_or(0.0);
        let adv = labels[i]["reward"].as_f64().ok_or("缺奖励")? + next_value - value
            + labels[i]["lambda"].as_f64().ok_or("缺lambda")? * next_adv;
        labels[i]["advantage"] = adv.into();
        next_adv = adv;
    }
    Ok(labels)
}

#[cfg(test)]
mod special_label_tests {
    use super::*;
    #[test]
    fn special_credit_telescopes_with_h_reversal_and_uses_same_gae_and_complete_target() {
        let end:LearningAccountReadV1=serde_json::from_value(json!({"confirmed":{"version":"confirmed_economics_v1","components":[],"unresolved_position_principal_pusd":R::zero().public(),"unknown_negative_cost_identities":[],"covered_unapportioned_negative_cost_identities":[]},"obligations":[],"pending_noe_record_count":0,"qualified_rebates":[]})).unwrap();
        let make = |credit: i64, t: i64| json!({"k_exact":R::zero().public(),"old_value":0.0,"old_joint_logp":-0.5,"processing_start_utc_ms":t,"special_credit":{"raw_pusd":R::from_decimal(rust_decimal::Decimal::new(credit,2)).public(),"clipped_pusd":R::from_decimal(rust_decimal::Decimal::new(credit,2)).public(),"necessary_unknown":[]}});
        let mut h_unknown = crate::special::SpecialStateV1::default();
        h_unknown
            .necessary_unknown
            .push("last到H才出现的原归属缺口".into());
        let explanation =
            special_terminal_credit(&h_unknown, &crate::special::SpecialPolicyV1::adopted(1));
        assert!(explanation["raw_before_terminal_pusd"].is_null());
        assert_eq!(
            explanation["terminal_credit_pusd"],
            json!(R::zero().public())
        );
        let rows = vec![make(0, 10), make(1, 260), make(-2, 280)];
        let labels = freeze_special_labels(
            &rows,
            &end,
            300.0,
            &crate::special::SpecialPolicyV1::adopted(1),
        )
        .unwrap();
        for (row, (reward, target)) in
            labels
                .iter()
                .zip([(0.01, 0.0), (-0.03, -0.01), (0.02, 0.02)])
        {
            assert!((row["reward"].as_f64().unwrap() - reward).abs() < 1e-12);
            assert!((row["critic_target"].as_f64().unwrap() - target).abs() < 1e-12);
        }
        assert!(
            (labels
                .iter()
                .map(|r| r["reward"].as_f64().unwrap())
                .sum::<f64>())
            .abs()
                < 1e-12
        );
        assert_eq!(
            freeze_special_labels(
                &[make(0, 10)],
                &end,
                300.0,
                &crate::special::SpecialPolicyV1::adopted(1)
            )
            .unwrap()[0]["reward"],
            0.0
        );
        assert!(
            freeze_special_labels(
                &[],
                &end,
                300.0,
                &crate::special::SpecialPolicyV1::adopted(1)
            )
            .unwrap()
            .is_empty()
        );
        let control = freeze_special_labels(
            &rows,
            &end,
            300.0,
            &crate::special::SpecialPolicyV1::adopted(0),
        )
        .unwrap();
        assert!(
            control
                .iter()
                .all(|r| r["reward"] == 0.0 && r["critic_target"] == 0.0)
        );
    }
    #[test]
    fn no_special_keeps_original_missing_field_binding_and_target() {
        let parameters: LearningParameters =
            serde_json::from_str(include_str!("../examples/learning-parameters.json")).unwrap();
        assert!(
            serde_json::to_value(&parameters)
                .unwrap()
                .get("special")
                .is_none()
        );
        assert_eq!(
            target_version(parameters.special.as_ref()),
            "confirmed-finite-j-v1"
        );
        assert_eq!(
            trajectory_version(parameters.special.as_ref()),
            crate::public_input::TRAJECTORY_VERSION
        );
    }
}
