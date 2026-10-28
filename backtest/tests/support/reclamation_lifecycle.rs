//! #378 唯一 8 typed 生命周期检查；仅显式运行一次，工作区回归默认忽略。
use super::*;
#[cfg(feature = "stage-timing")]
use contracts::*;
#[cfg(feature = "stage-timing")]
use data_pipeline_core::public_statistics::*;
#[cfg(feature = "stage-timing")]
use sim_system::execution::{FinitePublicConsumptionV1, MarketEnrollmentRequestV1};
#[cfg(feature = "stage-timing")]
use std::path::PathBuf;

#[cfg(feature = "stage-timing")]
struct HeldStatistics {
    group: Arc<PreparedPublicGroupV1>,
    receipt: PublicGroupAppliedReceiptV1,
    market: String,
    statistics: Arc<PublicMarketStatisticsV1>,
}

#[cfg(feature = "stage-timing")]
fn consume_one(
    index: usize,
    hub: &DayHubV1,
    candidate: &HistoricalPreparedRunConfigV1,
    rules: Arc<sim_system::platform_rules::SealedProcessRuleSnapshotV3>,
    prepared: &crate::PreparedHistoricalRunV1,
    stream: Arc<PublicStatisticsStreamV1>,
    expected_public: &[PublicGroupReferenceV1],
    expected_hidden: &[HiddenBucketReferenceV2],
    end: i64,
    held_market: Option<&str>,
) -> (serde_json::Value, HeldStatistics) {
    let delivery = DayLocalDeliveryCacheV1::shared(hub, index);
    let source = DayPublicSourceV1::new(hub.clone(), index, delivery.clone());
    let mut hidden = DayHiddenSourceV1::new(hub.clone(), index, delivery.clone());
    let mut public_count = 0;
    let mut hidden_count = 0;
    let mut statistic_reads = 0;
    let mut held = None;
    let mut public_prefetch = 0;
    let mut hidden_prefetch = 0;
    let mut before_finish = None;
    let mut closed_pending_reads = 0;
    let mut eof_economics_applied = None;
    let mut recording_drained = false;
    let mut observations = Vec::new();
    let lifecycle: Result<(), MaterialError> = (|| {
        let (execution, mut shared, _) = crate::run::assemble(
            candidate,
            rules,
            prepared.source_identity_sha256.clone(),
            None,
        )?;
        let mut driver = FinitePublicConsumptionV1::new(
            execution,
            source,
            candidate.consumption_sequence_ref.clone(),
        );
        driver.enable_public_statistics(stream)?;
        driver.enable_finite_recording_wait()?;
        let mut next_hidden = hidden.next().transpose()?;
        // 下一公共窥视只预取；返回到本地缓存不等于driver成功应用。
        driver.next_group_at()?;
        public_prefetch = delivery.borrow().public.len();
        hidden_prefetch = delivery.borrow().hidden.len();
        {
            let b = hub.0.0.lock().unwrap();
            assert_eq!(b.candidates[index].cursor.public, 0);
            assert_eq!(b.candidates[index].cursor.hidden, 1);
            assert!(public_prefetch > 0);
        }
        loop {
            let public_at = driver.next_group_at()?;
            let hidden_at = next_hidden
                .as_ref()
                .map(|b| b.reference().bucket_time_utc_ms());
            let required = driver
                .next_required_logical_drive()
                .map(|r| r.required_at_utc_ms);
            let Some(at) = [public_at, hidden_at, required]
                .into_iter()
                .flatten()
                .filter(|at| *at < end)
                .min()
            else {
                break;
            };
            driver.wait_for_recording_capacity()?;
            driver.advance_before(at)?;
            // 同刻全部公共组先应用，保持原组序和精确已消费前缀。
            while driver.next_group_at()? == Some(at) {
                let group = driver.next_prepared_group()?.expect("原公共组");
                for context in &group.pm_contexts {
                    if !driver.has_enrolled_market(&context.market.condition_id) {
                        driver.wait_for_recording_capacity()?;
                        driver.enroll_market(MarketEnrollmentRequestV1 {
                            enrollment_request_id: context.market.condition_id.0.clone(),
                            market: prepared.markets[&context.market.condition_id.0].clone(),
                        })?;
                    }
                }
                driver.wait_for_recording_capacity()?;
                let (receipt, cutoff) = driver.next_group()?.expect("成功应用原公共组");
                assert_eq!(receipt.group(), &expected_public[public_count]);
                assert_eq!(receipt.execution_time().logical_time_utc_ms(), at);
                let frames = driver.frames_at_observation();
                assert!(!frames.is_empty());
                for frame in &frames {
                    let statistics = frame.statistics.as_ref().expect("正式启用统计");
                    assert_eq!(&statistics.group, receipt.group());
                    assert_eq!(statistics.applied_at_utc_ms, at);
                    assert_eq!(
                        statistics.observed_at_utc_ms,
                        cutoff.decision_time().logical_time_utc_ms()
                    );
                    assert!(
                        statistics
                            .spot_variance
                            .iter()
                            .all(|v| v.sigma_usd_per_btc_per_sqrt_second.is_none()),
                        "缺BTC保留未知"
                    );
                    statistic_reads += 1;
                }
                if held.is_none()
                    && held_market.is_none_or(|id| {
                        group
                            .pm_contexts
                            .iter()
                            .any(|c| c.market.condition_id.0 == id)
                    })
                {
                    let first = held_market.map_or(&frames[0], |id| {
                        frames
                            .iter()
                            .find(|frame| frame.observation.market.condition_id.0 == id)
                            .expect("本组已应用市场的实际frame")
                    });
                    let statistics = first.statistics.as_ref().unwrap();
                    assert!(
                        statistics
                            .pm_quantities
                            .iter()
                            .all(|q| q.bid_shares.value.is_none()),
                        "初次metadata不能提前看到后续盘口"
                    );
                    held = Some(HeldStatistics {
                        group,
                        receipt,
                        market: first.observation.market.condition_id.0.clone(),
                        statistics: statistics.clone(),
                    });
                }
                public_count += 1;
            }
            // hidden尾部也走同一条登记/应用路径，不另设漏登记的尾循环。
            if hidden_at == Some(at) {
                let bucket = next_hidden.take().expect("原隐藏桶");
                assert_eq!(bucket.reference(), &expected_hidden[hidden_count]);
                for record in bucket.records() {
                    let market = match record {
                        HiddenExecutionRecordV1::Step { market, .. } => Some(market),
                        HiddenExecutionRecordV1::MarketFact(fact) => Some(&fact.market),
                        HiddenExecutionRecordV1::Reference(_)
                        | HiddenExecutionRecordV1::BinanceDepthReference { .. }
                        | HiddenExecutionRecordV1::BinanceDepthPositionReference { .. } => None,
                    };
                    if let Some(market) = market
                        && !driver.has_enrolled_market(&market.condition_id)
                    {
                        driver.wait_for_recording_capacity()?;
                        driver.enroll_market(MarketEnrollmentRequestV1 {
                            enrollment_request_id: market.condition_id.0.clone(),
                            market: prepared.markets[&market.condition_id.0].clone(),
                        })?;
                    }
                }
                driver.wait_for_recording_capacity()?;
                driver.hidden_bucket(bucket)?;
                hidden_count += 1;
                next_hidden = hidden.next().transpose()?;
            }
            driver.wait_for_recording_capacity()?;
            driver.observe_at(at)?;
            let applied = shared.apply_closed()?;
            closed_pending_reads += usize::from(!applied);
            observations.push(serde_json::json!({"at":at,"economics_applied":applied}));
            if required == Some(at) {
                driver.wait_for_recording_capacity()?;
                driver.advance_before(at.checked_add(1).expect("小例合法毫秒"))?;
            }
        }
        assert_eq!(public_count, expected_public.len());
        assert_eq!(hidden_count, expected_hidden.len());
        assert!(next_hidden.is_none(), "隐藏来源已实际返回EOF");
        driver.wait_for_recording_capacity()?;
        assert!(driver.next_group()?.is_none(), "有限公共来源正常EOF");
        // 材料已正常EOF，不为本回收消费者重开已封口前沿或另造end-1观察。
        let applied = shared.apply_closed()?;
        eof_economics_applied = Some(applied);
        closed_pending_reads += usize::from(!applied);
        {
            let b = hub.0.0.lock().unwrap();
            if index == 0 {
                assert_eq!((b.public_base, b.hidden_base), (0, 0), "慢候选前缀阻止回收");
                assert_eq!(
                    (b.public.len(), b.hidden.len()),
                    (expected_public.len(), expected_hidden.len())
                );
            }
            before_finish = Some(
                serde_json::json!({"public_base":b.public_base,"hidden_base":b.hidden_base,
                "retained_public":b.public.len(),"retained_hidden":b.hidden.len(),
                "cursor_public":b.candidates[index].cursor.public,"cursor_hidden":b.candidates[index].cursor.hidden}),
            );
        }
        driver.stop_and_drain()?;
        recording_drained = true;
        Ok(())
    })();
    // 唯一生命周期Result进入原结束路径，不伪造经营结果或比较身份。
    hub.finish(index, &lifecycle, &candidate.session.run_id);
    lifecycle.unwrap();
    let row = serde_json::json!({"index":index,"public_consumed":public_count,"hidden_consumed":hidden_count,
        "statistic_reads":statistic_reads,"prefetched_public_after_first":public_prefetch,
        "prefetched_hidden_after_first":hidden_prefetch,"before_finish":before_finish,
        "observations":observations,"pending_economic_reads":closed_pending_reads,
        "eof_economics_applied":eof_economics_applied,"economic_coverage":"EOF已到状态的apply_closed结果；不声明end-1覆盖；false保持待应用/未知，非经济终局",
        "recording_drained":recording_drained,"run_seal":false});
    (row, held.expect("首个真实成功应用的冻结组"))
}

#[test]
#[ignore = "#378 one authorized 8-typed lifecycle; exact stage-timing run only, never repeated by workspace"]
fn formal_material_reclamation_preserves_slow_prefix_and_frozen_reads() {
    run_lifecycle(false);
}

#[test]
#[ignore = "#378 approved context ownership before/after; one 8-typed material per side only"]
fn public_context_ownership_cost_preserves_formal_lifecycle() {
    run_lifecycle(true);
}

fn run_lifecycle(ownership_cost: bool) {
    #[cfg(not(feature = "stage-timing"))]
    {
        let _ = ownership_cost;
        panic!("本显式诊断测试须启用 stage-timing；默认工作区忽略，不冒通过");
    }
    #[cfg(feature = "stage-timing")]
    {
        let whole_started = std::time::Instant::now();
        let hz = ownership_cost
            .then(crate::process_clock_ticks_per_second_v1)
            .flatten();
        let whole_before = ownership_cost
            .then(|| crate::ProcessCountersV1::read().ok())
            .flatten();
        let root =
            PathBuf::from(std::env::var_os("BACKTEST_COST_TEST_OUTPUT_ROOT").expect("显式持久根"));
        std::fs::create_dir_all(&root).unwrap();
        let material = root.join("material");
        std::fs::create_dir(&material).unwrap();
        let (config, rules) = crate::test_common::setup(&material);
        let manifest: serde_json::Value = serde_json::from_reader(
            std::fs::File::open(&config.preparation.scope.manifest_paths[0]).unwrap(),
        )
        .unwrap();
        assert_eq!(
            manifest["files"]
                .as_array()
                .unwrap()
                .iter()
                .map(|f| f["rows"].as_u64().unwrap())
                .sum::<u64>(),
            8
        );
        let prepared = crate::prepare_historical_run_v1(&config.preparation).unwrap();
        let source = RecordedPublicGroupSourceV1::open(
            &prepared.public_root,
            config.preparation.read_page_rows,
            config.preparation.max_read_batch_bytes,
            crate::timing::DeliveryTimes {
                delays: crate::DelayScheduleV1::Fixed(0),
                last: None,
            },
        )
        .unwrap();
        let header = source.header().clone();
        let mut groups = source.collect::<Result<Vec<_>, _>>().unwrap();
        let public_refs = groups
            .iter()
            .map(|g| g.prepared.reference.clone())
            .collect::<Vec<_>>();
        let mut hidden_source = RecordedHiddenBucketSourceV1::open(
            &prepared.hidden,
            &prepared.public_root,
            config.preparation.read_page_rows,
            config.preparation.max_read_batch_bytes,
        )
        .unwrap();
        let mut hidden = Vec::new();
        while let Some(bucket) = hidden_source.next_bucket().unwrap() {
            hidden.push(bucket);
        }
        let hidden_refs = hidden
            .iter()
            .map(|b| b.reference().clone())
            .collect::<Vec<_>>();
        drop(hidden_source);
        assert!(public_refs.len() > 1);
        assert!(!hidden_refs.is_empty());
        let mut candidates = Vec::new();
        for index in 0..2 {
            let cell = root.join(format!("consumer-{index}"));
            std::fs::create_dir(&cell).unwrap();
            let mut c = HistoricalPreparedRunConfigV1::from(&config);
            c.public_statistics=Some(serde_json::from_str(include_str!("../../../baseline-mm/configs/strategies/complementary_pair/public-statistics.json")).unwrap());
            c.session.run_id = format!("reclamation-{index}");
            c.session.account_id = format!("account-{index}");
            c.sim_log = cell.join("sim.jsonl");
            c.noe_root = cell.join("noe");
            c.ledger_root = cell.join("ledger");
            c.metadata_path = cell.join("metadata.json");
            c.result_path = cell.join("result.json");
            candidates.push(c);
        }
        let stream = crate::run::statistics_stream(
            &candidates[0],
            prepared.source_identity_sha256.clone(),
            header,
            rules.file_sha256().clone(),
        )
        .unwrap()
        .unwrap();
        let checkpoint_root = root.join("context-payloads");
        let payload_root = std::env::var_os("ISSUE378_OLD_CONTEXT_PAYLOADS")
            .map(PathBuf::from)
            .unwrap_or_else(|| checkpoint_root.clone());
        let mut compatibility_preparation_ns = 0;
        let mut mechanism_check_ns = 0;
        let mut metadata_references = Vec::new();
        if ownership_cost {
            let compatibility_started = std::time::Instant::now();
            std::fs::create_dir(&checkpoint_root).unwrap();
            let mechanism_started = std::time::Instant::now();
            {
                let mut first_contexts = std::collections::BTreeMap::new();
                for group in &groups {
                    for context in &group.prepared.pm_contexts {
                        if let Some(previous) =
                            first_contexts.insert(&context.market.condition_id.0, context)
                        {
                            assert!(
                                Arc::ptr_eq(previous, context),
                                "同一原owner的固定context在组间共享"
                            );
                        }
                    }
                }
            }
            mechanism_check_ns = mechanism_started.elapsed().as_nanos();
            // 同一正式材料中，各市场首次metadata组的完整引用定位原应用位置。
            for condition in prepared.markets.keys() {
                let group = groups
                    .iter()
                    .find(|g| {
                        g.prepared
                            .pm_contexts
                            .iter()
                            .any(|c| &c.market.condition_id.0 == condition)
                    })
                    .expect("正式市场首次metadata组");
                metadata_references.push(group.prepared.reference.clone());
            }
            assert_eq!(metadata_references.len(), 2);
            assert_ne!(metadata_references[0], metadata_references[1]);
            let batch =
                sim_system::public_consumption::checkpoint::DeliveryBatch::capture(groups.iter());
            ciborium::into_writer(
                &batch,
                std::fs::File::create(checkpoint_root.join("deliveries.cbor")).unwrap(),
            )
            .unwrap();
            drop(batch);
            for (index, reference) in metadata_references.iter().enumerate() {
                let group = groups
                    .iter()
                    .find(|g| &g.prepared.reference == reference)
                    .unwrap();
                let saved = sim_system::public_consumption::checkpoint::PreparedGroup::capture(
                    &group.prepared,
                );
                ciborium::into_writer(
                    &saved,
                    std::fs::File::create(checkpoint_root.join(format!("prepared-{index}.cbor")))
                        .unwrap(),
                )
                .unwrap();
            }
            // 发布前恢复：空缓存对象替代原首次应用，不在流已推进后反向首推。
            let saved: sim_system::public_consumption::checkpoint::DeliveryBatch =
                ciborium::from_reader(
                    std::fs::File::open(payload_root.join("deliveries.cbor")).unwrap(),
                )
                .unwrap();
            let mut restored_batch = saved.restore().unwrap();
            assert_eq!(restored_batch.len(), groups.len());
            for (actual, expected) in restored_batch.iter().zip(&groups) {
                assert_eq!(actual.delivered_at_utc_ms, expected.delivered_at_utc_ms);
                assert_eq!(actual.prepared.reference, expected.prepared.reference);
                assert_eq!(actual.prepared.group, expected.prepared.group);
                assert_eq!(
                    serde_json::to_value(&actual.prepared.pm_contexts).unwrap(),
                    serde_json::to_value(&expected.prepared.pm_contexts).unwrap()
                );
            }
            let mut restored_independent = Vec::new();
            for (index, reference) in metadata_references.iter().enumerate() {
                let saved: sim_system::public_consumption::checkpoint::PreparedGroup =
                    ciborium::from_reader(
                        std::fs::File::open(payload_root.join(format!("prepared-{index}.cbor")))
                            .unwrap(),
                    )
                    .unwrap();
                let restored = saved.restore().unwrap();
                let original = &groups
                    .iter()
                    .find(|g| &g.prepared.reference == reference)
                    .unwrap()
                    .prepared;
                assert_eq!(restored.reference, *reference);
                assert_eq!(restored.group, original.group);
                assert_eq!(
                    serde_json::to_value(&restored.pm_contexts).unwrap(),
                    serde_json::to_value(&original.pm_contexts).unwrap()
                );
                restored_independent.push(restored);
            }
            let first = groups
                .iter()
                .position(|g| g.prepared.reference == metadata_references[0])
                .unwrap();
            let second = groups
                .iter()
                .position(|g| g.prepared.reference == metadata_references[1])
                .unwrap();
            groups[first].prepared = restored_independent.remove(0);
            groups[second].prepared = restored_batch.remove(second).prepared;
            // 未用于实际应用的恢复副本只核值，在Hub发布前释放。
            drop(restored_independent);
            drop(restored_batch);
            compatibility_preparation_ns = compatibility_started.elapsed().as_nanos();
        }
        let preparation_wall_ns = whole_started.elapsed().as_nanos();
        let lifecycle_before = ownership_cost
            .then(|| crate::ProcessCountersV1::read().ok())
            .flatten();
        let lifecycle_started = std::time::Instant::now();
        let hub = DayHubV1::new(&candidates);
        // 同一份正式产物由这一个发布者移交，不复制完整材料给两个消费者。
        {
            let mut b = hub.0.0.lock().unwrap();
            b.public.extend(groups.into_iter().map(Into::into));
            b.hidden.extend(hidden);
            b.production = ProductionV1::Ended;
        }
        let mut rows = Vec::new();
        let mut frozen = Vec::new();
        for index in 0..2 {
            let (row, held) = consume_one(
                index,
                &hub,
                &candidates[index],
                rules.clone(),
                &prepared,
                stream.clone(),
                &public_refs,
                &hidden_refs,
                config.preparation.scope.received_end_utc_ms,
                if ownership_cost {
                    Some(prepared.markets.keys().nth(index).unwrap().as_str())
                } else {
                    None
                },
            );
            rows.push(row);
            frozen.push(held);
        }
        let b = hub.0.0.lock().unwrap();
        assert_eq!(
            (b.public_base, b.hidden_base),
            (public_refs.len(), hidden_refs.len())
        );
        assert!(b.public.is_empty() && b.hidden.is_empty());
        assert!(b.failure.is_none());
        assert!(b.candidates.iter().all(|c| !c.active));
        drop(b);
        // 两个消费者完成且Hub根集合实际退休后，再读仍持有对象的旧合法前缀。
        for (row, held) in rows.iter_mut().zip(&frozen) {
            let prefix = held
                .group
                .statistics
                .consume(
                    &stream,
                    held.receipt.group().clone(),
                    held.receipt.execution_time().logical_time_utc_ms(),
                    &held.group.group,
                )
                .unwrap()
                .unwrap();
            let observation = held
                .group
                .group
                .observations
                .get_market(&held.market)
                .unwrap();
            let read = prefix.read(
                observation,
                held.receipt.execution_time().logical_time_utc_ms(),
            );
            assert_eq!(&*read, &*held.statistics, "Hub退休后旧前缀不进入后续组");
            assert!(
                read.pm_quantities
                    .iter()
                    .all(|q| q.bid_shares.value.is_none())
            );
            row["held_after_retirement"] = serde_json::json!({"group":held.receipt.group(),
                "at":held.receipt.execution_time().logical_time_utc_ms(),"market":held.market,
                "pm_quantities":read.pm_quantities,"variance":read.spot_variance});
        }
        let lifecycle_wall = lifecycle_started.elapsed().as_secs_f64();
        let lifecycle_after = ownership_cost
            .then(|| crate::ProcessCountersV1::read().ok())
            .flatten();
        let ownership_result = if ownership_cost {
            // 生命周期已完成、Hub已退休：只在两个原held槽之间轮流clone同一字段。
            let before = crate::ProcessCountersV1::read().ok();
            let started = std::time::Instant::now();
            let mut batch = Vec::with_capacity(64);
            for i in 0..4096 {
                let contexts = std::hint::black_box(frozen[i % 2].group.pm_contexts.clone());
                std::hint::black_box(contexts[0].market.condition_id.0.len());
                batch.push(contexts);
                if batch.len() == 64 {
                    batch.clear();
                }
            }
            drop(batch);
            let wall = started.elapsed().as_secs_f64();
            let after = crate::ProcessCountersV1::read().ok();
            let result = crate::process_measurement_v1(before.as_ref(), after.as_ref(), hz, wall);
            // Owner旧字段payload来自原held的合法context，构造/序列化不输入新事件。
            let compatibility_started = std::time::Instant::now();
            for (index, held) in frozen.iter().enumerate() {
                let context = held
                    .group
                    .pm_contexts
                    .iter()
                    .find(|c| c.market.condition_id.0 == held.market)
                    .unwrap()
                    .clone();
                let owner = data_pipeline_core::public_observation_core::PmOwnedStateV1::new(
                    context.clone(),
                );
                let pointer_check_started = std::time::Instant::now();
                assert!(
                    std::ptr::eq(owner.context(), context.as_ref()),
                    "原构造入口接收已有Arc不深复制"
                );
                mechanism_check_ns += pointer_check_started.elapsed().as_nanos();
                ciborium::into_writer(
                    &owner,
                    std::fs::File::create(checkpoint_root.join(format!("owner-{index}.cbor")))
                        .unwrap(),
                )
                .unwrap();
            }
            for (index, held) in frozen.iter().enumerate() {
                let old_owner: data_pipeline_core::public_observation_core::PmOwnedStateV1<
                    data_pipeline_core::pm_reduction::PreparedPmEvidenceV1<SourceEvidenceRefV3>,
                > = ciborium::from_reader(
                    std::fs::File::open(payload_root.join(format!("owner-{index}.cbor"))).unwrap(),
                )
                .unwrap();
                let context = held
                    .group
                    .pm_contexts
                    .iter()
                    .find(|c| c.market.condition_id.0 == held.market)
                    .unwrap();
                assert_eq!(
                    serde_json::to_value(old_owner.context()).unwrap(),
                    serde_json::to_value(context).unwrap()
                );
                assert_eq!(held.receipt.group(), &metadata_references[index]);
            }
            serde_json::json!({"operations":4096,"batch":64,"context_counts":frozen.iter().map(|h|h.group.pm_contexts.len()).collect::<Vec<_>>(),
                "context_representation":std::any::type_name_of_val(&frozen[0].group.pm_contexts[0]),
                "whole_vec_includes_common_allocation":true,"measurement":result,"checkpoint_preparation_wall_ns":compatibility_preparation_ns,"extra_shared_mechanism_assertions_wall_ns":mechanism_check_ns,
                "checkpoint_checks_wall_ns":compatibility_preparation_ns+compatibility_started.elapsed().as_nanos(),
                "restored_initial_groups":metadata_references,
                "payload_read_root":payload_root,"compatibility_scope":"真实PmOwnedState旧字段＋两类checkpoint在原位置由driver合法首次消费/同对象缓存退休旧读；非旧完整PublicCore payload实测"})
        } else {
            serde_json::Value::Null
        };
        // 人工临时引用已全部清空，原held/共享stream/Hub等正式owner释放计入独立收尾。
        let cleanup_before = ownership_cost
            .then(|| crate::ProcessCountersV1::read().ok())
            .flatten();
        let cleanup_started = std::time::Instant::now();
        drop(frozen);
        drop(stream);
        drop(hub);
        drop(prepared);
        drop(candidates);
        drop(rules);
        let cleanup_wall = cleanup_started.elapsed().as_secs_f64();
        let cleanup_after = ownership_cost
            .then(|| crate::ProcessCountersV1::read().ok())
            .flatten();
        let whole_wall = whole_started.elapsed().as_secs_f64();
        let whole_after = ownership_cost
            .then(|| crate::ProcessCountersV1::read().ok())
            .flatten();
        let result = serde_json::json!({"typed_inputs":8,"material_instances":1,"producer_publishers":1,"ownership_cost":ownership_result,"preparation_wall_ns":preparation_wall_ns,
            "normal_lifecycle_plus_cleanup_wall_seconds":lifecycle_wall+cleanup_wall,
            "normal_lifecycle":crate::process_measurement_v1(lifecycle_before.as_ref(),lifecycle_after.as_ref(),hz,lifecycle_wall),
            "formal_owner_cleanup":crate::process_measurement_v1(cleanup_before.as_ref(),cleanup_after.as_ref(),hz,cleanup_wall),
            "whole_including_artificial_and_cleanup":crate::process_measurement_v1(whole_before.as_ref(),whole_after.as_ref(),hz,whole_wall),
            "measurement_limits":"进程CPU tick为低分辨率端点；RSS为端点而非各区间峰值；完整命令峰值见time-v；人工段/整例下降不能单独证明正常生命周期净收益；计量扰动包含且Unknown。",
            "consumers":rows,"G":public_refs.len(),"H":hidden_refs.len(),"expected_retired_public_roots":public_refs.len(),"expected_retired_hidden_roots":hidden_refs.len(),"limits":"缺BTC；默认研究observer未启；两消费者顺序形成快慢前缀，不是吞吐基准；有限EOF/记录排空不当经济终局或完整RunSeal"});
        std::fs::write(
            root.join("lifecycle.json"),
            serde_json::to_vec_pretty(&result).unwrap(),
        )
        .unwrap();
        eprintln!("reclamation_lifecycle={result}");
    }
}
