use super::*;
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicUsize, Ordering},
};
struct AfterEof {
    inner: Arc<dyn HistoricalPublicObserverV1>,
    ended: (Mutex<bool>, Condvar),
    requests_after_eof: AtomicUsize,
    candidates: Vec<Arc<Mutex<Option<TraceSummary>>>>,
    fail_finish: bool,
}
impl std::fmt::Debug for AfterEof {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AfterEof").finish_non_exhaustive()
    }
}
impl HistoricalPublicObserverV1 for AfterEof {
    fn requirements(&self) -> PublicObservationRequirementsV1 {
        self.inner.requirements()
    }
    fn observe(
        &self,
        r: &PublicGroupReferenceV1,
        g: &ClosedBusinessGroupV1,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        self.inner.observe(r, g)
    }
    fn observe_at(
        &self,
        r: &PublicGroupReferenceV1,
        g: &ClosedBusinessGroupV1,
        at: i64,
    ) -> Result<Arc<HistoricalPublicEvidenceV1>, MaterialError> {
        self.inner.observe_at(r, g, at)
    }
    fn production_finished(&self) -> Result<(), MaterialError> {
        self.inner.production_finished()?;
        *self.ended.0.lock().unwrap() = true;
        self.ended.1.notify_all();
        Ok(())
    }
    fn record_request_evidence(
        &self,
        requests: &[HistoricalOriginalRequestV1],
    ) -> Result<serde_json::Value, MaterialError> {
        if !requests.is_empty() {
            // 候选真实请求在此暂停；生产领先上限大于整个有限输入，不存在互等。
            let ended = self.ended.0.lock().unwrap();
            let (ended, timeout) = self
                .ended
                .1
                .wait_timeout_while(ended, std::time::Duration::from_secs(10), |done| !*done)
                .unwrap();
            assert!(!timeout.timed_out() && *ended, "producer必须先达到真实EOF");
            self.requests_after_eof
                .fetch_add(requests.len(), Ordering::Relaxed);
        }
        self.inner.record_request_evidence(requests)
    }
    fn finish(&self) -> Result<(), MaterialError> {
        assert!(
            self.candidates.iter().all(|s| s.lock().unwrap().is_some()),
            "不得在候选结束前关闭公共输出"
        );
        assert!(self.requests_after_eof.load(Ordering::Relaxed) > 0);
        self.inner.finish()?;
        if self.fail_finish {
            Err("fixture required final shared output failure".into())
        } else {
            Ok(())
        }
    }
}
#[test]
fn day_saves_real_requests_after_eof_and_final_failure_keeps_business_reports() {
    for fail_finish in [false, true] {
        let temp = tempfile::tempdir().unwrap();
        let (mut config, rules) = fixture(temp.path(), "100010", &[800]);
        config.timing = HistoricalTimingV1 {
            decision_mode: backtest::HistoricalDecisionModeV1::EventDriven,
            public_delivery_delay_ms: DelayScheduleV1::Fixed(0),
            calculation_start_delay_ms: DelayScheduleV1::Fixed(0),
            calculation_duration_ms: DelayScheduleV1::Fixed(15),
            dispatch_delay_ms: DelayScheduleV1::Fixed(0),
        };
        let root = temp.path().join("public");
        let factory = streaming_research::StreamingResearchV1::open(&root).unwrap();
        let inner = factory
            .for_stream(&config.preparation.header.input_stream_id)
            .unwrap()
            .unwrap();
        let sinks = vec![Arc::new(Mutex::new(None)), Arc::new(Mutex::new(None))];
        let observer = Arc::new(AfterEof {
            inner,
            ended: (Mutex::new(false), Condvar::new()),
            requests_after_eof: Default::default(),
            candidates: sinks.clone(),
            fail_finish,
        });
        let mut candidates = vec![];
        for index in 0..2 {
            let r = temp.path().join(format!("account-{index}"));
            std::fs::create_dir(&r).unwrap();
            let mut run = HistoricalPreparedRunConfigV1::from(&config);
            run.session.run_id = format!("late-{index}");
            run.strategy_request_namespace = Some(format!("late-{index}"));
            run.sim_log = r.join("sim.log");
            run.noe_root = r.join("noe");
            run.ledger_root = r.join("ledger");
            run.metadata_path = r.join("meta.json");
            run.result_path = r.join("report.json");
            run.research_evidence_path = Some(r.join("evidence.jsonl"));
            candidates.push(run);
        }
        let day = HistoricalDayConfigV1 {
            preparation: config.preparation.clone(),
            candidates,
            progress_interval_seconds: 0,
            producer_lead_groups: Some(2048),
        };
        let make = |run: &HistoricalPreparedRunConfigV1| -> Result<Trace, MaterialError> {
            let index = usize::from(run.session.run_id == "late-1");
            Ok(minimal_export_cost::trace(
                ComplementaryPairV1::new(
                    strategy_config(),
                    rules.clone(),
                    run.strategy_request_namespace.clone().unwrap(),
                    StrategyContinuityV2::new_account(),
                )?,
                sinks[index].clone(),
            ))
        };
        let result = run_historical_days_with_options_v1(
            &[day],
            rules.clone(),
            &make,
            1,
            &HistoricalDayRunOptionsV1 {
                public_observer: Some(observer),
                ..Default::default()
            },
        );
        if fail_finish {
            let err = result.unwrap_err();
            let err = err.downcast_ref::<HistoricalDaysFailureV1>().unwrap();
            assert!(
                err.cause
                    .to_string()
                    .contains("final shared output failure")
            );
            assert_eq!(err.days[0].candidates.len(), 2);
            assert!(
                err.days[0]
                    .candidates
                    .iter()
                    .all(|c| c.report["end_reason"] == "RangeEnd")
            );
        } else {
            assert!(
                result.unwrap()[0]
                    .candidates
                    .iter()
                    .all(|c| c.outcome == CandidateOutcomeV1::Complete)
            );
        }
        let rows = std::fs::read_to_string(root.join("public-0.jsonl"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str::<serde_json::Value>(l).unwrap())
            .collect::<Vec<_>>();
        let eof = rows
            .iter()
            .position(|r| r["record_type"] == "production_end")
            .unwrap();
        assert!(
            rows[eof + 1..]
                .iter()
                .any(|r| r["record_type"] == "original_pair_blb")
        );
    }
}
