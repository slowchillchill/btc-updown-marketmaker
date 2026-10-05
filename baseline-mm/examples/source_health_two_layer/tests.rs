use super::*;
fn health(normal: u64, max: u64) -> SourceHealthConfigV1 {
    let mut c = ComplementaryPairConfigV1::parse_toml(include_str!(
        "../../configs/strategies/complementary_pair/synthetic-engineering.toml"
    ))
    .unwrap()
    .health;
    c.polymarket_silence_normal_ms = normal;
    c.polymarket_event_delay_normal_ms = normal;
    c.polymarket_silence_ms = max;
    c.polymarket_event_delay_ms = max;
    c
}
fn pm() -> SourceStreamScopeV1 {
    SourceStreamScopeV1 {
        source: SourceStreamKindV1::PolymarketBook,
        condition_id: Some(ConditionId("condition".into())),
        asset_or_symbol: "asset".into(),
    }
}
#[test]
fn intervals_keep_unknown_head_coalesce_same_ms_and_split_midnight_with_exact_states() {
    let d = stats::DAY;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("s.zst");
    let mut w = compact::Writer::new(&path).unwrap();
    for received in [d, d - 100] {
        w.row(compact::Row {
            at: d,
            scope: 0,
            received,
            event: received,
            flags: 29,
            samples: 1,
            selected: 0,
        })
        .unwrap();
    }
    let m = compact::Material {
        format: "health-scalars-zstd-v1".into(),
        start: d - 1000,
        end: d + 1000,
        selection: "fixture".into(),
        scopes: vec![compact::Scope {
            scope: pm(),
            window: Some(MarketWindowV2 {
                start_at_utc_ms: d - 500,
                end_at_utc_ms: d + 500,
            }),
        }],
        records: w.records,
    };
    w.finish().unwrap();
    let r = stats::analyze(
        &path,
        &m,
        m.start,
        m.end,
        vec![stats::Candidate {
            name: "test".into(),
            health: health(200, 400),
        }],
        0,
    )
    .unwrap();
    let dist = &r.groups["polymarket"];
    assert_eq!(dist.exclusions_ms["UnknownBusinessProgress"], 500);
    assert_eq!(dist.silence_time_ms.weight, 500);
    assert_eq!(dist.arrival_delay_samples.weight, 2);
    let t = &r.candidate_states["test"]["polymarket"];
    assert_eq!(t.duration_ms["normal"], 100);
    assert_eq!(t.duration_ms["elevated"], 200);
    assert_eq!(t.duration_ms["exceeded"], 200);
    assert_eq!(t.longest_exceeded_ms, 200);
    assert_eq!(t.exceeded_episodes, 1);
    assert_eq!(
        r.daily[&(d - stats::DAY)]["polymarket"].exclusions_ms["UnknownBusinessProgress"],
        500
    );
    assert_eq!(r.daily[&d]["polymarket"].silence_time_ms.weight, 500);
    let fit = stats::analyze(&path, &m, m.start, d, vec![], 0).unwrap();
    assert_eq!(fit.groups["polymarket"].silence_time_ms.weight, 0);
    assert_eq!(
        fit.groups["polymarket"].exclusions_ms["UnknownBusinessProgress"],
        500
    );
}
#[test]
fn checksum_or_truncation_is_reported_by_new_persistent_boundary() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("s.zst");
    let mut w = compact::Writer::new(&path).unwrap();
    w.row(compact::Row {
        at: 1,
        scope: 0,
        received: 1,
        event: 1,
        flags: 29,
        samples: 1,
        selected: 0,
    })
    .unwrap();
    let m = compact::Material {
        format: "health-scalars-zstd-v1".into(),
        start: 0,
        end: 2,
        selection: "fixture".into(),
        scopes: vec![compact::Scope {
            scope: pm(),
            window: None,
        }],
        records: w.records,
    };
    w.finish().unwrap();
    let b = fs::read(&path).unwrap();
    fs::write(&path, &b[..b.len() - 2]).unwrap();
    assert!(compact::read(&path, &m, |_| Ok(())).is_err());
}
#[test]
fn production_selection_weights_common_sources_once_and_never_calibrates_twap30() {
    let mut s = Selector {
        markets: BTreeMap::new(),
        boundaries: BTreeSet::new(),
        spot: false,
        mask: 0,
        binance: 3,
    };
    for id in ["a", "b"] {
        s.markets.insert(
            id.into(),
            (
                MarketWindowV2 {
                    start_at_utc_ms: 10,
                    end_at_utc_ms: 20,
                },
                Some(BtcSettlementReferenceV1::ChainlinkTwapSixtySeconds),
            ),
        );
    }
    assert_eq!(s.mask_at(10), 11);
    s.spot = true;
    assert_eq!(s.mask_at(10), 7);
    assert_eq!(s.mask_at(20), 0);
}
#[test]
fn body_rule_excludes_long_tail_and_keeps_daily_stability_inputs() {
    let mut h = stats::Histogram::default();
    h.bins.insert(100, 90);
    h.bins.insert(1_000_000, 10);
    h.weight = 100;
    assert_eq!(h.body_normal(), Some(200));
    assert_eq!(h.quantile(95, None), Some(1_001_000));
}

#[test]
fn changed_future_validation_and_development_do_not_change_fitted_candidates() {
    let t = tempfile::tempdir().unwrap();
    let scope_kinds = [
        SourceStreamKindV1::PolymarketBook,
        SourceStreamKindV1::ChainlinkSpot,
        SourceStreamKindV1::BinanceAggregateTrade,
        SourceStreamKindV1::BinanceDepth,
    ];
    let m = compact::Material {
        format: "health-scalars-zstd-v1".into(),
        start: 0,
        end: 30_000,
        selection: "fixture".into(),
        scopes: scope_kinds
            .into_iter()
            .map(|source| compact::Scope {
                scope: SourceStreamScopeV1 {
                    source,
                    condition_id: None,
                    asset_or_symbol: "BTCUSDT".into(),
                },
                window: None,
            })
            .collect(),
        records: 17,
    };
    let mut frozen = Vec::new();
    for (future_delay, development_delay) in [(0, 0), (9000, 0), (0, 15000)] {
        let path = t
            .path()
            .join(format!("{future_delay}-{development_delay}.zst"));
        let mut w = compact::Writer::new(&path).unwrap();
        w.row(compact::Row {
            at: 0,
            scope: compact::SELECT,
            received: 0,
            event: 0,
            flags: 0,
            samples: 0,
            selected: 15,
        })
        .unwrap();
        for at in [0, 5000, 15_000, 25_000] {
            for scope in 0..4 {
                let event = if at == 15_000 {
                    at - future_delay
                } else if at == 25_000 {
                    at - development_delay
                } else {
                    at
                };
                w.row(compact::Row {
                    at,
                    scope,
                    received: at,
                    event,
                    flags: 29,
                    samples: 1,
                    selected: 0,
                })
                .unwrap();
            }
        }
        w.finish().unwrap();
        let fit = stats::analyze(&path, &m, 0, 10_000, vec![], 0).unwrap();
        frozen.push(serde_json::to_value(stats::candidates(&fit).unwrap()).unwrap());
    }
    assert_eq!(frozen[0], frozen[1]);
    assert_eq!(frozen[0], frozen[2]);
}

#[test]
fn finite_prepare_writes_scalar_frames_and_source_counts_without_market_observations() {
    use data_collect::{
        source_hourly_layout::RtdsChainlinkBtcUsdHourlyRow, writer::TypedParquetSegmentWriter,
    };
    use data_pipeline_training::public_replay::HISTORICAL_SOURCE_HEAD_ORDERING_VERSION;
    let temp = tempfile::tempdir().unwrap();
    let source = "polymarket_rtds_chainlink_btc_usd";
    let version = "polymarket_rtds_chainlink_btc_usd_v4";
    let rows: Vec<_> = [86_399_500, 86_400_500]
        .into_iter()
        .map(|t| RtdsChainlinkBtcUsdHourlyRow {
            timestamp_received: t,
            message_timestamp: t - 1,
            payload_timestamp: t - 2,
            age_ms: Some(2),
            symbol: "btc/usd".into(),
            value: "100".into(),
            topic: "crypto_prices_chainlink".into(),
            message_type: "update".into(),
            freshness_status: "fresh".into(),
            subscription_filter: "{\"symbol\":\"btc/usd\"}".into(),
            source_native_payload_json: "{}".into(),
            collector_host_id: "host".into(),
            collector_region: "hk".into(),
            source_endpoint: "wss://ws-live-data.polymarket.com".into(),
            schema_version: version.into(),
        })
        .collect();
    let mut writer =
        TypedParquetSegmentWriter::create_for_current_source(temp.path(), "spot", source).unwrap();
    writer.write_rtds_chainlink_btc_usd_rows(&rows).unwrap();
    writer.append_key_value_metadata("data_collect_daily_file_fact_v1",json!({"source_kind":source,"schema_version":version,"utc_hour":"2026-09-09T23","part":0,"rows":2}).to_string()).unwrap();
    let f = writer.finalize().unwrap();
    let prep = FiniteSourceProgressConfigV1 {
        files: vec![data_pipeline_training::public_input::PublicFiniteFileV1 {
            bytes: fs::metadata(&f.final_path).unwrap().len(),
            path: f.final_path,
            source_kind: source.into(),
            schema_version: version.into(),
            rows: 2,
            sha256: f.sha256,
        }],
        received_start_utc_ms: 86_399_000,
        received_end_utc_ms: 86_402_000,
        work_root: temp.path().join("prepare"),
        header: PublicInputStreamHeaderV1 {
            input_stream_id: "scalar-test".into(),
            canonical_rule_version: "canonical-v1".into(),
            observation_version: "public-observation-v1".into(),
            ordering_version: HISTORICAL_SOURCE_HEAD_ORDERING_VERSION.into(),
            input_scope: "selected-chainlink".into(),
            max_pending_ws_terminals: 1024,
            initial_state: PublicInitialStateV1::Empty,
        },
        canonical: data_pipeline_training::gate_a_formal_canonical_config(),
        metadata_initialization: None,
    };
    let config = temp.path().join("config.json");
    save(&config,&json!({"preparation":prep,"progress_interval_seconds":0,"binance_trade_enabled":false,"binance_depth_enabled":false})).unwrap();
    let output = temp.path().join("scalars");
    prepare(&config, &output).unwrap();
    let m = material(&output).unwrap();
    assert_eq!(m.records, 2);
    let r = stats::analyze(&output.join("scalars.zst"), &m, m.start, m.end, vec![], 0).unwrap();
    assert_eq!(
        r.source_distributions["ChainlinkSpot"]
            .arrival_delay_samples
            .weight,
        2
    );
    assert_eq!(
        r.source_distributions["ChainlinkSpot"]
            .silence_time_ms
            .weight,
        2500
    );
    assert!(r.groups.is_empty());
    let summary: Value =
        serde_json::from_slice(&fs::read(output.join("preparation-summary.json")).unwrap())
            .unwrap();
    assert_eq!(summary["final_core_object_counts"]["markets"], 0);
}

#[test]
fn day_long_no_message_age_bins_do_not_report_a_quantile_below_observed_support() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("days.zst");
    let mut w = compact::Writer::new(&path).unwrap();
    w.row(compact::Row {
        at: 0,
        scope: compact::SELECT,
        received: 0,
        event: 0,
        flags: 0,
        samples: 0,
        selected: 4,
    })
    .unwrap();
    w.row(compact::Row {
        at: 0,
        scope: 0,
        received: 0,
        event: 0,
        flags: 29,
        samples: 1,
        selected: 0,
    })
    .unwrap();
    w.finish().unwrap();
    let m = compact::Material {
        format: "health-scalars-zstd-v1".into(),
        start: 0,
        end: stats::DAY + 1000,
        selection: "fixture".into(),
        scopes: vec![compact::Scope {
            scope: SourceStreamScopeV1 {
                source: SourceStreamKindV1::ChainlinkSpot,
                condition_id: None,
                asset_or_symbol: "btc/usd".into(),
            },
            window: None,
        }],
        records: 2,
    };
    let r = stats::analyze(&path, &m, m.start, m.end, vec![], 0).unwrap();
    let h = &r.source_distributions["ChainlinkSpot"].silence_time_ms;
    assert_eq!(h.weight, (stats::DAY + 1000) as u64);
    assert_eq!(h.quantile(100, None), Some(2 * stats::DAY as u64));
}

#[test]
fn same_ms_selection_and_facts_do_not_split_positive_duration_exceeded_runs() {
    let temp = tempfile::tempdir().unwrap();
    let mut reports = Vec::new();
    let mut h = health(0, 10);
    h.chainlink_silence_normal_ms = 0;
    h.chainlink_event_delay_normal_ms = 0;
    h.chainlink_silence_ms = 10;
    h.chainlink_event_delay_ms = 10;
    for case in ["continuous", "same-ms", "positive-gap"] {
        let path = temp.path().join(format!("{case}.zst"));
        let mut writer = compact::Writer::new(&path).unwrap();
        let selection = |at, mask| compact::Row {
            at,
            scope: compact::SELECT,
            received: 0,
            event: 0,
            flags: 0,
            samples: 0,
            selected: mask,
        };
        let fact = |at, received| compact::Row {
            at,
            scope: 0,
            received,
            event: received,
            flags: 29,
            samples: 1,
            selected: 0,
        };
        writer.row(selection(0, 4)).unwrap();
        writer.row(fact(0, 0)).unwrap();
        if case == "same-ms" {
            writer.row(selection(20, 0)).unwrap();
            writer.row(fact(20, 20)).unwrap();
            writer.row(selection(20, 4)).unwrap();
            writer.row(fact(20, 0)).unwrap();
        }
        if case == "positive-gap" {
            writer.row(selection(20, 0)).unwrap();
            writer.row(selection(25, 4)).unwrap();
        }
        let material = compact::Material {
            format: "health-scalars-zstd-v1".into(),
            start: 0,
            end: 30,
            selection: "fixture".into(),
            scopes: vec![compact::Scope {
                scope: SourceStreamScopeV1 {
                    source: SourceStreamKindV1::ChainlinkSpot,
                    condition_id: None,
                    asset_or_symbol: "btc/usd".into(),
                },
                window: None,
            }],
            records: writer.records,
        };
        writer.finish().unwrap();
        reports.push(
            stats::analyze(
                &path,
                &material,
                0,
                30,
                vec![stats::Candidate {
                    name: "test".into(),
                    health: h.clone(),
                }],
                0,
            )
            .unwrap(),
        );
    }
    let original = &reports[0].candidate_states["test"]["chainlink"];
    assert_eq!(original.duration_ms["exceeded"], 20);
    assert_eq!(original.exceeded_episodes, 1);
    assert_eq!(original.longest_exceeded_ms, 20);
    assert_eq!(original.unfinished_exceeded_at_end, 1);
    assert_eq!(
        serde_json::to_value(original).unwrap(),
        serde_json::to_value(&reports[1].candidate_states["test"]["chainlink"]).unwrap()
    );
    let gap = &reports[2].candidate_states["test"]["chainlink"];
    assert_eq!(gap.duration_ms["exceeded"], 15);
    assert_eq!(gap.exceeded_episodes, 2);
    assert_eq!(gap.longest_exceeded_ms, 10);
    assert_eq!(gap.unfinished_exceeded_at_end, 2);
    // 同毫秒暂态不占时间；三个真实有效确认仍各自进入来源样本。
    assert_eq!(
        reports[1].source_distributions["ChainlinkSpot"]
            .arrival_delay_samples
            .weight,
        3
    );
}

#[test]
fn daily_scope_and_market_statistics_keep_denominators_and_censor_only_day_segments() {
    let d = stats::DAY;
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("s.zst");
    let mut writer = compact::Writer::new(&path).unwrap();
    let mut scopes = Vec::new();
    for (condition, asset) in [("a", "up"), ("a", "down"), ("b", "up")] {
        scopes.push(compact::Scope {
            scope: SourceStreamScopeV1 {
                source: SourceStreamKindV1::PolymarketBook,
                condition_id: Some(ConditionId(condition.into())),
                asset_or_symbol: asset.into(),
            },
            window: Some(MarketWindowV2 {
                start_at_utc_ms: d - 20,
                end_at_utc_ms: d + 20,
            }),
        });
    }
    let fact = |at, scope| compact::Row {
        at,
        scope,
        received: at,
        event: at,
        flags: 29,
        samples: 1,
        selected: 0,
    };
    // a/up连续静默跨午夜；a/down每5ms确认；b/up直到午夜才有业务事实。
    writer.row(fact(d - 20, 0)).unwrap();
    for at in (d - 20..d + 20).step_by(5) {
        writer.row(fact(at, 1)).unwrap();
        if at == d {
            writer.row(fact(at, 2)).unwrap();
        }
    }
    let material = compact::Material {
        format: "health-scalars-zstd-v1".into(),
        start: d - 30,
        end: d + 30,
        selection: "fixture".into(),
        scopes,
        records: writer.records,
    };
    writer.finish().unwrap();
    let report = stats::analyze(
        &path,
        &material,
        material.start,
        material.end,
        vec![stats::Candidate {
            name: "test".into(),
            health: health(5, 10),
        }],
        0,
    )
    .unwrap();
    let up = &report.scopes[&0];
    let t = &up.totals.candidate_states["test"];
    assert_eq!(t.duration_ms["exceeded"], 30);
    assert_eq!(t.exceeded_episodes, 1);
    assert_eq!(t.longest_exceeded_ms, 30);
    assert_eq!(t.unfinished_exceeded_at_end, 1);
    let before = &up.daily[&0].candidate_states["test"];
    let after = &up.daily[&d].candidate_states["test"];
    assert_eq!(
        (
            before.duration_ms["exceeded"],
            after.duration_ms["exceeded"]
        ),
        (10, 20)
    );
    assert_eq!((before.exceeded_episodes, after.exceeded_episodes), (1, 1));
    assert_eq!(
        (before.longest_exceeded_ms, after.longest_exceeded_ms),
        (10, 20)
    );
    assert_eq!(after.left_censored_exceeded_at_start, 1);
    assert_eq!(before.unfinished_exceeded_at_end, 1);
    assert_eq!(after.transitions, 0); // 午夜截断不是健康切换。
    assert_eq!(
        report.scopes[&1].totals.candidate_states["test"].duration_ms["normal"],
        40
    );
    assert_eq!(
        report.scopes[&1].totals.candidate_states["test"].exceeded_episodes,
        0
    );
    assert_eq!(
        report.scopes[&2].daily[&0]
            .source_distribution
            .exclusions_ms["UnknownBusinessProgress"],
        20
    );
    assert_eq!(
        report.scopes[&2].daily[&d]
            .source_distribution
            .silence_time_ms
            .weight,
        20
    );
    assert_eq!(report.markets["a"].scope_ids, vec![0, 1]);
    assert_eq!(
        report.markets["a"]
            .totals
            .source_distribution
            .silence_time_ms
            .weight,
        80
    );
    assert_eq!(
        report.markets["b"]
            .totals
            .source_distribution
            .silence_time_ms
            .weight,
        20
    );
    assert_eq!(
        report.markets["b"].totals.source_distribution.exclusions_ms["UnknownBusinessProgress"],
        20
    );
    assert_eq!(report.scopes[&0].identity.scope.asset_or_symbol, "up");
    let group = &report.candidate_states["test"]["polymarket"];
    assert_eq!(group.duration_ms.values().sum::<u64>(), 120);
    assert_eq!(group.duration_ms["exceeded"], 40);
    assert_eq!(group.exceeded_episodes, 2);
    for day in [0, d] {
        for (state, duration) in
            &report.daily_candidate_states[&day]["test"]["polymarket"].duration_ms
        {
            let summed: u64 = report
                .scopes
                .values()
                .map(|s| {
                    s.daily[&day].candidate_states["test"]
                        .duration_ms
                        .get(state)
                        .copied()
                        .unwrap_or(0)
                })
                .sum();
            assert_eq!(*duration, summed);
        }
    }
}

#[test]
fn midnight_and_clipped_starts_use_actual_visible_prior_state() {
    let d = stats::DAY;
    let temp = tempfile::tempdir().unwrap();
    for case in [
        "threshold",
        "delayed-visible",
        "evaluation-continuation",
        "window-new-visible",
    ] {
        let path = temp.path().join(format!("{case}.zst"));
        let clipped = matches!(case, "evaluation-continuation" | "window-new-visible");
        let start = if clipped { d - 20 } else { d - 10 };
        let evaluation_start = if case == "evaluation-continuation" {
            d - 10
        } else {
            start
        };
        let window_start = if case == "window-new-visible" {
            d - 10
        } else {
            start
        };
        let rows = match case {
            "threshold" => vec![(d - 10, d - 10, d - 10)],
            "delayed-visible" => vec![(d - 5, d - 5, d - 5), (d, d - 1, d - 100)],
            "evaluation-continuation" => vec![(d - 20, d - 20, d - 100)],
            _ => vec![(d - 20, d - 20, d - 20), (d - 10, d - 11, d - 100)],
        };
        let mut writer = compact::Writer::new(&path).unwrap();
        for (at, received, event) in rows {
            writer
                .row(compact::Row {
                    at,
                    scope: 0,
                    received,
                    event,
                    flags: 29,
                    samples: 1,
                    selected: 0,
                })
                .unwrap();
        }
        let material = compact::Material {
            format: "health-scalars-zstd-v1".into(),
            start,
            end: d + 10,
            selection: "fixture".into(),
            scopes: vec![compact::Scope {
                scope: pm(),
                window: Some(MarketWindowV2 {
                    start_at_utc_ms: window_start,
                    end_at_utc_ms: d + 10,
                }),
            }],
            records: writer.records,
        };
        writer.finish().unwrap();
        let report = stats::analyze(
            &path,
            &material,
            evaluation_start,
            material.end,
            vec![stats::Candidate {
                name: "test".into(),
                health: health(0, 10),
            }],
            0,
        )
        .unwrap();
        let whole = &report.candidate_states["test"]["polymarket"];
        let previous_day = &report.daily_candidate_states[&0]["test"]["polymarket"];
        let new_day = &report.daily_candidate_states[&d]["test"]["polymarket"];
        // 恰在D发生的真实变化归D日；旧事实延续跨日不增加变化。
        let midnight_change = u64::from(matches!(case, "threshold" | "delayed-visible"));
        assert_eq!(new_day.transitions, midnight_change, "{case}");
        assert_eq!(
            whole.transitions,
            previous_day.transitions + new_day.transitions,
            "{case}"
        );
        assert_eq!(
            new_day.transitions_per_source_hour,
            Some(midnight_change as f64 * 360_000.0)
        );
        assert_eq!(
            new_day.left_censored_exceeded_at_start,
            1 - midnight_change,
            "{case}"
        );
        // 新可见事实即使携带旧接收/事件时间，也不能回写裁剪起点之前的状态。
        assert_eq!(
            whole.left_censored_exceeded_at_start,
            u64::from(case == "evaluation-continuation"),
            "{case}"
        );
        assert_eq!(whole.exceeded_episodes, 1);
        assert_eq!(
            whole.duration_ms["exceeded"],
            if midnight_change == 1 { 10 } else { 20 }
        );
        for dimensional in [
            &report.scopes[&0].daily[&d].candidate_states["test"],
            &report.markets["condition"].daily[&d].candidate_states["test"],
        ] {
            assert_eq!(
                serde_json::to_value(dimensional).unwrap(),
                serde_json::to_value(new_day).unwrap()
            );
        }
    }
}
