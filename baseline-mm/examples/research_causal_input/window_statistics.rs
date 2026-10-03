//! 已保留研究结果的单次统计输入；共用父模块的窗口/积分，不读取公共材料。
use super::*;
use sha2::{Digest, Sha256};

#[derive(Deserialize)]
struct RetainedInput {
    markets: Vec<RetainedMarket>,
}
#[derive(Deserialize)]
struct RetainedMarket {
    target: TargetMarket,
    spot_updates_original_visible_order: Vec<SpotUpdate>,
    frozen_sampling: Vec<Value>,
    posthoc_label_qualification: Value,
}

// 原Sampler在下一组可见时刻严格大于截止时才冻结，已消费截止同毫秒的全部组。
// 必须按首次可见时刻/原数组前缀重建；received不是可见性过滤轴，Unknown也保留。
fn frozen_history(updates: &[SpotUpdate], cutoff: i64) -> History {
    History {
        updates: updates
            .iter()
            .take_while(|u| u.first_visible_at_utc_ms <= cutoff)
            .cloned()
            .collect(),
        ..Default::default()
    }
}
pub(super) fn compact_hold(hold: &Value, updates: &[SpotUpdate]) -> Value {
    let index_of = |support: &Value| {
        support["group_position"]
            .as_u64()
            .and_then(|group| updates.iter().position(|u| u.group_position == group))
    };
    json!({"covered_ms":hold["covered_ms"],"missing_ms":hold["missing_ms"],
        "complete_hold_approximation":hold["complete_hold_approximation"],
        "known_price_times_seconds":hold["known_price_times_seconds"],
        "mean_price_if_complete":hold["mean_price_if_complete"],
        "left_support_update_index":index_of(&hold["left_support"]),
        "segments":hold["segments"].as_array().unwrap().iter().map(|s|json!({
            "from_received_axis_utc_ms":s["from_received_axis_utc_ms"],
            "to_received_axis_utc_ms":s["to_received_axis_utc_ms"],"duration_ms":s["duration_ms"],
            "support_update_index":index_of(&s["support"]),"price_present":!s["price_usd_per_btc"].is_null()
        })).collect::<Vec<_>>(),
        "regression_update_indices":hold["received_time_regressions_not_backfilled"].as_array().unwrap()
            .iter().filter_map(index_of).collect::<Vec<_>>()})
}
// PR200公开结果在这三处用组位置引用SpotUpdate；数值、时钟和其他字段不归一化。
fn retained_window_projection(window: &Value) -> Value {
    fn support_ref(v: &Value) -> Value {
        if v.is_null() {
            Value::Null
        } else {
            json!({"spot_update_group_position":v["group_position"]})
        }
    }
    let mut projected = window.clone();
    let hold = &mut projected["hold_coverage"];
    hold["left_support"] = support_ref(&hold["left_support"]);
    for segment in hold["segments"].as_array_mut().unwrap() {
        segment["support"] = support_ref(&segment["support"]);
    }
    for regression in hold["received_time_regressions_not_backfilled"]
        .as_array_mut()
        .unwrap()
    {
        *regression = support_ref(regression);
    }
    projected
}
fn summarize_rows(rows: &[Value], seconds: i64) -> Result<Value> {
    let selected = rows
        .iter()
        .filter(|r| r["window_seconds"] == seconds)
        .collect::<Vec<_>>();
    let mut sum_square = Decimal::ZERO;
    let mut sum_elapsed = 0i64;
    let mut pairs = 0u64;
    let mut zero_time = 0u64;
    let mut backward = 0u64;
    let mut covered = 0i64;
    let mut missing = 0i64;
    let mut complete = 0usize;
    let mut positive = 0usize;
    let mut calculable = 0usize;
    let mut zero = 0usize;
    let mut unique_pairs = BTreeSet::new();
    for r in &selected {
        sum_square += r["squared_price_change_sum"]
            .as_str()
            .unwrap()
            .parse::<Decimal>()?;
        sum_elapsed += r["positive_pair_elapsed_ms"].as_i64().unwrap();
        pairs += r["price_pairs"].as_u64().unwrap();
        zero_time += r["same_received_time_pairs"].as_u64().unwrap();
        backward += r["received_time_backward_pairs"].as_u64().unwrap();
        covered += r["hold_coverage"]["covered_ms"].as_i64().unwrap();
        missing += r["hold_coverage"]["missing_ms"].as_i64().unwrap();
        complete += usize::from(r["hold_coverage"]["complete_hold_approximation"] == true);
        positive += usize::from(r["has_positive_time_pair"] == true);
        calculable += usize::from(!r["squared_change_per_second_description"].is_null());
        zero += usize::from(r["true_zero_change_with_positive_time"] == true);
        for p in r["price_pair_refs"].as_array().unwrap() {
            unique_pairs.insert((
                r["condition_id"].as_str().unwrap().to_owned(),
                p["left_update_index"].as_u64().unwrap(),
                p["right_update_index"].as_u64().unwrap(),
            ));
        }
    }
    Ok(
        json!({"window_seconds":seconds,"market_count":selected.iter().filter_map(|r|r["condition_id"].as_str()).collect::<BTreeSet<_>>().len(),"frozen_window_rows":selected.len(),
        "complete_hold_rows":complete,"incomplete_hold_rows":selected.len()-complete,
        "rows_with_positive_time_pair":positive,"rows_with_calculable_description":calculable,
        "rows_without_calculable_description":selected.len()-calculable,
        "rows_with_true_zero_change_and_positive_time":zero,
        "summed_price_pairs_with_repetition":pairs,"summed_same_time_pairs_with_repetition":zero_time,
        "summed_backward_pairs_with_repetition":backward,"unique_market_update_pair_refs":unique_pairs.len(),
        "summed_square_with_repetition":sum_square.normalize().to_string(),
        "summed_positive_pair_elapsed_ms_with_repetition":sum_elapsed,
        "summed_hold_covered_ms_with_overlap":covered,"summed_hold_missing_ms_with_overlap":missing,
        "note":"仅窗口描述行汇总，重叠时间和共享价格对重复承载；不计算池化速率，不当独立样本或选窗依据。"}),
    )
}
fn calculate(input: &RetainedInput) -> Result<Value> {
    let mut rows = Vec::new();
    let mut labels = Vec::new();
    let mut markets = Vec::new();
    let mut differences = Vec::new();
    for (market_index, m) in input.markets.iter().enumerate() {
        let updates = &m.spot_updates_original_visible_order;
        markets.push(json!({"condition_id":m.target.condition_id,"window":m.target.window,
            "spot_state_updates":updates.len(),"initialization_state_updates":updates.iter().filter(|u|u.initialization_source_group).count(),
            "pre_window_state_updates":updates.iter().filter(|u|!u.initialization_source_group && u.first_visible_at_utc_ms<m.target.window.start_at_utc_ms).count(),
            "inside_window_state_updates":updates.iter().filter(|u|!u.initialization_source_group && u.first_visible_at_utc_ms>=m.target.window.start_at_utc_ms && u.first_visible_at_utc_ms<m.target.window.end_at_utc_ms).count(),
            "source_array_pointer":format!("/markets/{market_index}/spot_updates_original_visible_order")}));
        for (sample_index, sample) in m.frozen_sampling.iter().enumerate() {
            let cutoff = sample["frozen_visible_cutoff_utc_ms"]
                .as_i64()
                .ok_or("冻结截止缺失")?;
            let h = frozen_history(updates, cutoff);
            for &seconds in &WINDOWS_SECONDS {
                let (original, refs) = h.window_with_pairs(cutoff, seconds)?;
                let projected = retained_window_projection(&original);
                let old = sample["descriptive_windows"]
                    .as_array()
                    .ok_or("旧描述窗缺失")?
                    .iter()
                    .find(|v| v["window_seconds"] == seconds);
                if old != Some(&projected) {
                    differences.push(
                        json!({"condition_id":m.target.condition_id,"sample_index":sample_index,
                        "window_seconds":seconds,"original_retained":old,"recalculated":original,"recalculated_reference_projection":projected}),
                    );
                }
                let from = cutoff - seconds * 1000;
                let mut missing_reasons = Vec::new();
                if original["positive_pair_elapsed_ms"] == 0 {
                    missing_reasons.push("no_positive_time_pair");
                }
                if original["same_received_time_pairs"] != 0 {
                    missing_reasons.push("same_received_time_pair");
                }
                if original["received_time_backward_pairs"] != 0 {
                    missing_reasons.push("received_time_backward_pair");
                }
                let inside_updates = h
                    .updates
                    .iter()
                    .filter(|u| u.received_at_utc_ms >= from && u.received_at_utc_ms <= cutoff)
                    .count();
                let eligible = h
                    .updates
                    .iter()
                    .filter(|u| {
                        u.received_at_utc_ms >= from
                            && u.received_at_utc_ms <= cutoff
                            && !u.new_source_ids.is_empty()
                    })
                    .map(|u| u.price())
                    .collect::<Result<Vec<_>>>()?
                    .into_iter()
                    .filter(Option::is_some)
                    .count();
                let hold = compact_hold(&original["hold_coverage"], &h.updates);
                let has_positive = original["positive_pair_elapsed_ms"].as_i64().unwrap() > 0;
                let true_zero = has_positive && original["squared_price_change_sum"] == "0";
                let mut row = original.as_object().unwrap().clone();
                row.insert("hold_coverage".into(), hold);
                row.insert("condition_id".into(), json!(m.target.condition_id));
                row.insert("market_result_index".into(), json!(market_index));
                row.insert("sample_index".into(), json!(sample_index));
                row.insert(
                    "remaining_seconds".into(),
                    sample["remaining_seconds"].clone(),
                );
                row.insert("from_received_axis_utc_ms".into(), json!(from));
                row.insert("frozen_visible_cutoff_utc_ms".into(), json!(cutoff));
                row.insert(
                    "visible_update_prefix_length".into(),
                    json!(h.updates.len()),
                );
                row.insert(
                    "visible_prefix_last_group_position".into(),
                    json!(h.updates.last().map(|u| u.group_position)),
                );
                row.insert(
                    "retained_updates_received_inside_window".into(),
                    json!(inside_updates),
                );
                row.insert(
                    "eligible_new_source_price_updates_inside_window".into(),
                    json!(eligible),
                );
                row.insert("price_pair_refs".into(), json!(refs));
                row.insert("has_positive_time_pair".into(), json!(has_positive));
                row.insert(
                    "true_zero_change_with_positive_time".into(),
                    json!(true_zero),
                );
                row.insert(
                    "description_unavailable_reasons".into(),
                    json!(missing_reasons),
                );
                row.insert("reference_kind".into(), sample["reference"]["kind"].clone());
                row.insert(
                    "reference_quality".into(),
                    sample["reference"]["quality"].clone(),
                );
                row.insert(
                    "original_frozen_snapshot_pointer".into(),
                    json!(format!(
                        "/markets/{market_index}/frozen_sampling/{sample_index}"
                    )),
                );
                row.insert(
                    "original_values_equal".into(),
                    json!(old == Some(&projected)),
                );
                rows.push(Value::Object(row));
            }
        }
        // 标签只保留独立连接及原数组引用，绝不参与上面窗口选择或计算。
        let q = &m.posthoc_label_qualification;
        let first = &q["first_usable_terminal"];
        let matches = q["condition_id"] == json!(m.target.condition_id)
            && (first.is_null() || first["condition_id"] == json!(m.target.condition_id));
        labels.push(json!({"condition_id":m.target.condition_id,"same_condition":matches,
            "source_pointer":format!("/markets/{market_index}/posthoc_label_qualification"),
            "first_usable_terminal_visible_at_utc_ms":if matches {first["first_visible_at_utc_ms"].clone()}else{Value::Null},
            "first_usable_terminal_group_position":if matches {first["group_position"].clone()}else{Value::Null},
            "latest_terminal_usable":if matches {q["latest_terminal_usable"].clone()}else{Value::Null},
            "first_terminal_quality":if matches {first["fact"]["quality"].clone()}else{Value::Null},
            "winning_outcome":if matches {first["fact"]["value"]["winning_outcome"].clone()}else{Value::Null},
            "winning_asset_id":if matches {first["fact"]["value"]["winning_asset_id"].clone()}else{Value::Null},
            "terminal_quality_changes":if matches {q["changes_original_visible_order"].as_array().map(|changes|changes.iter().enumerate().map(|(i,c)|json!({"original_change_index":i,"group_position":c["group_position"],"first_visible_at_utc_ms":c["first_visible_at_utc_ms"],"quality":c["fact"]["quality"],"value_present":!c["fact"]["value"].is_null()})).collect::<Vec<_>>())}else{None},
            "used_in_features_or_filter":false}));
    }
    Ok(
        json!({"schema":"complementary_pair_window_statistics_v1","status":if differences.is_empty(){"success"}else{"original_value_difference"},"market_denominator":input.markets.len(),
        "market_context":markets,"rows":rows,"window_summaries":WINDOWS_SECONDS.iter().map(|&s|summarize_rows(&rows,s)).collect::<Result<Vec<_>>>()?,
        "original_value_differences":differences,"all_original_windows_equal":differences.is_empty(),"posthoc_label_associations":labels,
        "method":"原可见序前缀，保留Unknown/重置；两端接收时间均在闭区间描述窗，同一原循环生成对引用；不是生产sigma验证。",
        "units":{"price":"USD/BTC","square":"(USD/BTC)^2","description":"(USD/BTC)^2/s","hold_integral":"(USD/BTC)*s"}}),
    )
}
pub(super) fn run(input_path: &str, output_path: &str) -> Result<()> {
    let began = Instant::now();
    let bytes = fs::read(input_path)?;
    let hash = format!("{:x}", Sha256::digest(&bytes));
    let input: RetainedInput = serde_json::from_slice(&bytes)?;
    let mut report = calculate(&input)?;
    report["input_identity"] = json!({"logical_path":"baseline-mm/docs/research/complementary-pair-causal-labels-20260909-0000-0012.json","bytes":bytes.len(),"sha256":hash});
    report["measurement"] =
        json!({"read_decode_calculate_seconds":began.elapsed().as_secs_f64(),"input_file_reads":1});
    fs::write(output_path, serde_json::to_vec_pretty(&report)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn update(index: u64, received: i64, visible: i64, price: Option<&str>) -> SpotUpdate {
        SpotUpdate {
            group_position: index,
            received_at_utc_ms: received,
            first_visible_at_utc_ms: visible,
            source_ids: vec![Sha256Hex(format!("{index:064x}"))],
            new_source_ids: vec![Sha256Hex(format!("{index:064x}"))],
            source_event_at_utc_ms: vec![Some(received)],
            price_usd_per_btc: price.map(|p| ExactDecimal(p.into())),
            quality: if price.is_some() {
                MarketDataQuality::Fresh
            } else {
                MarketDataQuality::UnknownNotObserved
            },
            initialization_source_group: false,
        }
    }
    #[test]
    fn hand_calculated_left_endpoint_and_no_pair_are_distinct_from_real_zero_change() {
        let h = History {
            updates: vec![
                update(0, 900, 900, Some("100")),
                update(1, 1500, 1500, Some("101")),
                update(2, 2500, 2500, Some("103")),
            ],
            ..Default::default()
        };
        let (w, refs) = h.window_with_pairs(3000, 2).unwrap();
        assert_eq!(w["squared_price_change_sum"], "4");
        assert_eq!(w["positive_pair_elapsed_ms"], 1000);
        assert_eq!(w["squared_change_per_second_description"], "4");
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0]["left_update_index"], 1);
        // 生产右端纳入跨左端对的手算5/1.6秒由源码关系说明，不复制其估计器。
        let hold = History {
            updates: vec![update(0, 900, 900, Some("100"))],
            ..Default::default()
        }
        .window(3000, 2)
        .unwrap();
        assert_eq!(hold["hold_coverage"]["complete_hold_approximation"], true);
        assert_eq!(hold["positive_pair_elapsed_ms"], 0);
        assert!(hold["squared_change_per_second_description"].is_null());
        let zero = History {
            updates: vec![
                update(0, 1500, 1500, Some("100")),
                update(1, 2500, 2500, Some("100")),
            ],
            ..Default::default()
        }
        .window(3000, 2)
        .unwrap();
        assert_eq!(zero["positive_pair_elapsed_ms"], 1000);
        assert_eq!(zero["squared_price_change_sum"], "0");
        assert_eq!(zero["squared_change_per_second_description"], "0");
    }
    #[test]
    fn retained_support_projection_preserves_group_identity_and_all_numeric_fields() {
        let h = History {
            updates: vec![
                update(0, -1000, -1000, Some("100")),
                update(1, 1000, 1000, Some("101")),
                update(2, 500, 1800, Some("102")),
            ],
            ..Default::default()
        };
        let raw = h.window(2000, 2).unwrap();
        let mut expected = raw.clone();
        expected["hold_coverage"]["left_support"] = json!({"spot_update_group_position":0});
        expected["hold_coverage"]["segments"][0]["support"] =
            json!({"spot_update_group_position":0});
        expected["hold_coverage"]["segments"][1]["support"] =
            json!({"spot_update_group_position":1});
        expected["hold_coverage"]["received_time_regressions_not_backfilled"] =
            json!([{"spot_update_group_position":2}]);
        let projected = retained_window_projection(&raw);
        assert_eq!(projected, expected);
        expected["hold_coverage"]["segments"][0]["support"]["spot_update_group_position"] =
            json!(999);
        assert_ne!(projected, expected);
        expected = projected.clone();
        expected["positive_pair_elapsed_ms"] = json!(999);
        assert_ne!(projected, expected);
    }
    #[test]
    fn visible_prefix_excludes_late_same_receive_time_and_future_unknown_reset() {
        let mut updates = vec![
            update(0, 1000, 1000, Some("100")),
            update(1, 2000, 2000, Some("102")),
        ];
        let old = frozen_history(&updates, 2000).window(2000, 15).unwrap();
        updates.push(update(2, 2000, 2001, Some("999")));
        updates.push(update(3, 2000, 2500, None));
        assert_eq!(
            frozen_history(&updates, 2000).window(2000, 15).unwrap(),
            old
        );
        assert_eq!(frozen_history(&updates, 2000).updates.len(), 2);
        assert_eq!(frozen_history(&updates, 2500).updates.len(), 4);
        let mut same_millisecond = updates[..2].to_vec();
        same_millisecond.push(update(2, 2000, 2000, Some("103")));
        assert_eq!(
            frozen_history(&same_millisecond, 2000)
                .window(2000, 15)
                .unwrap()["same_received_time_pairs"],
            1
        );
    }
    #[test]
    fn label_suffix_and_other_market_label_do_not_change_feature_rows() {
        let target = TargetMarket {
            condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
            window: MarketWindowV2 {
                start_at_utc_ms: 0,
                end_at_utc_ms: 300000,
            },
        };
        let updates = vec![
            update(0, 1000, 1000, Some("100")),
            update(1, 2000, 2000, Some("102")),
        ];
        let h = frozen_history(&updates, 2000);
        let sample = json!({"frozen_visible_cutoff_utc_ms":2000,"remaining_seconds":298,"reference":{"kind":"missing"},"descriptive_windows":WINDOWS_SECONDS.iter().map(|&s|retained_window_projection(&h.window(2000,s).unwrap())).collect::<Vec<_>>()});
        let mut input = RetainedInput {
            markets: vec![RetainedMarket {
                target: target.clone(),
                spot_updates_original_visible_order: updates,
                frozen_sampling: vec![sample],
                posthoc_label_qualification: json!({"condition_id":target.condition_id}),
            }],
        };
        let old = calculate(&input).unwrap();
        input.markets[0].posthoc_label_qualification = json!({"condition_id":target.condition_id,"first_usable_terminal":{"condition_id":target.condition_id,"first_visible_at_utc_ms":400000,"fact":{"quality":"FRESH","value":{"winning_outcome":"Up"}}}});
        let relabeled = calculate(&input).unwrap();
        assert_eq!(relabeled["rows"], old["rows"]);
        assert_eq!(
            relabeled["posthoc_label_associations"][0]["same_condition"],
            true
        );
        assert_eq!(
            relabeled["posthoc_label_associations"][0]["winning_outcome"],
            "Up"
        );
        input.markets[0]
            .spot_updates_original_visible_order
            .push(update(2, 2000, 3000, None));
        input.markets[0].posthoc_label_qualification = json!({"condition_id":format!("0x{}","2".repeat(64)),"first_usable_terminal":{"first_visible_at_utc_ms":400000}});
        let new = calculate(&input).unwrap();
        assert_eq!(new["rows"], old["rows"]);
        assert_eq!(
            new["posthoc_label_associations"][0]["same_condition"],
            false
        );
        assert!(
            new["posthoc_label_associations"][0]["first_usable_terminal_visible_at_utc_ms"]
                .is_null()
        );
    }
}
