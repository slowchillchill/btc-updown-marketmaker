    let variant = "candidate";
    let first: u64 = std::env::var("FRAME_FIRST")?.parse()?;
    if std::env::var_os("FRAME_PROTOCOL").is_some() {
        control.borrow().ready();
        let mut groups = 0;
        loop {
            if control.borrow_mut().prepare_next()? {
                break;
            }
            let next = retry(
                &mut driver,
                FinitePublicConsumptionV1::next_group,
                FinitePublicConsumptionV1::wait_for_recording_capacity,
            )?;
            let Some((_, cutoff)) = next else {
                break;
            };
            let measured = cutoff.receipt().group().group_position >= first;
            shared.action_before(&mut driver)?;
            if measured {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            control.borrow_mut().acknowledge_if_empty()?;
            groups += 1;
        }
        driver.stop_and_drain()?;
        json(
            out.join("completion.json"),
            &serde_json::json!({"complete":true,"groups":groups,"ledger":shared.ledger().projection_v2(),"online":control.borrow().snapshot(),"protocol_validation_only":true}),
        )?;
        return Ok(());
    }
    let old_checkpoints = PathBuf::from(std::env::var("FRAME_CHECKPOINTS")?);
    let warm_checkpoints = PathBuf::from(std::env::var("FRAME_WARM_CHECKPOINTS")?);
    let model = std::env::var("FRAME_MODEL")?;
    let graphs = PathBuf::from(std::env::var("FRAME_GRAPHS")?);
    let mut measurements = BufWriter::new(File::create(out.join("measurements.jsonl"))?);
    let preparation = Instant::now();
    let preparation_cpu = process_cpu_ns();
    let mut runtime = frame_runtime::Runtime::new(
        &graphs.join(&model),
        if model == "MAIN-1x256" { 4 } else { 16 },
    );
    let old: serde_json::Value = serde_json::from_slice(&fs::read(
        old_checkpoints.join(format!("entry-{first}.json")),
    )?)?;
    let warm: serde_json::Value = serde_json::from_slice(&fs::read(
        warm_checkpoints.join(format!("warm-{first}.json")),
    )?)?;
    native.bench_restore_warm(&warm["native"]);
    runtime.restore(&old["runtimes"][if model == "MAIN-1x256" { 0 } else { 1 }]);
    runtime.restore_books(native.bench_previous_books(&warm["last_rows"]));
    control.borrow().ready();
    let executor = tokio::runtime::Builder::new_current_thread().build()?;
    let mut count = 0u64;
    let mut evidence_total = 0u128;
    let mut service_total = 0u128;
    loop {
        if control.borrow_mut().prepare_next()? {
            break;
        }
        let next = retry(
            &mut driver,
            FinitePublicConsumptionV1::next_group,
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        )?;
        let Some((_, cutoff)) = next else {
            break;
        };
        let reference = cutoff.receipt().group().clone();
        let position = reference.group_position;
        let now = cutoff.decision_time().logical_time_utc_ms();
        if position < first {
            shared.action_before(&mut driver)?;
            control.borrow_mut().acknowledge_if_empty()?;
            continue;
        }
        if position == first {
            assert_eq!(
                old["ledger"],
                serde_json::to_value(shared.ledger().projection_v2())?,
                "准确入口账本前驱"
            );
            line(
                &mut measurements,
                &serde_json::json!({"preparation_seconds":preparation.elapsed().as_secs_f64(),"preparation_cpu_ns":process_cpu_ns()-preparation_cpu,"graph_load_seconds":runtime.load_seconds,"graph_warmup_seconds":runtime.warmup_seconds,"entry_position":position,"restored_prior_model_rows":runtime.frames,"historical_model_inference_rerun":false,"online":control.borrow().snapshot()}),
            )?;
        }
        model_native_inputs::bench_profile::active(true);
        shared_run::bench_profile::active(true);
        let started = control.borrow().ns();
        let began = Instant::now();
        let cpu = process_cpu_ns();
        let group = native
            .consume(&mut shared, &mut driver)
            .map_err(|e| format!("consume: {e:?}"))?
            .map(Arc::new);
        let native_ns = began.elapsed().as_nanos();
        let (actors, timing) = if let Some(group) = &group {
            let (actors, timing, _) = runtime.advance(group.clone(), true);
            (actors, serde_json::to_value(timing)?)
        } else {
            (Vec::new(), serde_json::Value::Null)
        };
        let mut actions = Vec::new();
        for actor in actors {
            let at = Instant::now();
            driver.wait_for_recording_capacity()?;
            let sim_wait_ns = at.elapsed().as_nanos();
            let receipt = executor
                .block_on(actor.execute(
                    &mut shared,
                    &mut driver.decision().unwrap(),
                    RequestContextV2 {
                        global_request_id: format!("model-{position}"),
                        caller_correlation_id: None,
                    },
                ))
                .map_err(|e| format!("execute: {e:?}"))?;
            actions.push(serde_json::json!({"path":actor.path,"intent":format!("{:?}",actor.intent().unwrap()),"receipt":format!("{receipt:?}"),"execute_ns":at.elapsed().as_nanos(),"sim_recording_wait_ns":sim_wait_ns}));
        }
        let mut components = model_native_inputs::bench_profile::take();
        components.extend(shared_run::bench_profile::take());
        model_native_inputs::bench_profile::active(false);
        shared_run::bench_profile::active(false);
        let finished = control.borrow().ns();
        let service_ns = began.elapsed().as_nanos();
        service_total += service_ns;
        // 最后派生组的 native、hidden、Actor 与原 execute 都完成后，才解除真实 Received。
        control.borrow_mut().acknowledge_if_empty()?;
        let evidence = Instant::now();
        line(
            &mut measurements,
            &serde_json::json!({"position":position,"group_id":reference.group_id,"now":now,"model_begin_ns":started,"model_end_ns":finished,"source_delivery_lag_ns":control.borrow().delivery_lag_ns(now),"service_ns":service_ns,"native_ns":native_ns,"cpu_ns":process_cpu_ns()-cpu,"components":components,"rows":group.as_ref().map(|g|g.rows.iter().map(|r|(&r.hidden_state_key,r.book_levels.iter().map(|s|s.len()).collect::<Vec<_>>())).collect::<Vec<_>>()).unwrap_or_default(),"timing":timing,"actions":actions,"sim_recording":driver.measurement_resources()}),
        )?;
        count += 1;
        evidence_total += evidence.elapsed().as_nanos();
    }
    measurements.flush()?;
    let drain = Instant::now();
    driver.stop_and_drain()?;
    json(
        out.join("completion.json"),
        &serde_json::json!({"complete":true,"variant":variant,"groups":count,"drain_seconds":drain.elapsed().as_secs_f64(),"service_total_ns":service_total,"evidence_total_ns":evidence_total,"online":control.borrow().snapshot(),"hidden_sha256":runtime.hidden_digest(),"final_ledger":shared.ledger().projection_v2()}),
    )?;
    Ok(())
}
