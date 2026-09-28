    let variant = "VARIANT_NAME";
    assert_eq!(std::env::var("FRAME_MODE").unwrap(), variant);
    let first: u64 = std::env::var("FRAME_FIRST").unwrap().parse().unwrap();
    let model = std::env::var("FRAME_MODEL").unwrap();
    let threads = if model == "MAIN-1x256" { 4 } else { 16 };
    let graphs = PathBuf::from(std::env::var("FRAME_GRAPHS").unwrap());
    let mut runtime = frame_runtime::Runtime::new(&graphs.join(&model), threads);
    let executor = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let checkpoint = PathBuf::from(std::env::var("FRAME_CHECKPOINTS").unwrap()).join(format!("entry-{first}.json"));
    let mut measurements = BufWriter::new(File::create(out.join("measurements.jsonl"))?);
    let start = Instant::now();
    let preparation_cpu = process_cpu_ns();
    let mut count = 0;
    let mut service_total = 0u128;
    let mut evidence_total = 0u128;
    loop {
        let next = retry(&mut driver, FinitePublicConsumptionV1::next_group, FinitePublicConsumptionV1::wait_for_recording_capacity)?;
        let Some((_, cutoff)) = next else {break;};
        let position = cutoff.receipt().group().group_position;
        if position > first + 63 {break;}
        if position < first {shared.action_before(&mut driver)?;continue;}
        if position == first {
            let loaded: serde_json::Value = serde_json::from_slice(&fs::read(&checkpoint)?)?;
            assert_eq!(loaded["position"], position);
            assert_eq!(loaded["ledger"], serde_json::to_value(shared.ledger().projection_v2())?);
            native.restore_checkpoint(&loaded["native"]);
            runtime.restore(&loaded["runtimes"][if model == "MAIN-1x256" {0} else {1}]);
            line(&mut measurements, &serde_json::json!({"preparation_seconds":start.elapsed().as_secs_f64(),"preparation_cpu_ns":process_cpu_ns()-preparation_cpu,
                "graph_load_seconds":runtime.load_seconds,"graph_warmup_seconds":runtime.warmup_seconds,"entry_position":position,
                "restored_prior_model_rows":runtime.frames,"new_input_cache":"empty; first consume timed","ledger_equal":true,"native_threads":8,"ort_threads":threads}))?;
        }
        let cpu_before = process_cpu_ns();
        let began = Instant::now();
        let group = Arc::new(native.consume(&mut shared, &mut driver).map_err(|e|format!("consume: {e:?}"))?.unwrap());
        let native_ns = began.elapsed().as_nanos();
        let (actors, timing, states) = runtime.advance(group.clone(), true);
        let mut actions = Vec::new();
        for actor in actors {
            let at = Instant::now();
            driver.wait_for_recording_capacity()?;
            let receipt = executor.block_on(actor.execute(&mut shared, &mut driver.decision().unwrap(), RequestContextV2 {
                global_request_id:format!("model-{position}"),caller_correlation_id:None,
            })).map_err(|e|format!("execute: {e:?}"))?;
            actions.push((actor,receipt,at.elapsed().as_nanos()));
        }
        let service_ns = began.elapsed().as_nanos();
        let cpu_ns = process_cpu_ns() - cpu_before;
        service_total += service_ns;
        let evidence = Instant::now();
        let actions=actions.into_iter().map(|(actor,receipt,execute_ns)|serde_json::json!({"path":actor.path,"intent":format!("{:?}",actor.intent().unwrap()),"receipt":format!("{receipt:?}"),"execute_ns":execute_ns})).collect::<Vec<_>>();
        line(&mut measurements,&serde_json::json!({"position":position,"now":cutoff.decision_time().logical_time_utc_ms(),"service_ns":service_ns,"native_ns":native_ns,"cpu_ns":cpu_ns,
            "native_group":&*group,"native_bits_sha256":format!("{:x}",Sha256::digest(group.bench_f32_bits().iter().flat_map(|b|b.to_le_bytes()).collect::<Vec<_>>())),
            "native_business":native.bench_business(),"cache":native.bench_cache(),"ledger":shared.ledger().projection_v2(),
            "runtimes":[{"timing":timing,"hidden_sha256":runtime.hidden_digest(),"z_bits_sha256":format!("{:x}",Sha256::digest(states.iter().flatten().flat_map(|v|v.to_bits().to_le_bytes()).collect::<Vec<_>>())),"actions":actions}]}))?;
        count += 1;
        evidence_total += evidence.elapsed().as_nanos();
    }
    measurements.flush()?;
    time_audit.borrow_mut().flush()?;
    let loop_elapsed = start.elapsed().as_secs_f64();
    let drain = Instant::now();
    driver.stop_and_drain()?;
    assert_eq!(count,64);
    json(out.join("completion.json"), &serde_json::json!({"complete":true,"bounded_only":true,"variant":variant,"model":model,"groups":count,
        "actual_runtime_frames":runtime.frames,"loop_elapsed_seconds":loop_elapsed,"drain_seconds":drain.elapsed().as_secs_f64(),
        "elapsed_seconds":start.elapsed().as_secs_f64(),"service_total_ns":service_total,"evidence_total_ns":evidence_total}))?;
    Ok(())
}
