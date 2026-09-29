    let variant = "VARIANT_NAME";
    assert_eq!(std::env::var("FRAME_MODE").unwrap(), variant);
    let prepare = std::env::var_os("FRAME_PREPARE").is_some();
    let first: u64 = std::env::var("FRAME_FIRST").unwrap_or_else(|_| "201582".into()).parse().unwrap();
    let old_checkpoints = PathBuf::from(std::env::var("FRAME_CHECKPOINTS").unwrap());
    let mut measurements = BufWriter::new(File::create(out.join("measurements.jsonl"))?);
    let mut warmup_measurements = BufWriter::new(File::create(out.join("warmup-measurements.jsonl"))?);
    let start = Instant::now();
    let preparation_cpu = process_cpu_ns();
    let mut native_preparation_ns=0u128;
    let mut native_preparation_groups=0usize;
    let mut last_rows=serde_json::json!([]);
    let mut runtime = if prepare {None} else {
        let model=std::env::var("FRAME_MODEL").unwrap();
        let graphs=PathBuf::from(std::env::var("FRAME_GRAPHS").unwrap());
        Some(frame_runtime::Runtime::new(&graphs.join(&model),if model=="MAIN-1x256" {4} else {16}))
    };
    let executor = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let mut count=0;
    let mut service_total=0u128;
    let mut evidence_total=0u128;
    loop {
        let next=retry(&mut driver,FinitePublicConsumptionV1::next_group,FinitePublicConsumptionV1::wait_for_recording_capacity)?;
        let Some((_,cutoff))=next else {break;};
        let position=cutoff.receipt().group().group_position;
        if position>if prepare {418647} else {first+63} {break;}
        let now=cutoff.decision_time().logical_time_utc_ms();
        if prepare {
            if now<cfg.root_started_at {shared.action_before(&mut driver)?;continue;}
            if [201582,418584].contains(&position) {
                let old:serde_json::Value=serde_json::from_slice(&fs::read(old_checkpoints.join(format!("entry-{position}.json")))?)?;
                assert_eq!(old["ledger"],serde_json::to_value(shared.ledger().projection_v2())?);
                let actual=native.bench_business();
                for field in ["trajectory","account","started_at","reset","hidden","markets"] {assert_eq!(actual[field],old["native"][field],"{field}");}
                for (key,value) in actual["histories"].as_object().unwrap() {
                    let expected=&old["native"]["histories"][key];
                    for field in ["series","features"] {assert_eq!(value[field],expected[field],"{key} {field}");}
                    for (a,b) in value["sides"].as_array().unwrap().iter().zip(expected["sides"].as_array().unwrap()) {assert_eq!(a[0],b[0]);assert_eq!(a[1],b[1]);}
                }
                json(out.join(format!("warm-{position}.json")),&serde_json::json!({"position":position,"native":native.bench_warm_checkpoint(),"last_rows":last_rows,
                    "native_preparation_ns":native_preparation_ns,"native_preparation_groups":native_preparation_groups,
                    "preparation_elapsed_seconds":start.elapsed().as_secs_f64(),"preparation_cpu_ns":process_cpu_ns()-preparation_cpu,
                    "business_history_matches_previous_checkpoint":true,"historical_model_inference_rerun":false}))?;
                println!("warm checkpoint {position} complete after {:.3}s",start.elapsed().as_secs_f64());
            }
            let measured=(201582..201646).contains(&position)||(418584..418648).contains(&position);
            model_native_inputs::bench_profile::active(measured);
            shared_run::bench_profile::active(measured);
            let native_cpu=process_cpu_ns();
            let began=Instant::now();
            let group=native.consume(&mut shared,&mut driver).map_err(|e|format!("consume: {e:?}"))?;
            let native_ns=began.elapsed().as_nanos();
            native_preparation_ns+=native_ns;native_preparation_groups+=1;
            if let Some(group)=group {
                if (1789517070000..1789517100000).contains(&now)||(1789517370000..1789517400000).contains(&now) {
                    line(&mut warmup_measurements,&serde_json::json!({"position":position,"now":now,"native_ns":native_ns,
                        "native_cpu_ns":process_cpu_ns()-native_cpu,"rows":group.rows.iter().map(|r|(&r.hidden_state_key,r.book_levels.iter().map(|s|s.len()).collect::<Vec<_>>())).collect::<Vec<_>>(),
                        "historical_model_inference_rerun":false}))?;
                }
                if [201581,418583].contains(&position) {last_rows=serde_json::to_value(&group.rows)?;}
                if measured {
                    let mut components=model_native_inputs::bench_profile::take();components.extend(shared_run::bench_profile::take());
                    line(&mut measurements,&serde_json::json!({"position":position,"now":now,"native_ns":native_ns,"components":components,
                        "native_bits_sha256":format!("{:x}",Sha256::digest(group.bench_f32_bits().iter().flat_map(|b|b.to_le_bytes()).collect::<Vec<_>>())),
                        "rows":group.rows.len(),"depths":group.rows.iter().map(|r|r.book_levels.iter().map(|s|s.len()).collect::<Vec<_>>()).collect::<Vec<_>>()}))?;
                }
            }
            continue;
        }
        if position<first {shared.action_before(&mut driver)?;continue;}
        let runtime=runtime.as_mut().unwrap();
        if position==first {
            let old:serde_json::Value=serde_json::from_slice(&fs::read(old_checkpoints.join(format!("entry-{first}.json")))?)?;
            let warm:serde_json::Value=serde_json::from_slice(&fs::read(PathBuf::from(std::env::var("FRAME_WARM_CHECKPOINTS").unwrap()).join(format!("warm-{first}.json")))?)?;
            assert_eq!(old["ledger"],serde_json::to_value(shared.ledger().projection_v2())?);
            let restore=Instant::now();
            native.bench_restore_warm(&warm["native"]);
            let model=std::env::var("FRAME_MODEL").unwrap();
            runtime.restore(&old["runtimes"][if model=="MAIN-1x256" {0} else {1}]);
            runtime.restore_books(native.bench_previous_books(&warm["last_rows"]));
            line(&mut measurements,&serde_json::json!({"preparation_seconds":start.elapsed().as_secs_f64(),"preparation_cpu_ns":process_cpu_ns()-preparation_cpu,
                "graph_load_seconds":runtime.load_seconds,"graph_shape_warmup_seconds":runtime.warmup_seconds,"entry_position":position,
                "restored_prior_model_rows":runtime.frames,"cache_restore_seconds":restore.elapsed().as_secs_f64(),"native_cache":"restored from continuous real causal warmup",
                "ledger_equal":true,"native_threads":8,"ort_threads":if model=="MAIN-1x256" {4} else {16},"historical_model_inference_rerun":false}))?;
        }
        model_native_inputs::bench_profile::active(true);
        shared_run::bench_profile::active(true);
        let cpu_before=process_cpu_ns();let began=Instant::now();
        let group=Arc::new(native.consume(&mut shared,&mut driver).map_err(|e|format!("consume: {e:?}"))?.unwrap());
        let native_ns=began.elapsed().as_nanos();
        let mut components=model_native_inputs::bench_profile::take();components.extend(shared_run::bench_profile::take());
        model_native_inputs::bench_profile::active(false);shared_run::bench_profile::active(false);
        let (actors,timing,states)=runtime.advance(group.clone(),true);
        let mut actions=Vec::new();
        for actor in actors {
            let at=Instant::now();driver.wait_for_recording_capacity()?;
            let receipt=executor.block_on(actor.execute(&mut shared,&mut driver.decision().unwrap(),RequestContextV2 {
                global_request_id:format!("model-{position}"),caller_correlation_id:None,
            })).map_err(|e|format!("execute: {e:?}"))?;
            actions.push((actor,receipt,at.elapsed().as_nanos()));
        }
        let service_ns=began.elapsed().as_nanos();let cpu_ns=process_cpu_ns()-cpu_before;service_total+=service_ns;
        let evidence=Instant::now();
        let actions=actions.into_iter().map(|(actor,receipt,execute_ns)|serde_json::json!({"path":actor.path,"intent":format!("{:?}",actor.intent().unwrap()),"receipt":format!("{receipt:?}"),"execute_ns":execute_ns})).collect::<Vec<_>>();
        line(&mut measurements,&serde_json::json!({"position":position,"now":now,"service_ns":service_ns,"native_ns":native_ns,"cpu_ns":cpu_ns,"components":components,
            "native_group":&*group,"native_bits_sha256":format!("{:x}",Sha256::digest(group.bench_f32_bits().iter().flat_map(|b|b.to_le_bytes()).collect::<Vec<_>>())),
            "native_business":native.bench_business(),"cache":native.bench_cache(),"ledger":shared.ledger().projection_v2(),
            "runtimes":[{"timing":timing,"hidden_sha256":runtime.hidden_digest(),"z_bits_sha256":format!("{:x}",Sha256::digest(states.iter().flatten().flat_map(|v|v.to_bits().to_le_bytes()).collect::<Vec<_>>())),"actions":actions}]}))?;
        count+=1;evidence_total+=evidence.elapsed().as_nanos();
    }
    measurements.flush()?;warmup_measurements.flush()?;time_audit.borrow_mut().flush()?;
    let drain=Instant::now();driver.stop_and_drain()?;
    if !prepare {assert_eq!(count,64);}
    json(out.join("completion.json"),&serde_json::json!({"complete":true,"bounded_only":true,"variant":variant,"groups":count,"prepare":prepare,
        "elapsed_seconds":start.elapsed().as_secs_f64(),"drain_seconds":drain.elapsed().as_secs_f64(),"service_total_ns":service_total,"evidence_total_ns":evidence_total,
        "native_preparation_ns":native_preparation_ns,"native_preparation_groups":native_preparation_groups}))?;
    Ok(())
}
