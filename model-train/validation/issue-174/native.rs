    let first: u64 = std::env::var("FRAME_FIRST").unwrap().parse().unwrap();
    let checkpoint = PathBuf::from(std::env::var("FRAME_CHECKPOINTS").unwrap()).join(format!("entry-{first}.json"));
    let mut baseline = [native.baseline(), native.baseline()];
    let mut candidate = [native.clone(), native.clone()];
    let mut cleared = native.clone();
    let mut packed: [model_native_inputs::PackedBooks; 2] = std::array::from_fn(|_|Default::default());
    let mut measurements = BufWriter::new(File::create(out.join("measurements.jsonl"))?);
    let start = Instant::now();
    let preparation_cpu = process_cpu_ns();
    let mut count = 0;
    loop {
        let next = retry(&mut driver, FinitePublicConsumptionV1::next_group, FinitePublicConsumptionV1::wait_for_recording_capacity)?;
        let Some((_, cutoff)) = next else {break;};
        let position = cutoff.receipt().group().group_position;
        if position > first + 63 {break;}
        if position < first {shared.action_before(&mut driver)?;continue;}
        if position == first {
            let loaded: serde_json::Value = serde_json::from_slice(&fs::read(&checkpoint)?)?;
            assert_eq!(loaded["position"],position);
            assert_eq!(loaded["ledger"],serde_json::to_value(shared.ledger().projection_v2())?);
            for root in &mut baseline {root.restore_checkpoint(&loaded["native"]);}
            for root in &mut candidate {root.restore_checkpoint(&loaded["native"]);}
            cleared.restore_checkpoint(&loaded["native"]);
            assert_eq!(candidate[0].bench_business(),baseline[0].bench_business());
            line(&mut measurements,&serde_json::json!({"preparation_seconds":start.elapsed().as_secs_f64(),"preparation_cpu_ns":process_cpu_ns()-preparation_cpu,
                "entry_position":position,"new_input_cache":"empty; first consume timed","ledger_equal":true,"native_threads":rayon::current_num_threads()}))?;
        }
        let before=shared.bench_snapshot();
        let mut reference=None;
        let mut reference_bits=None;
        let mut reference_business=None;
        let mut reference_ledger=None;
        let mut reference_pack=None;
        for (round,variant) in [(0,"baseline"),(0,"candidate"),(1,"candidate"),(1,"baseline")] {
            shared.bench_restore(&before);
            let cpu = process_cpu_ns();
            let began = Instant::now();
            let (wall,cpu,packing_ns,packing_cpu,group,bits,business,cache,depth,values,mask) = if variant == "baseline" {
                let group = baseline[round].consume(&mut shared,&mut driver).map_err(|e|format!("baseline: {e:?}"))?.unwrap();
                let wall=began.elapsed().as_nanos();let cpu=process_cpu_ns()-cpu;
                let pack_cpu=process_cpu_ns();let at=Instant::now();
                let (depth,values,mask)=model_native_inputs::market_baseline::pad_books(group.rows.iter().map(|r|&r.book_levels));
                let packing_ns=at.elapsed().as_nanos();let packing_cpu=process_cpu_ns()-pack_cpu;
                let bits=group.bench_f32_bits();
                (wall,cpu,packing_ns,packing_cpu,serde_json::to_value(&group)?,bits,baseline[round].bench_business(),baseline[round].bench_cache(),depth,values,mask)
            } else {
                let group=candidate[round].consume(&mut shared,&mut driver).map_err(|e|format!("candidate: {e:?}"))?.unwrap();
                let wall=began.elapsed().as_nanos();let cpu=process_cpu_ns()-cpu;
                let pack_cpu=process_cpu_ns();let at=Instant::now();
                packed[round].update(group.rows.iter().map(|r|(&r.hidden_state_key,&r.book_levels)));
                let packing_ns=at.elapsed().as_nanos();let packing_cpu=process_cpu_ns()-pack_cpu;
                let bits=group.bench_f32_bits();
                (wall,cpu,packing_ns,packing_cpu,serde_json::to_value(&group)?,bits,candidate[round].bench_business(),candidate[round].bench_cache(),packed[round].depth(),packed[round].values().to_vec(),packed[round].mask().to_vec())
            };
            let ledger=serde_json::to_value(shared.ledger().projection_v2())?;
            let pack=(depth,values.into_iter().map(f32::to_bits).collect::<Vec<_>>(),mask);
            if let Some(expected)=&reference {
                assert_eq!(&group,expected,"group at {position} {variant} round {round}");
                assert_eq!(&bits,reference_bits.as_ref().unwrap(),"bits at {position} {variant} round {round}");
                assert_eq!(&business,reference_business.as_ref().unwrap(),"business at {position} {variant} round {round}");
                assert_eq!(&ledger,reference_ledger.as_ref().unwrap());
                assert_eq!(&pack,reference_pack.as_ref().unwrap());
            } else {
                reference=Some(group.clone());reference_bits=Some(bits.clone());reference_business=Some(business.clone());reference_ledger=Some(ledger.clone());reference_pack=Some(pack);
            }
            line(&mut measurements,&serde_json::json!({"position":position,"now":cutoff.decision_time().logical_time_utc_ms(),"round":round+1,"variant":variant,
                "native_ns":wall,"native_cpu_ns":cpu,"packing_ns":packing_ns,"packing_cpu_ns":packing_cpu,"cold_entry":position==first,
                "native_group":group,"native_business":business,"cache":cache,"ledger":ledger,
                "native_bits_sha256":format!("{:x}",Sha256::digest(bits.iter().flat_map(|b|b.to_le_bytes()).collect::<Vec<_>>())),
                "complete_group_bits_business_packing_equal":true}))?;
        }
        // 清除的只是新增可重建缓存；前帧统计、历史、账户和真实前沿继续推进。
        shared.bench_restore(&before);
        cleared.clear_added_inputs();
        let group=cleared.consume(&mut shared,&mut driver).map_err(|e|format!("cleared: {e:?}"))?.unwrap();
        assert_eq!(serde_json::to_value(&group)?,reference.unwrap());
        assert_eq!(group.bench_f32_bits(),reference_bits.unwrap());
        assert_eq!(cleared.bench_business(),reference_business.unwrap());
        assert_eq!(serde_json::to_value(shared.ledger().projection_v2())?,reference_ledger.unwrap());
        count+=1;
    }
    measurements.flush()?;
    time_audit.borrow_mut().flush()?;
    let loop_elapsed=start.elapsed().as_secs_f64();
    let drain=Instant::now();driver.stop_and_drain()?;
    assert_eq!(count,64);
    json(out.join("completion.json"),&serde_json::json!({"complete":true,"groups":count,"timed_calls":count*4,"cache_cleared_comparisons":count,
        "loop_elapsed_seconds":loop_elapsed,"drain_seconds":drain.elapsed().as_secs_f64(),"elapsed_seconds":start.elapsed().as_secs_f64()}))?;
    Ok(())
}
