    let window: serde_json::Value =
        serde_json::from_slice(&fs::read(std::env::var("FRAME_WINDOW")?)?)?;
    let last = window["last_group"].as_u64().unwrap();
    let mut groups = 0;
    loop {
        let next = retry(
            &mut driver,
            FinitePublicConsumptionV1::next_group,
            FinitePublicConsumptionV1::wait_for_recording_capacity,
        )?;
        let Some((_, cutoff)) = next else {
            return Err("原录制提前结束".into());
        };
        let position = cutoff.receipt().group().group_position;
        shared.action_before(&mut driver)?;
        groups += 1;
        if position == last {
            break;
        }
    }
    driver.stop_and_drain()?;
    json(
        out.join("completion.json"),
        &serde_json::json!({"complete":true,"groups":groups,"ledger":shared.ledger().projection_v2(),"protocol_validation_only":true}),
    )?;
    Ok(())
}
