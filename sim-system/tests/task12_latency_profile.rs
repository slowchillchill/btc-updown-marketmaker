use sim_system::latency_profile::{DelaySampleDomainV1, load_embedded_proxy_latency_profile};

#[test]
fn embedded_profile_recomputes_declared_statistics() {
    let profile = load_embedded_proxy_latency_profile().expect("正式配置档必须通过完整校验");
    let statistics = profile.statistics();

    assert_eq!(
        profile.profile_id(),
        "btc_updown_5m_collector_observed_delay_20260803_v1"
    );
    assert_eq!(profile.profile_version(), 1);
    assert_eq!(statistics.sample_count, 39_618_963);
    assert_eq!(statistics.histogram_bin_count, 9_666);
    assert_eq!(statistics.min_delay_ms, 6);
    assert_eq!(statistics.max_delay_ms, 29_697);
    assert_eq!(
        [
            statistics.p50_delay_ms,
            statistics.p85_delay_ms,
            statistics.p90_delay_ms,
            statistics.p95_delay_ms,
            statistics.p99_delay_ms,
        ],
        [10, 19, 30, 63, 187]
    );
}

#[test]
fn deterministic_samples_are_identity_stable_and_domain_separated() {
    let profile = load_embedded_proxy_latency_profile().expect("正式配置档必须通过完整校验");
    let seed = b"task12-fixed-session-seed";
    let request_id = "request-000002";

    let outbound = profile.sample_delay_ms(seed, request_id, DelaySampleDomainV1::RequestOutbound);
    let direct_inbound =
        profile.sample_delay_ms(seed, request_id, DelaySampleDomainV1::DirectResponseInbound);
    let lifecycle_inbound = profile.sample_delay_ms(
        seed,
        request_id,
        DelaySampleDomainV1::LifecycleFeedbackInbound,
    );

    assert_eq!(
        outbound,
        profile.sample_delay_ms(seed, request_id, DelaySampleDomainV1::RequestOutbound,)
    );
    assert_ne!(outbound, direct_inbound);
    assert_ne!(direct_inbound, lifecycle_inbound);
    assert_eq!([outbound, direct_inbound, lifecycle_inbound], [78, 9, 21]);
}
