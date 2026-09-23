use sim_system::latency_profile::{DelaySampleDomainV1, load_embedded_proxy_latency_profile};

#[test]
fn deterministic_samples_are_identity_stable_and_domain_separated() {
    let profile = load_embedded_proxy_latency_profile().expect("解析内嵌配置档");
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
