use sim_system::latency_profile::{
    DelaySampleDomainV1, SessionDelaySamplerV2, load_embedded_proxy_latency_profile,
};
#[test]
fn fixed_streams_resume_without_request_identity_and_domains_do_not_consume_each_other() {
    let profile = load_embedded_proxy_latency_profile().unwrap();
    let mut sampler = SessionDelaySamplerV2::from_hex(&"8".repeat(64)).unwrap();
    let mut renamed = sampler.clone();
    let first = sampler.sample(&profile, DelaySampleDomainV1::RequestOutbound);
    assert_eq!(
        first,
        renamed.sample(&profile, DelaySampleDomainV1::RequestOutbound)
    );
    sampler.sample(&profile, DelaySampleDomainV1::LifecycleFeedbackInbound);
    assert_eq!(
        sampler.sample(&profile, DelaySampleDomainV1::RequestOutbound),
        renamed.sample(&profile, DelaySampleDomainV1::RequestOutbound)
    );
    let saved = serde_json::to_vec(&sampler).unwrap();
    let mut restored: SessionDelaySamplerV2 = serde_json::from_slice(&saved).unwrap();
    for domain in [
        DelaySampleDomainV1::RequestOutbound,
        DelaySampleDomainV1::DirectResponseInbound,
        DelaySampleDomainV1::LifecycleFeedbackInbound,
    ] {
        assert_eq!(
            sampler.sample(&profile, domain),
            restored.sample(&profile, domain)
        );
    }
    assert!(SessionDelaySamplerV2::from_hex("old arbitrary seed").is_err());
}
