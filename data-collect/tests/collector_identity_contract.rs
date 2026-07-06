use data_collect::collector_identity::resolve_collector_region_from_values;

#[test]
fn collector_region_prefers_explicit_env_value() {
    let region = resolve_collector_region_from_values(Some(" eu-west-1 "), Some("us-east-1"));

    assert_eq!(region, "eu-west-1");
}

#[test]
fn collector_region_uses_imds_when_env_is_missing() {
    let region = resolve_collector_region_from_values(None, Some(" eu-west-1\n"));

    assert_eq!(region, "eu-west-1");
}

#[test]
fn collector_region_uses_imds_when_env_is_blank() {
    let region = resolve_collector_region_from_values(Some("  \n"), Some(" eu-west-1 "));

    assert_eq!(region, "eu-west-1");
}

#[test]
fn collector_region_is_unknown_only_when_no_source_is_available() {
    let region = resolve_collector_region_from_values(Some("  "), None);

    assert_eq!(region, "unknown");
}
