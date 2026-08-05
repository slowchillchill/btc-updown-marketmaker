use contracts::{
    CanonicalEventId, EVENT_TIME_INDEX_SCHEMA_V2, EventTimeIndexEntryV2,
    VISIBILITY_INDEX_SCHEMA_V2, VisibilityIndexEntryV2,
};
use data_pipeline_core::{validate_event_time_index_entry, validate_visibility_index_entry};

fn event_id(character: char) -> CanonicalEventId {
    CanonicalEventId(character.to_string().repeat(64))
}

fn event_time_entry(source_event_at_utc_ms: i64, identity: char) -> EventTimeIndexEntryV2 {
    EventTimeIndexEntryV2 {
        schema_version: EVENT_TIME_INDEX_SCHEMA_V2.to_owned(),
        event_id: event_id(identity),
        source_event_at_utc_ms,
    }
}

fn visibility_entry(received_at_utc_ms: i64, identity: char) -> VisibilityIndexEntryV2 {
    VisibilityIndexEntryV2 {
        schema_version: VISIBILITY_INDEX_SCHEMA_V2.to_owned(),
        event_id: event_id(identity),
        received_at_utc_ms,
    }
}

#[test]
fn source_event_order_and_model_visibility_order_are_independent() {
    let event_a_source = event_time_entry(1_000, 'a');
    let event_b_source = event_time_entry(1_100, 'b');
    let event_a_visibility = visibility_entry(1_300, 'a');
    let event_b_visibility = visibility_entry(1_200, 'b');

    validate_event_time_index_entry(&event_a_source).unwrap();
    validate_event_time_index_entry(&event_b_source).unwrap();
    validate_visibility_index_entry(&event_a_visibility).unwrap();
    validate_visibility_index_entry(&event_b_visibility).unwrap();

    assert!(event_a_source.source_event_at_utc_ms < event_b_source.source_event_at_utc_ms);
    assert!(event_b_visibility.received_at_utc_ms < event_a_visibility.received_at_utc_ms);
    assert_eq!(event_a_source.event_id, event_a_visibility.event_id);
    assert_eq!(event_b_source.event_id, event_b_visibility.event_id);
}
