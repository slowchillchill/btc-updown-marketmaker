use serde::{Deserialize, Serialize};

use crate::scenario::PlatformMarketV1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketEnrollmentRequestV1 {
    pub enrollment_request_id: String,
    pub market: PlatformMarketV1,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublicVisibilityCursorV1 {
    closed_through_received_at_utc_ms: Option<i64>,
    next_visibility_index_position: u64,
}

impl PublicVisibilityCursorV1 {
    pub fn closed_through_received_at_utc_ms(&self) -> Option<i64> {
        self.closed_through_received_at_utc_ms
    }

    pub fn next_visibility_index_position(&self) -> u64 {
        self.next_visibility_index_position
    }
}
