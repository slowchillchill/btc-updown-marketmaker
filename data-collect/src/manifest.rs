use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceCoverage {
    pub source_kind: String,
    pub first_recv_time_utc_ms: i64,
    pub last_recv_time_utc_ms: i64,
    pub rows: u64,
}
