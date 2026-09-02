use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SourceKind {
    PolymarketMarket,
    PolymarketMarketMetadata,
    BinanceDepthUpdate,
    BinanceDepthSnapshot,
    BinanceAggTrade,
    PolymarketRtdsChainlinkBtcUsd,
}

impl SourceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PolymarketMarket => "polymarket_market",
            Self::PolymarketMarketMetadata => "polymarket_market_metadata",
            Self::BinanceDepthUpdate => "binance_depth_update",
            Self::BinanceDepthSnapshot => "binance_depth_snapshot",
            Self::BinanceAggTrade => "binance_aggtrade",
            Self::PolymarketRtdsChainlinkBtcUsd => "polymarket_rtds_chainlink_btc_usd",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceTimestamp {
    Timestamp {
        value: String,
    },
    NoSourceTimestamp {
        reason: String,
        snapshot_boundary: String,
    },
}

impl SourceTimestamp {
    pub fn value(&self) -> Option<&str> {
        match self {
            Self::Timestamp { value } => Some(value),
            Self::NoSourceTimestamp { .. } => None,
        }
    }

    fn has_contract_evidence(&self) -> bool {
        match self {
            Self::Timestamp { value } => !value.trim().is_empty(),
            Self::NoSourceTimestamp {
                reason,
                snapshot_boundary,
            } => !reason.trim().is_empty() && !snapshot_boundary.trim().is_empty(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostClockSyncStatus {
    Synchronized { source: String },
    Unavailable { reason: String },
}

impl HostClockSyncStatus {
    fn has_contract_evidence(&self) -> bool {
        match self {
            Self::Synchronized { source } => !source.trim().is_empty(),
            Self::Unavailable { reason } => !reason.trim().is_empty(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RawEvidence {
    Json(String),
    ExternalReference { uri: String, sha256: String },
}

impl RawEvidence {
    pub fn raw_json(&self) -> Option<&str> {
        match self {
            Self::Json(raw_json) => Some(raw_json),
            Self::ExternalReference { .. } => None,
        }
    }

    fn has_contract_evidence(&self) -> bool {
        match self {
            Self::Json(raw_json) => serde_json::from_str::<Value>(raw_json).is_ok(),
            Self::ExternalReference { uri, sha256 } => {
                !uri.trim().is_empty() && !sha256.trim().is_empty()
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventEnvelope {
    pub source_kind: SourceKind,
    pub source_event_type: String,
    pub source_identifier: String,
    pub source_timestamp: SourceTimestamp,
    pub recv_time_utc_ms: Option<i64>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub host_clock_sync_status: HostClockSyncStatus,
    pub raw_evidence: RawEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvelopeValidationError {
    pub missing_fields: Vec<String>,
}

impl EventEnvelope {
    pub fn validate(&self) -> Result<(), EnvelopeValidationError> {
        let mut missing_fields = Vec::new();

        if self.source_event_type.trim().is_empty() {
            missing_fields.push("source_event_type".to_owned());
        }
        if self.source_identifier.trim().is_empty() {
            missing_fields.push("source_identifier".to_owned());
        }
        if !self.source_timestamp.has_contract_evidence() {
            missing_fields.push("source_timestamp_evidence".to_owned());
        }
        if self.recv_time_utc_ms.is_none_or(|value| value <= 0) {
            missing_fields.push("recv_time_utc".to_owned());
        }
        if self.collector_host_id.trim().is_empty() {
            missing_fields.push("collector_host_id".to_owned());
        }
        if self.collector_region.trim().is_empty() {
            missing_fields.push("collector_region".to_owned());
        }
        if self.source_endpoint.trim().is_empty() {
            missing_fields.push("source_endpoint".to_owned());
        }
        if !self.host_clock_sync_status.has_contract_evidence() {
            missing_fields.push("host_clock_sync_status".to_owned());
        }
        if !self.raw_evidence.has_contract_evidence() {
            match self.raw_evidence {
                RawEvidence::Json(_) => missing_fields.push("parseable_raw_json".to_owned()),
                RawEvidence::ExternalReference { .. } => {
                    missing_fields.push("raw_evidence_reference".to_owned());
                }
            }
        }
        if missing_fields.is_empty() {
            Ok(())
        } else {
            Err(EnvelopeValidationError { missing_fields })
        }
    }
}
