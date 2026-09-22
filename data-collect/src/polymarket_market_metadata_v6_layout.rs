use std::sync::Arc;

use arrow_array::{
    Array as _, ArrayRef, BooleanArray, RecordBatch, StringArray, TimestampMillisecondArray,
    UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

use crate::market_anchor::{MarketAnchorIdentity, MarketAnchorObservation, MarketAnchorOrigin};
use crate::polymarket_market_metadata_layout::{
    MarketLifecycleState, MarketRuntimeRole, POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION,
    PolymarketMarketMetadataRow, PolymarketMarketMetadataV5Row,
    polymarket_market_metadata_v5_schema,
};

pub const POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION: &str = "polymarket_market_metadata_v6";
pub const MARKET_ANCHOR_RULE_UPDATE: &str = "market_anchor_rule_update";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketAnchorRuleUpdateRow {
    pub timestamp_received: i64,
    #[serde(skip)]
    pub collector_admission_sequence: u64,
    #[serde(skip)]
    pub collector_admission_time_utc_ms: i64,
    pub metadata_origin: MarketAnchorOrigin,
    pub collector_host_id: String,
    pub collector_region: String,
    #[serde(flatten)]
    pub identity: MarketAnchorIdentity,
    pub market_lifecycle_state: MarketLifecycleState,
    pub market_runtime_role: MarketRuntimeRole,
    pub metadata_fetch_start_utc: i64,
    pub anchor_query_source_epoch_id: String,
    pub anchor_query_ordinal: u64,
    #[serde(flatten)]
    pub observation: MarketAnchorObservation,
}

/// 旧行继续由 V5 类型表达，新增行不能被解释为身份快照或终态。
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolymarketMarketMetadataV6Row {
    Existing(PolymarketMarketMetadataV5Row),
    AnchorRule(MarketAnchorRuleUpdateRow),
}

impl PolymarketMarketMetadataV6Row {
    pub const fn schema_version(&self) -> &'static str {
        POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION
    }

    pub fn existing(&self) -> Option<&PolymarketMarketMetadataV5Row> {
        match self {
            Self::Existing(row) => Some(row),
            Self::AnchorRule(_) => None,
        }
    }

    pub fn timestamp_received(&self) -> i64 {
        match self {
            Self::Existing(row) => row.common().timestamp_received,
            Self::AnchorRule(row) => row.timestamp_received,
        }
    }

    pub fn collector_admission_sequence(&self) -> u64 {
        match self {
            Self::Existing(row) => row.common().collector_admission_sequence,
            Self::AnchorRule(row) => row.collector_admission_sequence,
        }
    }

    pub fn collector_admission_time_utc_ms(&self) -> i64 {
        match self {
            Self::Existing(row) => row.common().collector_admission_time_utc_ms,
            Self::AnchorRule(row) => row.collector_admission_time_utc_ms,
        }
    }

    pub fn validate(&self) -> Result<(), String> {
        self.validate_stage(true)
    }

    pub(crate) fn validate_draft(&self) -> Result<(), String> {
        self.validate_stage(false)
    }

    fn validate_stage(&self, admitted: bool) -> Result<(), String> {
        match self {
            Self::Existing(row) => {
                let report = if admitted {
                    row.validate()
                } else {
                    row.validate_draft()
                };
                if report.is_complete() {
                    Ok(())
                } else {
                    Err(format!("{:?}", report.violations))
                }
            }
            Self::AnchorRule(row) => {
                if admitted
                    && (row.collector_admission_sequence == 0
                        || row.collector_admission_time_utc_ms == 0)
                {
                    return Err("collector admission is required".to_owned());
                }
                let observation = &row.observation;
                if let Some(received) = observation.http_received_at_utc {
                    if received != row.timestamp_received {
                        return Err("HTTP receive time contradicts anchor row time".to_owned());
                    }
                    let body = match (
                        observation.http_body_encoding.as_deref(),
                        observation.http_body.as_deref(),
                    ) {
                        (Some("utf8"), Some(body)) => body.as_bytes().to_vec(),
                        (Some("base64"), Some(body)) => base64::engine::general_purpose::STANDARD
                            .decode(body)
                            .map_err(|error| error.to_string())?,
                        _ => return Err("complete HTTP response is missing its body".to_owned()),
                    };
                    if observation.http_body_sha256.as_deref()
                        != Some(format!("{:x}", Sha256::digest(&body)).as_str())
                    {
                        return Err("HTTP body digest mismatch".to_owned());
                    }
                }
                Ok(())
            }
        }
    }

    pub fn to_raw_value(&self) -> Result<Value, serde_json::Error> {
        let mut value = match self {
            Self::Existing(row) => {
                let mut value = serde_json::to_value(row.common())?;
                let object = value.as_object_mut().expect("metadata struct is an object");
                object.remove("resolution_evidence_raw_sha256");
                object.insert(
                    "ws_claimant_source_sequence_epoch_id".to_owned(),
                    serde_json::to_value(&row.ws_claimant_source_sequence_epoch_id)?,
                );
                object.insert(
                    "ws_claimant_collector_admission_sequence".to_owned(),
                    serde_json::to_value(row.ws_claimant_collector_admission_sequence)?,
                );
                value
            }
            Self::AnchorRule(row) => {
                let mut value = serde_json::to_value(row)?;
                let object = value.as_object_mut().expect("anchor struct is an object");
                object.insert(
                    "metadata_row_kind".to_owned(),
                    Value::String(MARKET_ANCHOR_RULE_UPDATE.to_owned()),
                );
                object.insert(
                    "source_endpoint".to_owned(),
                    serde_json::to_value(&row.observation.http_request_url)?,
                );
                value
            }
        };
        let object = value.as_object_mut().expect("metadata struct is an object");
        object.insert(
            "schema_version".to_owned(),
            Value::String(POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION.to_owned()),
        );
        for field in polymarket_market_metadata_v6_schema().fields() {
            if !matches!(
                field.name().as_str(),
                "collector_admission_sequence" | "collector_admission_time_utc_ms"
            ) {
                object.entry(field.name().clone()).or_insert(Value::Null);
            }
        }
        Ok(value)
    }

    pub fn from_raw_value(
        mut value: Value,
        sequence: u64,
        admission_time_utc_ms: i64,
    ) -> Result<Self, String> {
        let object = value
            .as_object_mut()
            .ok_or("metadata v6 row is not an object")?;
        if object.get("schema_version").and_then(Value::as_str)
            != Some(POLYMARKET_MARKET_METADATA_V6_SCHEMA_VERSION)
        {
            return Err("metadata v6 row has another schema version".to_owned());
        }
        if object.contains_key("resolution_evidence_raw_sha256") {
            return Err("metadata v6 contains removed resolution_evidence_raw_sha256".to_owned());
        }
        if object.get("metadata_row_kind").and_then(Value::as_str)
            == Some(MARKET_ANCHOR_RULE_UPDATE)
        {
            for field in polymarket_market_metadata_v5_schema().fields() {
                if !ANCHOR_COMMON_FIELDS.contains(&field.name().as_str())
                    && object
                        .get(field.name())
                        .is_some_and(|value| !value.is_null())
                {
                    return Err(format!("anchor row cannot carry {}", field.name()));
                }
            }
            let mut row: MarketAnchorRuleUpdateRow =
                serde_json::from_value(value).map_err(|error| error.to_string())?;
            row.collector_admission_sequence = sequence;
            row.collector_admission_time_utc_ms = admission_time_utc_ms;
            Ok(Self::AnchorRule(row))
        } else {
            for field in anchor_fields() {
                if object
                    .remove(field.name())
                    .is_some_and(|value| !value.is_null())
                {
                    return Err(format!(
                        "existing metadata row cannot carry {}",
                        field.name()
                    ));
                }
            }
            let epoch = serde_json::from_value(
                object
                    .remove("ws_claimant_source_sequence_epoch_id")
                    .unwrap_or(Value::Null),
            )
            .map_err(|error| error.to_string())?;
            let claimant = serde_json::from_value(
                object
                    .remove("ws_claimant_collector_admission_sequence")
                    .unwrap_or(Value::Null),
            )
            .map_err(|error| error.to_string())?;
            object.insert(
                "schema_version".to_owned(),
                Value::String(POLYMARKET_MARKET_METADATA_V5_SCHEMA_VERSION.to_owned()),
            );
            let mut common: PolymarketMarketMetadataRow =
                serde_json::from_value(value).map_err(|error| error.to_string())?;
            common.collector_admission_sequence = sequence;
            common.collector_admission_time_utc_ms = admission_time_utc_ms;
            Ok(Self::Existing(PolymarketMarketMetadataV5Row::from_common(
                common, epoch, claimant,
            )))
        }
    }
}

const ANCHOR_COMMON_FIELDS: &[&str] = &[
    "metadata_row_kind",
    "timestamp_received",
    "collector_admission_sequence",
    "collector_admission_time_utc_ms",
    "metadata_origin",
    "collector_host_id",
    "collector_region",
    "schema_version",
    "slug",
    "condition_id",
    "clob_market_id",
    "asset_ids",
    "market_start_utc",
    "market_end_utc",
    "fixed_identity_verified_at_utc",
    "market_lifecycle_state",
    "market_runtime_role",
    "source_endpoint",
    "metadata_fetch_start_utc",
];

fn timestamp_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        true,
    )
}

fn anchor_fields() -> Vec<Field> {
    let mut fields = [
        "anchor_query_source_epoch_id",
        "anchor_query_result",
        "anchor_event_id",
        "price_to_beat_decimal",
        "price_to_beat_source_field",
        "crypto_market_config_id",
        "crypto_market_config_nested_id",
        "crypto_market_config_asset",
        "crypto_market_config_duration",
        "resolution_source",
        "http_request_url",
        "http_response_url",
        "http_body_encoding",
        "http_body",
        "http_body_sha256",
    ]
    .into_iter()
    .map(|name| Field::new(name, DataType::Utf8, true))
    .collect::<Vec<_>>();
    fields.extend([
        Field::new("anchor_query_ordinal", DataType::UInt64, true),
        Field::new("twap_enabled", DataType::Boolean, true),
        Field::new("twap_lookback_seconds", DataType::UInt32, true),
        Field::new("http_status_code", DataType::UInt16, true),
        timestamp_field("anchor_source_computed_at_utc"),
        timestamp_field("http_received_at_utc"),
    ]);
    fields
}

pub fn polymarket_market_metadata_v6_schema() -> Arc<Schema> {
    let mut fields = polymarket_market_metadata_v5_schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.extend(anchor_fields());
    Arc::new(Schema::new(fields))
}

pub fn polymarket_market_metadata_v6_rows_to_batch(
    schema: Arc<Schema>,
    rows: &[PolymarketMarketMetadataV6Row],
) -> Result<RecordBatch, String> {
    let values = rows
        .iter()
        .map(|row| {
            let mut value = row.to_raw_value().map_err(|error| error.to_string())?;
            value["collector_admission_sequence"] = row.collector_admission_sequence().into();
            value["collector_admission_time_utc_ms"] = row.collector_admission_time_utc_ms().into();
            Ok(value)
        })
        .collect::<Result<Vec<_>, String>>()?;
    let arrays = schema
        .fields()
        .iter()
        .map(|field| {
            let cells = values
                .iter()
                .map(|value| &value[field.name()])
                .collect::<Vec<_>>();
            let array: ArrayRef = match field.data_type() {
                DataType::Utf8 => Arc::new(StringArray::from(
                    cells.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
                )),
                DataType::Boolean => Arc::new(BooleanArray::from(
                    cells
                        .iter()
                        .map(|value| value.as_bool())
                        .collect::<Vec<_>>(),
                )),
                DataType::UInt8 => Arc::new(UInt8Array::from(
                    cells
                        .iter()
                        .map(|value| value.as_u64().map(|value| value as u8))
                        .collect::<Vec<_>>(),
                )),
                DataType::UInt16 => Arc::new(UInt16Array::from(
                    cells
                        .iter()
                        .map(|value| value.as_u64().map(|value| value as u16))
                        .collect::<Vec<_>>(),
                )),
                DataType::UInt32 => Arc::new(UInt32Array::from(
                    cells
                        .iter()
                        .map(|value| value.as_u64().map(|value| value as u32))
                        .collect::<Vec<_>>(),
                )),
                DataType::UInt64 => Arc::new(UInt64Array::from(
                    cells.iter().map(|value| value.as_u64()).collect::<Vec<_>>(),
                )),
                DataType::Timestamp(TimeUnit::Millisecond, _) => Arc::new(
                    TimestampMillisecondArray::from(
                        cells.iter().map(|value| value.as_i64()).collect::<Vec<_>>(),
                    )
                    .with_timezone("UTC"),
                ),
                datatype => return Err(format!("unsupported metadata v6 datatype {datatype}")),
            };
            Ok(array)
        })
        .collect::<Result<Vec<_>, String>>()?;
    RecordBatch::try_new(schema, arrays).map_err(|error| error.to_string())
}

pub fn polymarket_market_metadata_v6_row_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<PolymarketMarketMetadataV6Row, String> {
    let mut object = serde_json::Map::new();
    for (field, array) in batch.schema().fields().iter().zip(batch.columns()) {
        if matches!(
            field.name().as_str(),
            "terminal_predicate_passed" | "closed_time_check_passed"
        ) {
            continue;
        }
        let value = if array.is_null(row) {
            Value::Null
        } else {
            macro_rules! scalar {
                ($type:ty) => {
                    Value::from(
                        array
                            .as_any()
                            .downcast_ref::<$type>()
                            .ok_or_else(|| {
                                format!("metadata array type mismatch: {}", field.name())
                            })?
                            .value(row),
                    )
                };
            }
            match field.data_type() {
                DataType::Utf8 => scalar!(StringArray),
                DataType::Boolean => scalar!(BooleanArray),
                DataType::UInt8 => scalar!(UInt8Array),
                DataType::UInt16 => scalar!(UInt16Array),
                DataType::UInt32 => scalar!(UInt32Array),
                DataType::UInt64 => scalar!(UInt64Array),
                DataType::Timestamp(TimeUnit::Millisecond, _) => scalar!(TimestampMillisecondArray),
                datatype => return Err(format!("unsupported metadata v6 datatype {datatype}")),
            }
        };
        object.insert(field.name().clone(), value);
    }
    let sequence = object
        .get("collector_admission_sequence")
        .and_then(Value::as_u64)
        .ok_or("metadata admission sequence is missing")?;
    let time = object
        .get("collector_admission_time_utc_ms")
        .and_then(Value::as_i64)
        .ok_or("metadata admission time is missing")?;
    let result =
        PolymarketMarketMetadataV6Row::from_raw_value(Value::Object(object), sequence, time)?;
    result.validate()?;
    Ok(result)
}
