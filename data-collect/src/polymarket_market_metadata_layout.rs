use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataContext {
    pub timestamp_received_ms: i64,
    pub metadata_fetch_start_utc_ms: Option<i64>,
    pub metadata_recv_time_utc_ms: Option<i64>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataValidation {
    pub missing_fields: Vec<String>,
    pub source_gaps: Vec<String>,
}

impl PolymarketMarketMetadataValidation {
    pub fn is_complete(&self) -> bool {
        self.missing_fields.is_empty() && self.source_gaps.is_empty()
    }

    fn require_non_empty(&mut self, field: &str, value: Option<&str>) {
        if value.is_none_or(|value| value.trim().is_empty()) {
            self.missing_fields.push(field.to_owned());
        }
    }

    fn require_some<T>(&mut self, field: &str, value: Option<T>) {
        if value.is_none() {
            self.missing_fields.push(field.to_owned());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketMetadataRow {
    pub timestamp_received: i64,
    pub metadata_fetch_start_utc: Option<i64>,
    pub metadata_recv_time_utc: Option<i64>,
    pub slug: String,
    pub condition_id: String,
    pub clob_market_id: Option<String>,
    pub asset_ids: Option<String>,
    pub token_ids: Option<String>,
    pub market_start_utc: Option<i64>,
    pub market_end_utc: Option<i64>,
    pub active: Option<bool>,
    pub accepting_orders: Option<bool>,
    pub market_lifecycle_state: String,
    pub tick_size: Option<String>,
    pub min_size: Option<String>,
    pub fee_schedule_raw: Option<String>,
    pub reward_eligibility_raw: Option<String>,
    pub metadata_source: String,
    pub same_condition_market_verified: Option<bool>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

impl PolymarketMarketMetadataRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new_market(
        slug: &str,
        condition_id: &str,
        clob_market_id: Option<&str>,
        asset_ids: &[String],
        market_start_utc: Option<i64>,
        market_end_utc: Option<i64>,
        active: Option<bool>,
        accepting_orders: Option<bool>,
        market_lifecycle_state: &str,
        tick_size: Option<&str>,
        min_size: Option<&str>,
        fee_schedule_raw: Option<&str>,
        reward_eligibility_raw: Option<&str>,
        same_condition_market_verified: Option<bool>,
        context: PolymarketMarketMetadataContext,
    ) -> Self {
        metadata_row(
            "new_market",
            slug,
            condition_id,
            clob_market_id,
            asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            tick_size,
            min_size,
            fee_schedule_raw,
            reward_eligibility_raw,
            same_condition_market_verified,
            context,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn active_scan(
        slug: &str,
        condition_id: &str,
        clob_market_id: Option<&str>,
        asset_ids: &[String],
        market_start_utc: Option<i64>,
        market_end_utc: Option<i64>,
        active: Option<bool>,
        accepting_orders: Option<bool>,
        market_lifecycle_state: &str,
        tick_size: Option<&str>,
        min_size: Option<&str>,
        fee_schedule_raw: Option<&str>,
        reward_eligibility_raw: Option<&str>,
        same_condition_market_verified: Option<bool>,
        context: PolymarketMarketMetadataContext,
    ) -> Self {
        metadata_row(
            "active_scan",
            slug,
            condition_id,
            clob_market_id,
            asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            tick_size,
            min_size,
            fee_schedule_raw,
            reward_eligibility_raw,
            same_condition_market_verified,
            context,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn fallback_close(
        slug: &str,
        condition_id: &str,
        clob_market_id: Option<&str>,
        asset_ids: &[String],
        market_start_utc: Option<i64>,
        market_end_utc: Option<i64>,
        active: Option<bool>,
        accepting_orders: Option<bool>,
        market_lifecycle_state: &str,
        tick_size: Option<&str>,
        min_size: Option<&str>,
        fee_schedule_raw: Option<&str>,
        reward_eligibility_raw: Option<&str>,
        same_condition_market_verified: Option<bool>,
        context: PolymarketMarketMetadataContext,
    ) -> Self {
        metadata_row(
            "fallback_close",
            slug,
            condition_id,
            clob_market_id,
            asset_ids,
            market_start_utc,
            market_end_utc,
            active,
            accepting_orders,
            market_lifecycle_state,
            tick_size,
            min_size,
            fee_schedule_raw,
            reward_eligibility_raw,
            same_condition_market_verified,
            context,
        )
    }

    pub fn candidate_slug_only(
        slug: &str,
        market_start_utc: Option<i64>,
        market_end_utc: Option<i64>,
        context: PolymarketMarketMetadataContext,
    ) -> Self {
        metadata_row(
            "candidate_slug",
            slug,
            "",
            None,
            &[],
            market_start_utc,
            market_end_utc,
            None,
            None,
            "candidate",
            None,
            None,
            None,
            None,
            None,
            context,
        )
    }

    pub fn validate(&self) -> PolymarketMarketMetadataValidation {
        let mut report = PolymarketMarketMetadataValidation {
            missing_fields: Vec::new(),
            source_gaps: Vec::new(),
        };
        if self.timestamp_received <= 0 {
            report.missing_fields.push("timestamp_received".to_owned());
        }
        report.require_non_empty("slug", Some(&self.slug));
        report.require_non_empty("condition_id", Some(&self.condition_id));
        report.require_non_empty("asset_ids", self.asset_ids.as_deref());
        report.require_non_empty("token_ids", self.token_ids.as_deref());
        report.require_some("market_start_utc", self.market_start_utc);
        report.require_some("market_end_utc", self.market_end_utc);
        report.require_some("active", self.active);
        report.require_some("accepting_orders", self.accepting_orders);
        report.require_non_empty("market_lifecycle_state", Some(&self.market_lifecycle_state));
        report.require_non_empty("tick_size", self.tick_size.as_deref());
        report.require_non_empty("min_size", self.min_size.as_deref());
        report.require_non_empty("fee_schedule_raw", self.fee_schedule_raw.as_deref());
        report.require_non_empty(
            "reward_eligibility_raw",
            self.reward_eligibility_raw.as_deref(),
        );
        report.require_non_empty("metadata_source", Some(&self.metadata_source));
        report.require_some(
            "same_condition_market_verified",
            self.same_condition_market_verified,
        );
        report.require_non_empty("collector_host_id", Some(&self.collector_host_id));
        report.require_non_empty("collector_region", Some(&self.collector_region));
        report.require_non_empty("source_endpoint", Some(&self.source_endpoint));
        report.require_non_empty("schema_version", Some(&self.schema_version));

        if self.metadata_source == "candidate_slug" {
            report.source_gaps.push("slug_only_discovery".to_owned());
        }
        if self
            .same_condition_market_verified
            .is_some_and(|verified| !verified)
        {
            report
                .source_gaps
                .push("same_condition_market_unverified".to_owned());
        }
        if let (Some(start), Some(end)) = (self.market_start_utc, self.market_end_utc)
            && start >= end
        {
            report.missing_fields.push("market_start_end".to_owned());
        }
        report
    }
}

pub fn polymarket_market_metadata_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_received",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "metadata_fetch_start_utc",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "metadata_recv_time_utc",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("slug", DataType::Utf8, false),
        Field::new("condition_id", DataType::Utf8, false),
        Field::new("clob_market_id", DataType::Utf8, true),
        Field::new("asset_ids", DataType::Utf8, true),
        Field::new("token_ids", DataType::Utf8, true),
        Field::new(
            "market_start_utc",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "market_end_utc",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("active", DataType::Boolean, true),
        Field::new("accepting_orders", DataType::Boolean, true),
        Field::new("market_lifecycle_state", DataType::Utf8, false),
        Field::new("tick_size", DataType::Utf8, true),
        Field::new("min_size", DataType::Utf8, true),
        Field::new("fee_schedule_raw", DataType::Utf8, true),
        Field::new("reward_eligibility_raw", DataType::Utf8, true),
        Field::new("metadata_source", DataType::Utf8, false),
        Field::new("same_condition_market_verified", DataType::Boolean, true),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

#[allow(clippy::too_many_arguments)]
fn metadata_row(
    metadata_source: &str,
    slug: &str,
    condition_id: &str,
    clob_market_id: Option<&str>,
    asset_ids: &[String],
    market_start_utc: Option<i64>,
    market_end_utc: Option<i64>,
    active: Option<bool>,
    accepting_orders: Option<bool>,
    market_lifecycle_state: &str,
    tick_size: Option<&str>,
    min_size: Option<&str>,
    fee_schedule_raw: Option<&str>,
    reward_eligibility_raw: Option<&str>,
    same_condition_market_verified: Option<bool>,
    context: PolymarketMarketMetadataContext,
) -> PolymarketMarketMetadataRow {
    let asset_ids_json = non_empty_json_list(asset_ids);
    PolymarketMarketMetadataRow {
        timestamp_received: context.timestamp_received_ms,
        metadata_fetch_start_utc: context.metadata_fetch_start_utc_ms,
        metadata_recv_time_utc: context.metadata_recv_time_utc_ms,
        slug: slug.to_owned(),
        condition_id: condition_id.to_owned(),
        clob_market_id: clob_market_id.map(ToOwned::to_owned),
        asset_ids: asset_ids_json.clone(),
        token_ids: asset_ids_json,
        market_start_utc,
        market_end_utc,
        active,
        accepting_orders,
        market_lifecycle_state: market_lifecycle_state.to_owned(),
        tick_size: tick_size.map(ToOwned::to_owned),
        min_size: min_size.map(ToOwned::to_owned),
        fee_schedule_raw: fee_schedule_raw.map(ToOwned::to_owned),
        reward_eligibility_raw: reward_eligibility_raw.map(ToOwned::to_owned),
        metadata_source: metadata_source.to_owned(),
        same_condition_market_verified,
        collector_host_id: context.collector_host_id,
        collector_region: context.collector_region,
        source_endpoint: context.source_endpoint,
        schema_version: context.schema_version,
    }
}

fn non_empty_json_list(values: &[String]) -> Option<String> {
    if values.is_empty() {
        return None;
    }
    Some(serde_json::to_string(values).expect("string vec serializes"))
}
