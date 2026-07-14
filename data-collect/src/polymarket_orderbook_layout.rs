use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, TimeUnit};
use polymarket_client_sdk_v2::clob::ws::{BookUpdate, PriceChange};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketOrderbookContext {
    pub timestamp_received_ms: i64,
    pub slug: Option<String>,
    pub source_native_payload_json: String,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketOrderbookValidation {
    pub missing_fields: Vec<String>,
}

impl PolymarketOrderbookValidation {
    pub fn is_complete(&self) -> bool {
        self.missing_fields.is_empty()
    }

    fn require_non_empty(&mut self, field: &str, value: Option<&str>) {
        if value.is_none_or(|value| value.trim().is_empty()) {
            self.missing_fields.push(field.to_owned());
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketOrderbookRow {
    pub timestamp_received: i64,
    pub timestamp: Option<i64>,
    pub market: String,
    pub event_type: String,
    pub source_native_payload_json: String,
    pub asset_id: Option<String>,
    pub bids: Option<String>,
    pub asks: Option<String>,
    pub price: Option<String>,
    pub size: Option<String>,
    pub side: Option<String>,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
    pub hash: Option<String>,
    pub fee_rate_bps: Option<String>,
    pub transaction_hash: Option<String>,
    pub old_tick_size: Option<String>,
    pub new_tick_size: Option<String>,
    pub winning_asset_id: Option<String>,
    pub winning_outcome: Option<String>,
    pub close_reason_if_present: Option<String>,
    pub subscribed_asset_ids_for_market: Option<String>,
    pub resolved_event_asset_ids: Option<String>,
    pub slug: Option<String>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
}

impl PolymarketOrderbookRow {
    pub fn from_sdk_book(
        event: &BookUpdate,
        context: PolymarketOrderbookContext,
    ) -> Result<Self, String> {
        let mut row = base_row(
            "book",
            event.market.to_string(),
            Some(event.asset_id.to_string()),
            Some(event.timestamp),
            context,
        );
        row.bids =
            Some(depth_levels_to_json(event.bids.iter().map(|level| {
                (level.price.to_string(), level.size.to_string())
            }))?);
        row.asks =
            Some(depth_levels_to_json(event.asks.iter().map(|level| {
                (level.price.to_string(), level.size.to_string())
            }))?);
        row.hash = event.hash.clone();
        Ok(row)
    }

    pub fn book_from_raw_json(
        raw_json: &str,
        context: PolymarketOrderbookContext,
    ) -> Result<Self, String> {
        let value = serde_json::from_str::<Value>(raw_json)
            .map_err(|error| format!("book raw_json is not parseable: {error}"))?;
        if value.get("event_type").and_then(Value::as_str) != Some("book") {
            return Err("event_type=book missing".to_owned());
        }
        let market = required_json_string(&value, "market")?;
        let asset_id = required_json_string(&value, "asset_id")?;
        let timestamp = optional_json_i64(&value, "timestamp");
        let bids_result = value
            .get("bids")
            .ok_or_else(|| "bids full depth array missing".to_owned())
            .and_then(depth_value_to_json);
        let asks_result = value
            .get("asks")
            .ok_or_else(|| "asks full depth array missing".to_owned())
            .and_then(depth_value_to_json);
        let (bids, asks) = match (bids_result, asks_result) {
            (Ok(bids), Ok(asks)) => (bids, asks),
            (bids, asks) => {
                let mut errors = Vec::new();
                if let Err(error) = bids {
                    errors.push(error);
                }
                if let Err(error) = asks {
                    errors.push(error);
                }
                return Err(errors.join("; "));
            }
        };

        let mut row = base_row("book", market, Some(asset_id), timestamp, context);
        row.bids = Some(bids);
        row.asks = Some(asks);
        row.hash = value
            .get("hash")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        Ok(row)
    }

    pub fn from_sdk_price_change(
        event: &PriceChange,
        context: PolymarketOrderbookContext,
    ) -> Result<Vec<Self>, String> {
        if event.price_changes.is_empty() {
            return Err("price_changes empty".to_owned());
        }

        Ok(event
            .price_changes
            .iter()
            .map(|change| {
                let mut row = base_row(
                    "price_change",
                    event.market.to_string(),
                    Some(change.asset_id.to_string()),
                    Some(event.timestamp),
                    context.clone(),
                );
                row.price = Some(change.price.to_string());
                row.size = change.size.map(|size| size.to_string());
                row.side = Some(change.side.to_string());
                row.best_bid = change.best_bid.map(|price| price.to_string());
                row.best_ask = change.best_ask.map(|price| price.to_string());
                row.hash = change.hash.clone();
                row
            })
            .collect())
    }

    #[allow(clippy::too_many_arguments)]
    pub fn tick_size_change(
        market: &str,
        asset_id: &str,
        timestamp: i64,
        old_tick_size: &str,
        new_tick_size: &str,
        context: PolymarketOrderbookContext,
    ) -> Self {
        let mut row = base_row(
            "tick_size_change",
            market.to_owned(),
            Some(asset_id.to_owned()),
            Some(timestamp),
            context,
        );
        row.old_tick_size = Some(old_tick_size.to_owned());
        row.new_tick_size = Some(new_tick_size.to_owned());
        row
    }

    #[allow(clippy::too_many_arguments)]
    pub fn last_trade_price(
        market: &str,
        asset_id: &str,
        timestamp: i64,
        price: &str,
        fee_rate_bps: Option<&str>,
        side: Option<&str>,
        size: Option<&str>,
        transaction_hash: Option<&str>,
        context: PolymarketOrderbookContext,
    ) -> Self {
        let mut row = base_row(
            "last_trade_price",
            market.to_owned(),
            Some(asset_id.to_owned()),
            Some(timestamp),
            context,
        );
        row.price = Some(price.to_owned());
        row.fee_rate_bps = fee_rate_bps.map(ToOwned::to_owned);
        row.side = side.map(ToOwned::to_owned);
        row.size = size.map(ToOwned::to_owned);
        row.transaction_hash = transaction_hash.map(ToOwned::to_owned);
        row
    }

    pub fn market_resolved(
        market: &str,
        timestamp: i64,
        winning_asset_id: &str,
        winning_outcome: &str,
        close_reason_if_present: Option<&str>,
        resolved_event_asset_ids: &[String],
        context: PolymarketOrderbookContext,
    ) -> Self {
        let mut row = base_row(
            "market_resolved",
            market.to_owned(),
            None,
            Some(timestamp),
            context,
        );
        row.winning_asset_id = Some(winning_asset_id.to_owned());
        row.winning_outcome = Some(winning_outcome.to_owned());
        row.close_reason_if_present = close_reason_if_present.map(ToOwned::to_owned);
        let asset_ids_json =
            serde_json::to_string(resolved_event_asset_ids).expect("string vec serializes");
        row.subscribed_asset_ids_for_market = Some(asset_ids_json.clone());
        row.resolved_event_asset_ids = Some(asset_ids_json);
        row
    }

    pub fn validate(&self) -> PolymarketOrderbookValidation {
        let mut report = PolymarketOrderbookValidation {
            missing_fields: Vec::new(),
        };
        if self.timestamp_received <= 0 {
            report.missing_fields.push("timestamp_received".to_owned());
        }
        report.require_non_empty("market", Some(&self.market));
        report.require_non_empty("event_type", Some(&self.event_type));
        report.require_non_empty(
            "source_native_payload_json",
            Some(&self.source_native_payload_json),
        );
        if !self.source_native_payload_json.trim().is_empty()
            && serde_json::from_str::<Value>(&self.source_native_payload_json).is_err()
        {
            report
                .missing_fields
                .push("source_native_payload_json_parseable".to_owned());
        }
        report.require_non_empty("collector_host_id", Some(&self.collector_host_id));
        report.require_non_empty("collector_region", Some(&self.collector_region));
        report.require_non_empty("source_endpoint", Some(&self.source_endpoint));
        report.require_non_empty("schema_version", Some(&self.schema_version));

        match self.event_type.as_str() {
            "book" => {
                report.require_non_empty("asset_id", self.asset_id.as_deref());
                require_depth("bids", self.bids.as_deref(), &mut report);
                require_depth("asks", self.asks.as_deref(), &mut report);
            }
            "price_change" => {
                report.require_non_empty("asset_id", self.asset_id.as_deref());
                report.require_non_empty("price", self.price.as_deref());
                report.require_non_empty("size", self.size.as_deref());
                report.require_non_empty("side", self.side.as_deref());
                report.require_non_empty("best_bid", self.best_bid.as_deref());
                report.require_non_empty("best_ask", self.best_ask.as_deref());
            }
            "tick_size_change" => {
                report.require_non_empty("asset_id", self.asset_id.as_deref());
                report.require_non_empty("old_tick_size", self.old_tick_size.as_deref());
                report.require_non_empty("new_tick_size", self.new_tick_size.as_deref());
            }
            "last_trade_price" => {
                report.require_non_empty("asset_id", self.asset_id.as_deref());
                report.require_non_empty("price", self.price.as_deref());
                report.require_non_empty("size", self.size.as_deref());
                report.require_non_empty("side", self.side.as_deref());
            }
            "best_bid_ask" => {
                report.require_non_empty("asset_id", self.asset_id.as_deref());
                report.require_non_empty("best_bid", self.best_bid.as_deref());
                report.require_non_empty("best_ask", self.best_ask.as_deref());
            }
            "market_resolved" => {
                report.require_non_empty("winning_asset_id", self.winning_asset_id.as_deref());
                report.require_non_empty("winning_outcome", self.winning_outcome.as_deref());
                report.require_non_empty(
                    "resolved_event_asset_ids",
                    self.resolved_event_asset_ids.as_deref(),
                );
            }
            "new_market" => {
                report.require_non_empty(
                    "subscribed_asset_ids_for_market",
                    self.subscribed_asset_ids_for_market.as_deref(),
                );
                report.require_non_empty("slug", self.slug.as_deref());
            }
            _ => report.missing_fields.push("known_event_type".to_owned()),
        }

        report
    }
}

pub fn polymarket_orderbook_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "timestamp_received",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            true,
        ),
        Field::new("market", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("source_native_payload_json", DataType::Utf8, false),
        Field::new("asset_id", DataType::Utf8, true),
        Field::new("bids", DataType::Utf8, true),
        Field::new("asks", DataType::Utf8, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("size", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("best_bid", DataType::Utf8, true),
        Field::new("best_ask", DataType::Utf8, true),
        Field::new("hash", DataType::Utf8, true),
        Field::new("fee_rate_bps", DataType::Utf8, true),
        Field::new("transaction_hash", DataType::Utf8, true),
        Field::new("old_tick_size", DataType::Utf8, true),
        Field::new("new_tick_size", DataType::Utf8, true),
        Field::new("winning_asset_id", DataType::Utf8, true),
        Field::new("winning_outcome", DataType::Utf8, true),
        Field::new("close_reason_if_present", DataType::Utf8, true),
        Field::new("subscribed_asset_ids_for_market", DataType::Utf8, true),
        Field::new("resolved_event_asset_ids", DataType::Utf8, true),
        Field::new("slug", DataType::Utf8, true),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
    ]))
}

fn base_row(
    event_type: &str,
    market: String,
    asset_id: Option<String>,
    timestamp: Option<i64>,
    context: PolymarketOrderbookContext,
) -> PolymarketOrderbookRow {
    PolymarketOrderbookRow {
        timestamp_received: context.timestamp_received_ms,
        timestamp,
        market,
        event_type: event_type.to_owned(),
        source_native_payload_json: context.source_native_payload_json,
        asset_id,
        bids: None,
        asks: None,
        price: None,
        size: None,
        side: None,
        best_bid: None,
        best_ask: None,
        hash: None,
        fee_rate_bps: None,
        transaction_hash: None,
        old_tick_size: None,
        new_tick_size: None,
        winning_asset_id: None,
        winning_outcome: None,
        close_reason_if_present: None,
        subscribed_asset_ids_for_market: None,
        resolved_event_asset_ids: None,
        slug: context.slug,
        collector_host_id: context.collector_host_id,
        collector_region: context.collector_region,
        source_endpoint: context.source_endpoint,
        schema_version: context.schema_version,
    }
}

fn depth_levels_to_json<I>(levels: I) -> Result<String, String>
where
    I: IntoIterator<Item = (String, String)>,
{
    let levels = levels
        .into_iter()
        .map(|(price, size)| {
            if price.trim().is_empty() || size.trim().is_empty() {
                return Err("depth level price/size empty".to_owned());
            }
            Ok([price, size])
        })
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_string(&levels)
        .map_err(|error| format!("depth json serialization failed: {error}"))
}

fn depth_value_to_json(value: &Value) -> Result<String, String> {
    let levels = value
        .as_array()
        .ok_or_else(|| "depth is not an array".to_owned())?
        .iter()
        .map(depth_level_value)
        .collect::<Result<Vec<_>, _>>()?;
    serde_json::to_string(&levels)
        .map_err(|error| format!("depth json serialization failed: {error}"))
}

fn depth_level_value(value: &Value) -> Result<[String; 2], String> {
    if let Some(level) = value.as_array() {
        if level.len() != 2 {
            return Err("depth array level must have price and size".to_owned());
        }
        return Ok([
            json_scalar_to_string(&level[0])?,
            json_scalar_to_string(&level[1])?,
        ]);
    }
    if let Some(level) = value.as_object() {
        let price = level
            .get("price")
            .ok_or_else(|| "depth object level missing price".to_owned())
            .and_then(json_scalar_to_string)?;
        let size = level
            .get("size")
            .ok_or_else(|| "depth object level missing size".to_owned())
            .and_then(json_scalar_to_string)?;
        return Ok([price, size]);
    }
    Err("depth level must be array or object".to_owned())
}

fn json_scalar_to_string(value: &Value) -> Result<String, String> {
    let rendered = match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        _ => return Err("depth price/size must be string or number".to_owned()),
    };
    if rendered.trim().is_empty() {
        return Err("depth price/size empty".to_owned());
    }
    Ok(rendered)
}

fn required_json_string(value: &Value, field: &str) -> Result<String, String> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| format!("{field} missing"))
}

fn optional_json_i64(value: &Value, field: &str) -> Option<i64> {
    let value = value.get(field)?;
    value
        .as_i64()
        .or_else(|| value.as_str().and_then(|value| value.parse().ok()))
}

fn require_depth(field: &str, value: Option<&str>, report: &mut PolymarketOrderbookValidation) {
    let Some(value) = value else {
        report.missing_fields.push(field.to_owned());
        return;
    };
    let parsed = serde_json::from_str::<Value>(value);
    if parsed
        .as_ref()
        .ok()
        .and_then(Value::as_array)
        .is_none_or(|items| items.iter().any(|item| depth_level_value(item).is_err()))
    {
        report.missing_fields.push(field.to_owned());
    }
}
