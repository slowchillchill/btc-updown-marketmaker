use serde_json::json;
use sha2::{Digest as _, Sha256};
use std::error::Error;
use std::io;
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, StringBuilder, StructBuilder};
use arrow_array::{
    Array, ArrayRef, ListArray, RecordBatch, StringArray, StructArray, TimestampMillisecondArray,
    UInt64Array,
};
use arrow_schema::{DataType, Field, Fields, Schema, TimeUnit};
use polymarket_client_sdk_v2::clob::ws::{EventMessage, MarketResolved, NewMarket, WsMessage};
use polymarket_client_sdk_v2::types::{B256, Decimal, U256};

pub const POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION: &str = "polymarket_orderbook_v3";

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketOrderbookV3Admission {
    pub timestamp_received: i64,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub source_sequence_epoch_id: String,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketOrderbookV3Envelope {
    pub timestamp_received: i64,
    pub timestamp: Option<i64>,
    pub market: String,
    pub slug: Option<String>,
    pub collector_host_id: String,
    pub collector_region: String,
    pub source_endpoint: String,
    pub schema_version: String,
    pub source_sequence_epoch_id: String,
    pub collector_admission_sequence: u64,
    pub collector_admission_time_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketOrderbookV3Projection {
    pub timestamp: Option<i64>,
    pub market: String,
    pub slug: Option<String>,
    pub event: PolymarketOrderbookV3Event,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketOrderbookV3Message {
    pub envelope: PolymarketOrderbookV3Envelope,
    pub event: PolymarketOrderbookV3Event,
}

pub(crate) struct PendingPolymarketOrderbookV3Admission {
    message: PolymarketOrderbookV3Message,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolymarketOrderbookV3Event {
    Book(PolymarketBookV3),
    PriceChange(PolymarketPriceChangeV3),
    TickSizeChange(PolymarketTickSizeChangeV3),
    LastTradePrice(PolymarketLastTradePriceV3),
    BestBidAsk(PolymarketBestBidAskV3),
    NewMarket(PolymarketNewMarketV3),
    MarketResolved(PolymarketMarketResolvedV3),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketPriceLevelV3 {
    pub price: String,
    pub size: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketBookV3 {
    pub asset_id: String,
    pub bids: Vec<PolymarketPriceLevelV3>,
    pub asks: Vec<PolymarketPriceLevelV3>,
    pub last_trade_price: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketPriceChangeEntryV3 {
    pub asset_id: String,
    pub price: String,
    pub size: Option<String>,
    pub side: String,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketPriceChangeV3 {
    pub price_changes: Vec<PolymarketPriceChangeEntryV3>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketTickSizeChangeV3 {
    pub asset_id: String,
    pub old_tick_size: Option<String>,
    pub new_tick_size: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketLastTradePriceV3 {
    pub asset_id: String,
    pub price: String,
    pub size: Option<String>,
    pub side: Option<String>,
    pub fee_rate_bps: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketBestBidAskV3 {
    pub asset_id: String,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
    pub spread: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketEventMessageV3 {
    pub id: String,
    pub ticker: Option<String>,
    pub slug: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketNewMarketV3 {
    pub clob_market_id: String,
    pub question: Option<String>,
    pub description: Option<String>,
    pub asset_ids: Vec<String>,
    pub outcomes: Vec<String>,
    pub event_message: Option<PolymarketEventMessageV3>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolymarketMarketResolvedV3 {
    pub clob_market_id: String,
    pub question: Option<String>,
    pub description: Option<String>,
    pub asset_ids: Vec<String>,
    pub outcomes: Vec<String>,
    pub event_message: Option<PolymarketEventMessageV3>,
    pub winning_asset_id: String,
    pub winning_outcome: String,
}

impl PolymarketOrderbookV3Projection {
    pub fn from_target_admitted_sdk_message(message: &WsMessage) -> Result<Option<Self>> {
        let projection = match message {
            WsMessage::Book(event) => Self {
                timestamp: event.timestamp,
                market: event.market.to_string(),
                slug: None,
                event: PolymarketOrderbookV3Event::Book(PolymarketBookV3 {
                    asset_id: event.asset_id.to_string(),
                    bids: event
                        .bids
                        .iter()
                        .map(|level| PolymarketPriceLevelV3 {
                            price: level.price.to_string(),
                            size: level.size.to_string(),
                        })
                        .collect(),
                    asks: event
                        .asks
                        .iter()
                        .map(|level| PolymarketPriceLevelV3 {
                            price: level.price.to_string(),
                            size: level.size.to_string(),
                        })
                        .collect(),
                    last_trade_price: event.last_trade_price.map(|value| value.to_string()),
                }),
            },
            WsMessage::PriceChange(event) => Self {
                timestamp: event.timestamp,
                market: event.market.to_string(),
                slug: None,
                event: PolymarketOrderbookV3Event::PriceChange(PolymarketPriceChangeV3 {
                    price_changes: event
                        .price_changes
                        .iter()
                        .map(|change| PolymarketPriceChangeEntryV3 {
                            asset_id: change.asset_id.to_string(),
                            price: change.price.to_string(),
                            size: change.size.map(|value| value.to_string()),
                            side: change.side.to_string(),
                            best_bid: change.best_bid.map(|value| value.to_string()),
                            best_ask: change.best_ask.map(|value| value.to_string()),
                        })
                        .collect(),
                }),
            },
            WsMessage::TickSizeChange(event) => Self {
                timestamp: event.timestamp,
                market: event.market.to_string(),
                slug: None,
                event: PolymarketOrderbookV3Event::TickSizeChange(PolymarketTickSizeChangeV3 {
                    asset_id: event.asset_id.to_string(),
                    old_tick_size: event.old_tick_size.map(|value| value.to_string()),
                    new_tick_size: event.new_tick_size.to_string(),
                }),
            },
            WsMessage::LastTradePrice(event) => Self {
                timestamp: event.timestamp,
                market: event.market.to_string(),
                slug: None,
                event: PolymarketOrderbookV3Event::LastTradePrice(PolymarketLastTradePriceV3 {
                    asset_id: event.asset_id.to_string(),
                    price: event.price.to_string(),
                    size: event.size.map(|value| value.to_string()),
                    side: event.side.map(|value| value.to_string()),
                    fee_rate_bps: event.fee_rate_bps.map(|value| value.to_string()),
                }),
            },
            WsMessage::BestBidAsk(event) => Self {
                timestamp: event.timestamp,
                market: event.market.to_string(),
                slug: None,
                event: PolymarketOrderbookV3Event::BestBidAsk(PolymarketBestBidAskV3 {
                    asset_id: event.asset_id.to_string(),
                    best_bid: event.best_bid.map(|value| value.to_string()),
                    best_ask: event.best_ask.map(|value| value.to_string()),
                    spread: event.spread.map(|value| value.to_string()),
                }),
            },
            WsMessage::NewMarket(event) => Self {
                timestamp: event.timestamp,
                market: event.market.to_string(),
                slug: event.slug.clone(),
                event: PolymarketOrderbookV3Event::NewMarket(new_market(event)),
            },
            WsMessage::MarketResolved(event) => Self {
                timestamp: event.timestamp,
                market: event.market.to_string(),
                slug: event.slug.clone(),
                event: PolymarketOrderbookV3Event::MarketResolved(market_resolved(event)?),
            },
            WsMessage::Trade(_) | WsMessage::Order(_) => return Ok(None),
            _ => {
                return Err(invalid_data(
                    "unsupported SDK market-data variant for polymarket_orderbook_v3",
                ));
            }
        };
        projection.validate()?;
        Ok(Some(projection))
    }

    pub fn with_slug(mut self, slug: Option<String>) -> Self {
        self.slug = slug;
        self
    }

    pub fn admit(
        self,
        admission: PolymarketOrderbookV3Admission,
    ) -> Result<PolymarketOrderbookV3Message> {
        let message = self
            .prepare_admission(
                admission.timestamp_received,
                admission.collector_host_id,
                admission.collector_region,
                admission.source_endpoint,
                admission.source_sequence_epoch_id,
            )?
            .admit(
                admission.collector_admission_sequence,
                admission.collector_admission_time_utc_ms,
            );
        message.envelope.validate_admission()?;
        Ok(message)
    }

    pub(crate) fn prepare_admission(
        self,
        timestamp_received: i64,
        collector_host_id: String,
        collector_region: String,
        source_endpoint: String,
        source_sequence_epoch_id: String,
    ) -> Result<PendingPolymarketOrderbookV3Admission> {
        if timestamp_received <= 0 {
            return Err(invalid_data(
                "Polymarket v3 timestamp_received must be positive",
            ));
        }
        require_non_empty("collector_host_id", &collector_host_id)?;
        require_non_empty("collector_region", &collector_region)?;
        require_non_empty("source_endpoint", &source_endpoint)?;
        require_non_empty("source_sequence_epoch_id", &source_sequence_epoch_id)?;
        Ok(PendingPolymarketOrderbookV3Admission {
            message: PolymarketOrderbookV3Message {
                envelope: PolymarketOrderbookV3Envelope {
                    timestamp_received,
                    timestamp: self.timestamp,
                    market: self.market,
                    slug: self.slug,
                    collector_host_id,
                    collector_region,
                    source_endpoint,
                    schema_version: POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION.to_owned(),
                    source_sequence_epoch_id,
                    collector_admission_sequence: 0,
                    collector_admission_time_utc_ms: 0,
                },
                event: self.event,
            },
        })
    }

    fn validate(&self) -> Result<()> {
        require_condition_id("market", &self.market)?;
        if self.timestamp.is_some_and(|timestamp| timestamp <= 0) {
            return Err(invalid_data(
                "Polymarket v3 platform timestamp must be positive",
            ));
        }
        validate_event(&self.event)
    }
}

impl PendingPolymarketOrderbookV3Admission {
    pub(crate) fn owned_bytes(&self) -> usize {
        self.message.owned_bytes()
    }

    pub(crate) fn admit(
        mut self,
        collector_admission_sequence: u64,
        collector_admission_time_utc_ms: i64,
    ) -> PolymarketOrderbookV3Message {
        self.message.envelope.collector_admission_sequence = collector_admission_sequence;
        self.message.envelope.collector_admission_time_utc_ms = collector_admission_time_utc_ms;
        self.message
    }
}

impl PolymarketOrderbookV3Message {
    pub fn event_type(&self) -> &'static str {
        self.event.event_type()
    }

    pub(crate) fn owned_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(self.envelope.market.capacity())
            .saturating_add(self.envelope.slug.as_ref().map_or(0, String::capacity))
            .saturating_add(self.envelope.collector_host_id.capacity())
            .saturating_add(self.envelope.collector_region.capacity())
            .saturating_add(self.envelope.source_endpoint.capacity())
            .saturating_add(self.envelope.schema_version.capacity())
            .saturating_add(self.envelope.source_sequence_epoch_id.capacity())
            .saturating_add(self.event.dynamic_owned_bytes())
    }

    pub fn validate(&self) -> Result<()> {
        self.envelope.validate()?;
        validate_event(&self.event)
    }
}

impl PolymarketOrderbookV3Envelope {
    fn validate(&self) -> Result<()> {
        if self.timestamp_received <= 0 {
            return Err(invalid_data(
                "Polymarket v3 timestamp_received must be positive",
            ));
        }
        if self.timestamp.is_some_and(|timestamp| timestamp <= 0) {
            return Err(invalid_data(
                "Polymarket v3 platform timestamp must be positive",
            ));
        }
        require_condition_id("market", &self.market)?;
        require_non_empty("collector_host_id", &self.collector_host_id)?;
        require_non_empty("collector_region", &self.collector_region)?;
        require_non_empty("source_endpoint", &self.source_endpoint)?;
        if self.schema_version != POLYMARKET_ORDERBOOK_V3_SCHEMA_VERSION {
            return Err(invalid_data(
                "Polymarket v3 message carries an unexpected schema version",
            ));
        }
        require_non_empty("source_sequence_epoch_id", &self.source_sequence_epoch_id)?;
        self.validate_admission()
    }

    fn validate_admission(&self) -> Result<()> {
        if self.collector_admission_sequence == 0 {
            return Err(invalid_data(
                "Polymarket v3 collector admission sequence must be positive",
            ));
        }
        if self.collector_admission_time_utc_ms <= 0 {
            return Err(invalid_data(
                "Polymarket v3 collector admission time must be positive",
            ));
        }
        if self.timestamp_received > self.collector_admission_time_utc_ms {
            return Err(invalid_data(
                "Polymarket v3 receive time exceeds collector admission time",
            ));
        }
        Ok(())
    }
}

impl PolymarketOrderbookV3Event {
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::Book(_) => "book",
            Self::PriceChange(_) => "price_change",
            Self::TickSizeChange(_) => "tick_size_change",
            Self::LastTradePrice(_) => "last_trade_price",
            Self::BestBidAsk(_) => "best_bid_ask",
            Self::NewMarket(_) => "new_market",
            Self::MarketResolved(_) => "market_resolved",
        }
    }

    fn dynamic_owned_bytes(&self) -> usize {
        match self {
            Self::Book(book) => book
                .asset_id
                .capacity()
                .saturating_add(owned_price_levels_bytes(&book.bids))
                .saturating_add(owned_price_levels_bytes(&book.asks))
                .saturating_add(book.last_trade_price.as_ref().map_or(0, String::capacity)),
            Self::PriceChange(price_change) => price_change
                .price_changes
                .capacity()
                .saturating_mul(std::mem::size_of::<PolymarketPriceChangeEntryV3>())
                .saturating_add(
                    price_change
                        .price_changes
                        .iter()
                        .map(|change| {
                            change
                                .asset_id
                                .capacity()
                                .saturating_add(change.price.capacity())
                                .saturating_add(change.size.as_ref().map_or(0, String::capacity))
                                .saturating_add(change.side.capacity())
                                .saturating_add(
                                    change.best_bid.as_ref().map_or(0, String::capacity),
                                )
                                .saturating_add(
                                    change.best_ask.as_ref().map_or(0, String::capacity),
                                )
                        })
                        .sum::<usize>(),
                ),
            Self::TickSizeChange(tick) => tick
                .asset_id
                .capacity()
                .saturating_add(tick.old_tick_size.as_ref().map_or(0, String::capacity))
                .saturating_add(tick.new_tick_size.capacity()),
            Self::LastTradePrice(trade) => trade
                .asset_id
                .capacity()
                .saturating_add(trade.price.capacity())
                .saturating_add(trade.size.as_ref().map_or(0, String::capacity))
                .saturating_add(trade.side.as_ref().map_or(0, String::capacity))
                .saturating_add(trade.fee_rate_bps.as_ref().map_or(0, String::capacity)),
            Self::BestBidAsk(best) => best
                .asset_id
                .capacity()
                .saturating_add(best.best_bid.as_ref().map_or(0, String::capacity))
                .saturating_add(best.best_ask.as_ref().map_or(0, String::capacity))
                .saturating_add(best.spread.as_ref().map_or(0, String::capacity)),
            Self::NewMarket(market) => market
                .clob_market_id
                .capacity()
                .saturating_add(market.question.as_ref().map_or(0, String::capacity))
                .saturating_add(market.description.as_ref().map_or(0, String::capacity))
                .saturating_add(owned_string_vec_bytes(&market.asset_ids))
                .saturating_add(owned_string_vec_bytes(&market.outcomes))
                .saturating_add(
                    market
                        .event_message
                        .as_ref()
                        .map_or(0, owned_event_message_bytes),
                ),
            Self::MarketResolved(market) => market
                .clob_market_id
                .capacity()
                .saturating_add(market.question.as_ref().map_or(0, String::capacity))
                .saturating_add(market.description.as_ref().map_or(0, String::capacity))
                .saturating_add(owned_string_vec_bytes(&market.asset_ids))
                .saturating_add(owned_string_vec_bytes(&market.outcomes))
                .saturating_add(
                    market
                        .event_message
                        .as_ref()
                        .map_or(0, owned_event_message_bytes),
                )
                .saturating_add(market.winning_asset_id.capacity())
                .saturating_add(market.winning_outcome.capacity()),
        }
    }
}

#[allow(clippy::ptr_arg)]
fn owned_price_levels_bytes(levels: &Vec<PolymarketPriceLevelV3>) -> usize {
    levels
        .capacity()
        .saturating_mul(std::mem::size_of::<PolymarketPriceLevelV3>())
        .saturating_add(
            levels
                .iter()
                .map(|level| level.price.capacity().saturating_add(level.size.capacity()))
                .sum::<usize>(),
        )
}

#[allow(clippy::ptr_arg)]
fn owned_string_vec_bytes(values: &Vec<String>) -> usize {
    values
        .capacity()
        .saturating_mul(std::mem::size_of::<String>())
        .saturating_add(values.iter().map(String::capacity).sum::<usize>())
}

fn owned_event_message_bytes(message: &PolymarketEventMessageV3) -> usize {
    message
        .id
        .capacity()
        .saturating_add(message.ticker.as_ref().map_or(0, String::capacity))
        .saturating_add(message.slug.as_ref().map_or(0, String::capacity))
        .saturating_add(message.title.as_ref().map_or(0, String::capacity))
        .saturating_add(message.description.as_ref().map_or(0, String::capacity))
}

fn new_market(event: &NewMarket) -> PolymarketNewMarketV3 {
    PolymarketNewMarketV3 {
        clob_market_id: event.id.clone(),
        question: event.question.clone(),
        description: event.description.clone(),
        asset_ids: event.asset_ids.iter().map(ToString::to_string).collect(),
        outcomes: event.outcomes.clone(),
        event_message: event.event_message.as_ref().map(event_message),
    }
}

fn market_resolved(event: &MarketResolved) -> Result<PolymarketMarketResolvedV3> {
    let winning_asset_id = event
        .winning_asset_id
        .as_ref()
        .ok_or_else(|| invalid_data("target-admitted market_resolved is missing winning asset"))?
        .to_string();
    let winning_outcome = event.winning_outcome.clone().ok_or_else(|| {
        invalid_data("target-admitted market_resolved is missing winning outcome")
    })?;
    Ok(PolymarketMarketResolvedV3 {
        clob_market_id: event.id.clone(),
        question: event.question.clone(),
        description: event.description.clone(),
        asset_ids: event.asset_ids.iter().map(ToString::to_string).collect(),
        outcomes: event.outcomes.clone(),
        event_message: event.event_message.as_ref().map(event_message),
        winning_asset_id,
        winning_outcome,
    })
}

fn event_message(message: &EventMessage) -> PolymarketEventMessageV3 {
    PolymarketEventMessageV3 {
        id: message.id.clone(),
        ticker: message.ticker.clone(),
        slug: message.slug.clone(),
        title: message.title.clone(),
        description: message.description.clone(),
    }
}

fn validate_event(event: &PolymarketOrderbookV3Event) -> Result<()> {
    match event {
        PolymarketOrderbookV3Event::Book(book) => {
            require_asset_id("book.asset_id", &book.asset_id)?;
            for level in book.bids.iter().chain(&book.asks) {
                require_decimal_text("book.depth.price", &level.price)?;
                require_decimal_text("book.depth.size", &level.size)?;
            }
            require_optional_decimal_text(
                "book.last_trade_price",
                book.last_trade_price.as_deref(),
            )?;
        }
        PolymarketOrderbookV3Event::PriceChange(price_change) => {
            if price_change.price_changes.is_empty() {
                return Err(invalid_data(
                    "target-admitted Polymarket v3 price_change must retain an entry",
                ));
            }
            for change in &price_change.price_changes {
                require_asset_id("price_change.asset_id", &change.asset_id)?;
                require_decimal_text("price_change.price", &change.price)?;
                require_optional_decimal_text("price_change.size", change.size.as_deref())?;
                require_side_text("price_change.side", &change.side)?;
                require_optional_decimal_text("price_change.best_bid", change.best_bid.as_deref())?;
                require_optional_decimal_text("price_change.best_ask", change.best_ask.as_deref())?;
            }
        }
        PolymarketOrderbookV3Event::TickSizeChange(tick) => {
            require_asset_id("tick_size_change.asset_id", &tick.asset_id)?;
            require_optional_decimal_text(
                "tick_size_change.old_tick_size",
                tick.old_tick_size.as_deref(),
            )?;
            require_decimal_text("tick_size_change.new_tick_size", &tick.new_tick_size)?;
        }
        PolymarketOrderbookV3Event::LastTradePrice(trade) => {
            require_asset_id("last_trade_price.asset_id", &trade.asset_id)?;
            require_decimal_text("last_trade_price.price", &trade.price)?;
            require_optional_decimal_text("last_trade_price.size", trade.size.as_deref())?;
            if let Some(side) = trade.side.as_deref() {
                require_side_text("last_trade_price.side", side)?;
            }
            require_optional_decimal_text(
                "last_trade_price.fee_rate_bps",
                trade.fee_rate_bps.as_deref(),
            )?;
        }
        PolymarketOrderbookV3Event::BestBidAsk(best) => {
            require_asset_id("best_bid_ask.asset_id", &best.asset_id)?;
            require_optional_decimal_text("best_bid_ask.best_bid", best.best_bid.as_deref())?;
            require_optional_decimal_text("best_bid_ask.best_ask", best.best_ask.as_deref())?;
            require_optional_decimal_text("best_bid_ask.spread", best.spread.as_deref())?;
        }
        PolymarketOrderbookV3Event::NewMarket(market) => {
            validate_market_identity(
                &market.clob_market_id,
                &market.asset_ids,
                &market.outcomes,
                market.event_message.as_ref(),
                true,
            )?;
        }
        PolymarketOrderbookV3Event::MarketResolved(market) => {
            validate_market_identity(
                &market.clob_market_id,
                &market.asset_ids,
                &market.outcomes,
                market.event_message.as_ref(),
                false,
            )?;
            require_asset_id("market_resolved.winning_asset_id", &market.winning_asset_id)?;
            require_non_empty("market_resolved.winning_outcome", &market.winning_outcome)?;
            if !market.asset_ids.contains(&market.winning_asset_id) {
                return Err(invalid_data(
                    "market_resolved winner is outside the admitted asset set",
                ));
            }
            if !market.outcomes.is_empty() {
                let winner_index = market
                    .asset_ids
                    .iter()
                    .position(|asset| asset == &market.winning_asset_id)
                    .expect("winner membership was checked");
                if !same_outcome_label(&market.outcomes[winner_index], &market.winning_outcome) {
                    return Err(invalid_data(
                        "market_resolved winning outcome contradicts the admitted asset mapping",
                    ));
                }
            }
        }
    }
    Ok(())
}

fn validate_market_identity(
    clob_market_id: &str,
    asset_ids: &[String],
    outcomes: &[String],
    event_message: Option<&PolymarketEventMessageV3>,
    outcomes_required: bool,
) -> Result<()> {
    require_non_empty("clob_market_id", clob_market_id)?;
    if asset_ids.is_empty() {
        return Err(invalid_data(
            "target-admitted Polymarket v3 market identity has incomplete assets",
        ));
    }
    for asset_id in asset_ids {
        require_asset_id("market.asset_ids", asset_id)?;
    }
    if (outcomes_required && outcomes.is_empty())
        || (!outcomes.is_empty() && outcomes.len() != asset_ids.len())
        || outcomes.iter().any(|outcome| outcome.trim().is_empty())
    {
        return Err(invalid_data(
            "target-admitted Polymarket v3 market identity has incomplete outcomes",
        ));
    }
    if let Some(event_message) = event_message {
        require_non_empty("event_message.id", &event_message.id)?;
    }
    Ok(())
}

fn require_non_empty(name: &str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(invalid_data(format!("{name} must be non-empty")));
    }
    Ok(())
}

fn require_decimal_text(name: &str, value: &str) -> Result<()> {
    require_non_empty(name, value)?;
    value
        .parse::<Decimal>()
        .map_err(|_| invalid_data(format!("{name} must be SDK-compatible decimal text")))?;
    Ok(())
}

fn require_optional_decimal_text(name: &str, value: Option<&str>) -> Result<()> {
    if let Some(value) = value {
        require_decimal_text(name, value)?;
    }
    Ok(())
}

fn require_asset_id(name: &str, value: &str) -> Result<()> {
    require_non_empty(name, value)?;
    value
        .parse::<U256>()
        .map_err(|_| invalid_data(format!("{name} must be an SDK-compatible asset id")))?;
    Ok(())
}

fn require_condition_id(name: &str, value: &str) -> Result<()> {
    require_non_empty(name, value)?;
    value
        .parse::<B256>()
        .map_err(|_| invalid_data(format!("{name} must be an SDK-compatible condition id")))?;
    Ok(())
}

fn require_side_text(name: &str, value: &str) -> Result<()> {
    if !matches!(value, "BUY" | "SELL" | "UNKNOWN") {
        return Err(invalid_data(format!(
            "{name} must be an SDK-compatible side"
        )));
    }
    Ok(())
}

pub(crate) fn same_outcome_label(left: &str, right: &str) -> bool {
    left.trim().eq_ignore_ascii_case(right.trim())
}

fn invalid_data(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    Box::new(io::Error::new(io::ErrorKind::InvalidData, message.into()))
}

pub fn polymarket_orderbook_v3_messages_to_batch(
    messages: &[PolymarketOrderbookV3Message],
) -> Result<RecordBatch> {
    for message in messages {
        message.validate()?;
    }

    let schema = polymarket_orderbook_v3_schema();
    let columns: Vec<ArrayRef> =
        vec![
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    messages
                        .iter()
                        .map(|message| message.envelope.timestamp_received),
                )
                .with_timezone("UTC"),
            ),
            Arc::new(
                TimestampMillisecondArray::from_iter(
                    messages.iter().map(|message| message.envelope.timestamp),
                )
                .with_timezone("UTC"),
            ),
            string_values(
                messages
                    .iter()
                    .map(PolymarketOrderbookV3Message::event_type),
            ),
            string_values(
                messages
                    .iter()
                    .map(|message| message.envelope.market.as_str()),
            ),
            optional_strings(
                messages
                    .iter()
                    .map(|message| message.envelope.slug.as_deref()),
            ),
            string_values(
                messages
                    .iter()
                    .map(|message| message.envelope.collector_host_id.as_str()),
            ),
            string_values(
                messages
                    .iter()
                    .map(|message| message.envelope.collector_region.as_str()),
            ),
            string_values(
                messages
                    .iter()
                    .map(|message| message.envelope.source_endpoint.as_str()),
            ),
            string_values(
                messages
                    .iter()
                    .map(|message| message.envelope.schema_version.as_str()),
            ),
            string_values(
                messages
                    .iter()
                    .map(|message| message.envelope.source_sequence_epoch_id.as_str()),
            ),
            Arc::new(UInt64Array::from_iter_values(
                messages
                    .iter()
                    .map(|message| message.envelope.collector_admission_sequence),
            )),
            Arc::new(
                TimestampMillisecondArray::from_iter_values(
                    messages
                        .iter()
                        .map(|message| message.envelope.collector_admission_time_utc_ms),
                )
                .with_timezone("UTC"),
            ),
            optional_strings(
                messages
                    .iter()
                    .map(|message| event_asset_id(&message.event)),
            ),
            Arc::new(price_levels_array(messages, PriceLevelSide::Bids)),
            Arc::new(price_levels_array(messages, PriceLevelSide::Asks)),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::Book(book) => book.last_trade_price.as_deref(),
                _ => None,
            })),
            Arc::new(price_changes_array(messages)),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::TickSizeChange(tick) => tick.old_tick_size.as_deref(),
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::TickSizeChange(tick) => {
                    Some(tick.new_tick_size.as_str())
                }
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::LastTradePrice(trade) => Some(trade.price.as_str()),
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::LastTradePrice(trade) => trade.size.as_deref(),
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::LastTradePrice(trade) => trade.side.as_deref(),
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::LastTradePrice(trade) => trade.fee_rate_bps.as_deref(),
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::BestBidAsk(best) => best.best_bid.as_deref(),
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::BestBidAsk(best) => best.best_ask.as_deref(),
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::BestBidAsk(best) => best.spread.as_deref(),
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| {
                market_identity(&message.event).map(|market| market.clob_market_id())
            })),
            optional_strings(messages.iter().map(|message| {
                market_identity(&message.event).and_then(|market| market.question())
            })),
            optional_strings(messages.iter().map(|message| {
                market_identity(&message.event).and_then(|market| market.description())
            })),
            Arc::new(market_string_list_array(
                messages,
                MarketStringList::AssetIds,
            )),
            Arc::new(market_string_list_array(
                messages,
                MarketStringList::Outcomes,
            )),
            Arc::new(event_messages_array(messages)),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::MarketResolved(market) => {
                    Some(market.winning_asset_id.as_str())
                }
                _ => None,
            })),
            optional_strings(messages.iter().map(|message| match &message.event {
                PolymarketOrderbookV3Event::MarketResolved(market) => {
                    Some(market.winning_outcome.as_str())
                }
                _ => None,
            })),
        ];

    RecordBatch::try_new(schema, columns).map_err(|error| error.into())
}

pub fn polymarket_orderbook_v3_messages_from_batch(
    batch: &RecordBatch,
) -> Result<Vec<PolymarketOrderbookV3Message>> {
    let expected_schema = polymarket_orderbook_v3_schema();
    if batch.schema().as_ref() != expected_schema.as_ref() {
        return Err(invalid_data(
            "Polymarket v3 batch schema does not match the exact canonical schema",
        ));
    }

    let timestamp_received =
        typed_column::<TimestampMillisecondArray>(batch, "timestamp_received")?;
    let timestamp = typed_column::<TimestampMillisecondArray>(batch, "timestamp")?;
    let event_types = typed_column::<StringArray>(batch, "event_type")?;
    let markets = typed_column::<StringArray>(batch, "market")?;
    let slugs = typed_column::<StringArray>(batch, "slug")?;
    let collector_host_ids = typed_column::<StringArray>(batch, "collector_host_id")?;
    let collector_regions = typed_column::<StringArray>(batch, "collector_region")?;
    let source_endpoints = typed_column::<StringArray>(batch, "source_endpoint")?;
    let schema_versions = typed_column::<StringArray>(batch, "schema_version")?;
    let source_epochs = typed_column::<StringArray>(batch, "source_sequence_epoch_id")?;
    let admission_sequences = typed_column::<UInt64Array>(batch, "collector_admission_sequence")?;
    let admission_times =
        typed_column::<TimestampMillisecondArray>(batch, "collector_admission_time_utc_ms")?;

    let mut messages = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let event_type = required_string(event_types, row, "event_type")?;
        let event = decode_event(batch, row, &event_type)?;
        let message = PolymarketOrderbookV3Message {
            envelope: PolymarketOrderbookV3Envelope {
                timestamp_received: required_timestamp(
                    timestamp_received,
                    row,
                    "timestamp_received",
                )?,
                timestamp: optional_timestamp(timestamp, row),
                market: required_string(markets, row, "market")?,
                slug: optional_string(slugs, row),
                collector_host_id: required_string(collector_host_ids, row, "collector_host_id")?,
                collector_region: required_string(collector_regions, row, "collector_region")?,
                source_endpoint: required_string(source_endpoints, row, "source_endpoint")?,
                schema_version: required_string(schema_versions, row, "schema_version")?,
                source_sequence_epoch_id: required_string(
                    source_epochs,
                    row,
                    "source_sequence_epoch_id",
                )?,
                collector_admission_sequence: required_u64(
                    admission_sequences,
                    row,
                    "collector_admission_sequence",
                )?,
                collector_admission_time_utc_ms: required_timestamp(
                    admission_times,
                    row,
                    "collector_admission_time_utc_ms",
                )?,
            },
            event,
        };
        message.validate()?;
        messages.push(message);
    }
    Ok(messages)
}

fn decode_event(
    batch: &RecordBatch,
    row: usize,
    event_type: &str,
) -> Result<PolymarketOrderbookV3Event> {
    let event = match event_type {
        "book" => {
            require_only_payload_fields(
                batch,
                row,
                event_type,
                &["asset_id", "bids", "asks", "last_trade_price"],
            )?;
            PolymarketOrderbookV3Event::Book(PolymarketBookV3 {
                asset_id: required_string_column(batch, row, "asset_id")?,
                bids: required_price_levels(batch, row, "bids")?,
                asks: required_price_levels(batch, row, "asks")?,
                last_trade_price: optional_string_column(batch, row, "last_trade_price")?,
            })
        }
        "price_change" => {
            require_only_payload_fields(batch, row, event_type, &["price_changes"])?;
            PolymarketOrderbookV3Event::PriceChange(PolymarketPriceChangeV3 {
                price_changes: required_price_changes(batch, row)?,
            })
        }
        "tick_size_change" => {
            require_only_payload_fields(
                batch,
                row,
                event_type,
                &["asset_id", "old_tick_size", "new_tick_size"],
            )?;
            PolymarketOrderbookV3Event::TickSizeChange(PolymarketTickSizeChangeV3 {
                asset_id: required_string_column(batch, row, "asset_id")?,
                old_tick_size: optional_string_column(batch, row, "old_tick_size")?,
                new_tick_size: required_string_column(batch, row, "new_tick_size")?,
            })
        }
        "last_trade_price" => {
            require_only_payload_fields(
                batch,
                row,
                event_type,
                &["asset_id", "price", "size", "side", "fee_rate_bps"],
            )?;
            PolymarketOrderbookV3Event::LastTradePrice(PolymarketLastTradePriceV3 {
                asset_id: required_string_column(batch, row, "asset_id")?,
                price: required_string_column(batch, row, "price")?,
                size: optional_string_column(batch, row, "size")?,
                side: optional_string_column(batch, row, "side")?,
                fee_rate_bps: optional_string_column(batch, row, "fee_rate_bps")?,
            })
        }
        "best_bid_ask" => {
            require_only_payload_fields(
                batch,
                row,
                event_type,
                &["asset_id", "best_bid", "best_ask", "spread"],
            )?;
            PolymarketOrderbookV3Event::BestBidAsk(PolymarketBestBidAskV3 {
                asset_id: required_string_column(batch, row, "asset_id")?,
                best_bid: optional_string_column(batch, row, "best_bid")?,
                best_ask: optional_string_column(batch, row, "best_ask")?,
                spread: optional_string_column(batch, row, "spread")?,
            })
        }
        "new_market" => {
            require_only_payload_fields(
                batch,
                row,
                event_type,
                &[
                    "clob_market_id",
                    "question",
                    "description",
                    "asset_ids",
                    "outcomes",
                    "event_message",
                ],
            )?;
            PolymarketOrderbookV3Event::NewMarket(PolymarketNewMarketV3 {
                clob_market_id: required_string_column(batch, row, "clob_market_id")?,
                question: optional_string_column(batch, row, "question")?,
                description: optional_string_column(batch, row, "description")?,
                asset_ids: required_string_list(batch, row, "asset_ids")?,
                outcomes: required_string_list(batch, row, "outcomes")?,
                event_message: optional_event_message(batch, row)?,
            })
        }
        "market_resolved" => {
            require_only_payload_fields(
                batch,
                row,
                event_type,
                &[
                    "clob_market_id",
                    "question",
                    "description",
                    "asset_ids",
                    "outcomes",
                    "event_message",
                    "winning_asset_id",
                    "winning_outcome",
                ],
            )?;
            PolymarketOrderbookV3Event::MarketResolved(market_resolved_from_batch(batch, row)?)
        }
        _ => {
            return Err(invalid_data(format!(
                "unknown Polymarket v3 persisted event discriminator: {event_type}"
            )));
        }
    };
    Ok(event)
}

fn require_only_payload_fields(
    batch: &RecordBatch,
    row: usize,
    event_type: &str,
    allowed: &[&str],
) -> Result<()> {
    for (index, field) in batch.schema().fields().iter().enumerate().skip(12) {
        if !allowed.contains(&field.name().as_str()) && !batch.column(index).is_null(row) {
            return Err(invalid_data(format!(
                "Polymarket v3 {event_type} row has contradictory field {}",
                field.name()
            )));
        }
    }
    Ok(())
}

fn typed_column<'a, T: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a T> {
    let index = batch
        .schema()
        .index_of(name)
        .map_err(|_| invalid_data(format!("Polymarket v3 batch is missing column {name}")))?;
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| invalid_data(format!("Polymarket v3 column {name} has corrupt type")))
}

fn required_string(array: &StringArray, row: usize, name: &str) -> Result<String> {
    if array.is_null(row) {
        return Err(invalid_data(format!(
            "Polymarket v3 required column {name} is null"
        )));
    }
    Ok(array.value(row).to_owned())
}

fn optional_string(array: &StringArray, row: usize) -> Option<String> {
    (!array.is_null(row)).then(|| array.value(row).to_owned())
}

fn required_string_column(batch: &RecordBatch, row: usize, name: &str) -> Result<String> {
    required_string(typed_column::<StringArray>(batch, name)?, row, name)
}

fn optional_string_column(batch: &RecordBatch, row: usize, name: &str) -> Result<Option<String>> {
    Ok(optional_string(
        typed_column::<StringArray>(batch, name)?,
        row,
    ))
}

fn required_timestamp(array: &TimestampMillisecondArray, row: usize, name: &str) -> Result<i64> {
    if array.is_null(row) {
        return Err(invalid_data(format!(
            "Polymarket v3 required timestamp {name} is null"
        )));
    }
    Ok(array.value(row))
}

fn optional_timestamp(array: &TimestampMillisecondArray, row: usize) -> Option<i64> {
    (!array.is_null(row)).then(|| array.value(row))
}

fn required_u64(array: &UInt64Array, row: usize, name: &str) -> Result<u64> {
    if array.is_null(row) {
        return Err(invalid_data(format!(
            "Polymarket v3 required column {name} is null"
        )));
    }
    Ok(array.value(row))
}

fn list_range(array: &ListArray, row: usize, name: &str) -> Result<std::ops::Range<usize>> {
    if array.is_null(row) {
        return Err(invalid_data(format!(
            "Polymarket v3 required list {name} is null"
        )));
    }
    let offsets = array.value_offsets();
    let start = usize::try_from(offsets[row])
        .map_err(|_| invalid_data(format!("Polymarket v3 list {name} has a negative offset")))?;
    let end = usize::try_from(offsets[row + 1])
        .map_err(|_| invalid_data(format!("Polymarket v3 list {name} has a negative offset")))?;
    Ok(start..end)
}

fn required_price_levels(
    batch: &RecordBatch,
    row: usize,
    name: &str,
) -> Result<Vec<PolymarketPriceLevelV3>> {
    let list = typed_column::<ListArray>(batch, name)?;
    let range = list_range(list, row, name)?;
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_data(format!("Polymarket v3 list {name} has corrupt items")))?;
    let prices = struct_string_child(values, 0, name)?;
    let sizes = struct_string_child(values, 1, name)?;
    let price_name = format!("{name}.price");
    let size_name = format!("{name}.size");
    range
        .map(|index| {
            if values.is_null(index) {
                return Err(invalid_data(format!(
                    "Polymarket v3 list {name} has a null struct item"
                )));
            }
            Ok(PolymarketPriceLevelV3 {
                price: required_string(prices, index, &price_name)?,
                size: required_string(sizes, index, &size_name)?,
            })
        })
        .collect()
}

fn required_price_changes(
    batch: &RecordBatch,
    row: usize,
) -> Result<Vec<PolymarketPriceChangeEntryV3>> {
    let name = "price_changes";
    let list = typed_column::<ListArray>(batch, name)?;
    let range = list_range(list, row, name)?;
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| invalid_data("Polymarket v3 price_changes has corrupt items"))?;
    let asset_ids = struct_string_child(values, 0, name)?;
    let prices = struct_string_child(values, 1, name)?;
    let sizes = struct_string_child(values, 2, name)?;
    let sides = struct_string_child(values, 3, name)?;
    let best_bids = struct_string_child(values, 4, name)?;
    let best_asks = struct_string_child(values, 5, name)?;
    range
        .map(|index| {
            if values.is_null(index) {
                return Err(invalid_data(
                    "Polymarket v3 price_changes has a null struct item",
                ));
            }
            Ok(PolymarketPriceChangeEntryV3 {
                asset_id: required_string(asset_ids, index, "price_changes.asset_id")?,
                price: required_string(prices, index, "price_changes.price")?,
                size: optional_string(sizes, index),
                side: required_string(sides, index, "price_changes.side")?,
                best_bid: optional_string(best_bids, index),
                best_ask: optional_string(best_asks, index),
            })
        })
        .collect()
}

fn struct_string_child<'a>(
    array: &'a StructArray,
    index: usize,
    parent_name: &str,
) -> Result<&'a StringArray> {
    array
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            invalid_data(format!(
                "Polymarket v3 struct {parent_name} has corrupt child {index}"
            ))
        })
}

fn required_string_list(batch: &RecordBatch, row: usize, name: &str) -> Result<Vec<String>> {
    let list = typed_column::<ListArray>(batch, name)?;
    let range = list_range(list, row, name)?;
    let values = list
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| invalid_data(format!("Polymarket v3 list {name} has corrupt items")))?;
    let item_name = format!("{name}.item");
    range
        .map(|index| required_string(values, index, &item_name))
        .collect()
}

pub(crate) fn optional_event_message(
    batch: &RecordBatch,
    row: usize,
) -> Result<Option<PolymarketEventMessageV3>> {
    let array = typed_column::<StructArray>(batch, "event_message")?;
    if array.is_null(row) {
        return Ok(None);
    }
    let ids = struct_string_child(array, 0, "event_message")?;
    let tickers = struct_string_child(array, 1, "event_message")?;
    let slugs = struct_string_child(array, 2, "event_message")?;
    let titles = struct_string_child(array, 3, "event_message")?;
    let descriptions = struct_string_child(array, 4, "event_message")?;
    Ok(Some(PolymarketEventMessageV3 {
        id: required_string(ids, row, "event_message.id")?,
        ticker: optional_string(tickers, row),
        slug: optional_string(slugs, row),
        title: optional_string(titles, row),
        description: optional_string(descriptions, row),
    }))
}

fn string_values<'a>(values: impl Iterator<Item = &'a str>) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values))
}

fn optional_strings<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn event_asset_id(event: &PolymarketOrderbookV3Event) -> Option<&str> {
    match event {
        PolymarketOrderbookV3Event::Book(book) => Some(&book.asset_id),
        PolymarketOrderbookV3Event::TickSizeChange(tick) => Some(&tick.asset_id),
        PolymarketOrderbookV3Event::LastTradePrice(trade) => Some(&trade.asset_id),
        PolymarketOrderbookV3Event::BestBidAsk(best) => Some(&best.asset_id),
        PolymarketOrderbookV3Event::PriceChange(_)
        | PolymarketOrderbookV3Event::NewMarket(_)
        | PolymarketOrderbookV3Event::MarketResolved(_) => None,
    }
}

#[derive(Clone, Copy)]
enum MarketIdentityRef<'a> {
    New(&'a PolymarketNewMarketV3),
    Resolved(&'a PolymarketMarketResolvedV3),
}

impl<'a> MarketIdentityRef<'a> {
    fn clob_market_id(self) -> &'a str {
        match self {
            Self::New(market) => &market.clob_market_id,
            Self::Resolved(market) => &market.clob_market_id,
        }
    }

    fn question(self) -> Option<&'a str> {
        match self {
            Self::New(market) => market.question.as_deref(),
            Self::Resolved(market) => market.question.as_deref(),
        }
    }

    fn description(self) -> Option<&'a str> {
        match self {
            Self::New(market) => market.description.as_deref(),
            Self::Resolved(market) => market.description.as_deref(),
        }
    }

    fn asset_ids(self) -> &'a [String] {
        match self {
            Self::New(market) => &market.asset_ids,
            Self::Resolved(market) => &market.asset_ids,
        }
    }

    fn outcomes(self) -> &'a [String] {
        match self {
            Self::New(market) => &market.outcomes,
            Self::Resolved(market) => &market.outcomes,
        }
    }

    fn event_message(self) -> Option<&'a PolymarketEventMessageV3> {
        match self {
            Self::New(market) => market.event_message.as_ref(),
            Self::Resolved(market) => market.event_message.as_ref(),
        }
    }
}

fn market_identity(event: &PolymarketOrderbookV3Event) -> Option<MarketIdentityRef<'_>> {
    match event {
        PolymarketOrderbookV3Event::NewMarket(market) => Some(MarketIdentityRef::New(market)),
        PolymarketOrderbookV3Event::MarketResolved(market) => {
            Some(MarketIdentityRef::Resolved(market))
        }
        _ => None,
    }
}

enum PriceLevelSide {
    Bids,
    Asks,
}

fn price_levels_array(
    messages: &[PolymarketOrderbookV3Message],
    side: PriceLevelSide,
) -> ListArray {
    let mut builder = ListBuilder::new(StructBuilder::from_fields(
        price_level_fields(),
        messages.len(),
    ))
    .with_field(Arc::new(Field::new_list_field(
        DataType::Struct(price_level_fields()),
        false,
    )));
    for message in messages {
        let levels = match &message.event {
            PolymarketOrderbookV3Event::Book(book) => Some(match side {
                PriceLevelSide::Bids => book.bids.as_slice(),
                PriceLevelSide::Asks => book.asks.as_slice(),
            }),
            _ => None,
        };
        if let Some(levels) = levels {
            for level in levels {
                let values = builder.values();
                values
                    .field_builder::<StringBuilder>(0)
                    .expect("price level price builder")
                    .append_value(&level.price);
                values
                    .field_builder::<StringBuilder>(1)
                    .expect("price level size builder")
                    .append_value(&level.size);
                values.append(true);
            }
            builder.append(true);
        } else {
            builder.append(false);
        }
    }
    builder.finish()
}

fn price_changes_array(messages: &[PolymarketOrderbookV3Message]) -> ListArray {
    let mut builder = ListBuilder::new(StructBuilder::from_fields(
        price_change_fields(),
        messages.len(),
    ))
    .with_field(Arc::new(Field::new_list_field(
        DataType::Struct(price_change_fields()),
        false,
    )));
    for message in messages {
        let Some(price_change) = (match &message.event {
            PolymarketOrderbookV3Event::PriceChange(price_change) => Some(price_change),
            _ => None,
        }) else {
            builder.append(false);
            continue;
        };
        for change in &price_change.price_changes {
            let values = builder.values();
            values
                .field_builder::<StringBuilder>(0)
                .expect("price change asset builder")
                .append_value(&change.asset_id);
            values
                .field_builder::<StringBuilder>(1)
                .expect("price change price builder")
                .append_value(&change.price);
            append_optional_string(
                values
                    .field_builder::<StringBuilder>(2)
                    .expect("price change size builder"),
                change.size.as_deref(),
            );
            values
                .field_builder::<StringBuilder>(3)
                .expect("price change side builder")
                .append_value(&change.side);
            append_optional_string(
                values
                    .field_builder::<StringBuilder>(4)
                    .expect("price change best bid builder"),
                change.best_bid.as_deref(),
            );
            append_optional_string(
                values
                    .field_builder::<StringBuilder>(5)
                    .expect("price change best ask builder"),
                change.best_ask.as_deref(),
            );
            values.append(true);
        }
        builder.append(true);
    }
    builder.finish()
}

enum MarketStringList {
    AssetIds,
    Outcomes,
}

fn market_string_list_array(
    messages: &[PolymarketOrderbookV3Message],
    kind: MarketStringList,
) -> ListArray {
    let mut builder = ListBuilder::new(StringBuilder::new())
        .with_field(Arc::new(Field::new_list_field(DataType::Utf8, false)));
    for message in messages {
        let values = market_identity(&message.event).map(|market| match kind {
            MarketStringList::AssetIds => market.asset_ids(),
            MarketStringList::Outcomes => market.outcomes(),
        });
        if let Some(values) = values {
            for value in values {
                builder.values().append_value(value);
            }
            builder.append(true);
        } else {
            builder.append(false);
        }
    }
    builder.finish()
}

fn event_messages_array(messages: &[PolymarketOrderbookV3Message]) -> StructArray {
    let mut builder = StructBuilder::from_fields(event_message_fields(), messages.len());
    for message in messages {
        let event_message =
            market_identity(&message.event).and_then(|market| market.event_message());
        append_optional_string(
            builder
                .field_builder::<StringBuilder>(0)
                .expect("event message id builder"),
            event_message.map(|message| message.id.as_str()),
        );
        append_optional_string(
            builder
                .field_builder::<StringBuilder>(1)
                .expect("event message ticker builder"),
            event_message.and_then(|message| message.ticker.as_deref()),
        );
        append_optional_string(
            builder
                .field_builder::<StringBuilder>(2)
                .expect("event message slug builder"),
            event_message.and_then(|message| message.slug.as_deref()),
        );
        append_optional_string(
            builder
                .field_builder::<StringBuilder>(3)
                .expect("event message title builder"),
            event_message.and_then(|message| message.title.as_deref()),
        );
        append_optional_string(
            builder
                .field_builder::<StringBuilder>(4)
                .expect("event message description builder"),
            event_message.and_then(|message| message.description.as_deref()),
        );
        builder.append(event_message.is_some());
    }
    builder.finish()
}

fn append_optional_string(builder: &mut StringBuilder, value: Option<&str>) {
    if let Some(value) = value {
        builder.append_value(value);
    } else {
        builder.append_null();
    }
}

pub fn polymarket_orderbook_v3_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        timestamp_field("timestamp_received", false),
        timestamp_field("timestamp", true),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("market", DataType::Utf8, false),
        Field::new("slug", DataType::Utf8, true),
        Field::new("collector_host_id", DataType::Utf8, false),
        Field::new("collector_region", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("schema_version", DataType::Utf8, false),
        Field::new("source_sequence_epoch_id", DataType::Utf8, false),
        Field::new("collector_admission_sequence", DataType::UInt64, false),
        timestamp_field("collector_admission_time_utc_ms", false),
        Field::new("asset_id", DataType::Utf8, true),
        list_of_struct_field("bids", price_level_fields()),
        list_of_struct_field("asks", price_level_fields()),
        Field::new("last_trade_price", DataType::Utf8, true),
        list_of_struct_field("price_changes", price_change_fields()),
        Field::new("old_tick_size", DataType::Utf8, true),
        Field::new("new_tick_size", DataType::Utf8, true),
        Field::new("price", DataType::Utf8, true),
        Field::new("size", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("fee_rate_bps", DataType::Utf8, true),
        Field::new("best_bid", DataType::Utf8, true),
        Field::new("best_ask", DataType::Utf8, true),
        Field::new("spread", DataType::Utf8, true),
        Field::new("clob_market_id", DataType::Utf8, true),
        Field::new("question", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
        list_of_utf8_field("asset_ids"),
        list_of_utf8_field("outcomes"),
        Field::new(
            "event_message",
            DataType::Struct(event_message_fields()),
            true,
        ),
        Field::new("winning_asset_id", DataType::Utf8, true),
        Field::new("winning_outcome", DataType::Utf8, true),
    ]))
}

fn timestamp_field(name: &str, nullable: bool) -> Field {
    Field::new(
        name,
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
        nullable,
    )
}

fn price_level_fields() -> Fields {
    Fields::from(vec![
        Field::new("price", DataType::Utf8, false),
        Field::new("size", DataType::Utf8, false),
    ])
}

fn price_change_fields() -> Fields {
    Fields::from(vec![
        Field::new("asset_id", DataType::Utf8, false),
        Field::new("price", DataType::Utf8, false),
        Field::new("size", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, false),
        Field::new("best_bid", DataType::Utf8, true),
        Field::new("best_ask", DataType::Utf8, true),
    ])
}

fn event_message_fields() -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Utf8, false),
        Field::new("ticker", DataType::Utf8, true),
        Field::new("slug", DataType::Utf8, true),
        Field::new("title", DataType::Utf8, true),
        Field::new("description", DataType::Utf8, true),
    ])
}

fn list_of_struct_field(name: &str, fields: Fields) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(
            DataType::Struct(fields),
            false,
        ))),
        true,
    )
}

fn list_of_utf8_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::List(Arc::new(Field::new_list_field(DataType::Utf8, false))),
        true,
    )
}

fn market_resolved_from_batch(
    batch: &RecordBatch,
    row: usize,
) -> Result<PolymarketMarketResolvedV3> {
    Ok(PolymarketMarketResolvedV3 {
        clob_market_id: required_string_column(batch, row, "clob_market_id")?,
        question: optional_string_column(batch, row, "question")?,
        description: optional_string_column(batch, row, "description")?,
        asset_ids: required_string_list(batch, row, "asset_ids")?,
        outcomes: required_string_list(batch, row, "outcomes")?,
        event_message: optional_event_message(batch, row)?,
        winning_asset_id: required_string_column(batch, row, "winning_asset_id")?,
        winning_outcome: required_string_column(batch, row, "winning_outcome")?,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PolymarketMarketResolvedAdmissionEvidence {
    pub(crate) source_evidence_payload_json: String,
    pub(crate) source_evidence_raw_sha256: String,
    pub(crate) platform_timestamp_utc_ms: i64,
    pub(crate) winning_asset_id: String,
    pub(crate) winning_outcome: String,
}

pub(crate) fn polymarket_market_resolved_admission_evidence(
    market: &str,
    slug: Option<&str>,
    timestamp: Option<i64>,
    resolved: &PolymarketMarketResolvedV3,
) -> Result<PolymarketMarketResolvedAdmissionEvidence> {
    let platform_timestamp_utc_ms = timestamp.ok_or_else(|| {
        invalid_data("admitted Polymarket market_resolved is missing its platform timestamp")
    })?;
    let event_message = resolved.event_message.as_ref().map(|event_message| {
        json!({
            "id": event_message.id,
            "ticker": event_message.ticker,
            "slug": event_message.slug,
            "title": event_message.title,
            "description": event_message.description,
        })
    });
    let source_evidence_payload_json = serde_json::to_string(&json!({
        "event_type": "market_resolved",
        "market": market,
        "slug": slug,
        "clob_market_id": resolved.clob_market_id,
        "question": resolved.question,
        "description": resolved.description,
        "asset_ids": resolved.asset_ids,
        "outcomes": resolved.outcomes,
        "event_message": event_message,
        "winning_asset_id": resolved.winning_asset_id,
        "winning_outcome": resolved.winning_outcome,
        "timestamp": platform_timestamp_utc_ms,
    }))?;
    let source_evidence_raw_sha256 = Sha256::digest(source_evidence_payload_json.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    Ok(PolymarketMarketResolvedAdmissionEvidence {
        source_evidence_payload_json,
        source_evidence_raw_sha256,
        platform_timestamp_utc_ms,
        winning_asset_id: resolved.winning_asset_id.clone(),
        winning_outcome: resolved.winning_outcome.clone(),
    })
}
