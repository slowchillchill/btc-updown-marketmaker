use std::str::FromStr as _;

use polymarket_client_sdk_v2::clob::ws::SubscriptionRequest;
use polymarket_client_sdk_v2::gamma::types::response::Market as SdkGammaMarket;
use polymarket_client_sdk_v2::types::U256;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

pub const BTC_UPDOWN_5M_SERIES_PREFIX: &str = "btc-updown-5m";
pub const BTC_UPDOWN_5M_STEP_SECONDS: i64 = 300;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarketWindow {
    pub target_ts_s: i64,
    pub end_ts_s: i64,
    pub slug: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveryWindows {
    pub now_ms: i64,
    pub current: MarketWindow,
    pub next: MarketWindow,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BtcUpdownMarketCandidate {
    pub id: String,
    pub slug: Option<String>,
    pub question: Option<String>,
    pub event_slug: Option<String>,
    pub event_title: Option<String>,
    pub series_slug: Option<String>,
    pub condition_id: Option<String>,
    pub clob_token_ids: Vec<String>,
    pub outcomes: Vec<String>,
    pub start_ts_s: Option<i64>,
    pub end_ts_s: Option<i64>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    pub archived: Option<bool>,
    pub enable_order_book: Option<bool>,
    pub accepting_orders: Option<bool>,
    pub tick_size: Option<String>,
    pub min_size: Option<String>,
    pub fee_schedule_raw: Option<String>,
    pub reward_eligibility_raw: Option<String>,
    pub raw_json_sha256: Option<String>,
    pub raw_json: Option<String>,
}

impl BtcUpdownMarketCandidate {
    pub fn from_sdk_market(market: &SdkGammaMarket) -> Self {
        let event = market.events.as_ref().and_then(|events| events.first());
        let raw_json = serde_json::to_string(market).ok();
        let raw_json_sha256 = raw_json.as_deref().map(|raw| sha256_hex(raw.as_bytes()));

        Self {
            id: market.id.clone(),
            slug: market.slug.clone(),
            question: market.question.clone(),
            event_slug: event.and_then(|event| event.slug.clone()),
            event_title: event.and_then(|event| event.title.clone()),
            series_slug: event.and_then(|event| event.series_slug.clone()),
            condition_id: market
                .condition_id
                .as_ref()
                .map(|condition_id| format!("{condition_id:#x}")),
            clob_token_ids: market
                .clob_token_ids
                .as_ref()
                .map(|ids| ids.iter().map(ToString::to_string).collect())
                .unwrap_or_default(),
            outcomes: market.outcomes.clone().unwrap_or_default(),
            start_ts_s: market.start_date.map(|dt| dt.timestamp()),
            end_ts_s: market.end_date.map(|dt| dt.timestamp()),
            active: market.active,
            closed: market.closed,
            archived: market.archived,
            enable_order_book: market.enable_order_book,
            accepting_orders: market.accepting_orders,
            tick_size: market
                .order_price_min_tick_size
                .as_ref()
                .map(ToString::to_string),
            min_size: market.order_min_size.as_ref().map(ToString::to_string),
            fee_schedule_raw: fee_schedule_raw(market),
            reward_eligibility_raw: reward_eligibility_raw(market),
            raw_json_sha256,
            raw_json,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiscoveredBtcUpdownMarket {
    pub id: String,
    pub slug: String,
    pub condition_id: String,
    pub up_asset_id: String,
    pub down_asset_id: String,
    pub start_ts_s: i64,
    pub end_ts_s: i64,
    pub candidate_start_ts_s: i64,
    pub candidate_end_ts_s: i64,
    pub active: bool,
    pub accepting_orders: bool,
    pub tick_size: Option<String>,
    pub min_size: Option<String>,
    pub fee_schedule_raw: Option<String>,
    pub reward_eligibility_raw: Option<String>,
    pub raw_json_sha256: Option<String>,
    pub raw_json: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BtcUpdownPairRelation {
    pub same_series_prefix: bool,
    pub windows_contiguous: bool,
    pub condition_ids_distinct: bool,
    pub cadence_seconds: i64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BtcUpdownDiscoveryPair {
    pub current: DiscoveredBtcUpdownMarket,
    pub next: DiscoveredBtcUpdownMarket,
    pub relation: BtcUpdownPairRelation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CandidateRejectReason {
    SlugMismatch,
    MissingConditionId,
    InvalidConditionId,
    InvalidTokenCount,
    UntrustedOutcomeMapping,
    NonBtcFiveMinuteMarket,
    WindowBoundsMismatch,
    RuntimeOutsideCurrentWindow,
    Inactive,
    Closed,
    Archived,
    OrderBookDisabled,
    NotAcceptingOrders,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiscoverySelectionError {
    MissingCurrentCandidate {
        expected_slug: String,
    },
    MissingNextCandidate {
        expected_slug: String,
    },
    MultipleCurrentCandidates {
        expected_slug: String,
        count: usize,
    },
    MultipleNextCandidates {
        expected_slug: String,
        count: usize,
    },
    InvalidCurrent {
        expected_slug: String,
        candidate_slug: Option<String>,
        reason: CandidateRejectReason,
    },
    InvalidNext {
        expected_slug: String,
        candidate_slug: Option<String>,
        reason: CandidateRejectReason,
    },
    PairRelationInvalid,
}

pub fn discovery_windows(now_ms: i64) -> DiscoveryWindows {
    let now_s = now_ms.div_euclid(1000);
    let current_target_ts_s =
        now_s.div_euclid(BTC_UPDOWN_5M_STEP_SECONDS) * BTC_UPDOWN_5M_STEP_SECONDS;
    let current = market_window(current_target_ts_s);
    let next = market_window(current_target_ts_s + BTC_UPDOWN_5M_STEP_SECONDS);
    DiscoveryWindows {
        now_ms,
        current,
        next,
    }
}

pub fn market_window(target_ts_s: i64) -> MarketWindow {
    MarketWindow {
        target_ts_s,
        end_ts_s: target_ts_s + BTC_UPDOWN_5M_STEP_SECONDS,
        slug: format!("{BTC_UPDOWN_5M_SERIES_PREFIX}-{target_ts_s}"),
    }
}

pub fn select_btc_updown_pair(
    now_ms: i64,
    current_candidates: &[BtcUpdownMarketCandidate],
    next_candidates: &[BtcUpdownMarketCandidate],
) -> Result<BtcUpdownDiscoveryPair, DiscoverySelectionError> {
    let windows = discovery_windows(now_ms);
    let current = select_one(current_candidates, &windows.current, CandidateKind::Current)?;
    let next = select_one(next_candidates, &windows.next, CandidateKind::Next)?;

    let relation = BtcUpdownPairRelation {
        same_series_prefix: current.slug.starts_with(BTC_UPDOWN_5M_SERIES_PREFIX)
            && next.slug.starts_with(BTC_UPDOWN_5M_SERIES_PREFIX),
        windows_contiguous: current.end_ts_s == next.start_ts_s,
        condition_ids_distinct: current.condition_id != next.condition_id,
        cadence_seconds: next.start_ts_s - current.start_ts_s,
    };

    if !relation.same_series_prefix
        || !relation.windows_contiguous
        || !relation.condition_ids_distinct
        || relation.cadence_seconds != BTC_UPDOWN_5M_STEP_SECONDS
    {
        return Err(DiscoverySelectionError::PairRelationInvalid);
    }

    Ok(BtcUpdownDiscoveryPair {
        current,
        next,
        relation,
    })
}

pub fn build_market_subscription_request_json(asset_ids: &[String]) -> Result<String, String> {
    let parsed_assets = asset_ids
        .iter()
        .map(|asset_id| {
            U256::from_str(asset_id)
                .map_err(|err| format!("invalid polymarket asset id {asset_id}: {err}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let request = SubscriptionRequest::market(parsed_assets).with_custom_features(true);
    serde_json::to_string(&request)
        .map_err(|err| format!("serialize market subscription request: {err}"))
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[derive(Clone, Copy)]
enum CandidateKind {
    Current,
    Next,
}

fn select_one(
    candidates: &[BtcUpdownMarketCandidate],
    window: &MarketWindow,
    kind: CandidateKind,
) -> Result<DiscoveredBtcUpdownMarket, DiscoverySelectionError> {
    let [candidate] = candidates else {
        return Err(match (kind, candidates.is_empty()) {
            (CandidateKind::Current, true) => DiscoverySelectionError::MissingCurrentCandidate {
                expected_slug: window.slug.clone(),
            },
            (CandidateKind::Next, true) => DiscoverySelectionError::MissingNextCandidate {
                expected_slug: window.slug.clone(),
            },
            (CandidateKind::Current, false) => DiscoverySelectionError::MultipleCurrentCandidates {
                expected_slug: window.slug.clone(),
                count: candidates.len(),
            },
            (CandidateKind::Next, false) => DiscoverySelectionError::MultipleNextCandidates {
                expected_slug: window.slug.clone(),
                count: candidates.len(),
            },
        });
    };

    validate_candidate(candidate, window).map_err(|reason| match kind {
        CandidateKind::Current => DiscoverySelectionError::InvalidCurrent {
            expected_slug: window.slug.clone(),
            candidate_slug: candidate.slug.clone(),
            reason,
        },
        CandidateKind::Next => DiscoverySelectionError::InvalidNext {
            expected_slug: window.slug.clone(),
            candidate_slug: candidate.slug.clone(),
            reason,
        },
    })
}

fn validate_candidate(
    candidate: &BtcUpdownMarketCandidate,
    window: &MarketWindow,
) -> Result<DiscoveredBtcUpdownMarket, CandidateRejectReason> {
    let slug = candidate
        .slug
        .as_deref()
        .filter(|slug| *slug == window.slug)
        .ok_or(CandidateRejectReason::SlugMismatch)?;
    let condition_id = candidate
        .condition_id
        .as_deref()
        .ok_or(CandidateRejectReason::MissingConditionId)?;
    validate_condition_id(condition_id)?;
    validate_btc_updown_5m_metadata(candidate)?;
    let (up_asset_id, down_asset_id) =
        trusted_up_down_mapping(&candidate.outcomes, &candidate.clob_token_ids)?;

    let Some(candidate_start_ts_s) = candidate.start_ts_s else {
        return Err(CandidateRejectReason::WindowBoundsMismatch);
    };
    let Some(candidate_end_ts_s) = candidate.end_ts_s else {
        return Err(CandidateRejectReason::WindowBoundsMismatch);
    };
    if candidate_start_ts_s > window.target_ts_s || candidate_end_ts_s != window.end_ts_s {
        return Err(CandidateRejectReason::WindowBoundsMismatch);
    }
    if candidate.active != Some(true) {
        return Err(CandidateRejectReason::Inactive);
    }
    if candidate.closed != Some(false) {
        return Err(CandidateRejectReason::Closed);
    }
    if candidate.archived == Some(true) {
        return Err(CandidateRejectReason::Archived);
    }
    if candidate.enable_order_book != Some(true) {
        return Err(CandidateRejectReason::OrderBookDisabled);
    }
    if candidate.accepting_orders != Some(true) {
        return Err(CandidateRejectReason::NotAcceptingOrders);
    }

    Ok(DiscoveredBtcUpdownMarket {
        id: candidate.id.clone(),
        slug: slug.to_owned(),
        condition_id: condition_id.to_owned(),
        up_asset_id,
        down_asset_id,
        start_ts_s: window.target_ts_s,
        end_ts_s: window.end_ts_s,
        candidate_start_ts_s,
        candidate_end_ts_s,
        active: true,
        accepting_orders: true,
        tick_size: candidate.tick_size.clone(),
        min_size: candidate.min_size.clone(),
        fee_schedule_raw: candidate.fee_schedule_raw.clone(),
        reward_eligibility_raw: candidate.reward_eligibility_raw.clone(),
        raw_json_sha256: candidate.raw_json_sha256.clone(),
        raw_json: candidate.raw_json.clone(),
    })
}

fn fee_schedule_raw(market: &SdkGammaMarket) -> Option<String> {
    let value = serde_json::json!({
        "fee": market.fee.as_ref().map(ToString::to_string),
        "maker_base_fee": market.maker_base_fee,
        "taker_base_fee": market.taker_base_fee,
        "maker_rebates_fee_share_bps": market.maker_rebates_fee_share_bps,
        "fees_enabled": market.fees_enabled,
    });
    (!json_object_values_all_null(&value)).then(|| value.to_string())
}

fn reward_eligibility_raw(market: &SdkGammaMarket) -> Option<String> {
    let value = serde_json::json!({
        "uma_reward": market.uma_reward.as_ref().map(ToString::to_string),
        "rewards_min_size": market.rewards_min_size.as_ref().map(ToString::to_string),
        "rewards_max_spread": market.rewards_max_spread.as_ref().map(ToString::to_string),
        "holding_rewards_enabled": market.holding_rewards_enabled,
        "clob_rewards": market.clob_rewards,
    });
    (!json_object_values_all_null(&value)).then(|| value.to_string())
}

fn json_object_values_all_null(value: &serde_json::Value) -> bool {
    value
        .as_object()
        .is_none_or(|object| object.values().all(serde_json::Value::is_null))
}

fn validate_btc_updown_5m_metadata(
    candidate: &BtcUpdownMarketCandidate,
) -> Result<(), CandidateRejectReason> {
    let slug_ok = candidate
        .slug
        .as_deref()
        .is_some_and(|slug| slug.starts_with(BTC_UPDOWN_5M_SERIES_PREFIX));
    let supporting_metadata_ok = candidate
        .question
        .as_deref()
        .is_some_and(contains_btc_up_down_text)
        || candidate
            .event_slug
            .as_deref()
            .is_some_and(|slug| slug.contains(BTC_UPDOWN_5M_SERIES_PREFIX))
        || candidate
            .series_slug
            .as_deref()
            .is_some_and(|slug| slug.contains(BTC_UPDOWN_5M_SERIES_PREFIX))
        || candidate
            .event_title
            .as_deref()
            .is_some_and(contains_btc_up_down_text);

    if slug_ok && supporting_metadata_ok {
        Ok(())
    } else {
        Err(CandidateRejectReason::NonBtcFiveMinuteMarket)
    }
}

fn contains_btc_up_down_text(value: &str) -> bool {
    let lower = value.to_ascii_lowercase();
    (lower.contains("btc") || lower.contains("bitcoin"))
        && lower.contains("up")
        && lower.contains("down")
}

fn trusted_up_down_mapping(
    outcomes: &[String],
    token_ids: &[String],
) -> Result<(String, String), CandidateRejectReason> {
    if outcomes.len() != 2 || token_ids.len() != 2 {
        return Err(CandidateRejectReason::InvalidTokenCount);
    }

    let mut up_asset_id = None;
    let mut down_asset_id = None;
    for (outcome, token_id) in outcomes.iter().zip(token_ids.iter()) {
        if token_id.trim().is_empty() {
            return Err(CandidateRejectReason::InvalidTokenCount);
        }
        match outcome.trim().to_ascii_lowercase().as_str() {
            "up" => up_asset_id = Some(token_id.clone()),
            "down" => down_asset_id = Some(token_id.clone()),
            _ => return Err(CandidateRejectReason::UntrustedOutcomeMapping),
        }
    }

    match (up_asset_id, down_asset_id) {
        (Some(up), Some(down)) if up != down => Ok((up, down)),
        _ => Err(CandidateRejectReason::UntrustedOutcomeMapping),
    }
}

fn validate_condition_id(condition_id: &str) -> Result<(), CandidateRejectReason> {
    let hex = condition_id
        .strip_prefix("0x")
        .ok_or(CandidateRejectReason::InvalidConditionId)?;
    if hex.len() == 64 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(CandidateRejectReason::InvalidConditionId)
    }
}
