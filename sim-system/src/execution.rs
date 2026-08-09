use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::sync::Arc;

use contracts::Sha256Hex;
pub use contracts::{ConditionId, TokenId};
pub use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

use crate::latency_profile::EmpiricalDelayProfile;
use crate::platform_rules::SealedProcessRuleSnapshot;
pub use crate::session::{
    ModelVisibleCursorV1, RequestContextV1, RequestRegistrationDispositionV1, RequestTransitV1,
    SIM_EXECUTION_CONTRACT_V1, SessionError as SimExecutionContractErrorV1, SessionSpecV1,
    SimCallerRoleV1,
};
use crate::session::{
    ParsedLogicalActionV1, ParsedMethodTimingV1, RegisteredLogicalScheduleV1, SimProcessRoot,
    SimSession,
};

const POST_ORDERS_MAX_ITEMS: usize = 15;
const CANCEL_ORDERS_MAX_ITEMS: usize = 1_000;
const CANONICAL_REQUEST_HASH_DOMAIN: &str = "SIM_EXECUTION_CANONICAL_REQUEST_V1";
const MUTATION_PAYLOAD_HASH_DOMAIN: &str = "SIM_EXECUTION_MUTATION_PAYLOAD_V1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimBuyOrderKindV1 {
    GtcLimit {
        limit_price_pusd_per_share: Decimal,
        size_shares: Decimal,
        post_only: bool,
    },
    GtdLimit {
        limit_price_pusd_per_share: Decimal,
        size_shares: Decimal,
        post_only: bool,
        expires_at_utc_ms: i64,
    },
    FokMarket {
        spend_pusd: Decimal,
        worst_price_pusd_per_share: Decimal,
    },
    FakMarket {
        spend_pusd: Decimal,
        worst_price_pusd_per_share: Decimal,
    },
}

impl SimBuyOrderKindV1 {
    fn method_timing(&self) -> ParsedMethodTimingV1 {
        match self {
            Self::GtcLimit { .. } | Self::GtdLimit { .. } => ParsedMethodTimingV1::Ordinary,
            Self::FokMarket { .. } | Self::FakMarket { .. } => {
                ParsedMethodTimingV1::FokFakMarketBuy
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SimBuyOrderV1 {
    pub condition_id: ConditionId,
    pub token_id: TokenId,
    pub kind: SimBuyOrderKindV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrderItemV1 {
    pub item_id: String,
    pub order: SimBuyOrderV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrderItemV1 {
    pub item_id: String,
    pub platform_order_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrderRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub item: PostOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrdersRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub items: Vec<PostOrderItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrderRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub item: CancelOrderItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrdersRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
    pub items: Vec<CancelOrderItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelAllOrdersRequestV1 {
    pub context: RequestContextV1,
    pub idempotency_key: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrderRequestV1 {
    pub context: RequestContextV1,
    pub platform_order_id: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrdersFilterV1 {
    pub condition_id: Option<ConditionId>,
    pub token_id: Option<TokenId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TradesFilterV1 {
    pub trade_id: Option<String>,
    pub condition_id: Option<ConditionId>,
    pub token_id: Option<TokenId>,
    pub after_utc_ms: Option<i64>,
    pub before_utc_ms: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpaquePageCursorV1(String);

impl OpaquePageCursorV1 {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrdersRequestV1 {
    pub context: RequestContextV1,
    pub filter: OrdersFilterV1,
    pub cursor: Option<OpaquePageCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TradesRequestV1 {
    pub context: RequestContextV1,
    pub filter: TradesFilterV1,
    pub cursor: Option<OpaquePageCursorV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestRegistrationReceiptV1 {
    pub execution_contract_version: String,
    pub global_request_id: String,
    pub disposition: RequestRegistrationDispositionV1,
    pub transit: RequestTransitV1,
    pub scheduled_action_count: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisteredItemV1 {
    pub item_id: String,
    pub scheduled_action_ordinal: u16,
    pub effective_processing_at_utc_ms: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrderRegistrationV1 {
    pub receipt: RequestRegistrationReceiptV1,
    pub item: RegisteredItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostOrdersRegistrationV1 {
    pub receipt: RequestRegistrationReceiptV1,
    pub items: Vec<RegisteredItemV1>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrderRegistrationV1 {
    pub receipt: RequestRegistrationReceiptV1,
    pub item: RegisteredItemV1,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelOrdersRegistrationV1 {
    pub receipt: RequestRegistrationReceiptV1,
    pub items: Vec<RegisteredItemV1>,
}

macro_rules! simple_registration {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $name {
            pub receipt: RequestRegistrationReceiptV1,
        }
    };
}

simple_registration!(CancelAllOrdersRegistrationV1);
simple_registration!(OrderRegistrationV1);
simple_registration!(OrdersRegistrationV1);
simple_registration!(TradesRegistrationV1);

pub struct SimExecutionProcessV1 {
    inner: SimProcessRoot,
}

impl SimExecutionProcessV1 {
    #[must_use]
    pub fn new(
        rule_snapshot: Arc<SealedProcessRuleSnapshot>,
        latency_profile: Arc<EmpiricalDelayProfile>,
    ) -> Self {
        Self {
            inner: SimProcessRoot::new(rule_snapshot, latency_profile),
        }
    }

    pub fn create_session(
        &mut self,
        spec: SessionSpecV1,
    ) -> Result<SimExecutionSessionV1, SimExecutionContractErrorV1> {
        let session = self.inner.create_session(spec)?;
        Ok(SimExecutionSessionV1 {
            session,
            pending_requests: BTreeMap::new(),
            mutation_payloads: BTreeMap::new(),
        })
    }
}

/// 八方法模拟领域登记入口。
///
/// 这些方法不接受旧名称、改单或 SDK 的签名订单：
///
/// ```compile_fail,E0599
/// use sim_system::execution::{PostOrderRequestV1, SimExecutionSessionV1};
/// fn old_name(session: &mut SimExecutionSessionV1, request: PostOrderRequestV1) {
///     session.submit_order(request);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use sim_system::execution::{OrderRequestV1, SimExecutionSessionV1};
/// fn old_query_name(session: &mut SimExecutionSessionV1, request: OrderRequestV1) {
///     session.query_order(request);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use sim_system::execution::{PostOrderRequestV1, SimExecutionSessionV1};
/// fn no_amend(session: &mut SimExecutionSessionV1, request: PostOrderRequestV1) {
///     session.amend_order(request);
/// }
/// ```
///
/// ```compile_fail,E0599
/// use sim_system::execution::{CancelAllOrdersRequestV1, SimExecutionSessionV1};
/// fn no_extra_sdk_method(
///     session: &mut SimExecutionSessionV1,
///     request: CancelAllOrdersRequestV1,
/// ) {
///     session.cancel_market_orders(request);
/// }
/// ```
///
/// ```compile_fail,E0308
/// use polymarket_client_sdk_v2::clob::types::SignedOrder;
/// use sim_system::execution::SimExecutionSessionV1;
/// fn no_signed_order(session: &mut SimExecutionSessionV1, order: SignedOrder) {
///     session.post_order(order);
/// }
/// ```
pub struct SimExecutionSessionV1 {
    session: SimSession,
    pending_requests: BTreeMap<String, PendingDomainRequestV1>,
    mutation_payloads: BTreeMap<String, Sha256Hex>,
}

impl SimExecutionSessionV1 {
    #[must_use]
    pub fn session_spec(&self) -> &SessionSpecV1 {
        self.session.session_spec()
    }

    #[must_use]
    pub fn model_visible_cursor(&self) -> ModelVisibleCursorV1 {
        self.session.model_visible_cursor()
    }

    pub fn post_order(
        &mut self,
        request: PostOrderRequestV1,
    ) -> Result<PostOrderRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        validate_post_item(&request.item)?;
        let item_id = request.item.item_id.clone();
        let method_timing = request.item.order.kind.method_timing();
        let schedule = self.register(
            PendingDomainRequestV1::PostOrder(request),
            vec![method_action(&item_id, 0, method_timing)],
        )?;
        Ok(PostOrderRegistrationV1 {
            receipt: registration_receipt(&schedule),
            item: registered_item(item_id, 0, &schedule),
        })
    }

    pub fn post_orders(
        &mut self,
        request: PostOrdersRequestV1,
    ) -> Result<PostOrdersRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        validate_batch_len(
            request.items.len(),
            POST_ORDERS_MAX_ITEMS,
            "批量提交项数必须为 1 至 15",
        )?;
        validate_unique_item_ids(request.items.iter().map(|item| item.item_id.as_str()))?;
        for item in &request.items {
            validate_post_item(item)?;
        }
        let item_specs = request
            .items
            .iter()
            .enumerate()
            .map(|(ordinal, item)| {
                (
                    item.item_id.clone(),
                    u16::try_from(ordinal).expect("批量提交上限适合 u16"),
                    item.order.kind.method_timing(),
                )
            })
            .collect::<Vec<_>>();
        let actions = item_specs
            .iter()
            .map(|(item_id, ordinal, timing)| method_action(item_id, *ordinal, *timing))
            .collect();
        let schedule = self.register(PendingDomainRequestV1::PostOrders(request), actions)?;
        Ok(PostOrdersRegistrationV1 {
            receipt: registration_receipt(&schedule),
            items: item_specs
                .into_iter()
                .map(|(item_id, ordinal, _)| registered_item(item_id, ordinal, &schedule))
                .collect(),
        })
    }

    pub fn cancel_order(
        &mut self,
        request: CancelOrderRequestV1,
    ) -> Result<CancelOrderRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        validate_cancel_item(&request.item)?;
        let item_id = request.item.item_id.clone();
        let schedule = self.register(
            PendingDomainRequestV1::CancelOrder(request),
            vec![method_action(&item_id, 0, ParsedMethodTimingV1::Ordinary)],
        )?;
        Ok(CancelOrderRegistrationV1 {
            receipt: registration_receipt(&schedule),
            item: registered_item(item_id, 0, &schedule),
        })
    }

    pub fn cancel_orders(
        &mut self,
        request: CancelOrdersRequestV1,
    ) -> Result<CancelOrdersRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        validate_batch_len(
            request.items.len(),
            CANCEL_ORDERS_MAX_ITEMS,
            "批量撤单项数必须为 1 至 1000",
        )?;
        validate_unique_item_ids(request.items.iter().map(|item| item.item_id.as_str()))?;
        for item in &request.items {
            validate_cancel_item(item)?;
        }

        let mut ordinal_by_order_id = BTreeMap::new();
        let mut actions = Vec::new();
        let mut item_mappings = Vec::with_capacity(request.items.len());
        for item in &request.items {
            let ordinal = if let Some(ordinal) = ordinal_by_order_id.get(&item.platform_order_id) {
                *ordinal
            } else {
                let ordinal = u16::try_from(actions.len()).expect("批量撤单上限适合 u16");
                ordinal_by_order_id.insert(item.platform_order_id.clone(), ordinal);
                actions.push(method_action(
                    &item.item_id,
                    ordinal,
                    ParsedMethodTimingV1::Ordinary,
                ));
                ordinal
            };
            item_mappings.push((item.item_id.clone(), ordinal));
        }
        let schedule = self.register(PendingDomainRequestV1::CancelOrders(request), actions)?;
        Ok(CancelOrdersRegistrationV1 {
            receipt: registration_receipt(&schedule),
            items: item_mappings
                .into_iter()
                .map(|(item_id, ordinal)| registered_item(item_id, ordinal, &schedule))
                .collect(),
        })
    }

    pub fn cancel_all_orders(
        &mut self,
        request: CancelAllOrdersRequestV1,
    ) -> Result<CancelAllOrdersRegistrationV1, SimExecutionContractErrorV1> {
        validate_idempotency_key(&request.idempotency_key)?;
        let schedule = self.register(
            PendingDomainRequestV1::CancelAllOrders(request),
            vec![method_action(
                "cancel-all",
                0,
                ParsedMethodTimingV1::Ordinary,
            )],
        )?;
        Ok(CancelAllOrdersRegistrationV1 {
            receipt: registration_receipt(&schedule),
        })
    }

    pub fn order(
        &mut self,
        request: OrderRequestV1,
    ) -> Result<OrderRegistrationV1, SimExecutionContractErrorV1> {
        validate_nonempty(&request.platform_order_id, "查询订单 ID 为空")?;
        let schedule = self.register(
            PendingDomainRequestV1::Order(request),
            vec![method_action("order", 0, ParsedMethodTimingV1::Ordinary)],
        )?;
        Ok(OrderRegistrationV1 {
            receipt: registration_receipt(&schedule),
        })
    }

    pub fn orders(
        &mut self,
        request: OrdersRequestV1,
    ) -> Result<OrdersRegistrationV1, SimExecutionContractErrorV1> {
        validate_orders_request(&request)?;
        let schedule = self.register(
            PendingDomainRequestV1::Orders(request),
            vec![method_action("orders", 0, ParsedMethodTimingV1::Ordinary)],
        )?;
        Ok(OrdersRegistrationV1 {
            receipt: registration_receipt(&schedule),
        })
    }

    pub fn trades(
        &mut self,
        request: TradesRequestV1,
    ) -> Result<TradesRegistrationV1, SimExecutionContractErrorV1> {
        validate_trades_request(&request)?;
        let schedule = self.register(
            PendingDomainRequestV1::Trades(request),
            vec![method_action("trades", 0, ParsedMethodTimingV1::Ordinary)],
        )?;
        Ok(TradesRegistrationV1 {
            receipt: registration_receipt(&schedule),
        })
    }

    fn register(
        &mut self,
        request: PendingDomainRequestV1,
        mut actions: Vec<ParsedLogicalActionV1>,
    ) -> Result<RegisteredLogicalScheduleV1, SimExecutionContractErrorV1> {
        let context = request.context().clone();
        let request_id = context.global_request_id.clone();
        if let Some(existing) = self.pending_requests.get(&request_id)
            && existing != &request
        {
            return Err(SimExecutionContractErrorV1::RequestIdentityConflict(
                request_id,
            ));
        }
        if let Some((idempotency_key, payload_sha256)) = mutation_binding(&request)
            && let Some(existing) = self.mutation_payloads.get(&idempotency_key)
            && existing != &payload_sha256
        {
            return Err(SimExecutionContractErrorV1::MutationIdentityConflict(
                idempotency_key,
            ));
        }

        for action in &mut actions {
            action.action_identity = format!(
                "{}:{}:{}",
                request.method_tag(),
                request_id,
                action.batch_item_ordinal
            );
        }
        let canonical_request_sha256 = canonical_request_sha256(&request);
        let schedule = self.session.register_and_schedule_request(
            context,
            canonical_request_sha256,
            actions,
        )?;

        self.pending_requests
            .entry(request_id)
            .or_insert(request.clone());
        if let Some((idempotency_key, payload_sha256)) = mutation_binding(&request) {
            self.mutation_payloads
                .entry(idempotency_key)
                .or_insert(payload_sha256);
        }
        Ok(schedule)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingDomainRequestV1 {
    PostOrder(PostOrderRequestV1),
    PostOrders(PostOrdersRequestV1),
    CancelOrder(CancelOrderRequestV1),
    CancelOrders(CancelOrdersRequestV1),
    CancelAllOrders(CancelAllOrdersRequestV1),
    Order(OrderRequestV1),
    Orders(OrdersRequestV1),
    Trades(TradesRequestV1),
}

impl PendingDomainRequestV1 {
    fn context(&self) -> &RequestContextV1 {
        match self {
            Self::PostOrder(request) => &request.context,
            Self::PostOrders(request) => &request.context,
            Self::CancelOrder(request) => &request.context,
            Self::CancelOrders(request) => &request.context,
            Self::CancelAllOrders(request) => &request.context,
            Self::Order(request) => &request.context,
            Self::Orders(request) => &request.context,
            Self::Trades(request) => &request.context,
        }
    }

    fn method_tag(&self) -> &'static str {
        match self {
            Self::PostOrder(_) => "post_order",
            Self::PostOrders(_) => "post_orders",
            Self::CancelOrder(_) => "cancel_order",
            Self::CancelOrders(_) => "cancel_orders",
            Self::CancelAllOrders(_) => "cancel_all_orders",
            Self::Order(_) => "order",
            Self::Orders(_) => "orders",
            Self::Trades(_) => "trades",
        }
    }
}

fn method_action(
    stable_item_identity: &str,
    batch_item_ordinal: u16,
    method_timing: ParsedMethodTimingV1,
) -> ParsedLogicalActionV1 {
    ParsedLogicalActionV1 {
        action_identity: stable_item_identity.to_owned(),
        batch_item_ordinal,
        method_timing,
    }
}

fn registration_receipt(schedule: &RegisteredLogicalScheduleV1) -> RequestRegistrationReceiptV1 {
    RequestRegistrationReceiptV1 {
        execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
        global_request_id: schedule
            .actions
            .first()
            .expect("登记调度至少包含一个动作")
            .global_request_id
            .clone(),
        disposition: schedule.request.disposition,
        transit: schedule.request.transit.clone(),
        scheduled_action_count: u16::try_from(schedule.actions.len())
            .expect("领域入口批量上限适合 u16"),
    }
}

fn registered_item(
    item_id: String,
    scheduled_action_ordinal: u16,
    schedule: &RegisteredLogicalScheduleV1,
) -> RegisteredItemV1 {
    let action = schedule
        .actions
        .get(usize::from(scheduled_action_ordinal))
        .expect("登记项映射必须指向已调度动作");
    RegisteredItemV1 {
        item_id,
        scheduled_action_ordinal,
        effective_processing_at_utc_ms: action.key.effective_processing_at_utc_ms,
    }
}

fn validate_idempotency_key(key: &str) -> Result<(), SimExecutionContractErrorV1> {
    validate_nonempty(key, "变更幂等键为空")
}

fn validate_post_item(item: &PostOrderItemV1) -> Result<(), SimExecutionContractErrorV1> {
    validate_nonempty(&item.item_id, "提交稳定项身份为空")?;
    validate_nonempty(&item.order.condition_id.0, "提交市场身份为空")?;
    validate_nonempty(&item.order.token_id.0, "提交资产身份为空")?;
    match &item.order.kind {
        SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            ..
        } => validate_positive_order_amounts(*limit_price_pusd_per_share, *size_shares),
        SimBuyOrderKindV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            expires_at_utc_ms,
            ..
        } => {
            validate_positive_order_amounts(*limit_price_pusd_per_share, *size_shares)?;
            if *expires_at_utc_ms <= 0 {
                return Err(SimExecutionContractErrorV1::InvalidRequest(
                    "GTD 到期时点必须为正整数毫秒",
                ));
            }
            Ok(())
        }
        SimBuyOrderKindV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        }
        | SimBuyOrderKindV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => validate_positive_order_amounts(*spend_pusd, *worst_price_pusd_per_share),
    }
}

fn validate_positive_order_amounts(
    primary: Decimal,
    price: Decimal,
) -> Result<(), SimExecutionContractErrorV1> {
    if primary <= Decimal::ZERO || price <= Decimal::ZERO {
        return Err(SimExecutionContractErrorV1::InvalidRequest(
            "订单金额、份额和价格必须为正数",
        ));
    }
    Ok(())
}

fn validate_cancel_item(item: &CancelOrderItemV1) -> Result<(), SimExecutionContractErrorV1> {
    validate_nonempty(&item.item_id, "撤单稳定项身份为空")?;
    validate_nonempty(&item.platform_order_id, "撤单平台订单 ID 为空")
}

fn validate_batch_len(
    actual: usize,
    maximum: usize,
    message: &'static str,
) -> Result<(), SimExecutionContractErrorV1> {
    if actual == 0 || actual > maximum {
        return Err(SimExecutionContractErrorV1::InvalidRequest(message));
    }
    Ok(())
}

fn validate_unique_item_ids<'a>(
    item_ids: impl Iterator<Item = &'a str>,
) -> Result<(), SimExecutionContractErrorV1> {
    let mut seen = BTreeSet::new();
    for item_id in item_ids {
        validate_nonempty(item_id, "批量稳定项身份为空")?;
        if !seen.insert(item_id) {
            return Err(SimExecutionContractErrorV1::InvalidRequest(
                "批量稳定项身份重复",
            ));
        }
    }
    Ok(())
}

fn validate_orders_request(request: &OrdersRequestV1) -> Result<(), SimExecutionContractErrorV1> {
    validate_optional_identity(
        request.filter.condition_id.as_ref().map(|id| id.0.as_str()),
        "订单列表市场过滤身份为空",
    )?;
    validate_optional_identity(
        request.filter.token_id.as_ref().map(|id| id.0.as_str()),
        "订单列表资产过滤身份为空",
    )?;
    validate_optional_identity(
        request.cursor.as_ref().map(OpaquePageCursorV1::as_str),
        "订单列表游标为空",
    )
}

fn validate_trades_request(request: &TradesRequestV1) -> Result<(), SimExecutionContractErrorV1> {
    validate_optional_identity(request.filter.trade_id.as_deref(), "成交过滤身份为空")?;
    validate_optional_identity(
        request.filter.condition_id.as_ref().map(|id| id.0.as_str()),
        "成交列表市场过滤身份为空",
    )?;
    validate_optional_identity(
        request.filter.token_id.as_ref().map(|id| id.0.as_str()),
        "成交列表资产过滤身份为空",
    )?;
    validate_optional_identity(
        request.cursor.as_ref().map(OpaquePageCursorV1::as_str),
        "成交列表游标为空",
    )?;
    if matches!(
        (request.filter.after_utc_ms, request.filter.before_utc_ms),
        (Some(after), Some(before)) if after >= before
    ) {
        return Err(SimExecutionContractErrorV1::InvalidRequest(
            "成交过滤起点必须早于终点",
        ));
    }
    Ok(())
}

fn validate_optional_identity(
    value: Option<&str>,
    message: &'static str,
) -> Result<(), SimExecutionContractErrorV1> {
    if value.is_some_and(str::is_empty) {
        return Err(SimExecutionContractErrorV1::InvalidRequest(message));
    }
    Ok(())
}

fn validate_nonempty(
    value: &str,
    message: &'static str,
) -> Result<(), SimExecutionContractErrorV1> {
    if value.is_empty() {
        return Err(SimExecutionContractErrorV1::InvalidRequest(message));
    }
    Ok(())
}

fn canonical_request_sha256(request: &PendingDomainRequestV1) -> Sha256Hex {
    let mut hash = CanonicalHasher::new(CANONICAL_REQUEST_HASH_DOMAIN);
    hash.text(request.method_tag());
    hash_context(&mut hash, request.context());
    hash_request_payload(&mut hash, request, true);
    hash.finish()
}

fn mutation_binding(request: &PendingDomainRequestV1) -> Option<(String, Sha256Hex)> {
    let idempotency_key = match request {
        PendingDomainRequestV1::PostOrder(request) => &request.idempotency_key,
        PendingDomainRequestV1::PostOrders(request) => &request.idempotency_key,
        PendingDomainRequestV1::CancelOrder(request) => &request.idempotency_key,
        PendingDomainRequestV1::CancelOrders(request) => &request.idempotency_key,
        PendingDomainRequestV1::CancelAllOrders(request) => &request.idempotency_key,
        PendingDomainRequestV1::Order(_)
        | PendingDomainRequestV1::Orders(_)
        | PendingDomainRequestV1::Trades(_) => return None,
    };
    let mut hash = CanonicalHasher::new(MUTATION_PAYLOAD_HASH_DOMAIN);
    hash.text(request.method_tag());
    hash_request_payload(&mut hash, request, false);
    Some((idempotency_key.clone(), hash.finish()))
}

fn hash_context(hash: &mut CanonicalHasher, context: &RequestContextV1) {
    hash.text(&context.execution_contract_version);
    hash.text(&context.session_id);
    hash.text(&context.account_id);
    hash.text(match context.caller_role {
        SimCallerRoleV1::ModelTrain => "MODEL_TRAIN",
        SimCallerRoleV1::OfflineEvaluation => "OFFLINE_EVALUATION",
        SimCallerRoleV1::Paper => "PAPER",
    });
    hash.text(&context.caller_instance_id);
    hash.text(&context.global_request_id);
    hash.optional_text(context.caller_correlation_id.as_deref());
    hash.u64(context.caller_sequence);
    hash.i64(context.request_sent_at_utc_ms);
    hash.text(&context.model_visible_cursor.session_id);
    hash.optional_i64(
        context
            .model_visible_cursor
            .public_visibility
            .closed_through_received_at_utc_ms(),
    );
    hash.u64(
        context
            .model_visible_cursor
            .public_visibility
            .next_visibility_index_position(),
    );
    hash.u64(context.model_visible_cursor.private_feedback_sequence);
}

fn hash_request_payload(
    hash: &mut CanonicalHasher,
    request: &PendingDomainRequestV1,
    include_idempotency_key: bool,
) {
    match request {
        PendingDomainRequestV1::PostOrder(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
            hash_post_item(hash, &request.item);
        }
        PendingDomainRequestV1::PostOrders(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
            hash.u64(request.items.len() as u64);
            for item in &request.items {
                hash_post_item(hash, item);
            }
        }
        PendingDomainRequestV1::CancelOrder(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
            hash_cancel_item(hash, &request.item);
        }
        PendingDomainRequestV1::CancelOrders(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
            hash.u64(request.items.len() as u64);
            for item in &request.items {
                hash_cancel_item(hash, item);
            }
        }
        PendingDomainRequestV1::CancelAllOrders(request) => {
            hash_optional_idempotency(hash, &request.idempotency_key, include_idempotency_key);
        }
        PendingDomainRequestV1::Order(request) => hash.text(&request.platform_order_id),
        PendingDomainRequestV1::Orders(request) => {
            hash.optional_text(request.filter.condition_id.as_ref().map(|id| id.0.as_str()));
            hash.optional_text(request.filter.token_id.as_ref().map(|id| id.0.as_str()));
            hash.optional_text(request.cursor.as_ref().map(OpaquePageCursorV1::as_str));
        }
        PendingDomainRequestV1::Trades(request) => {
            hash.optional_text(request.filter.trade_id.as_deref());
            hash.optional_text(request.filter.condition_id.as_ref().map(|id| id.0.as_str()));
            hash.optional_text(request.filter.token_id.as_ref().map(|id| id.0.as_str()));
            hash.optional_i64(request.filter.after_utc_ms);
            hash.optional_i64(request.filter.before_utc_ms);
            hash.optional_text(request.cursor.as_ref().map(OpaquePageCursorV1::as_str));
        }
    }
}

fn hash_optional_idempotency(hash: &mut CanonicalHasher, key: &str, include: bool) {
    if include {
        hash.text(key);
    }
}

fn hash_post_item(hash: &mut CanonicalHasher, item: &PostOrderItemV1) {
    hash.text(&item.item_id);
    hash.text(&item.order.condition_id.0);
    hash.text(&item.order.token_id.0);
    match &item.order.kind {
        SimBuyOrderKindV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
        } => {
            hash.text("GTC_LIMIT");
            hash.decimal(*limit_price_pusd_per_share);
            hash.decimal(*size_shares);
            hash.boolean(*post_only);
        }
        SimBuyOrderKindV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
            expires_at_utc_ms,
        } => {
            hash.text("GTD_LIMIT");
            hash.decimal(*limit_price_pusd_per_share);
            hash.decimal(*size_shares);
            hash.boolean(*post_only);
            hash.i64(*expires_at_utc_ms);
        }
        SimBuyOrderKindV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            hash.text("FOK_MARKET");
            hash.decimal(*spend_pusd);
            hash.decimal(*worst_price_pusd_per_share);
        }
        SimBuyOrderKindV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            hash.text("FAK_MARKET");
            hash.decimal(*spend_pusd);
            hash.decimal(*worst_price_pusd_per_share);
        }
    }
}

fn hash_cancel_item(hash: &mut CanonicalHasher, item: &CancelOrderItemV1) {
    hash.text(&item.item_id);
    hash.text(&item.platform_order_id);
}

struct CanonicalHasher(Sha256);

impl CanonicalHasher {
    fn new(domain: &str) -> Self {
        let mut hash = Self(Sha256::new());
        hash.text(domain);
        hash
    }

    fn text(&mut self, value: &str) {
        self.bytes(value.as_bytes());
    }

    fn bytes(&mut self, value: &[u8]) {
        self.0.update((value.len() as u64).to_be_bytes());
        self.0.update(value);
    }

    fn boolean(&mut self, value: bool) {
        self.bytes(&[u8::from(value)]);
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes(&value.to_be_bytes());
    }

    fn decimal(&mut self, value: Decimal) {
        self.text(&value.normalize().to_string());
    }

    fn optional_text(&mut self, value: Option<&str>) {
        match value {
            Some(value) => {
                self.boolean(true);
                self.text(value);
            }
            None => self.boolean(false),
        }
    }

    fn optional_i64(&mut self, value: Option<i64>) {
        match value {
            Some(value) => {
                self.boolean(true);
                self.i64(value);
            }
            None => self.boolean(false),
        }
    }

    fn finish(self) -> Sha256Hex {
        let mut encoded = String::with_capacity(64);
        for byte in self.0.finalize() {
            write!(&mut encoded, "{byte:02x}").expect("写入 String 不会失败");
        }
        Sha256Hex(encoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::latency_profile::{DelaySampleDomainV1, load_embedded_proxy_latency_profile};
    use crate::platform_rules::tests::successful_snapshot_for_sim_tests;
    use contracts::EXECUTION_MARKET_EVENT_SCHEMA_V3;

    fn sha(byte: &str) -> Sha256Hex {
        Sha256Hex(byte.repeat(64))
    }

    fn session_spec() -> SessionSpecV1 {
        SessionSpecV1 {
            session_id: "session-task13".to_owned(),
            account_id: "account-task13".to_owned(),
            caller_role: SimCallerRoleV1::ModelTrain,
            caller_instance_id: "caller-task13".to_owned(),
            market_set_sha256: sha("a"),
            scenario_sha256: sha("b"),
            session_seed: sha("c"),
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            market_event_schema_version: EXECUTION_MARKET_EVENT_SCHEMA_V3.to_owned(),
        }
    }

    fn context(
        session: &SimExecutionSessionV1,
        request_id: &str,
        caller_sequence: u64,
        request_sent_at_utc_ms: i64,
    ) -> RequestContextV1 {
        RequestContextV1 {
            execution_contract_version: SIM_EXECUTION_CONTRACT_V1.to_owned(),
            session_id: session.session_spec().session_id.clone(),
            account_id: session.session_spec().account_id.clone(),
            caller_role: session.session_spec().caller_role,
            caller_instance_id: session.session_spec().caller_instance_id.clone(),
            global_request_id: request_id.to_owned(),
            caller_correlation_id: Some("audit-only".to_owned()),
            caller_sequence,
            request_sent_at_utc_ms,
            model_visible_cursor: session.model_visible_cursor(),
        }
    }

    fn gtc_item(item_id: &str, price: i64) -> PostOrderItemV1 {
        PostOrderItemV1 {
            item_id: item_id.to_owned(),
            order: SimBuyOrderV1 {
                condition_id: ConditionId("condition-a".to_owned()),
                token_id: TokenId("token-up".to_owned()),
                kind: SimBuyOrderKindV1::GtcLimit {
                    limit_price_pusd_per_share: Decimal::new(price, 2),
                    size_shares: Decimal::new(200, 2),
                    post_only: false,
                },
            },
        }
    }

    fn fok_item(item_id: &str) -> PostOrderItemV1 {
        PostOrderItemV1 {
            item_id: item_id.to_owned(),
            order: SimBuyOrderV1 {
                condition_id: ConditionId("condition-a".to_owned()),
                token_id: TokenId("token-up".to_owned()),
                kind: SimBuyOrderKindV1::FokMarket {
                    spend_pusd: Decimal::new(100, 2),
                    worst_price_pusd_per_share: Decimal::new(60, 2),
                },
            },
        }
    }

    fn gtd_item(item_id: &str) -> PostOrderItemV1 {
        PostOrderItemV1 {
            item_id: item_id.to_owned(),
            order: SimBuyOrderV1 {
                condition_id: ConditionId("condition-a".to_owned()),
                token_id: TokenId("token-up".to_owned()),
                kind: SimBuyOrderKindV1::GtdLimit {
                    limit_price_pusd_per_share: Decimal::new(50, 2),
                    size_shares: Decimal::new(200, 2),
                    post_only: true,
                    expires_at_utc_ms: 1_000_000,
                },
            },
        }
    }

    fn fak_item(item_id: &str) -> PostOrderItemV1 {
        PostOrderItemV1 {
            item_id: item_id.to_owned(),
            order: SimBuyOrderV1 {
                condition_id: ConditionId("condition-a".to_owned()),
                token_id: TokenId("token-up".to_owned()),
                kind: SimBuyOrderKindV1::FakMarket {
                    spend_pusd: Decimal::new(100, 2),
                    worst_price_pusd_per_share: Decimal::new(60, 2),
                },
            },
        }
    }

    async fn execution_session() -> SimExecutionSessionV1 {
        let rules = successful_snapshot_for_sim_tests().await;
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let mut process = SimExecutionProcessV1::new(rules, profile);
        process.create_session(session_spec()).expect("session")
    }

    #[tokio::test(flavor = "current_thread")]
    async fn all_eight_methods_return_only_method_specific_registration_receipts() {
        let mut session = execution_session().await;

        let post_order = session
            .post_order(PostOrderRequestV1 {
                context: context(&session, "request-1", 1, 1_000),
                idempotency_key: "idem-1".to_owned(),
                item: gtc_item("item-1", 55),
            })
            .expect("post_order registration");
        let post_orders = session
            .post_orders(PostOrdersRequestV1 {
                context: context(&session, "request-2", 2, 1_001),
                idempotency_key: "idem-2".to_owned(),
                items: vec![gtc_item("item-2a", 54), fok_item("item-2b")],
            })
            .expect("post_orders registration");
        let cancel_order = session
            .cancel_order(CancelOrderRequestV1 {
                context: context(&session, "request-3", 3, 1_002),
                idempotency_key: "idem-3".to_owned(),
                item: CancelOrderItemV1 {
                    item_id: "item-3".to_owned(),
                    platform_order_id: "order-3".to_owned(),
                },
            })
            .expect("cancel_order registration");
        let cancel_orders = session
            .cancel_orders(CancelOrdersRequestV1 {
                context: context(&session, "request-4", 4, 1_003),
                idempotency_key: "idem-4".to_owned(),
                items: vec![CancelOrderItemV1 {
                    item_id: "item-4".to_owned(),
                    platform_order_id: "order-4".to_owned(),
                }],
            })
            .expect("cancel_orders registration");
        let cancel_all_orders = session
            .cancel_all_orders(CancelAllOrdersRequestV1 {
                context: context(&session, "request-5", 5, 1_004),
                idempotency_key: "idem-5".to_owned(),
            })
            .expect("cancel_all_orders registration");
        let order = session
            .order(OrderRequestV1 {
                context: context(&session, "request-6", 6, 1_005),
                platform_order_id: "order-6".to_owned(),
            })
            .expect("order registration");
        let orders = session
            .orders(OrdersRequestV1 {
                context: context(&session, "request-7", 7, 1_006),
                filter: OrdersFilterV1::default(),
                cursor: None,
            })
            .expect("orders registration");
        let trades = session
            .trades(TradesRequestV1 {
                context: context(&session, "request-8", 8, 1_007),
                filter: TradesFilterV1::default(),
                cursor: None,
            })
            .expect("trades registration");

        assert_eq!(post_order.receipt.scheduled_action_count, 1);
        assert_eq!(post_orders.receipt.scheduled_action_count, 2);
        assert_eq!(cancel_order.receipt.scheduled_action_count, 1);
        assert_eq!(cancel_orders.receipt.scheduled_action_count, 1);
        assert_eq!(cancel_all_orders.receipt.scheduled_action_count, 1);
        assert_eq!(order.receipt.scheduled_action_count, 1);
        assert_eq!(orders.receipt.scheduled_action_count, 1);
        assert_eq!(trades.receipt.scheduled_action_count, 1);
        for receipt in [
            &post_order.receipt,
            &post_orders.receipt,
            &cancel_order.receipt,
            &cancel_orders.receipt,
            &cancel_all_orders.receipt,
            &order.receipt,
            &orders.receipt,
            &trades.receipt,
        ] {
            assert_eq!(
                receipt.disposition,
                RequestRegistrationDispositionV1::NewLogicalRequest
            );
            assert_eq!(
                receipt.execution_contract_version,
                SIM_EXECUTION_CONTRACT_V1
            );
            assert_eq!(receipt.transit.platform_responded_at_utc_ms, None);
            assert_eq!(receipt.transit.response_received_at_utc_ms, None);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn physical_retry_reuses_timing_and_never_requeues_the_action() {
        let mut session = execution_session().await;
        let request = PostOrderRequestV1 {
            context: context(&session, "retry-request", 1, 2_000),
            idempotency_key: "retry-idem".to_owned(),
            item: gtc_item("retry-item", 50),
        };
        let first = session.post_order(request.clone()).expect("first");
        let pending_retry = session
            .post_order(request.clone())
            .expect("pending physical retry");

        assert_eq!(
            first.receipt.disposition,
            RequestRegistrationDispositionV1::NewLogicalRequest
        );
        assert_eq!(
            pending_retry.receipt.disposition,
            RequestRegistrationDispositionV1::PhysicalRetry
        );
        assert_eq!(first.receipt.transit, pending_retry.receipt.transit);
        assert_eq!(first.item, pending_retry.item);
        assert_eq!(session.pending_requests.len(), 1);

        let effective_at = first.item.effective_processing_at_utc_ms;
        session
            .session
            .close_platform_prefix_through(effective_at, 1)
            .expect("close platform prefix");
        let action = session
            .session
            .take_next_action_through(effective_at)
            .expect("take action")
            .expect("one action");
        let in_flight_retry = session
            .post_order(request.clone())
            .expect("in-flight physical retry");
        assert_eq!(first.receipt.transit, in_flight_retry.receipt.transit);
        assert_eq!(first.item, in_flight_retry.item);
        session
            .session
            .complete_logical_item(&action.action_identity)
            .expect("complete action");
        let completed_retry = session
            .post_order(request)
            .expect("completed physical retry");
        assert_eq!(first.receipt.transit, completed_retry.receipt.transit);
        assert_eq!(first.item, completed_retry.item);
        assert!(
            session
                .session
                .take_next_action_through(effective_at)
                .expect("no duplicate action")
                .is_none()
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn order_kind_alone_selects_the_frozen_itode_processing_delay() {
        let mut session = execution_session().await;
        let registrations = [
            session
                .post_order(PostOrderRequestV1 {
                    context: context(&session, "timing-gtc", 1, 4_000),
                    idempotency_key: "timing-gtc".to_owned(),
                    item: gtc_item("timing-gtc", 50),
                })
                .expect("GTC"),
            session
                .post_order(PostOrderRequestV1 {
                    context: context(&session, "timing-gtd", 2, 4_001),
                    idempotency_key: "timing-gtd".to_owned(),
                    item: gtd_item("timing-gtd"),
                })
                .expect("GTD"),
            session
                .post_order(PostOrderRequestV1 {
                    context: context(&session, "timing-fok", 3, 4_002),
                    idempotency_key: "timing-fok".to_owned(),
                    item: fok_item("timing-fok"),
                })
                .expect("FOK"),
            session
                .post_order(PostOrderRequestV1 {
                    context: context(&session, "timing-fak", 4, 4_003),
                    idempotency_key: "timing-fak".to_owned(),
                    item: fak_item("timing-fak"),
                })
                .expect("FAK"),
        ];

        let processing_delays = registrations.map(|registration| {
            registration.item.effective_processing_at_utc_ms
                - registration.receipt.transit.platform_arrived_at_utc_ms
        });
        assert_eq!(processing_delays, [0, 0, 250, 250]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn invalid_later_batch_item_leaves_the_request_identity_reusable() {
        let mut session = execution_session().await;
        let request_context = context(&session, "invalid-later-item", 1, 4_500);
        let mut invalid_item = gtc_item("item-b", 50);
        invalid_item.order.token_id = TokenId(String::new());
        assert!(matches!(
            session.post_orders(PostOrdersRequestV1 {
                context: request_context.clone(),
                idempotency_key: "batch-idem".to_owned(),
                items: vec![gtc_item("item-a", 50), invalid_item],
            }),
            Err(SimExecutionContractErrorV1::InvalidRequest(_))
        ));
        assert!(session.pending_requests.is_empty());
        assert!(session.mutation_payloads.is_empty());

        let corrected = session
            .post_orders(PostOrdersRequestV1 {
                context: request_context,
                idempotency_key: "batch-idem".to_owned(),
                items: vec![gtc_item("item-a", 50), gtc_item("item-b", 50)],
            })
            .expect("same request identity remains usable");
        assert_eq!(corrected.receipt.scheduled_action_count, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn batch_limits_accept_the_frozen_maxima_and_reject_the_next_item_atomically() {
        let mut session = execution_session().await;
        let post_context = context(&session, "post-limit", 1, 4_750);
        let post_items = (0..=POST_ORDERS_MAX_ITEMS)
            .map(|index| gtc_item(&format!("post-{index}"), 50))
            .collect::<Vec<_>>();
        assert!(matches!(
            session.post_orders(PostOrdersRequestV1 {
                context: post_context.clone(),
                idempotency_key: "post-limit".to_owned(),
                items: post_items.clone(),
            }),
            Err(SimExecutionContractErrorV1::InvalidRequest(_))
        ));
        let posts = session
            .post_orders(PostOrdersRequestV1 {
                context: post_context,
                idempotency_key: "post-limit".to_owned(),
                items: post_items[..POST_ORDERS_MAX_ITEMS].to_vec(),
            })
            .expect("15 post items");
        assert_eq!(posts.receipt.scheduled_action_count, 15);

        let cancel_context = context(&session, "cancel-limit", 2, 4_751);
        let cancel_items = (0..=CANCEL_ORDERS_MAX_ITEMS)
            .map(|index| CancelOrderItemV1 {
                item_id: format!("cancel-{index}"),
                platform_order_id: format!("platform-order-{index}"),
            })
            .collect::<Vec<_>>();
        assert!(matches!(
            session.cancel_orders(CancelOrdersRequestV1 {
                context: cancel_context.clone(),
                idempotency_key: "cancel-limit".to_owned(),
                items: cancel_items.clone(),
            }),
            Err(SimExecutionContractErrorV1::InvalidRequest(_))
        ));
        let cancellations = session
            .cancel_orders(CancelOrdersRequestV1 {
                context: cancel_context,
                idempotency_key: "cancel-limit".to_owned(),
                items: cancel_items[..CANCEL_ORDERS_MAX_ITEMS].to_vec(),
            })
            .expect("1000 cancel items");
        assert_eq!(cancellations.receipt.scheduled_action_count, 1_000);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn delayed_item_overflow_and_idempotency_conflict_leave_no_partial_registration() {
        let rules = successful_snapshot_for_sim_tests().await;
        assert!(rules.synchronized_rules().taker_order_delay_enabled);
        let profile = load_embedded_proxy_latency_profile().expect("profile");
        let spec = session_spec();
        let request_id = "overflow-request";
        let outbound_delay_ms = profile.sample_delay_ms(
            spec.session_seed.0.as_bytes(),
            request_id,
            DelaySampleDomainV1::RequestOutbound,
        );
        let sent_at = i64::MAX - i64::try_from(outbound_delay_ms).expect("delay fits i64") - 100;
        let mut process = SimExecutionProcessV1::new(rules, profile);
        let mut session = process.create_session(spec).expect("session");
        let overflow_context = context(&session, request_id, 1, sent_at);
        assert_eq!(
            session.post_order(PostOrderRequestV1 {
                context: overflow_context.clone(),
                idempotency_key: "overflow-idem".to_owned(),
                item: fok_item("overflow-item"),
            }),
            Err(SimExecutionContractErrorV1::TimeOverflow)
        );
        assert!(session.pending_requests.is_empty());
        assert!(session.mutation_payloads.is_empty());

        session
            .post_order(PostOrderRequestV1 {
                context: overflow_context,
                idempotency_key: "overflow-idem".to_owned(),
                item: gtc_item("overflow-item", 50),
            })
            .expect("same identity succeeds after zero-mutation failure");

        assert!(matches!(
            session.cancel_all_orders(CancelAllOrdersRequestV1 {
                context: context(&session, request_id, 1, sent_at),
                idempotency_key: "cross-method-idem".to_owned(),
            }),
            Err(SimExecutionContractErrorV1::RequestIdentityConflict(_))
        ));

        let conflicting_context = context(&session, "conflicting-request", 2, sent_at);
        assert!(matches!(
            session.post_order(PostOrderRequestV1 {
                context: conflicting_context.clone(),
                idempotency_key: "overflow-idem".to_owned(),
                item: gtc_item("different-item", 51),
            }),
            Err(SimExecutionContractErrorV1::MutationIdentityConflict(_))
        ));
        session
            .post_order(PostOrderRequestV1 {
                context: conflicting_context,
                idempotency_key: "corrected-idem".to_owned(),
                item: gtc_item("different-item", 51),
            })
            .expect("failed idempotency conflict does not consume sequence");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn duplicate_cancel_targets_schedule_once_but_keep_each_stable_item_mapping() {
        let mut session = execution_session().await;
        let registration = session
            .cancel_orders(CancelOrdersRequestV1 {
                context: context(&session, "duplicate-cancel", 1, 3_000),
                idempotency_key: "duplicate-cancel-idem".to_owned(),
                items: vec![
                    CancelOrderItemV1 {
                        item_id: "cancel-item-a".to_owned(),
                        platform_order_id: "same-order".to_owned(),
                    },
                    CancelOrderItemV1 {
                        item_id: "cancel-item-b".to_owned(),
                        platform_order_id: "same-order".to_owned(),
                    },
                ],
            })
            .expect("duplicate target registration");

        assert_eq!(registration.receipt.scheduled_action_count, 1);
        assert_eq!(registration.items.len(), 2);
        assert_eq!(registration.items[0].scheduled_action_ordinal, 0);
        assert_eq!(registration.items[1].scheduled_action_ordinal, 0);
        assert_eq!(
            registration.items[0].effective_processing_at_utc_ms,
            registration.items[1].effective_processing_at_utc_ms
        );
    }
}
