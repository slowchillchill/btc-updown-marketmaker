//! 只由公开请求身份准备订单身份；不读取受理、账户或隐藏行情状态。
use crate::execution::BusinessRequestV2;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) enum OrderIdentityModeV1 {
    #[default]
    LegacySequence,
    PublicRequestV1,
}
impl OrderIdentityModeV1 {
    pub(crate) fn is_legacy(&self) -> bool {
        *self == Self::LegacySequence
    }
    pub(crate) fn order_id(self, request: &str, ordinal: u16) -> Option<String> {
        match self {
            Self::LegacySequence => None,
            Self::PublicRequestV1 => Some(prepared_order_id_v1(request, ordinal)),
        }
    }
}

pub fn prepared_order_id_v1(global_request_id: &str, batch_item_ordinal: u16) -> String {
    format!(
        "sim-order-v1:{}:{}:{}",
        global_request_id.len(),
        global_request_id,
        batch_item_ordinal
    )
}
pub(crate) fn is_prepared_order_id(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("sim-order-v1:") else {
        return false;
    };
    let Some((length, rest)) = rest.split_once(':') else {
        return false;
    };
    let Ok(length_value) = length.parse::<usize>() else {
        return false;
    };
    if length_value == 0 || length_value.to_string() != length {
        return false;
    }
    let Some(request) = rest.get(..length_value) else {
        return false;
    };
    let Some(ordinal) = rest.get(length_value..).and_then(|s| s.strip_prefix(':')) else {
        return false;
    };
    let Ok(ordinal_value) = ordinal.parse::<u16>() else {
        return false;
    };
    prepared_order_id_v1(request, ordinal_value) == value
}
/// 已准备的原请求；只读身份按原 batch_item_ordinal 交接，发送消费同一正文。
pub struct PreparedBusinessRequestV1 {
    request: BusinessRequestV2,
}
impl PreparedBusinessRequestV1 {
    pub fn order_id(&self, ordinal: u16) -> Option<String> {
        let request = &self.request.context().global_request_id;
        match &self.request {
            BusinessRequestV2::PostOrder(r) if ordinal == 0 => {
                r.item.prepared_order_id(request, ordinal)
            }
            BusinessRequestV2::PostOrders(r) => r
                .items
                .iter()
                .find(|i| i.batch_item_ordinal == ordinal)
                .and_then(|i| i.item.prepared_order_id(request, ordinal)),
            _ => None,
        }
    }
    pub fn into_request(self) -> BusinessRequestV2 {
        self.request
    }
}
pub fn prepare_business_request_v1(mut request: BusinessRequestV2) -> PreparedBusinessRequestV1 {
    match &mut request {
        BusinessRequestV2::PostOrder(r) => {
            r.item.order_identity_mode = OrderIdentityModeV1::PublicRequestV1
        }
        BusinessRequestV2::PostOrders(r) => {
            for item in &mut r.items {
                item.item.order_identity_mode = OrderIdentityModeV1::PublicRequestV1;
            }
        }
        _ => {}
    }
    PreparedBusinessRequestV1 { request }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{PostOrderItemV1, SimBuyOrderKindV1, SimBuyOrderV1};
    use crate::runtime_v2::{PostOrderRequestV2, RequestContextV2};
    use contracts::{ConditionId, TokenId};
    use rust_decimal::Decimal;

    #[test]
    fn request_material_cannot_supply_another_order_id_and_legacy_mode_stays_legacy() {
        let request = BusinessRequestV2::PostOrder(PostOrderRequestV2 {
            context: RequestContextV2 {
                global_request_id: "新:request".into(),
                caller_correlation_id: None,
            },
            item: PostOrderItemV1::new(
                "item".into(),
                SimBuyOrderV1 {
                    condition_id: ConditionId(format!("0x{}", "1".repeat(64))),
                    token_id: TokenId("1".into()),
                    kind: SimBuyOrderKindV1::GtcLimit {
                        limit_price_pusd_per_share: Decimal::new(1, 2),
                        size_shares: Decimal::from(10),
                        post_only: true,
                    },
                },
            ),
        });
        let legacy = serde_json::to_value(&request).unwrap();
        assert!(
            legacy["request"]["item"]
                .get("order_identity_mode")
                .is_none()
        );
        let restored: BusinessRequestV2 = serde_json::from_value(legacy).unwrap();
        let BusinessRequestV2::PostOrder(old) = restored else {
            unreachable!()
        };
        assert!(
            old.item
                .prepared_order_id(&old.context.global_request_id, 0)
                .is_none()
        );
        let prepared = prepare_business_request_v1(request);
        let expected = prepared.order_id(0).unwrap();
        assert!(is_prepared_order_id(&expected));
        assert_ne!(expected, prepared_order_id_v1("新:request", 1));
        let mut material = serde_json::to_value(prepared.into_request()).unwrap();
        assert_eq!(material["method"], "POST_ORDER");
        assert_eq!(
            material["request"]["item"]["order_identity_mode"],
            "PublicRequestV1"
        );
        material["request"]["item"]["prepared_order_id"] = serde_json::json!("sim-1");
        let restored: BusinessRequestV2 = serde_json::from_value(material).unwrap();
        let BusinessRequestV2::PostOrder(new) = restored else {
            unreachable!()
        };
        assert_eq!(
            new.item
                .prepared_order_id(&new.context.global_request_id, 0),
            Some(expected)
        );
        assert!(
            serde_json::to_value(new.item)
                .unwrap()
                .get("prepared_order_id")
                .is_none()
        );
    }
}
