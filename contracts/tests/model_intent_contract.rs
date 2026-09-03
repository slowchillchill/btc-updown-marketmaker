use contracts::{BuyLegIntentV1, BuyOrderIntentV1, ConditionId, ModelIntentV1, TokenId};
use rust_decimal::Decimal;

fn assert_buy_order(order: BuyOrderIntentV1, expected_kind: &str) {
    match order {
        BuyOrderIntentV1::GtcLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
        } => {
            assert_eq!(expected_kind, "gtc");
            assert_eq!(limit_price_pusd_per_share, Decimal::new(51, 2));
            assert_eq!(size_shares, Decimal::new(12, 0));
            assert!(post_only);
        }
        BuyOrderIntentV1::GtdLimit {
            limit_price_pusd_per_share,
            size_shares,
            post_only,
            expires_at_utc_sec,
        } => {
            assert_eq!(expected_kind, "gtd");
            assert_eq!(limit_price_pusd_per_share, Decimal::new(49, 2));
            assert_eq!(size_shares, Decimal::new(8, 0));
            assert!(!post_only);
            assert_eq!(expires_at_utc_sec, 1_800_000_123);
        }
        BuyOrderIntentV1::FokMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            assert_eq!(expected_kind, "fok");
            assert_eq!(spend_pusd, Decimal::new(25, 0));
            assert_eq!(worst_price_pusd_per_share, Decimal::new(55, 2));
        }
        BuyOrderIntentV1::FakMarket {
            spend_pusd,
            worst_price_pusd_per_share,
        } => {
            assert_eq!(expected_kind, "fak");
            assert_eq!(spend_pusd, Decimal::new(17, 0));
            assert_eq!(worst_price_pusd_per_share, Decimal::new(54, 2));
        }
    }
}

fn gtc() -> BuyOrderIntentV1 {
    BuyOrderIntentV1::GtcLimit {
        limit_price_pusd_per_share: Decimal::new(51, 2),
        size_shares: Decimal::new(12, 0),
        post_only: true,
    }
}

fn gtd() -> BuyOrderIntentV1 {
    BuyOrderIntentV1::GtdLimit {
        limit_price_pusd_per_share: Decimal::new(49, 2),
        size_shares: Decimal::new(8, 0),
        post_only: false,
        expires_at_utc_sec: 1_800_000_123,
    }
}

fn fok() -> BuyOrderIntentV1 {
    BuyOrderIntentV1::FokMarket {
        spend_pusd: Decimal::new(25, 0),
        worst_price_pusd_per_share: Decimal::new(55, 2),
    }
}

fn fak() -> BuyOrderIntentV1 {
    BuyOrderIntentV1::FakMarket {
        spend_pusd: Decimal::new(17, 0),
        worst_price_pusd_per_share: Decimal::new(54, 2),
    }
}

#[test]
fn buy_order_contract_has_exactly_the_four_frozen_variants() {
    for (order, kind) in [
        (gtc(), "gtc"),
        (gtd(), "gtd"),
        (fok(), "fok"),
        (fak(), "fak"),
    ] {
        assert_buy_order(order, kind);
    }
}

#[test]
fn model_intent_contract_has_exactly_five_actions_and_preserves_order() {
    let intents = [
        ModelIntentV1::SubmitSingleBuy {
            condition_id: ConditionId("condition-single".to_owned()),
            order: BuyLegIntentV1 {
                token_id: TokenId("token-single".to_owned()),
                order: gtc(),
            },
        },
        ModelIntentV1::SubmitPairedBuy {
            condition_id: ConditionId("condition-paired".to_owned()),
            orders: [
                BuyLegIntentV1 {
                    token_id: TokenId("token-a".to_owned()),
                    order: gtd(),
                },
                BuyLegIntentV1 {
                    token_id: TokenId("token-b".to_owned()),
                    order: fok(),
                },
            ],
        },
        ModelIntentV1::CancelOrder {
            platform_order_id: "order-one".to_owned(),
        },
        ModelIntentV1::CancelOrders {
            platform_order_ids: vec!["order-three".to_owned(), "order-one".to_owned()],
        },
        ModelIntentV1::CancelMarketOrders {
            condition_id: ConditionId("condition-cancel".to_owned()),
        },
    ];

    for (ordinal, intent) in intents.into_iter().enumerate() {
        match intent {
            ModelIntentV1::SubmitSingleBuy {
                condition_id,
                order: BuyLegIntentV1 { token_id, order },
            } => {
                assert_eq!(ordinal, 0);
                assert_eq!(condition_id.0, "condition-single");
                assert_eq!(token_id.0, "token-single");
                assert_buy_order(order, "gtc");
            }
            ModelIntentV1::SubmitPairedBuy {
                condition_id,
                orders: [first, second],
            } => {
                assert_eq!(ordinal, 1);
                assert_eq!(condition_id.0, "condition-paired");
                assert_eq!(first.token_id.0, "token-a");
                assert_eq!(second.token_id.0, "token-b");
                assert_buy_order(first.order, "gtd");
                assert_buy_order(second.order, "fok");
            }
            ModelIntentV1::CancelOrder { platform_order_id } => {
                assert_eq!(ordinal, 2);
                assert_eq!(platform_order_id, "order-one");
            }
            ModelIntentV1::CancelOrders { platform_order_ids } => {
                assert_eq!(ordinal, 3);
                assert_eq!(platform_order_ids, ["order-three", "order-one"]);
            }
            ModelIntentV1::CancelMarketOrders { condition_id } => {
                assert_eq!(ordinal, 4);
                assert_eq!(condition_id.0, "condition-cancel");
            }
        }
    }
}
