use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use crate::scenario::{PlatformMarketV1, PlatformScenarioV1};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionMarketDirectoryErrorV1 {
    Condition(String),
    ClobMarket(String),
    Token(String),
}

impl fmt::Display for SessionMarketDirectoryErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Condition(condition_id) => {
                write!(formatter, "会话市场目录 condition_id 重复：{condition_id}")
            }
            Self::ClobMarket(clob_market_id) => {
                write!(
                    formatter,
                    "会话市场目录 CLOB 市场身份重复：{clob_market_id}"
                )
            }
            Self::Token(token_id) => {
                write!(formatter, "会话市场目录 token_id 重复：{token_id}")
            }
        }
    }
}

impl std::error::Error for SessionMarketDirectoryErrorV1 {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SessionMarketDirectoryV1 {
    markets: BTreeMap<String, PlatformMarketV1>,
}

impl SessionMarketDirectoryV1 {
    pub(crate) fn try_from_scenario(
        scenario: &PlatformScenarioV1,
    ) -> Result<Self, SessionMarketDirectoryErrorV1> {
        let mut directory = Self {
            markets: BTreeMap::new(),
        };
        for market in scenario.market_set().markets() {
            directory.try_insert(market.clone())?;
        }
        Ok(directory)
    }

    pub(crate) fn try_insert(
        &mut self,
        market: PlatformMarketV1,
    ) -> Result<(), SessionMarketDirectoryErrorV1> {
        let condition_id = &market.market().condition_id.0;
        if self.markets.contains_key(condition_id) {
            return Err(SessionMarketDirectoryErrorV1::Condition(
                condition_id.clone(),
            ));
        }

        let clob_market_id = &market.market().clob_market_id.0;
        if self
            .markets
            .values()
            .any(|existing| existing.market().clob_market_id.0 == *clob_market_id)
        {
            return Err(SessionMarketDirectoryErrorV1::ClobMarket(
                clob_market_id.clone(),
            ));
        }

        for asset in market.assets() {
            let token_id = &asset.token_id.0;
            if self.markets.values().any(|existing| {
                existing
                    .assets()
                    .iter()
                    .any(|existing_asset| existing_asset.token_id.0 == *token_id)
            }) {
                return Err(SessionMarketDirectoryErrorV1::Token(token_id.clone()));
            }
        }

        self.markets.insert(condition_id.clone(), market);
        Ok(())
    }

    pub(crate) fn market(&self, condition_id: &str) -> Option<&PlatformMarketV1> {
        self.markets.get(condition_id)
    }

    pub(crate) fn markets(
        &self,
    ) -> impl ExactSizeIterator<Item = &PlatformMarketV1> + DoubleEndedIterator {
        self.markets.values()
    }

    pub(crate) fn contains(&self, condition_id: &str) -> bool {
        self.markets.contains_key(condition_id)
    }

    pub(crate) fn contains_token(&self, token_id: &str) -> bool {
        self.markets.values().any(|market| {
            market
                .assets()
                .iter()
                .any(|asset| asset.token_id.0 == token_id)
        })
    }
}

#[cfg(test)]
mod tests {
    use contracts::{
        AssetIdentityV1, ClobMarketId, ConditionId, MarketIdentityV2, MarketWindowV2,
        OutcomeIdentityV1, TokenId,
    };

    use super::*;

    fn market(
        condition_digit: char,
        clob_market_id: &str,
        first_token_id: &str,
        second_token_id: &str,
    ) -> PlatformMarketV1 {
        PlatformMarketV1::try_new(
            MarketIdentityV2 {
                condition_id: ConditionId(format!("0x{}", condition_digit.to_string().repeat(64))),
                clob_market_id: ClobMarketId(clob_market_id.to_owned()),
                window: MarketWindowV2 {
                    start_at_utc_ms: 1_000,
                    end_at_utc_ms: 2_000,
                },
            },
            [
                AssetIdentityV1 {
                    token_id: TokenId(first_token_id.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 0,
                        label: "Up".to_owned(),
                    },
                },
                AssetIdentityV1 {
                    token_id: TokenId(second_token_id.to_owned()),
                    outcome: OutcomeIdentityV1 {
                        index: 1,
                        label: "Down".to_owned(),
                    },
                },
            ],
        )
        .expect("valid platform market")
    }

    #[test]
    fn directory_appends_a_new_market_without_changing_the_initial_prefix() {
        let initial = market('1', "101", "201", "202");
        let scenario = PlatformScenarioV1::try_new(vec![initial.clone()]).expect("scenario");
        let mut directory =
            SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("directory");
        let appended = market('2', "102", "203", "204");

        directory
            .try_insert(appended.clone())
            .expect("append market");

        assert_eq!(
            directory.market(&initial.market().condition_id.0),
            Some(&initial)
        );
        assert_eq!(
            directory.market(&appended.market().condition_id.0),
            Some(&appended)
        );
        assert!(directory.contains(&appended.market().condition_id.0));
        assert_eq!(directory.markets().len(), 2);
    }

    #[test]
    fn rejected_identity_reuse_leaves_the_directory_unchanged() {
        let initial = market('1', "101", "201", "202");
        let scenario = PlatformScenarioV1::try_new(vec![initial]).expect("scenario");
        let directory = SessionMarketDirectoryV1::try_from_scenario(&scenario).expect("directory");
        let cases = [
            (
                market('1', "102", "203", "204"),
                SessionMarketDirectoryErrorV1::Condition(format!("0x{}", "1".repeat(64))),
            ),
            (
                market('2', "101", "203", "204"),
                SessionMarketDirectoryErrorV1::ClobMarket("101".to_owned()),
            ),
            (
                market('2', "102", "202", "204"),
                SessionMarketDirectoryErrorV1::Token("202".to_owned()),
            ),
        ];

        for (candidate, expected_error) in cases {
            let mut candidate_directory = directory.clone();
            assert_eq!(
                candidate_directory.try_insert(candidate),
                Err(expected_error)
            );
            assert_eq!(candidate_directory, directory);
        }
    }

    #[test]
    fn initial_prefix_rejects_cross_market_identity_reuse() {
        let scenario = PlatformScenarioV1::try_new(vec![
            market('1', "101", "201", "202"),
            market('2', "102", "202", "204"),
        ])
        .expect("scenario accepts cross-market token reuse");

        assert_eq!(
            SessionMarketDirectoryV1::try_from_scenario(&scenario),
            Err(SessionMarketDirectoryErrorV1::Token("202".to_owned()))
        );
    }
}
