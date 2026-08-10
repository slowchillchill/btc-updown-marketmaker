use std::fmt;

use contracts::Sha256Hex;
use polymarket_client_sdk_v2::clob::types::Amount;
use rust_decimal::Decimal;
use sha2::{Digest, Sha256};

pub const INITIAL_ACCOUNT_STATE_SCHEMA_V1: &str = "initial_account_state_v1";
const INITIAL_ACCOUNT_STATE_HASH_DOMAIN_V1: &str = "SIM_INITIAL_ACCOUNT_STATE_V1";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InitialAccountStateV1 {
    available_pusd: Decimal,
    sha256: Sha256Hex,
}

impl InitialAccountStateV1 {
    pub fn try_new(available_pusd: Decimal) -> Result<Self, InitialAccountStateErrorV1> {
        if available_pusd.is_sign_negative() {
            return Err(InitialAccountStateErrorV1::NegativeAvailablePusd);
        }
        let available_pusd = Amount::usdc(available_pusd)
            .map_err(|_| InitialAccountStateErrorV1::InvalidPusdPrecision)?
            .as_inner();
        let mut hash = Sha256::new();
        hash.update(INITIAL_ACCOUNT_STATE_HASH_DOMAIN_V1);
        hash.update((INITIAL_ACCOUNT_STATE_SCHEMA_V1.len() as u64).to_be_bytes());
        hash.update(INITIAL_ACCOUNT_STATE_SCHEMA_V1);
        let canonical = available_pusd.normalize().to_string();
        hash.update((canonical.len() as u64).to_be_bytes());
        hash.update(canonical);
        let sha256 = Sha256Hex(format!("{:x}", hash.finalize()));
        Ok(Self {
            available_pusd,
            sha256,
        })
    }

    #[must_use]
    pub fn available_pusd(&self) -> Decimal {
        self.available_pusd
    }

    #[must_use]
    pub fn initial_outcome_balance_shares(&self) -> Decimal {
        Decimal::ZERO
    }

    #[must_use]
    pub fn initial_encumbered_pusd(&self) -> Decimal {
        Decimal::ZERO
    }

    #[must_use]
    pub fn initial_rebate_receivable_pusd(&self) -> Decimal {
        Decimal::ZERO
    }

    #[must_use]
    pub fn sha256(&self) -> &Sha256Hex {
        &self.sha256
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitialAccountStateErrorV1 {
    NegativeAvailablePusd,
    InvalidPusdPrecision,
}

impl fmt::Display for InitialAccountStateErrorV1 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegativeAvailablePusd => write!(formatter, "初始可用 pUSD 不能为负数"),
            Self::InvalidPusdPrecision => write!(formatter, "初始可用 pUSD 最多保留 6 位小数"),
        }
    }
}

impl std::error::Error for InitialAccountStateErrorV1 {}
