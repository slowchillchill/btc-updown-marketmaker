//! #181步骤6：真实模型消费者；环境与经济权威继续归公共模块。
pub mod domain;
pub mod encoding;
mod strategy;
pub mod worker;
pub use strategy::*;
mod run;
pub use run::*;

pub mod learning;

#[cfg(test)]
#[allow(dead_code, unused_imports)]
#[path = "../../backtest/examples/support/continuous_fixture.rs"]
mod learning_test_fixture;

mod rules;
pub use rules::{
    ConsumerRulesV1, FrozenRuleBindingV1, OpenedRulesV1, QueriedSourceV1, RuleOpenV1,
    open_consumer_rules,
};

pub mod diagnostic;
