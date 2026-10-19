//! #181步骤6：真实模型消费者；环境与经济权威继续归公共模块。
pub mod domain;
pub mod encoding;
mod strategy;
pub mod worker;
pub use strategy::*;
mod run;
pub use run::*;
