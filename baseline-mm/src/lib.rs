//! 人工策略库。宿主组合公共观察与执行；本库不维护环境运行循环。
pub mod strategies;
pub use strategies::complementary_pair::*;

pub mod blb_research;
