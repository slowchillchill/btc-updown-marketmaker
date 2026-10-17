//! 历史运行单元测试共用的默认装配。
#[path = "default.rs"]
mod fixture_default;
mod fixture_material;
pub(crate) use fixture_default::*;
pub(crate) use fixture_material::*;
#[path = "initialization.rs"]
mod fixture_initialization;
pub(crate) use fixture_initialization::*;
