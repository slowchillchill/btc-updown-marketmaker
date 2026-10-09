use contracts::PublicGroupReferenceV1;
use serde::{Deserialize, Serialize};
use sim_system::{
    execution::SimRunActorErrorV2, public_consumption::PublicConsumptionTimeSourceV1,
};

/// 所有延迟显式提供。序列不隐式重复或在耗尽后改用零延迟。
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DelayScheduleV1 {
    Fixed(u64),
    Sequence(Vec<u64>),
}
impl DelayScheduleV1 {
    pub(crate) fn at(&self, index: usize) -> Result<u64, SimRunActorErrorV2> {
        match self {
            Self::Fixed(value) => Ok(*value),
            Self::Sequence(values) => {
                values
                    .get(index)
                    .copied()
                    .ok_or(SimRunActorErrorV2::InvalidBusinessRequest(
                        "显式历史时序序列已耗尽",
                    ))
            }
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalTimingV1 {
    pub public_delivery_delay_ms: DelayScheduleV1,
    pub calculation_start_delay_ms: DelayScheduleV1,
    pub calculation_duration_ms: DelayScheduleV1,
    pub dispatch_delay_ms: DelayScheduleV1,
}
pub(crate) fn after(at: i64, delay: u64) -> Result<i64, SimRunActorErrorV2> {
    i64::try_from(delay)
        .ok()
        .and_then(|delay| at.checked_add(delay))
        .ok_or(SimRunActorErrorV2::InvalidBusinessRequest(
            "历史时序超出 UTC 毫秒定义域",
        ))
}
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct DeliveryTimes {
    pub delays: DelayScheduleV1,
    pub last: Option<i64>,
}
impl PublicConsumptionTimeSourceV1 for DeliveryTimes {
    fn execution_time_for_group(
        &mut self,
        _: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        Err(SimRunActorErrorV2::InvalidBusinessRequest(
            "历史供料需要原来源可见时刻",
        ))
    }
    fn execution_time_for_visible_group(
        &mut self,
        group: &PublicGroupReferenceV1,
        visible: i64,
    ) -> Result<i64, SimRunActorErrorV2> {
        let index = usize::try_from(group.group_position).map_err(|_| {
            SimRunActorErrorV2::InvalidBusinessRequest("公开组序号超出时序索引定义域")
        })?;
        let at = after(visible, self.delays.at(index)?)?;
        let at = self.last.map_or(at, |last| last.max(at));
        self.last = Some(at);
        Ok(at)
    }
}
