use contracts::*;
use sim_system::{execution::*, public_consumption::*};

#[derive(Clone)]
pub struct TradingTimes(pub Vec<i64>);
impl PublicConsumptionTimeSourceV1 for TradingTimes {
    fn execution_time_for_group(
        &mut self,
        group: &PublicGroupReferenceV1,
    ) -> Result<i64, SimRunActorErrorV2> {
        Ok(self.0[group.group_position as usize])
    }
}
