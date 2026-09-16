use sim_system::execution::SimRunActorErrorV2;

// 两条有限入口只等待日志暂满；操作内部已成功的关闭/到期沿原状态继续。
pub(crate) fn feed<T, R>(
    state: &mut T,
    mut operation: impl FnMut(&mut T) -> Result<R, SimRunActorErrorV2>,
    mut wait: impl FnMut(&mut T) -> Result<(), SimRunActorErrorV2>,
) -> Result<R, SimRunActorErrorV2> {
    loop {
        match operation(state) {
            Err(SimRunActorErrorV2::RecordingBackpressured) => wait(state)?,
            result => return result,
        }
    }
}
