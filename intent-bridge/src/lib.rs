use std::future::Future;

use contracts::ModelIntentV1;

mod live_adapter;
mod sim_adapter;

pub use live_adapter::{
    LiveAdapterV1, LiveIntentCallContextV1, LiveIntentErrorV1, LiveIntentImmediateSuccessV1,
};
pub use sim_adapter::{SimGroupAdapterV1, request_from_intent_v2};

mod private {
    use super::{BridgeBackendV1, Future, ModelIntentV1};

    pub trait DispatchKey {}

    pub struct BridgeDispatchKey;

    impl DispatchKey for BridgeDispatchKey {}

    pub trait Sealed {
        type DispatchFuture<'a>: Future<
                Output = Result<
                    <Self as BridgeBackendV1>::ImmediateSuccess,
                    <Self as BridgeBackendV1>::Error,
                >,
            > + 'a
        where
            Self: BridgeBackendV1 + 'a;

        fn dispatch<'a, K>(
            &'a mut self,
            context: <Self as BridgeBackendV1>::CallContext,
            intent: &'a ModelIntentV1,
        ) -> Self::DispatchFuture<'a>
        where
            Self: BridgeBackendV1 + 'a,
            K: DispatchKey;
    }
}

/// [`IntentBridge`] 使用的封闭后端能力。
///
/// 后端集合由当前 crate 持有；下游 crate 不能增加另一种后端。
///
/// ```compile_fail
/// use intent_bridge::BridgeBackendV1;
///
/// struct ExternalBackend;
///
/// impl BridgeBackendV1 for ExternalBackend {
///     type CallContext = ();
///     type ImmediateSuccess = ();
///     type Error = ();
/// }
/// ```
///
/// ```compile_fail
/// use contracts::ModelIntentV1;
/// use intent_bridge::BridgeBackendV1;
///
/// fn bypass_bridge<B: BridgeBackendV1>(
///     backend: &mut B,
///     context: B::CallContext,
///     intent: &ModelIntentV1,
/// ) {
///     let _ = backend.dispatch(context, intent);
/// }
/// ```
pub trait BridgeBackendV1: private::Sealed {
    type CallContext;
    type ImmediateSuccess;
    type Error;
}

pub struct IntentBridge<B> {
    backend: B,
}

impl<B> IntentBridge<B> {
    pub const fn new(backend: B) -> Self {
        Self { backend }
    }
}

impl<'live, S, K> IntentBridge<LiveAdapterV1<'live, S, K>>
where
    S: polymarket_client_sdk_v2::auth::Signer,
    K: polymarket_client_sdk_v2::auth::Kind,
{
    pub const fn bind_live(
        client: &'live polymarket_client_sdk_v2::clob::Client<
            polymarket_client_sdk_v2::auth::state::Authenticated<K>,
        >,
        signer: &'live S,
    ) -> Self {
        Self::new(LiveAdapterV1::bind(client, signer))
    }
}

impl<B: BridgeBackendV1> IntentBridge<B> {
    pub async fn execute(
        &mut self,
        context: B::CallContext,
        intent: &ModelIntentV1,
    ) -> Result<B::ImmediateSuccess, B::Error> {
        <B as private::Sealed>::dispatch::<private::BridgeDispatchKey>(
            &mut self.backend,
            context,
            intent,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use std::future::{Future, Ready, ready};
    use std::task::{Context, Poll, Waker};

    use super::{
        BridgeBackendV1, IntentBridge, LiveIntentCallContextV1, LiveIntentErrorV1,
        LiveIntentImmediateSuccessV1, private,
    };
    use contracts::{ConditionId, ModelIntentV1};
    use polymarket_client_sdk_v2::clob::types::response::{
        CancelOrdersResponse, PostOrderResponse,
    };

    #[derive(Debug, PartialEq, Eq)]
    struct ProbeContext(&'static str);

    #[derive(Debug, PartialEq, Eq)]
    struct ProbeSuccess(&'static str);

    #[derive(Debug, PartialEq, Eq)]
    struct ProbeError(&'static str);

    struct ProbeBackend {
        calls: usize,
        seen_context: Option<ProbeContext>,
        seen_intent: Option<*const ModelIntentV1>,
        result: Option<Result<ProbeSuccess, ProbeError>>,
    }

    impl ProbeBackend {
        fn returning(result: Result<ProbeSuccess, ProbeError>) -> Self {
            Self {
                calls: 0,
                seen_context: None,
                seen_intent: None,
                result: Some(result),
            }
        }
    }

    impl private::Sealed for ProbeBackend {
        type DispatchFuture<'a> = Ready<Result<ProbeSuccess, ProbeError>>;

        fn dispatch<'a, K>(
            &'a mut self,
            context: <Self as BridgeBackendV1>::CallContext,
            intent: &'a ModelIntentV1,
        ) -> Self::DispatchFuture<'a>
        where
            Self: 'a,
            K: private::DispatchKey,
        {
            self.calls += 1;
            self.seen_context = Some(context);
            self.seen_intent = Some(std::ptr::from_ref(intent));
            ready(self.result.take().expect("测试后端只应分派一次"))
        }
    }

    impl BridgeBackendV1 for ProbeBackend {
        type CallContext = ProbeContext;
        type ImmediateSuccess = ProbeSuccess;
        type Error = ProbeError;
    }

    fn resolve_ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("测试后端应当即时返回"),
        }
    }

    fn cancel_intent() -> ModelIntentV1 {
        ModelIntentV1::CancelMarketOrders {
            condition_id: ConditionId("condition-82".to_owned()),
        }
    }

    #[test]
    fn bridge_dispatches_once_and_returns_success_unchanged() {
        let intent = cancel_intent();
        let backend = ProbeBackend::returning(Ok(ProbeSuccess("accepted")));
        let mut bridge = IntentBridge::new(backend);

        let result = resolve_ready(bridge.execute(ProbeContext("context-82"), &intent));

        assert_eq!(result, Ok(ProbeSuccess("accepted")));
        assert_eq!(bridge.backend.calls, 1);
        assert_eq!(
            bridge.backend.seen_context,
            Some(ProbeContext("context-82"))
        );
        assert!(std::ptr::eq(
            bridge.backend.seen_intent.expect("应记录分派的意图"),
            std::ptr::from_ref(&intent),
        ));
    }

    #[test]
    fn bridge_dispatches_once_and_returns_error_unchanged() {
        let intent = cancel_intent();
        let backend = ProbeBackend::returning(Err(ProbeError("backend-error")));
        let mut bridge = IntentBridge::new(backend);

        let result = resolve_ready(bridge.execute(ProbeContext("error-context"), &intent));

        assert_eq!(result, Err(ProbeError("backend-error")));
        assert_eq!(bridge.backend.calls, 1);
        assert_eq!(
            bridge.backend.seen_context,
            Some(ProbeContext("error-context"))
        );
    }

    #[test]
    fn live_boundary_has_unit_context_sdk_error_and_three_response_shapes() {
        let context: LiveIntentCallContextV1 = ();
        assert_eq!(context, ());

        fn error_type(value: polymarket_client_sdk_v2::error::Error) -> LiveIntentErrorV1 {
            value
        }
        let _ = error_type as fn(polymarket_client_sdk_v2::error::Error) -> LiveIntentErrorV1;

        fn assert_response_shape(success: LiveIntentImmediateSuccessV1) {
            match success {
                LiveIntentImmediateSuccessV1::PostOrder(response) => {
                    let _: PostOrderResponse = response;
                }
                LiveIntentImmediateSuccessV1::PostOrders(responses) => {
                    let _: Vec<PostOrderResponse> = responses;
                }
                LiveIntentImmediateSuccessV1::Cancellation(response) => {
                    let _: CancelOrdersResponse = response;
                }
            }
        }
        let _ = assert_response_shape as fn(LiveIntentImmediateSuccessV1);
    }
}
