use std::future::Future;

use contracts::ModelIntentV1;

mod live_adapter;
mod sim_adapter;

pub use live_adapter::{LiveIntentCallContextV1, LiveIntentErrorV1, LiveIntentImmediateSuccessV1};
pub use sim_adapter::{
    SimAdapterV1, SimIntentCallContextV1, SimIntentErrorV1, SimIntentImmediateSuccessV1,
};

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

impl<'execution> IntentBridge<SimAdapterV1<'execution>> {
    pub fn bind_sim(execution: &'execution mut sim_system::execution::SimExecution) -> Self {
        Self::new(SimAdapterV1::bind(execution))
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

    use contracts::{ConditionId, ModelIntentV1};
    use polymarket_client_sdk_v2::clob::types::response::{
        CancelOrdersResponse, PostOrderResponse,
    };
    use sim_system::execution::{
        MutationRequestReceiptV2, PublicVisibilityCursorV1, RequestContextV2, SimRunActorErrorV2,
        TrustedDecisionCutoffV2,
    };

    use super::{
        BridgeBackendV1, IntentBridge, LiveIntentCallContextV1, LiveIntentErrorV1,
        LiveIntentImmediateSuccessV1, SimIntentCallContextV1, SimIntentErrorV1,
        SimIntentImmediateSuccessV1, private,
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
    fn sim_call_context_contains_only_the_two_existing_contexts() {
        let call_context = SimIntentCallContextV1 {
            request_context: RequestContextV2 {
                global_request_id: "request-82".to_owned(),
                caller_correlation_id: Some("caller-82".to_owned()),
            },
            trusted_decision_cutoff: TrustedDecisionCutoffV2 {
                logical_time_utc_ms: 82_000,
                public_visibility: PublicVisibilityCursorV1::default(),
                private_feedback_sequence: 82,
            },
        };

        let SimIntentCallContextV1 {
            request_context,
            trusted_decision_cutoff,
        } = call_context;
        assert_eq!(request_context.global_request_id, "request-82");
        assert_eq!(trusted_decision_cutoff.logical_time_utc_ms, 82_000);

        fn success_type(value: MutationRequestReceiptV2) -> SimIntentImmediateSuccessV1 {
            value
        }
        fn error_type(value: SimRunActorErrorV2) -> SimIntentErrorV1 {
            value
        }
        let _ = success_type as fn(MutationRequestReceiptV2) -> SimIntentImmediateSuccessV1;
        let _ = error_type as fn(SimRunActorErrorV2) -> SimIntentErrorV1;
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
