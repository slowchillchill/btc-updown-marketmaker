use polymarket_client_sdk_v2::clob::types::response::{CancelOrdersResponse, PostOrderResponse};

pub type LiveIntentCallContextV1 = ();
pub type LiveIntentErrorV1 = polymarket_client_sdk_v2::error::Error;

#[derive(Debug, PartialEq)]
pub enum LiveIntentImmediateSuccessV1 {
    PostOrder(PostOrderResponse),
    PostOrders(Vec<PostOrderResponse>),
    Cancellation(CancelOrdersResponse),
}
