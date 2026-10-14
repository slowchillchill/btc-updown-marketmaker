#![cfg(feature = "clob")]
mod common;
use alloy::primitives::{B256, U256};
use alloy::signers::{Signer, local::LocalSigner};
use httpmock::MockServer;
use polymarket_client_sdk_v2::{
    auth::Credentials,
    clob::{
        Client, Config,
        types::{OrderPayload, OrderType, OrderV1, OrderV2, SignableOrder, SignatureType},
    },
    types::address,
};
use std::str::FromStr;

// Independent expected hashes were generated with the fixed official TS builders,
// not with the Rust function under test. Includes the upstream V1 published vector.
#[tokio::test]
async fn official_order_hash_vectors_cover_v1_v2_domains_and_poly1271_base_id() -> anyhow::Result<()>
{
    let fixture: serde_json::Value = serde_json::from_str(include_str!("order_hash_vectors.json"))?;
    for row in fixture["vectors"].as_array().unwrap() {
        let server = MockServer::start_async().await;
        let neg = row["neg_risk"].as_bool().unwrap();
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/neg-risk");
            then.status(200)
                .json_body(serde_json::json!({"neg_risk":neg}));
        });
        let signer = LocalSigner::from_str(common::PRIVATE_KEY)?
            .with_chain_id(Some(row["chain_id"].as_u64().unwrap()));
        let o = &row["order"];
        let signature_type = o["signatureType"].as_u64().unwrap();
        let mut auth = Client::new(&server.base_url(), Config::default())?
            .authentication_builder(&signer)
            .credentials(Credentials::new(
                common::API_KEY,
                common::SECRET.to_owned(),
                common::PASSPHRASE.to_owned(),
            ));
        if signature_type == 3 {
            auth = auth
                .funder(address!("0x1111111111111111111111111111111111111111"))
                .signature_type(SignatureType::Poly1271);
        }
        let client = auth.authenticate().await?;
        let amount = |key: &str| U256::from_str(o[key].as_str().unwrap()).unwrap();
        let payload = if row["version"] == 1 {
            let mut order = OrderV1::default();
            order.salt = amount("salt");
            order.maker = o["maker"].as_str().unwrap().parse()?;
            order.signer = o["signer"].as_str().unwrap().parse()?;
            order.taker = o["taker"].as_str().unwrap().parse()?;
            order.tokenId = amount("tokenId");
            order.makerAmount = amount("makerAmount");
            order.takerAmount = amount("takerAmount");
            order.expiration = amount("expiration");
            order.nonce = amount("nonce");
            order.feeRateBps = amount("feeRateBps");
            order.side = 0;
            order.signatureType = signature_type as u8;
            OrderPayload::new_v1(order)
        } else {
            let mut order = OrderV2::default();
            order.salt = amount("salt");
            order.maker = o["maker"].as_str().unwrap().parse()?;
            order.signer = o["signer"].as_str().unwrap().parse()?;
            order.tokenId = amount("tokenId");
            order.makerAmount = amount("makerAmount");
            order.takerAmount = amount("takerAmount");
            order.timestamp = amount("timestamp");
            order.metadata = o["metadata"].as_str().unwrap().parse()?;
            order.builder = o["builder"].as_str().unwrap().parse()?;
            order.side = 0;
            order.signatureType = signature_type as u8;
            OrderPayload::new(order, amount("expiration"))
        };
        let signable = SignableOrder::builder()
            .payload(payload)
            .order_type(OrderType::GTC)
            .build();
        let (signed, hash) = client.sign_and_hash(&signer, signable).await?;
        assert_eq!(
            hash,
            B256::from_str(row["hash"].as_str().unwrap())?,
            "{row}"
        );
        if signature_type == 3 {
            assert!(signed.signature.to_string().len() > 132);
        }
        let wire = serde_json::to_value(&signed)?;
        assert!(wire.get("orderHash").is_none() && wire.get("order_hash").is_none());
    }
    Ok(())
}
