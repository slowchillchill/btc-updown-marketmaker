use std::time::Duration;

const UNKNOWN_REGION: &str = "unknown";
const AWS_IMDS_TOKEN_URL: &str = "http://169.254.169.254/latest/api/token";
const AWS_IMDS_REGION_URL: &str = "http://169.254.169.254/latest/meta-data/placement/region";

pub fn resolve_collector_region_from_values(
    env_region: Option<&str>,
    imds_region: Option<&str>,
) -> String {
    env_region
        .and_then(non_empty_trimmed)
        .or_else(|| imds_region.and_then(non_empty_trimmed))
        .unwrap_or_else(|| UNKNOWN_REGION.to_owned())
}

pub async fn resolve_collector_region() -> String {
    let env_region = std::env::var("COLLECTOR_REGION").ok();
    if env_region.as_deref().and_then(non_empty_trimmed).is_some() {
        return resolve_collector_region_from_values(env_region.as_deref(), None);
    }

    let imds_region = fetch_aws_imds_region(Duration::from_secs(2)).await;
    resolve_collector_region_from_values(env_region.as_deref(), imds_region.as_deref())
}

async fn fetch_aws_imds_region(timeout: Duration) -> Option<String> {
    let client = reqwest::Client::builder().timeout(timeout).build().ok()?;
    let token = fetch_aws_imds_token(&client).await;
    let mut request = client.get(AWS_IMDS_REGION_URL);
    if let Some(token) = token.as_deref() {
        request = request.header("X-aws-ec2-metadata-token", token);
    }

    let response = request.send().await.ok()?;
    if !response.status().is_success() {
        return None;
    }
    response
        .text()
        .await
        .ok()
        .and_then(|value| non_empty_trimmed(&value))
}

async fn fetch_aws_imds_token(client: &reqwest::Client) -> Option<String> {
    let response = client
        .put(AWS_IMDS_TOKEN_URL)
        .header("X-aws-ec2-metadata-token-ttl-seconds", "60")
        .send()
        .await
        .ok()?;
    if !response.status().is_success() {
        return None;
    }
    response
        .text()
        .await
        .ok()
        .and_then(|value| non_empty_trimmed(&value))
}

fn non_empty_trimmed(value: &str) -> Option<String> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}
