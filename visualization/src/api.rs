use crate::{
    dto::{Snapshot, SourcesConfig},
    sources::{backtest::load_document, public_recording::PublicPages},
};
use axum::{
    Router,
    body::Body,
    extract::{Path, Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};

pub struct Run {
    label: String,
    snapshot: Bytes,
    public: Option<Mutex<PublicPages>>,
    public_status: Value,
}
pub type AppState = Arc<Vec<Run>>;
pub fn load_sources(config: SourcesConfig) -> Result<AppState, serde_json::Error> {
    let mut runs = Vec::new();
    for (index, config) in config.runs.into_iter().enumerate() {
        eprintln!("读取来源 {index}：报告/元数据/显式公共前缀");
        let report = load_document(Some(&config.report), true);
        let metadata = load_document(config.metadata.as_deref(), false);
        let (public, public_status) = match config.public_recording.as_ref() {
            None => (
                None,
                json!({"status":"not_configured","detail":"未交接显式公共根目录与已返回稳定前缀"}),
            ),
            Some(config) => match PublicPages::open(config) {
                Ok((reader, status)) => (Some(Mutex::new(reader)), status),
                Err(error) => (
                    None,
                    json!({"status":"read_error","root":config.root,"detail":error}),
                ),
            },
        };
        let snapshot = Snapshot {
            label: config.label.clone(),
            report,
            metadata,
            public_recording: public_status.clone(),
            gaps: vec![
                "NOE 历史：尚无稳定引用来源与安全枚举，未接入",
                "Ledger 历史：尚无独立耐久纯读接口，未接入",
                "策略逐次诊断与经济历史：原报告未保存，未接入",
                "训练/MLflow、PAPER/LIVE、独立评价：未接入",
            ],
        };
        runs.push(Run {
            label: config.label,
            snapshot: serde_json::to_vec(&snapshot)?.into(),
            public,
            public_status,
        });
    }
    Ok(Arc::new(runs))
}
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/runs", get(list))
        .route("/api/runs/{run}/snapshot", get(snapshot))
        .route("/api/runs/{run}/public", get(public))
        .with_state(state)
}
fn json_response(status: StatusCode, text: Bytes) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        Body::from(text),
    )
        .into_response()
}
fn error(status: StatusCode, detail: impl Into<String>) -> Response {
    json_response(
        status,
        json!({"detail":detail.into()})
            .to_string()
            .into_bytes()
            .into(),
    )
}
async fn list(State(state): State<AppState>) -> Response {
    json_response(StatusCode::OK, json!({"runs":state.iter().enumerate().map(|(i,r)|json!({"key":i.to_string(),"label":r.label})).collect::<Vec<_>>()}).to_string().into_bytes().into())
}
async fn snapshot(State(state): State<AppState>, Path(run): Path<usize>) -> Response {
    match state.get(run) {
        Some(run) => json_response(StatusCode::OK, run.snapshot.clone()),
        None => error(StatusCode::NOT_FOUND, "运行未在显式来源配置中"),
    }
}
#[derive(Deserialize)]
struct PageQuery {
    page: Option<u64>,
}
async fn public(
    State(state): State<AppState>,
    Path(run): Path<usize>,
    Query(query): Query<PageQuery>,
) -> Response {
    let Some(source) = state.get(run) else {
        return error(StatusCode::NOT_FOUND, "运行未在显式来源配置中");
    };
    if source.public.is_none() {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            source.public_status.to_string(),
        );
    };
    match tokio::task::spawn_blocking(move || {
        let mut pages = state[run]
            .public
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|e| e.to_string())?;
        match query.page {
            Some(page) => pages.page(page),
            None => pages.current(),
        }
    })
    .await
    {
        Ok(Ok(response)) => json_response(StatusCode::OK, response),
        Ok(Err(detail)) => error(StatusCode::CONFLICT, detail),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}
