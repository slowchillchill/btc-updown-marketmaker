use crate::query::{BatchMatcher, BrowseQuery};
use crate::{
    dto::{ReportSourceKind, Snapshot, SourcesConfig, exact_json},
    sources::{
        backtest::{load_document, load_engineering_projection},
        noe::NoeResults,
        public_recording::PublicPages,
        research::{ResearchDocument, ResearchQuery},
    },
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
    noe: Mutex<NoeResults>,
    research: Mutex<ResearchDocument>,
}
pub type AppState = Arc<Vec<Run>>;
pub fn load_sources(config: SourcesConfig) -> Result<AppState, serde_json::Error> {
    let mut runs = Vec::new();
    for (index, config) in config.runs.into_iter().enumerate() {
        eprintln!("读取来源 {index}：报告/元数据/显式公共前缀");
        let report = match config.report_kind {
            ReportSourceKind::HistoricalBacktest => load_document(config.report.as_deref(), true),
            ReportSourceKind::EngineeringLedgerProjection => {
                load_engineering_projection(config.report.as_deref())
            }
        };
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
        let noe = NoeResults::new(config.noe_results);
        let research = ResearchDocument::open(config.research.as_deref());
        let snapshot = Snapshot {
            label: config.label.clone(),
            report,
            research: research.status(),
            report_kind: config.report_kind,
            metadata,
            public_recording: public_status.clone(),
            noe_results: noe.index(),
            gaps: vec![
                "NOE 历史：无安全枚举/批读；显式原引用可按需独立读取",
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
            noe: Mutex::new(noe),
            research: Mutex::new(research),
        });
    }
    Ok(Arc::new(runs))
}
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/runs", get(list))
        .route("/api/runs/{run}/snapshot", get(snapshot))
        .route("/api/runs/{run}/public", get(public))
        .route("/api/runs/{run}/public-query", get(public_query))
        .route("/api/runs/{run}/noe/{index}", get(noe))
        .route("/api/runs/{run}/research", get(research))
        .with_state(state)
}

async fn noe(State(state): State<AppState>, Path((run, index)): Path<(usize, usize)>) -> Response {
    if state.get(run).is_none() {
        return error(StatusCode::NOT_FOUND, "来源未在显式配置中");
    }
    match tokio::task::spawn_blocking(move || {
        state[run]
            .noe
            .lock()
            .map_err(|e| e.to_string())?
            .load(index)
    })
    .await
    {
        Ok(Ok(body)) => json_response(StatusCode::OK, body),
        Ok(Err(detail)) => error(StatusCode::NOT_FOUND, detail),
        Err(error_message) => error(StatusCode::INTERNAL_SERVER_ERROR, error_message.to_string()),
    }
}

async fn public_query(
    State(state): State<AppState>,
    Path(run): Path<usize>,
    Query(query): Query<BrowseQuery>,
) -> Response {
    let Some(source) = state.get(run) else {
        return error(StatusCode::NOT_FOUND, "运行未在显式来源配置中");
    };
    if source.public.is_none() {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            source.public_status.to_string(),
        );
    }
    match tokio::task::spawn_blocking(move || {
        let mut pages = state[run]
            .public
            .as_ref()
            .unwrap()
            .lock()
            .map_err(|e| e.to_string())?;
        let matcher = match BatchMatcher::new(&query) {
            Ok(matcher) => matcher,
            Err(detail) => {
                return Ok((
                    StatusCode::BAD_REQUEST,
                    Bytes::from(
                        serde_json::to_vec(
                            &json!({"detail":detail,"cursor":pages.cursor_status()}),
                        )
                        .map_err(|e| e.to_string())?,
                    ),
                ));
            }
        };
        // 一个 HTTP 只调用一次 page/current；空匹配是否继续由显式浏览动作控制。
        let batch = match query.page {
            Some(page) => pages.page(page),
            None => pages.current(),
        };
        let cursor = pages.cursor_status();
        let echoed = exact_json(serde_json::to_value(&query).map_err(|e| e.to_string())?);
        let (status, envelope) = match &batch {
            Ok(_) => {
                let (matching, reused) = pages.match_current(&matcher)?;
                (StatusCode::OK, json!({"query":echoed,"cursor":cursor,"matching":matching,"matching_reused":reused}))
            },
            Err(detail) => (
                StatusCode::CONFLICT,
                json!({"query":echoed,"cursor":cursor,"detail":detail}),
            ),
        };
        let mut response = serde_json::to_vec(&envelope).map_err(|e| e.to_string())?;
        if let Ok(batch) = batch {
            // 原批编码直接嵌入；新条件匹配解析当前批一次，最近相同条件复用小结果；传输不再解析/编码 rows，也不缓存第二份全量 JSON。
            response.pop();
            response.extend_from_slice(b",\"batch\":");
            response.extend_from_slice(&batch);
            response.push(b'}');
        }
        Ok::<_, String>((status, Bytes::from(response)))
    })
    .await
    {
        Ok(Ok((status, response))) => json_response(status, response),
        Ok(Err(detail)) => error(StatusCode::INTERNAL_SERVER_ERROR, detail),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
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

async fn research(
    State(state): State<AppState>,
    Path(run): Path<usize>,
    Query(query): Query<ResearchQuery>,
) -> Response {
    if state.get(run).is_none() {
        return error(StatusCode::NOT_FOUND, "来源未在显式配置中");
    }
    match tokio::task::spawn_blocking(move || {
        state[run]
            .research
            .lock()
            .map_err(|e| e.to_string())?
            .select(query)
    })
    .await
    {
        Ok(Ok(body)) => json_response(StatusCode::OK, body),
        Ok(Err(detail)) => error(StatusCode::UNPROCESSABLE_ENTITY, detail),
        Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
    }
}
