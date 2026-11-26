use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::Json;
use rdkafka::consumer::Consumer;

use crate::core::cluster::Cluster;
use crate::core::groups;
use crate::core::messages::{self, ReadOptions, StartPosition};
use crate::core::produce;
use crate::core::topics;
use crate::core::overview;
use crate::error::{AppError, AppResult};
use crate::state::AppState;
use crate::web::dto::{
    AddPartitionsBody, ClusterBody, ClusterQuery, MessagesQuery, ProduceBody, ResetOffsetsBody,
    TopicQuery,
};
use crate::web::sse::message_stream;

pub async fn health() -> &'static str {
    "ok"
}

pub async fn clusters(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "clusters": state.cluster_meta().await }))
}

/// 新增或更新一个集群配置（页面端维护，落盘到 config/clusters.json）
pub async fn save_cluster(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterBody>,
) -> AppResult<Json<serde_json::Value>> {
    let cfg = resolve_cluster_body(&state, body).await?;
    let name = cfg.name.clone();
    state.upsert_cluster(cfg).await?;
    tracing::info!(cluster = %name, "集群配置已保存");
    Ok(Json(serde_json::json!({ "ok": true, "name": name })))
}

pub async fn delete_cluster(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    let removed = state.remove_cluster(&name).await?;
    if !removed {
        return Err(AppError::ClusterNotFound(name));
    }
    tracing::info!(cluster = %name, "集群配置已删除");
    Ok(Json(serde_json::json!({ "ok": true })))
}

/// 断开并释放某个集群的客户端连接（页面端切换集群时调用）。
/// 只释放内存里的连接与 librdkafka 线程，不动落盘的配置。
pub async fn disconnect_cluster(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> AppResult<Json<serde_json::Value>> {
    let released = state.disconnect_cluster(&name).await;
    tracing::info!(cluster = %name, released, "集群连接已断开");
    Ok(Json(serde_json::json!({ "ok": true, "released": released })))
}

/// 测试连通性：临时建一个客户端取 metadata，不影响已保存的配置
pub async fn test_cluster(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ClusterBody>,
) -> AppResult<Json<serde_json::Value>> {
    let cfg = resolve_cluster_body(&state, body).await?;
    let started = std::time::Instant::now();
    let cluster = Cluster::new(&cfg, Duration::from_secs(state.server.request_timeout_secs))
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    let timeout = cluster.timeout;
    let consumer = Arc::clone(&cluster.consumer);
    let meta = tokio::task::spawn_blocking(move || consumer.fetch_metadata(None, timeout))
        .await
        .map_err(|e| AppError::Internal(format!("metadata 任务失败: {}", e)))?
        .map_err(|e| AppError::BadRequest(format!("连接失败: {}", e)))?;

    Ok(Json(serde_json::json!({
        "ok": true,
        "brokers": meta.brokers().len(),
        "topics": meta.topics().len(),
        "elapsed_ms": started.elapsed().as_millis() as u64,
    })))
}

/// 把请求体转成 ClusterConfig：补默认值、校验字段、密码留空时沿用原值
async fn resolve_cluster_body(
    state: &AppState,
    body: ClusterBody,
) -> AppResult<crate::config::ClusterConfig> {
    let name = body.name.trim().to_string();
    if name.is_empty() {
        return Err(AppError::BadRequest("集群名称不能为空".into()));
    }
    if name.len() > 64
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(AppError::BadRequest(
            "集群名称只允许字母、数字、下划线、短横线和点，且不超过 64 字符".into(),
        ));
    }
    let servers = body.bootstrap_servers.trim().to_string();
    if servers.is_empty() {
        return Err(AppError::BadRequest("bootstrap.servers 不能为空".into()));
    }

    let protocol = body
        .security_protocol
        .as_deref()
        .unwrap_or("PLAINTEXT")
        .trim()
        .to_uppercase();
    if !matches!(
        protocol.as_str(),
        "PLAINTEXT" | "SSL" | "SASL_PLAINTEXT" | "SASL_SSL"
    ) {
        return Err(AppError::BadRequest(format!(
            "不支持的 security.protocol: {}",
            protocol
        )));
    }

    let old = state.store.get(&name).await;
    // 空字符串视为「不修改」，避免前端脱敏回填后把密码清空
    let keep_or = |new: Option<String>, old: Option<String>| -> Option<String> {
        match new {
            Some(v) if !v.trim().is_empty() => Some(v),
            _ => old,
        }
    };

    Ok(crate::config::ClusterConfig {
        name,
        bootstrap_servers: servers,
        security_protocol: protocol,
        sasl_mechanism: blank_to_none(body.sasl_mechanism),
        sasl_username: blank_to_none(body.sasl_username),
        sasl_password: keep_or(
            body.sasl_password,
            old.as_ref().and_then(|o| o.sasl_password.clone()),
        ),
        ssl_ca_location: blank_to_none(body.ssl_ca_location),
        ssl_certificate_location: blank_to_none(body.ssl_certificate_location),
        ssl_key_location: blank_to_none(body.ssl_key_location),
        ssl_key_password: keep_or(
            body.ssl_key_password,
            old.as_ref().and_then(|o| o.ssl_key_password.clone()),
        ),
        log_level: body
            .log_level
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| "3".to_string()),
    })
}

fn blank_to_none(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

/// 集群概览：broker 拓扑、总量、健康度、消费组分布、消息量 top topic
pub async fn overview(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ClusterQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let dto = overview::overview(Arc::clone(&state), &q.cluster).await?;
    Ok(Json(serde_json::json!(dto)))
}

/// 发送一条消息
pub async fn produce(
    State(state): State<Arc<AppState>>,
    Json(body): Json<ProduceBody>,
) -> AppResult<Json<serde_json::Value>> {
    let req = produce::ProduceRequest {
        topic: body.topic,
        partition: body.partition,
        key: body.key,
        value: body.value,
        headers: body.headers,
    };
    let result = produce::produce(Arc::clone(&state), &body.cluster, req).await?;
    Ok(Json(serde_json::json!(result)))
}

/// broker 关键配置（只读）
pub async fn broker_configs(
    State(state): State<Arc<AppState>>,
    Path(broker_id): Path<i32>,
    Query(q): Query<ClusterQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let cluster = state.cluster(&q.cluster).await?;
    let list = topics::broker_configs(&cluster, broker_id).await?;
    Ok(Json(serde_json::json!({ "broker_id": broker_id, "configs": list })))
}

pub async fn topics(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ClusterQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let cluster = state.cluster(&q.cluster).await?;
    let list = topics::list_topics_with_watermarks(cluster).await?;
    Ok(Json(serde_json::json!({ "topics": list })))
}

pub async fn partitions(
    State(state): State<Arc<AppState>>,
    Query(q): Query<TopicQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let cluster = state.cluster(&q.cluster).await?;
    let list = topics::partitions(cluster, &q.topic).await?;
    Ok(Json(serde_json::json!({ "partitions": list })))
}

pub async fn groups(
    State(state): State<Arc<AppState>>,
    Query(q): Query<ClusterQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let list = groups::list_groups(Arc::clone(&state), &q.cluster).await?;
    Ok(Json(serde_json::json!({ "groups": list })))
}

pub async fn group_lag(
    State(state): State<Arc<AppState>>,
    Path(group): Path<String>,
    Query(q): Query<ClusterQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let lag = groups::group_lag(Arc::clone(&state), &q.cluster, &group).await?;
    Ok(Json(serde_json::json!(lag)))
}

/// topic 详情：分区（含 leader/副本/ISR/水位）+ 配置项
pub async fn topic_detail(
    State(state): State<Arc<AppState>>,
    Path(topic): Path<String>,
    Query(q): Query<ClusterQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let cluster = state.cluster(&q.cluster).await?;
    let detail = topics::topic_detail(cluster, &topic).await?;
    Ok(Json(serde_json::json!(detail)))
}

/// 删除 topic（不可逆）
pub async fn delete_topic(
    State(state): State<Arc<AppState>>,
    Path(topic): Path<String>,
    Query(q): Query<ClusterQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let cluster = state.cluster(&q.cluster).await?;
    let name = topics::delete_topic(cluster, &topic).await?;
    tracing::warn!(topic = %name, "已删除 topic");
    Ok(Json(serde_json::json!({ "deleted": name })))
}

/// 增加分区：body 里的 partition_count 为操作完成后的分区总数
pub async fn add_partitions(
    State(state): State<Arc<AppState>>,
    Path(topic): Path<String>,
    Query(q): Query<ClusterQuery>,
    Json(body): Json<AddPartitionsBody>,
) -> AppResult<Json<serde_json::Value>> {
    let cluster = state.cluster(&q.cluster).await?;
    let total = topics::add_partitions(cluster, &topic, body.partition_count).await?;
    Ok(Json(serde_json::json!({
        "topic": topic,
        "partition_count": total,
    })))
}

/// 按 topic 查消费详情：哪些组在消费、逐分区积压
pub async fn topic_consumers(
    State(state): State<Arc<AppState>>,
    Path(topic): Path<String>,
    Query(q): Query<ClusterQuery>,
) -> AppResult<Json<serde_json::Value>> {
    let list = groups::topic_consumers(Arc::clone(&state), &q.cluster, &topic).await?;
    Ok(Json(serde_json::json!({ "groups": list })))
}

/// 重置消费位点：POST /groups/{group}/offsets
pub async fn reset_group_offsets(
    State(state): State<Arc<AppState>>,
    Path(group): Path<String>,
    Json(body): Json<ResetOffsetsBody>,
) -> AppResult<Json<serde_json::Value>> {
    let mode = match body.mode.as_str() {
        "earliest" => groups::ResetMode::Earliest,
        "latest" => groups::ResetMode::Latest,
        "offset" => {
            let v = body
                .value
                .ok_or_else(|| AppError::BadRequest("mode=offset 时必须提供 value".to_string()))?;
            groups::ResetMode::Offset(v)
        }
        "timestamp" => {
            let v = body.value.ok_or_else(|| {
                AppError::BadRequest("mode=timestamp 时必须提供 value（毫秒时间戳）".to_string())
            })?;
            groups::ResetMode::Timestamp(v)
        }
        other => {
            return Err(AppError::BadRequest(format!(
                "mode 必须是 earliest / latest / offset / timestamp，收到 {}",
                other
            )))
        }
    };

    let results = groups::reset_offsets(
        Arc::clone(&state),
        &body.cluster,
        &group,
        &body.topic,
        body.partition,
        mode,
    )
    .await?;

    Ok(Json(serde_json::json!({
        "group": group,
        "topic": body.topic,
        "results": results,
    })))
}

pub async fn messages(
    State(state): State<Arc<AppState>>,
    Query(q): Query<MessagesQuery>,
) -> AppResult<impl axum::response::IntoResponse> {
    if q.limit == 0 || q.limit > 10_000 {
        return Err(AppError::BadRequest("limit 必须在 1..10000 之间".to_string()));
    }
    let start = if q.start == "beginning" {
        StartPosition::Beginning
    } else if q.start == "end" {
        StartPosition::End
    } else if let Ok(n) = q.start.parse::<i64>() {
        // 大于等于 10^12 视为毫秒时间戳（2021 年之后），否则视为 offset
        if n >= 1_000_000_000_000 {
            StartPosition::Timestamp(n)
        } else {
            StartPosition::Offset(n)
        }
    } else {
        return Err(AppError::BadRequest(
            "start 必须是 beginning / end / offset / 毫秒时间戳".to_string(),
        ));
    };

    let opts = ReadOptions {
        cluster: q.cluster,
        topic: q.topic,
        partition: q.partition,
        start,
        limit: q.limit,
        max_wait_ms: q.max_wait_ms.min(60_000),
        follow: q.follow,
    };
    let rx = messages::stream_messages(state, opts).await?;
    Ok(message_stream(rx))
}
