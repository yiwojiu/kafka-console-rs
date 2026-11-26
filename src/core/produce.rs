use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::consumer::Consumer;
use rdkafka::message::{Header, OwnedHeaders};
use rdkafka::producer::FutureRecord;
use serde::{Deserialize, Serialize};

use crate::error::{AppError, AppResult};
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct ProduceHeader {
    pub key: String,
    #[serde(default)]
    pub value: String,
}

#[derive(Debug, Deserialize)]
pub struct ProduceRequest {
    pub topic: String,
    /// 不传则由 librdkafka 按 key 哈希或粘性策略分配
    #[serde(default)]
    pub partition: Option<i32>,
    #[serde(default)]
    pub key: Option<String>,
    pub value: String,
    #[serde(default)]
    pub headers: Vec<ProduceHeader>,
}

#[derive(Debug, Serialize)]
pub struct ProduceResult {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub bytes: usize,
    pub latency_ms: u64,
}

const MAX_VALUE_BYTES: usize = 4 * 1024 * 1024;

/// 发送一条消息到指定 topic，返回落盘的分区与 offset
pub async fn produce(
    state: Arc<AppState>,
    cluster_name: &str,
    req: ProduceRequest,
) -> AppResult<ProduceResult> {
    if req.topic.trim().is_empty() {
        return Err(AppError::BadRequest("topic 不能为空".to_string()));
    }
    if req.value.len() > MAX_VALUE_BYTES {
        return Err(AppError::BadRequest(format!(
            "消息体过大（{} 字节），上限 {} 字节",
            req.value.len(),
            MAX_VALUE_BYTES
        )));
    }
    if let Some(p) = req.partition {
        if p < 0 {
            return Err(AppError::BadRequest("partition 不能为负数".to_string()));
        }
    }

    let cluster = state.cluster(cluster_name).await?;
    let cfg = state.cluster_cfg(cluster_name).await?;

    // broker 若开启 auto.create.topics.enable，往写错的 topic 发消息会静默新建 topic。
    // 控制台里先校验存在性，避免手误在集群里留下垃圾 topic。
    let timeout = cluster.timeout;
    let topic_for_check = req.topic.clone();
    let c = Arc::clone(&cluster.consumer);
    let meta = tokio::task::spawn_blocking(move || c.fetch_metadata(Some(&topic_for_check), timeout))
        .await
        .map_err(|e| AppError::Internal(format!("metadata 任务失败: {}", e)))?
        .map_err(AppError::Kafka)?;
    let exists = meta
        .topics()
        .iter()
        .any(|x| x.name() == req.topic.as_str() && !x.partitions().is_empty());
    if !exists {
        return Err(AppError::BadRequest(format!(
            "topic 不存在: {}（本工具不会自动建 topic，请先在 Topic 页核对名称）",
            req.topic
        )));
    }

    let producer = cluster.producer(&cfg)?;

    let mut record = FutureRecord::to(&req.topic).payload(&req.value);
    if let Some(k) = &req.key {
        if !k.is_empty() {
            record = record.key(k.as_str());
        }
    }
    if let Some(p) = req.partition {
        record = record.partition(p);
    }
    if !req.headers.is_empty() {
        let mut hdrs = OwnedHeaders::new();
        for h in &req.headers {
            if h.key.is_empty() {
                continue;
            }
            hdrs = hdrs.insert(Header {
                key: &h.key,
                value: Some(h.value.as_bytes()),
            });
        }
        record = record.headers(hdrs);
    }

    let started = Instant::now();
    let delivery = producer
        .send(record, Duration::from_secs(10))
        .await
        .map_err(|(e, _)| AppError::AdminOp(format!("发送消息失败: {}", e)))?;

    let result = ProduceResult {
        topic: req.topic,
        partition: delivery.partition,
        offset: delivery.offset,
        bytes: req.value.len(),
        latency_ms: started.elapsed().as_millis() as u64,
    };
    tracing::info!(
        topic = %result.topic,
        partition = result.partition,
        offset = result.offset,
        bytes = result.bytes,
        "消息已发送"
    );
    Ok(result)
}
