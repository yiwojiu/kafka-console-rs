use std::collections::HashMap;
use std::sync::Arc;

use futures::future::try_join_all;
use rdkafka::admin::{AdminOptions, NewPartitions, ResourceSpecifier};
use rdkafka::consumer::Consumer;
use serde::Serialize;

use crate::core::cluster::Cluster;
use crate::error::{AppError, AppResult};

#[derive(Debug, Serialize)]
pub struct TopicInfo {
    pub name: String,
    pub partitions: usize,
    pub replication_factor: usize,
    pub messages: i64,
}

#[derive(Debug, Serialize)]
pub struct PartitionInfo {
    pub partition: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
    pub low: i64,
    pub high: i64,
    pub messages: i64,
}

#[derive(Debug, Serialize)]
pub struct ConfigItem {
    pub name: String,
    pub value: Option<String>,
    pub source: String,
    pub is_default: bool,
    pub is_read_only: bool,
}

#[derive(Debug, Serialize)]
pub struct TopicDetail {
    pub name: String,
    pub partition_count: usize,
    pub replication_factor: usize,
    pub messages: i64,
    pub partitions: Vec<PartitionInfo>,
    pub configs: Vec<ConfigItem>,
    /// 配置读取失败时仍返回分区信息，这里给出原因
    pub configs_error: Option<String>,
}

/// 带上每个 topic 消息总量（high - low 求和）。分区多时并发拉取 watermark。
pub async fn list_topics_with_watermarks(cluster: Arc<Cluster>) -> AppResult<Vec<TopicInfo>> {
    let timeout = cluster.timeout;
    let c = Arc::clone(&cluster);
    let metadata = tokio::task::spawn_blocking(move || c.consumer.fetch_metadata(None, timeout))
        .await
        .map_err(|e| AppError::Internal(format!("metadata 任务失败: {}", e)))?
        .map_err(AppError::Kafka)?;

    let mut tasks = Vec::new();
    for topic in metadata.topics() {
        for partition in topic.partitions() {
            let c = Arc::clone(&cluster);
            let name = topic.name().to_string();
            let id = partition.id();
            tasks.push(async move {
                let name2 = name.clone();
                let (low, high) =
                    tokio::task::spawn_blocking(move || c.consumer.fetch_watermarks(&name2, id, timeout))
                        .await
                        .map_err(|e| AppError::Internal(format!("watermark 任务失败: {}", e)))?
                        .map_err(AppError::Kafka)?;
                Ok::<(String, i32, i64, i64), AppError>((name, id, low, high))
            });
        }
    }

    let results: Vec<(String, i32, i64, i64)> = try_join_all(tasks).await?;
    let mut totals: HashMap<String, i64> = HashMap::new();
    for (name, _, low, high) in results {
        *totals.entry(name).or_insert(0) += (high - low).max(0);
    }

    let mut topics = Vec::with_capacity(metadata.topics().len());
    for topic in metadata.topics() {
        let partitions = topic.partitions();
        let replication = partitions.first().map_or(0, |p| p.replicas().len());
        topics.push(TopicInfo {
            name: topic.name().to_string(),
            partitions: partitions.len(),
            replication_factor: replication,
            messages: *totals.get(topic.name()).unwrap_or(&0),
        });
    }
    topics.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(topics)
}

/// 一次 metadata 拿全分区（leader/副本/ISR），再并发补 watermark
pub async fn partitions(cluster: Arc<Cluster>, topic: &str) -> AppResult<Vec<PartitionInfo>> {
    let timeout = cluster.timeout;
    let t = topic.to_string();
    let c = Arc::clone(&cluster.consumer);
    let meta = tokio::task::spawn_blocking(move || c.fetch_metadata(Some(&t), timeout))
        .await
        .map_err(|e| AppError::Internal(format!("metadata 任务失败: {}", e)))?
        .map_err(AppError::Kafka)?;

    let topic_meta = meta
        .topics()
        .iter()
        .find(|x| x.name() == topic)
        .ok_or_else(|| AppError::BadRequest(format!("topic 不存在: {}", topic)))?;

    // topic 不存在时 Kafka 会返回一个分区为空的占位条目
    if topic_meta.partitions().is_empty() {
        return Err(AppError::BadRequest(format!("topic 不存在: {}", topic)));
    }

    let mut tasks = Vec::new();
    for p in topic_meta.partitions() {
        let id = p.id();
        let leader = p.leader();
        let replicas: Vec<i32> = p.replicas().to_vec();
        let isr: Vec<i32> = p.isr().to_vec();
        let c = Arc::clone(&cluster);
        let name = topic.to_string();
        tasks.push(async move {
            let name2 = name.clone();
            let (low, high) =
                tokio::task::spawn_blocking(move || c.consumer.fetch_watermarks(&name2, id, timeout))
                    .await
                    .map_err(|e| AppError::Internal(format!("watermark 任务失败: {}", e)))?
                    .map_err(AppError::Kafka)?;
            Ok::<PartitionInfo, AppError>(PartitionInfo {
                partition: id,
                leader,
                replicas,
                isr,
                low,
                high,
                messages: (high - low).max(0),
            })
        });
    }
    let mut result: Vec<PartitionInfo> = try_join_all(tasks).await?;
    result.sort_by_key(|p| p.partition);
    Ok(result)
}

/// topic 详情：分区信息 + 配置项（配置读取失败不影响分区信息返回）
pub async fn topic_detail(cluster: Arc<Cluster>, topic: &str) -> AppResult<TopicDetail> {
    let partitions = partitions(Arc::clone(&cluster), topic).await?;
    let messages: i64 = partitions.iter().map(|p| p.messages).sum();
    let replication = partitions.first().map_or(0, |p| p.replicas.len());

    let (configs, configs_error) = match describe_configs(&cluster, topic).await {
        Ok(v) => (v, None),
        Err(e) => (Vec::new(), Some(e.to_string())),
    };

    Ok(TopicDetail {
        name: topic.to_string(),
        partition_count: partitions.len(),
        replication_factor: replication,
        messages,
        partitions,
        configs,
        configs_error,
    })
}

async fn describe_configs(cluster: &Cluster, topic: &str) -> AppResult<Vec<ConfigItem>> {
    let opts = AdminOptions::new().request_timeout(Some(cluster.timeout));
    let results = cluster
        .admin
        .describe_configs([&ResourceSpecifier::Topic(topic)], &opts)
        .await
        .map_err(AppError::Kafka)?;

    let resource = results
        .into_iter()
        .next()
        .ok_or_else(|| AppError::AdminOp("describe_configs 返回空结果".to_string()))?
        .map_err(|e| AppError::AdminOp(format!("读取 topic 配置失败: {:?}", e)))?;

    let mut items: Vec<ConfigItem> = resource
        .entries
        .into_iter()
        .map(|e| ConfigItem {
            name: e.name,
            value: if e.is_sensitive { Some("******".to_string()) } else { e.value },
            source: format!("{:?}", e.source),
            is_default: e.is_default,
            is_read_only: e.is_read_only,
        })
        .collect();
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(items)
}

/// 读取 broker 的关键配置项（不是全量，只挑运维最常看的那些）
pub async fn broker_configs(cluster: &Cluster, broker_id: i32) -> AppResult<Vec<ConfigItem>> {
    const KEYS: &[&str] = &[
        "broker.id",
        "log.dirs",
        "num.partitions",
        "default.replication.factor",
        "min.insync.replicas",
        "auto.create.topics.enable",
        "delete.topic.enable",
        "unclean.leader.election.enable",
        "log.retention.hours",
        "log.retention.bytes",
        "log.segment.bytes",
        "message.max.bytes",
        "num.io.threads",
        "num.network.threads",
        "offsets.topic.replication.factor",
    ];

    let opts = AdminOptions::new().request_timeout(Some(cluster.timeout));
    let results = cluster
        .admin
        .describe_configs([&ResourceSpecifier::Broker(broker_id)], &opts)
        .await
        .map_err(AppError::Kafka)?;

    let resource = results
        .into_iter()
        .next()
        .ok_or_else(|| AppError::AdminOp("describe_configs 返回空结果".to_string()))?
        .map_err(|e| AppError::AdminOp(format!("读取 broker 配置失败: {:?}", e)))?;

    let mut items: Vec<ConfigItem> = resource
        .entries
        .into_iter()
        .filter(|e| KEYS.contains(&e.name.as_str()))
        .map(|e| ConfigItem {
            name: e.name,
            value: if e.is_sensitive { Some("******".to_string()) } else { e.value },
            source: format!("{:?}", e.source),
            is_default: e.is_default,
            is_read_only: e.is_read_only,
        })
        .collect();
    items.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(items)
}

/// 删除 topic（不可逆，调用方必须已做二次确认）
pub async fn delete_topic(cluster: Arc<Cluster>, topic: &str) -> AppResult<String> {
    if topic.starts_with("__") {
        return Err(AppError::BadRequest(format!(
            "拒绝删除 Kafka 内部 topic: {}",
            topic
        )));
    }
    let opts = AdminOptions::new().request_timeout(Some(cluster.timeout));
    let results = cluster
        .admin
        .delete_topics(&[topic], &opts)
        .await
        .map_err(AppError::Kafka)?;

    match results.into_iter().next() {
        Some(Ok(name)) => Ok(name),
        Some(Err((name, code))) => Err(AppError::AdminOp(format!(
            "删除 topic {} 失败: {:?}",
            name, code
        ))),
        None => Err(AppError::AdminOp("删除 topic 返回空结果".to_string())),
    }
}

/// 增加分区。new_total 是操作完成后的分区总数（Kafka 语义），不是增量。
pub async fn add_partitions(
    cluster: Arc<Cluster>,
    topic: &str,
    new_total: usize,
) -> AppResult<usize> {
    let current = partitions(Arc::clone(&cluster), topic).await?.len();
    if new_total <= current {
        return Err(AppError::BadRequest(format!(
            "目标分区数 {} 必须大于当前分区数 {}",
            new_total, current
        )));
    }
    if new_total > 100_000 {
        return Err(AppError::BadRequest("分区数上限 100000".to_string()));
    }

    let opts = AdminOptions::new().request_timeout(Some(cluster.timeout));
    let spec = NewPartitions::new(topic, new_total);
    let results = cluster
        .admin
        .create_partitions([&spec], &opts)
        .await
        .map_err(AppError::Kafka)?;

    match results.into_iter().next() {
        Some(Ok(name)) => {
            tracing::info!(topic = %name, from = current, to = new_total, "分区扩容成功");
            Ok(new_total)
        }
        Some(Err((name, code))) => Err(AppError::AdminOp(format!(
            "topic {} 增加分区失败: {:?}",
            name, code
        ))),
        None => Err(AppError::AdminOp("增加分区返回空结果".to_string())),
    }
}
