use std::collections::HashMap;
use std::sync::Arc;

use rdkafka::consumer::Consumer;
use serde::Serialize;

use crate::core::{groups, topics};
use crate::error::{AppError, AppResult};

#[derive(Debug, Serialize)]
pub struct BrokerInfo {
    pub id: i32,
    pub host: String,
    pub port: i32,
    /// 当前请求实际连上的 broker
    pub is_connected: bool,
    /// 该 broker 担任 leader 的分区数
    pub leader_partitions: usize,
    /// 该 broker 承载的副本总数
    pub replicas: usize,
    /// 该 broker 在 ISR 中的次数
    pub in_sync: usize,
    /// 该 broker 担任 leader 的 topic 去重数
    pub topics_led: usize,
}

#[derive(Debug, Serialize)]
pub struct TopicBrief {
    pub name: String,
    pub partitions: usize,
    pub messages: i64,
}

#[derive(Debug, Serialize)]
pub struct GroupStateCount {
    pub state: String,
    pub count: usize,
}

#[derive(Debug, Serialize)]
pub struct OverviewDto {
    pub cluster: String,
    pub bootstrap_servers: String,
    pub security_protocol: String,
    pub connected_broker_id: i32,
    pub connected_broker_name: String,
    pub brokers: Vec<BrokerInfo>,
    pub topic_count: usize,
    pub internal_topic_count: usize,
    pub partition_count: usize,
    pub replica_count: usize,
    pub total_messages: i64,
    /// 业务 topic 消息量（不含 __ 开头的内部 topic）
    pub business_messages: i64,
    /// 内部 topic 消息量（主要是 __consumer_offsets）
    pub internal_messages: i64,
    /// ISR 数量少于副本数的分区
    pub under_replicated: usize,
    /// leader 为 -1 的分区
    pub no_leader: usize,
    pub group_count: usize,
    pub group_states: Vec<GroupStateCount>,
    /// 消息量前 10 的 topic
    pub top_topics: Vec<TopicBrief>,
    pub fetched_at: String,
}

/// 集群概览：broker 拓扑与分区分布、总量统计、健康度、消费组分布
pub async fn overview(
    state: Arc<crate::state::AppState>,
    cluster_name: &str,
) -> AppResult<OverviewDto> {
    let cluster = state.cluster(cluster_name).await?;
    let cfg = state.cluster_cfg(cluster_name).await?;
    let timeout = cluster.timeout;

    // 1. 一次 metadata 拿 brokers + 全部 topic/分区拓扑
    let c = Arc::clone(&cluster);
    let metadata = tokio::task::spawn_blocking(move || c.consumer.fetch_metadata(None, timeout))
        .await
        .map_err(|e| AppError::Internal(format!("metadata 任务失败: {}", e)))?
        .map_err(AppError::Kafka)?;

    let connected_broker_id = metadata.orig_broker_id();
    let connected_broker_name = metadata.orig_broker_name().to_string();

    let mut leader_count: HashMap<i32, usize> = HashMap::new();
    let mut replica_count: HashMap<i32, usize> = HashMap::new();
    let mut isr_count: HashMap<i32, usize> = HashMap::new();
    let mut topics_led: HashMap<i32, std::collections::HashSet<String>> = HashMap::new();

    let mut partition_count = 0usize;
    let mut replica_total = 0usize;
    let mut under_replicated = 0usize;
    let mut no_leader = 0usize;
    let mut internal_topic_count = 0usize;

    for topic in metadata.topics() {
        if topic.name().starts_with("__") {
            internal_topic_count += 1;
        }
        for p in topic.partitions() {
            partition_count += 1;
            replica_total += p.replicas().len();
            if p.isr().len() < p.replicas().len() {
                under_replicated += 1;
            }
            if p.leader() < 0 {
                no_leader += 1;
            } else {
                *leader_count.entry(p.leader()).or_insert(0) += 1;
                topics_led
                    .entry(p.leader())
                    .or_default()
                    .insert(topic.name().to_string());
            }
            for r in p.replicas() {
                *replica_count.entry(*r).or_insert(0) += 1;
            }
            for r in p.isr() {
                *isr_count.entry(*r).or_insert(0) += 1;
            }
        }
    }

    let mut brokers: Vec<BrokerInfo> = metadata
        .brokers()
        .iter()
        .map(|b| BrokerInfo {
            id: b.id(),
            host: b.host().to_string(),
            port: b.port(),
            is_connected: b.id() == connected_broker_id,
            leader_partitions: *leader_count.get(&b.id()).unwrap_or(&0),
            replicas: *replica_count.get(&b.id()).unwrap_or(&0),
            in_sync: *isr_count.get(&b.id()).unwrap_or(&0),
            topics_led: topics_led.get(&b.id()).map_or(0, |s| s.len()),
        })
        .collect();
    brokers.sort_by_key(|b| b.id);

    // 2. 各 topic 消息量（并发拉 watermark）
    let topic_list = topics::list_topics_with_watermarks(Arc::clone(&cluster)).await?;
    let total_messages: i64 = topic_list.iter().map(|t| t.messages).sum();
    let internal_messages: i64 = topic_list
        .iter()
        .filter(|t| t.name.starts_with("__"))
        .map(|t| t.messages)
        .sum();
    let business_messages = total_messages - internal_messages;
    let mut top_topics: Vec<TopicBrief> = topic_list
        .iter()
        .filter(|t| !t.name.starts_with("__"))
        .map(|t| TopicBrief {
            name: t.name.clone(),
            partitions: t.partitions,
            messages: t.messages,
        })
        .collect();
    top_topics.sort_by(|a, b| b.messages.cmp(&a.messages).then(a.name.cmp(&b.name)));
    top_topics.truncate(10);

    // 3. 消费组状态分布
    let group_list = groups::list_groups(Arc::clone(&state), cluster_name).await?;
    let mut state_map: HashMap<String, usize> = HashMap::new();
    for g in &group_list {
        *state_map.entry(g.state.clone()).or_insert(0) += 1;
    }
    let mut group_states: Vec<GroupStateCount> = state_map
        .into_iter()
        .map(|(state, count)| GroupStateCount { state, count })
        .collect();
    group_states.sort_by(|a, b| b.count.cmp(&a.count).then(a.state.cmp(&b.state)));

    Ok(OverviewDto {
        cluster: cluster_name.to_string(),
        bootstrap_servers: cfg.bootstrap_servers,
        security_protocol: cfg.security_protocol.to_uppercase(),
        connected_broker_id,
        connected_broker_name,
        brokers,
        topic_count: metadata.topics().len(),
        internal_topic_count,
        partition_count,
        replica_count: replica_total,
        total_messages,
        business_messages,
        internal_messages,
        under_replicated,
        no_leader,
        group_count: group_list.len(),
        group_states,
        top_topics,
        fetched_at: chrono::Local::now().to_rfc3339(),
    })
}
