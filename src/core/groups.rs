use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::future::try_join_all;
use futures::StreamExt;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::Serialize;

use crate::core::cluster::build_client_config;
use crate::core::topics;
use crate::error::{AppError, AppResult};
use crate::state::AppState;

/// 解析 Kafka consumer assignment 二进制格式，提取 topic 名。
/// 格式: [version i16][topic_count i32]{[topic_len i16][topic bytes][part_count i32][part i32...]}
fn decode_assignment_topics(bytes: &[u8]) -> Vec<String> {
    fn read_i16(b: &[u8], pos: &mut usize) -> Option<i16> {
        let v = i16::from_be_bytes(b.get(*pos..*pos + 2)?.try_into().ok()?);
        *pos += 2;
        Some(v)
    }
    fn read_i32(b: &[u8], pos: &mut usize) -> Option<i32> {
        let v = i32::from_be_bytes(b.get(*pos..*pos + 4)?.try_into().ok()?);
        *pos += 4;
        Some(v)
    }

    let mut pos = 0usize;
    if read_i16(bytes, &mut pos).is_none() {
        return Vec::new();
    }
    let topic_count = match read_i32(bytes, &mut pos) {
        Some(n) if (0..=10_000).contains(&n) => n as usize,
        _ => return Vec::new(),
    };
    let mut result = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let len = match read_i16(bytes, &mut pos) {
            Some(n) if n >= 0 => n as usize,
            _ => return result,
        };
        let end = match pos.checked_add(len) {
            Some(v) => v,
            None => return result,
        };
        let slice = match bytes.get(pos..end) {
            Some(v) => v,
            None => return result,
        };
        let name = String::from_utf8_lossy(slice).into_owned();
        pos = end;
        let part_count = match read_i32(bytes, &mut pos) {
            Some(n) if (0..=1_000_000).contains(&n) => n as usize,
            _ => return result,
        };
        for _ in 0..part_count {
            if read_i32(bytes, &mut pos).is_none() {
                return result;
            }
        }
        result.push(name);
    }
    result
}

#[derive(Debug, Serialize)]
pub struct GroupInfoDto {
    pub name: String,
    pub state: String,
    pub protocol: String,
    pub protocol_type: String,
    pub members: usize,
    pub topics: Vec<String>,
    pub total_lag: i64,
}

#[derive(Debug, Serialize)]
pub struct PartitionLag {
    pub topic: String,
    pub partition: i32,
    pub committed: Option<i64>,
    pub log_end: i64,
    pub lag: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct GroupLagDto {
    pub group: String,
    pub state: String,
    pub total_lag: i64,
    pub partitions: Vec<PartitionLag>,
}

pub async fn list_groups(state: Arc<AppState>, cluster_name: &str) -> AppResult<Vec<GroupInfoDto>> {
    let cluster = state.cluster(cluster_name).await?;
    let timeout = cluster.timeout;
    let c = Arc::clone(&cluster);

    // GroupList 不是 Send，必须在 spawn_blocking 内部提取为普通数据
    let mut groups: Vec<GroupInfoDto> = tokio::task::spawn_blocking(move || {
        let group_list = c
            .consumer
            .fetch_group_list(None, timeout)
            .map_err(AppError::Kafka)?;
        let mut result = Vec::with_capacity(group_list.groups().len());
        for group in group_list.groups() {
            let mut topics = HashSet::new();
            for member in group.members() {
                if let Some(bytes) = member.assignment() {
                    topics.extend(decode_assignment_topics(bytes));
                }
            }
            let mut topics: Vec<String> = topics.into_iter().collect();
            topics.sort();
            result.push(GroupInfoDto {
                name: group.name().to_string(),
                state: group.state().to_string(),
                protocol: group.protocol().to_string(),
                protocol_type: group.protocol_type().to_string(),
                members: group.members().len(),
                topics,
                total_lag: 0,
            });
        }
        Ok::<Vec<GroupInfoDto>, AppError>(result)
    })
    .await
    .map_err(|e| AppError::Internal(format!("group list 任务失败: {}", e)))??;

    groups.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(groups)
}

/// 查询指定消费组的逐分区积压：committed offset vs high watermark。
/// 关键：用 group.id=<目标组> 创建一个独立 consumer，仅做 OffsetFetch，
/// 不 subscribe / 不 assign / 不 poll，因此不会触发 rebalance。
pub async fn group_lag(
    state: Arc<AppState>,
    cluster_name: &str,
    group: &str,
) -> AppResult<GroupLagDto> {
    let cluster = state.cluster(cluster_name).await?;
    let cfg = state.cluster_cfg(cluster_name).await?;

    let mut builder = build_client_config(&cfg)?;
    builder.set("group.id", group);
    let offset_consumer: rdkafka::consumer::BaseConsumer = builder
        .create()
        .map_err(AppError::Kafka)?;
    let offset_consumer = Arc::new(offset_consumer);
    let timeout = cluster.timeout;

    // 取集群全量 topic/partition
    let c = Arc::clone(&cluster);
    let metadata = tokio::task::spawn_blocking(move || c.consumer.fetch_metadata(None, timeout))
        .await
        .map_err(|e| AppError::Internal(format!("metadata 任务失败: {}", e)))?.map_err(AppError::Kafka)?;

    let mut tpl = TopicPartitionList::new();
    let mut partition_keys = Vec::new();
    for topic in metadata.topics() {
        for p in topic.partitions() {
            tpl.add_partition(topic.name(), p.id());
            partition_keys.push((topic.name().to_string(), p.id()));
        }
    }

    // OffsetFetch：拿到该组已提交的 offset
    let oc = Arc::clone(&offset_consumer);
    let committed = tokio::task::spawn_blocking(move || oc.committed_offsets(tpl, timeout))
        .await
        .map_err(|e| AppError::Internal(format!("offset 查询任务失败: {}", e)))?.map_err(AppError::Kafka)?;

    let mut committed_map: HashMap<(String, i32), i64> = HashMap::new();
    for elem in committed.elements() {
        if let Offset::Offset(v) = elem.offset() {
            committed_map.insert((elem.topic().to_string(), elem.partition()), v);
        }
    }

    // 并发查 high watermark：只查有提交位点的分区，避免全集群扫描
    let relevant: Vec<(String, i32)> = partition_keys
        .into_iter()
        .filter(|(t, p)| committed_map.contains_key(&(t.clone(), *p)))
        .collect();
    let mut tasks = Vec::with_capacity(relevant.len());
    for (topic, partition) in relevant {
        let c = Arc::clone(&cluster);
        let t = topic.clone();
        tasks.push(async move {
            let t2 = t.clone();
            let (low, high) = tokio::task::spawn_blocking(move || {
                c.consumer.fetch_watermarks(&t2, partition, timeout)
            })
            .await
            .map_err(|e| AppError::Internal(format!("watermark 任务失败: {}", e)))?.map_err(AppError::Kafka)?;
            let _ = low;
            Ok::<(String, i32, i64), AppError>((t, partition, high))
        });
    }
    let watermarks = try_join_all(tasks).await?;

    let mut partitions = Vec::with_capacity(watermarks.len());
    let mut total_lag = 0i64;
    for (topic, partition, high) in watermarks {
        let committed_offset = committed_map.get(&(topic.clone(), partition)).copied();
        let lag = committed_offset.map(|c| (high - c).max(0));
        if let Some(l) = lag {
            total_lag += l;
        }
        partitions.push(PartitionLag {
            topic,
            partition,
            committed: committed_offset,
            log_end: high,
            lag,
        });
    }
    partitions.sort_by(|a, b| a.topic.cmp(&b.topic).then(a.partition.cmp(&b.partition)));

    // 取 group 状态
    let c = Arc::clone(&cluster);
    let g = group.to_string();
    let state = tokio::task::spawn_blocking(move || {
        c.consumer
            .fetch_group_list(Some(&g), timeout)
            .ok()
            .and_then(|l| l.groups().first().map(|x| x.state().to_string()))
    })
    .await
    .map_err(|e| AppError::Internal(format!("group 状态任务失败: {}", e)))?
    .unwrap_or_else(|| "UNKNOWN".to_string());

    Ok(GroupLagDto {
        group: group.to_string(),
        state,
        total_lag,
        partitions,
    })
}

#[derive(Debug, Serialize)]
pub struct TopicGroupDetail {
    pub group: String,
    pub state: String,
    pub members: usize,
    pub total_lag: i64,
    pub committed_partitions: usize,
    pub partitions: Vec<PartitionLag>,
    /// 该组在此 topic 上有已提交位点
    #[serde(skip)]
    pub has_commits: bool,
    /// 该组当前成员正在消费此 topic
    #[serde(skip)]
    pub subscribed: bool,
}

/// 按 topic 维度查消费详情：找出消费该 topic 的所有消费组及逐分区积压。
/// 判定依据：该组在这个 topic 上有已提交位点，或当前成员的 assignment 里包含该 topic。
pub async fn topic_consumers(
    state: Arc<AppState>,
    cluster_name: &str,
    topic: &str,
) -> AppResult<Vec<TopicGroupDetail>> {
    let cluster = state.cluster(cluster_name).await?;
    let cfg = state.cluster_cfg(cluster_name).await?;
    let timeout = cluster.timeout;

    let parts = topics::partitions(Arc::clone(&cluster), topic).await?;
    if parts.is_empty() {
        return Ok(Vec::new());
    }
    let high: HashMap<i32, i64> = parts.iter().map(|p| (p.partition, p.high)).collect();
    let partition_ids: Vec<i32> = parts.iter().map(|p| p.partition).collect();

    // 1. 取集群全部消费组（GroupList 不是 Send，必须在 blocking 内提取为普通数据）
    let c = Arc::clone(&cluster);
    let target = topic.to_string();
    let groups: Vec<(String, String, usize, bool)> = tokio::task::spawn_blocking(move || {
        let list = c
            .consumer
            .fetch_group_list(None, timeout)
            .map_err(AppError::Kafka)?;
        let mut out = Vec::with_capacity(list.groups().len());
        for g in list.groups() {
            let mut in_assignment = false;
            for m in g.members() {
                if let Some(bytes) = m.assignment() {
                    if decode_assignment_topics(bytes).iter().any(|t| t == &target) {
                        in_assignment = true;
                        break;
                    }
                }
            }
            out.push((
                g.name().to_string(),
                g.state().to_string(),
                g.members().len(),
                in_assignment,
            ));
        }
        Ok::<_, AppError>(out)
    })
    .await
    .map_err(|e| AppError::Internal(format!("group list 任务失败: {}", e)))??;

    // 2. 并发查各消费组在该 topic 上的已提交位点（限并发 6，避免打爆 broker）
    let tasks = groups.into_iter().map(|(name, group_state, members, in_assignment)| {
        let cfg = cfg.clone();
        let topic = topic.to_string();
        let partition_ids = partition_ids.clone();
        let high = high.clone();
        async move {
            let committed = match committed_for_group(&cfg, &topic, &name, &partition_ids, timeout).await {
                Ok(v) => v,
                // 单个组查询失败不影响其它组（如组刚被删除）
                Err(e) => {
                    tracing::debug!(group = %name, error = %e, "查询消费组位点失败");
                    HashMap::new()
                }
            };
            let mut rows = Vec::new();
            let mut total_lag = 0i64;
            for pid in &partition_ids {
                let Some(c_off) = committed.get(pid).copied() else { continue };
                let log_end = high.get(pid).copied().unwrap_or(c_off);
                let lag = (log_end - c_off).max(0);
                total_lag += lag;
                rows.push(PartitionLag {
                    topic: topic.clone(),
                    partition: *pid,
                    committed: Some(c_off),
                    log_end,
                    lag: Some(lag),
                });
            }
            let has_commits = !rows.is_empty();
            TopicGroupDetail {
                group: name,
                state: group_state,
                members,
                total_lag,
                committed_partitions: rows.len(),
                partitions: rows,
                has_commits,
                subscribed: in_assignment,
            }
        }
    });

    let mut list: Vec<TopicGroupDetail> =
        futures::stream::iter(tasks).buffer_unordered(6).collect().await;

    // 3. 只保留与该 topic 有关的组：有提交位点，或当前成员正在消费该 topic
    list.retain(|g| g.has_commits || g.subscribed);
    list.sort_by(|a, b| b.total_lag.cmp(&a.total_lag).then(a.group.cmp(&b.group)));
    Ok(list)
}

/// 查询某个消费组在指定 topic 各分区上的已提交位点。
/// 新建 group.id=<组名> 的 consumer，仅做 OffsetFetch，不 subscribe / 不 assign / 不 poll，
/// 因此不会加入消费组、不会触发 rebalance。
async fn committed_for_group(
    cfg: &crate::config::ClusterConfig,
    topic: &str,
    group: &str,
    partition_ids: &[i32],
    timeout: std::time::Duration,
) -> AppResult<HashMap<i32, i64>> {
    let mut builder = build_client_config(cfg)?;
    builder.set("group.id", group);
    let consumer: rdkafka::consumer::BaseConsumer = builder.create().map_err(AppError::Kafka)?;

    let mut tpl = TopicPartitionList::new();
    for pid in partition_ids {
        tpl.add_partition(topic, *pid);
    }
    let topic = topic.to_string();

    let map = tokio::task::spawn_blocking(move || {
        let committed = consumer
            .committed_offsets(tpl, timeout)
            .map_err(AppError::Kafka)?;
        let mut m: HashMap<i32, i64> = HashMap::new();
        for elem in committed.elements() {
            // 只取该 topic 的分区（防御性过滤）
            if elem.topic() != topic {
                continue;
            }
            if let Offset::Offset(v) = elem.offset() {
                m.insert(elem.partition(), v);
            }
        }
        Ok::<_, AppError>(m)
    })
    .await
    .map_err(|e| AppError::Internal(format!("offset 查询任务失败: {}", e)))??;

    Ok(map)
}

// ===== 重置消费位点 =====
//
// rdkafka 0.40 没有封装 AdminClient 的 AlterConsumerGroupOffsets，
// 这里用等价做法：以「目标消费组」身份建 consumer → assign 到目标分区 →
// 定向 commit 新的 offset。assign 是纯本地操作（不 subscribe），不会加入组、
// 不触发 rebalance；commit 会把位点写进 __consumer_offsets，效果与 Admin 接口一致。

#[derive(Debug, Clone, Copy)]
pub enum ResetMode {
    Earliest,
    Latest,
    Offset(i64),
    Timestamp(i64),
}

#[derive(Debug, Serialize)]
pub struct ResetOutcome {
    pub partition: i32,
    /// 重置前的已提交位点
    pub from: Option<i64>,
    /// 重置后的目标位点
    pub to: i64,
    /// true 表示原目标不可用（如时间戳晚于全部消息、offset 越界），已兜底到边界
    pub adjusted: bool,
}

fn build_group_consumer(
    cfg: &crate::config::ClusterConfig,
    group: &str,
) -> AppResult<BaseConsumer> {
    let mut builder = build_client_config(cfg)?;
    builder
        .set("group.id", group)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    builder.create().map_err(AppError::Kafka)
}

/// 重置某个消费组在指定 topic（可单个分区或全部）上的位点。
pub async fn reset_offsets(
    state: Arc<AppState>,
    cluster_name: &str,
    group: &str,
    topic: &str,
    partition: Option<i32>,
    mode: ResetMode,
) -> AppResult<Vec<ResetOutcome>> {
    let cfg = state.cluster_cfg(cluster_name).await?;
    let cluster = state.cluster(cluster_name).await?;
    let timeout = cluster.timeout;

    // 目标分区：指定了就用单个，否则该 topic 全部分区
    let partition_ids: Vec<i32> = match partition {
        Some(p) => vec![p],
        None => topics::partitions(Arc::clone(&cluster), topic)
            .await?
            .iter()
            .map(|x| x.partition)
            .collect(),
    };
    if partition_ids.is_empty() {
        return Err(AppError::BadRequest(format!("topic {} 没有可用分区", topic)));
    }

    // 有活跃成员时，成员会用内存里的 offset 覆盖刚写入的位点，直接拒绝比事后扯皮好
    let c = Arc::clone(&cluster);
    let g = group.to_string();
    let members = tokio::task::spawn_blocking(move || {
        c.consumer
            .fetch_group_list(Some(&g), timeout)
            .ok()
            .and_then(|l| l.groups().first().map(|x| x.members().len()))
    })
    .await
    .map_err(|e| AppError::Internal(format!("group 状态任务失败: {}", e)))?
    .unwrap_or(0);
    if members > 0 {
        return Err(AppError::BadRequest(format!(
            "消费组 {} 当前有 {} 个活跃成员，重置的位点会被成员内存中的 offset 覆盖。请先停掉消费端再重置。",
            group, members
        )));
    }

    // 重置前的位点，仅用于回显
    let committed = committed_for_group(&cfg, topic, group, &partition_ids, timeout)
        .await
        .unwrap_or_default();

    // 计算每个分区的目标 offset
    let mut targets: Vec<(i32, i64, bool)> = Vec::with_capacity(partition_ids.len());

    // 先拿各分区 low/high，earliest/latest/offset 都要用
    let mut bounds: HashMap<i32, (i64, i64)> = HashMap::new();
    if !matches!(mode, ResetMode::Timestamp(_)) {
        for p in &partition_ids {
            let c = Arc::clone(&cluster);
            let t = topic.to_string();
            let pid = *p;
            let (low, high) = tokio::task::spawn_blocking(move || {
                c.consumer.fetch_watermarks(&t, pid, timeout)
            })
            .await
            .map_err(|e| AppError::Internal(format!("watermark 任务失败: {}", e)))?
            .map_err(AppError::Kafka)?;
            bounds.insert(pid, (low, high));
        }
    }

    match mode {
        ResetMode::Earliest => {
            for p in &partition_ids {
                targets.push((*p, bounds.get(p).map(|b| b.0).unwrap_or(0), false));
            }
        }
        ResetMode::Latest => {
            for p in &partition_ids {
                targets.push((*p, bounds.get(p).map(|b| b.1).unwrap_or(0), false));
            }
        }
        ResetMode::Offset(n) => {
            if n < 0 {
                return Err(AppError::BadRequest("offset 不能为负数".to_string()));
            }
            // 越界的 offset 会让消费端下次 poll 直接报错，这里夹到 [low, high]
            for p in &partition_ids {
                let (low, high) = bounds.get(p).copied().unwrap_or((0, 0));
                let clamped = n.clamp(low, high.max(low));
                targets.push((*p, clamped, clamped != n));
            }
        }
        ResetMode::Timestamp(ms) => {
            let mut ts_tpl = TopicPartitionList::new();
            for p in &partition_ids {
                ts_tpl
                    .add_partition_offset(topic, *p, Offset::Offset(ms))
                    .map_err(AppError::Kafka)?;
            }
            let consumer = build_group_consumer(&cfg, group)?;
            let resolved = tokio::task::spawn_blocking(move || {
                consumer.offsets_for_times(ts_tpl, timeout)
            })
            .await
            .map_err(|e| AppError::Internal(format!("按时间查位点失败: {}", e)))?
            .map_err(AppError::Kafka)?;

            let mut resolved_map: HashMap<i32, i64> = HashMap::new();
            for e in resolved.elements() {
                if let Offset::Offset(v) = e.offset() {
                    resolved_map.insert(e.partition(), v);
                }
            }

            for p in &partition_ids {
                match resolved_map.get(p).copied().filter(|v| *v >= 0) {
                    Some(v) => targets.push((*p, v, false)),
                    None => {
                        // 时间戳晚于该分区全部消息：兜底到 log end（等价 latest），
                        // 不能落到 low，否则会重复消费一整轮历史数据
                        let c = Arc::clone(&cluster);
                        let t = topic.to_string();
                        let pid = *p;
                        let (_low, high) =
                            tokio::task::spawn_blocking(move || {
                                c.consumer.fetch_watermarks(&t, pid, timeout)
                            })
                            .await
                            .map_err(|e| AppError::Internal(format!("watermark 任务失败: {}", e)))?
                            .map_err(AppError::Kafka)?;
                        targets.push((pid, high, true));
                    }
                }
            }
        }
    }

    // 一次性提交（commit 语义：提交的是「下一条要消费的 offset」）
    let consumer = build_group_consumer(&cfg, group)?;
    let mut tpl = TopicPartitionList::new();
    for (p, to, _) in &targets {
        tpl.add_partition_offset(topic, *p, Offset::Offset(*to))
            .map_err(AppError::Kafka)?;
    }
    tokio::task::spawn_blocking(move || {
        consumer.assign(&tpl).map_err(AppError::Kafka)?;
        consumer
            .commit(&tpl, CommitMode::Sync)
            .map_err(|e| AppError::AdminOp(format!("提交位点失败: {}", e)))
    })
    .await
    .map_err(|e| AppError::Internal(format!("提交位点任务失败: {}", e)))??;

    Ok(targets
        .into_iter()
        .map(|(partition, to, adjusted)| ResetOutcome {
            partition,
            from: committed.get(&partition).copied(),
            to,
            adjusted,
        })
        .collect())
}
