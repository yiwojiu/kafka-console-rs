use std::sync::Arc;
use std::time::Duration;

use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{BorrowedMessage, Headers, Message};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use serde::Serialize;
use tokio::sync::mpsc::{channel, Receiver, Sender};

use crate::core::cluster::build_client_config;
use crate::error::{AppError, AppResult};
use crate::state::AppState;

#[derive(Debug, Serialize)]
pub struct MessageRecord {
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: Option<i64>,
    pub timestamp_iso: Option<String>,
    pub key: Option<String>,
    pub value: Option<String>,
    pub value_bytes: usize,
    pub headers: Vec<(String, Option<String>)>,
}

/// SSE 流事件：要么是一条消息，要么是流正常结束的信号。
/// 前端据此区分「读完了」与「连接出错」，避免正常结束误报为错误。
#[derive(Debug, Serialize)]
pub enum StreamEvent {
    Message(MessageRecord),
    Done { sent: usize },
}

#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub cluster: String,
    pub topic: String,
    pub partition: i32,
    pub start: StartPosition,
    pub limit: usize,
    pub max_wait_ms: u64,
    /// 跟随模式（tail）：不设总时长上限，空闲时继续等待新消息，
    /// 直到读满 limit、客户端断开或用户主动停止。
    /// 读历史（beginning / offset / 时间）用 false，读完即结束。
    pub follow: bool,
}

#[derive(Debug, Clone)]
pub enum StartPosition {
    Beginning,
    End,
    Offset(i64),
    Timestamp(i64),
}

fn format_ts(ms: i64) -> Option<String> {
    // 转本地时区展示，避免页面上出现比北京时间慢 8 小时的 UTC 时间
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|dt| dt.with_timezone(&chrono::Local).to_rfc3339())
}

/// 二进制内容不可读时输出 hex 预览，避免 SSE 传乱码
fn display_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) if s.chars().all(|c| !c.is_control() || c == '\n' || c == '\r' || c == '\t') => {
            s.to_string()
        }
        _ => {
            let preview = &bytes[..bytes.len().min(512)];
            let hex: String = preview.iter().map(|b| format!("{:02x}", b)).collect();
            if bytes.len() > 512 {
                format!("0x{}... ({} bytes)", hex, bytes.len())
            } else {
                format!("0x{}", hex)
            }
        }
    }
}

fn message_to_record(msg: &BorrowedMessage<'_>) -> MessageRecord {
    let ts = match msg.timestamp() {
        rdkafka::message::Timestamp::CreateTime(ms)
        | rdkafka::message::Timestamp::LogAppendTime(ms) => Some(ms),
        rdkafka::message::Timestamp::NotAvailable => None,
    };
    let key = msg.key().map(display_bytes);
    let value = msg.payload().map(display_bytes);
    let value_bytes = msg.payload().map_or(0, |b| b.len());
    let headers = msg
        .headers()
        .map(|h| {
            h.iter()
                .map(|e| {
                    (
                        e.key.to_string(),
                        e.value
                            .map(display_bytes),
                    )
                })
                .collect()
        })
        .unwrap_or_default();

    MessageRecord {
        partition: msg.partition(),
        offset: msg.offset(),
        timestamp_ms: ts,
        timestamp_iso: ts.and_then(format_ts),
        key,
        value,
        value_bytes,
        headers,
    }
}

/// 启动一次消息浏览，返回 mpsc Receiver 供 SSE 流式消费。
/// 关键：独立 consumer + assign + seek，不 subscribe、不 commit、不加入消费组。
pub async fn stream_messages(
    state: Arc<AppState>,
    opts: ReadOptions,
) -> AppResult<Receiver<StreamEvent>> {
    let cfg = state.cluster_cfg(&opts.cluster).await?;
    let mut builder = build_client_config(&cfg)?;
    builder
        // librdkafka 要求 BaseConsumer 必须有 group.id；用独立 ID + assign 模式，
        // 不 subscribe、不 commit，因此不会加入该组、不触发 rebalance。
        .set("group.id", "kafka-console-rs-browse")
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("fetch.min.bytes", "1")
        .set("check.crcs", "false");

    let consumer: BaseConsumer = builder.create().map_err(AppError::Kafka)?;
    let consumer = Arc::new(consumer);
    let timeout = Duration::from_secs(state.server.request_timeout_secs);

    // partition < 0 表示该 topic 的全部有消息分区（用于「扫描全部」+ 持续监听，
    // 由单个 consumer 一次 assign 所有分区，避免前端为每个分区各开一条 SSE 连接）
    let partitions: Vec<i32> = if opts.partition < 0 {
        let cluster = state.cluster(&opts.cluster).await?;
        let t = opts.topic.clone();
        let md = tokio::task::spawn_blocking(move || {
            cluster.consumer.fetch_metadata(Some(&t), timeout)
        })
        .await
        .map_err(|e| AppError::Internal(format!("metadata 任务失败: {}", e)))?
        .map_err(AppError::Kafka)?;
        let mut ids: Vec<i32> = md
            .topics()
            .iter()
            .filter(|x| x.name() == opts.topic && x.error().is_none())
            .flat_map(|x| x.partitions().iter().map(|p| p.id()))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        ids
    } else {
        vec![opts.partition]
    };
    if partitions.is_empty() {
        return Err(AppError::BadRequest(format!(
            "topic {} 没有可用分区",
            opts.topic
        )));
    }

    // 构造初始 assignment，按起始位置设置 offset
    let mut tpl = TopicPartitionList::new();
    let initial_offset = match &opts.start {
        StartPosition::Beginning => Offset::Beginning,
        StartPosition::End => Offset::End,
        StartPosition::Offset(n) => Offset::Offset(*n),
        StartPosition::Timestamp(_) => Offset::Beginning, // 稍后由 offsets_for_times 解析
    };
    for p in &partitions {
        tpl.add_partition_offset(&opts.topic, *p, initial_offset)
            .map_err(AppError::Kafka)?;
    }
    consumer.assign(&tpl).map_err(AppError::Kafka)?;

    // 按时间戳定位：librdkafka offsets_for_times 用 Offset::Offset(ms) 作为输入
    if let StartPosition::Timestamp(ts) = opts.start {
        let mut ts_tpl = TopicPartitionList::new();
        for p in &partitions {
            ts_tpl
                .add_partition_offset(&opts.topic, *p, Offset::Offset(ts))
                .map_err(AppError::Kafka)?;
        }
        let resolved = consumer
            .offsets_for_times(ts_tpl, timeout)
            .map_err(AppError::Kafka)?;
        for elem in resolved.elements_for_topic(&opts.topic) {
            let pid = elem.partition();
            match elem.offset().to_raw() {
                Some(raw) if raw >= 0 => {
                    consumer
                        .seek(&opts.topic, pid, Offset::Offset(raw), timeout)
                        .map_err(AppError::Kafka)?;
                }
                _ => {
                    // 该时间点晚于本分区全部消息：直接定位到末尾，别把历史数据全读一遍
                    let (_, high) = consumer
                        .fetch_watermarks(&opts.topic, pid, timeout)
                        .map_err(AppError::Kafka)?;
                    consumer
                        .seek(&opts.topic, pid, Offset::Offset(high), timeout)
                        .map_err(AppError::Kafka)?;
                }
            }
        }
    }

    let (tx, rx): (Sender<StreamEvent>, Receiver<StreamEvent>) = channel(64);
    let c = Arc::clone(&consumer);
    let topic = opts.topic.clone();
    let partition_count = partitions.len();

    tokio::task::spawn_blocking(move || {
        let poll_timeout = Duration::from_millis(200);
        // 跟随模式下忽略总时长上限，由 limit / 客户端断开 / 主动停止来终止
        let max_wait = if opts.follow {
            None
        } else {
            Some(Duration::from_millis(opts.max_wait_ms))
        };
        let started = std::time::Instant::now();
        let mut empty_streak = 0u32;
        let mut sent = 0usize;

        loop {
            if sent >= opts.limit {
                break;
            }
            if let Some(limit) = max_wait {
                if started.elapsed() >= limit {
                    break;
                }
            }
            // 客户端断开（前端关掉 EventSource / 点停止）后立即回收线程，
            // 否则跟随模式在无新消息时会一直占用线程。
            if tx.is_closed() {
                break;
            }

            match c.poll(poll_timeout) {
                Some(Ok(msg)) => {
                    empty_streak = 0;
                    let record = message_to_record(&msg);
                    if tx.blocking_send(StreamEvent::Message(record)).is_err() {
                        break; // 客户端断开，背压自然停止
                    }
                    sent += 1;
                }
                Some(Err(e)) => {
                    tracing::warn!(topic = %topic, partitions = partition_count, error = %e, "读取消息失败");
                    empty_streak += 1;
                }
                None => {
                    empty_streak += 1;
                }
            }
            // 仅「读历史」时判停：连续 5 次 poll 无数据（约 1 秒）且已读到数据，
            // 认为历史数据读尽。跟随模式则继续等待，不能因为空闲就退出。
            if max_wait.is_some() && empty_streak >= 5 && sent > 0 {
                break;
            }
        }
        // 流正常结束：显式通知前端，区分「读完了」和「连接出错」
        let _ = tx.blocking_send(StreamEvent::Done { sent });
        drop(c);
        tracing::debug!(topic = %topic, partitions = partition_count, sent, "消息浏览结束");
    });

    Ok(rx)
}
