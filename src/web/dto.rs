use serde::Deserialize;

/// 新增 / 更新集群请求体。密码为空字符串或省略时，沿用原值（前端不回显密码）
#[derive(Debug, Deserialize)]
pub struct ClusterBody {
    pub name: String,
    pub bootstrap_servers: String,
    #[serde(default)]
    pub security_protocol: Option<String>,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    #[serde(default)]
    pub sasl_username: Option<String>,
    #[serde(default)]
    pub sasl_password: Option<String>,
    #[serde(default)]
    pub ssl_ca_location: Option<String>,
    #[serde(default)]
    pub ssl_certificate_location: Option<String>,
    #[serde(default)]
    pub ssl_key_location: Option<String>,
    #[serde(default)]
    pub ssl_key_password: Option<String>,
    #[serde(default)]
    pub log_level: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ClusterQuery {
    pub cluster: String,
}

#[derive(Debug, Deserialize)]
pub struct TopicQuery {
    pub cluster: String,
    pub topic: String,
}

/// 增加分区请求体：partition_count 是操作完成后的分区总数（Kafka 语义）
#[derive(Debug, Deserialize)]
pub struct AddPartitionsBody {
    pub partition_count: usize,
}

/// 重置消费位点请求体
#[derive(Debug, Deserialize)]
pub struct ResetOffsetsBody {
    pub cluster: String,
    pub topic: String,
    /// 不传表示该 topic 全部分区
    #[serde(default)]
    pub partition: Option<i32>,
    /// earliest | latest | offset | timestamp
    pub mode: String,
    /// mode=offset 时是目标 offset；mode=timestamp 时是毫秒时间戳
    #[serde(default)]
    pub value: Option<i64>,
}

/// 发送消息请求体
#[derive(Debug, Deserialize)]
pub struct ProduceBody {
    pub cluster: String,
    pub topic: String,
    #[serde(default)]
    pub partition: Option<i32>,
    #[serde(default)]
    pub key: Option<String>,
    pub value: String,
    #[serde(default)]
    pub headers: Vec<crate::core::produce::ProduceHeader>,
}

#[derive(Debug, Deserialize)]
pub struct MessagesQuery {
    pub cluster: String,
    pub topic: String,
    #[serde(default = "default_partition")]
    pub partition: i32,
    /// beginning | end | 数字 offset | 毫秒时间戳
    #[serde(default = "default_start")]
    pub start: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default = "default_wait")]
    pub max_wait_ms: u64,
    /// 跟随模式：一直等新消息到来，不自动结束（用于实时 tail）
    #[serde(default)]
    pub follow: bool,
}

fn default_partition() -> i32 {
    0
}
fn default_start() -> String {
    "end".to_string()
}
fn default_limit() -> usize {
    100
}
fn default_wait() -> u64 {
    5000
}
