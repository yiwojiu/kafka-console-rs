use std::sync::Arc;
use std::time::Duration;

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::BaseConsumer;
use rdkafka::producer::FutureProducer;
use tokio::sync::OnceCell;

use crate::config::ClusterConfig;
use crate::error::{AppError, AppResult};

/// 一个集群对应的共享 Kafka 客户端集合。
/// rdkafka 的 Client/AdminClient/Consumer/Producer 都是 Send + Sync，可安全跨请求共享。
pub struct Cluster {
    pub consumer: Arc<BaseConsumer>,
    pub admin: Arc<AdminClient<DefaultClientContext>>,
    /// 首次发送消息时惰性创建，避免没人用也占连接
    pub producer: OnceCell<FutureProducer>,
    pub timeout: Duration,
}

/// 只设置连接与安全相关参数，供 consumer / producer 共用
pub fn base_client_config(cfg: &ClusterConfig) -> AppResult<ClientConfig> {
    let protocol = cfg.security_protocol.to_uppercase();
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &cfg.bootstrap_servers)
        .set("security.protocol", &protocol)
        .set("log_level", &cfg.log_level);

    match protocol.as_str() {
        // 无 TLS：PLAINTEXT 与 SASL_PLAINTEXT
        "PLAINTEXT" | "SASL_PLAINTEXT" => {}
        "SSL" | "SASL_SSL" => {
            if let Some(v) = &cfg.ssl_ca_location {
                c.set("ssl.ca.location", v);
            }
            if let Some(v) = &cfg.ssl_certificate_location {
                c.set("ssl.certificate.location", v);
            }
            if let Some(v) = &cfg.ssl_key_location {
                c.set("ssl.key.location", v);
            }
            if let Some(v) = &cfg.ssl_key_password {
                c.set("ssl.key.password", v);
            }
        }
        _ => {
            return Err(AppError::Config(format!(
                "集群 {} 不支持的 security.protocol: {}",
                cfg.name, cfg.security_protocol
            )))
        }
    }

    if protocol.contains("SASL") {
        let mechanism = cfg.sasl_mechanism.as_deref().ok_or_else(|| {
            AppError::Config(format!("集群 {} 启用 SASL 但未配置 sasl_mechanism", cfg.name))
        })?;
        let username = cfg.sasl_username.as_deref().ok_or_else(|| {
            AppError::Config(format!("集群 {} 启用 SASL 但未配置 sasl_username", cfg.name))
        })?;
        let password = cfg.sasl_password.as_deref().ok_or_else(|| {
            AppError::Config(format!("集群 {} 启用 SASL 但未配置 sasl_password", cfg.name))
        })?;
        c.set("sasl.mechanism", mechanism)
            .set("sasl.username", username)
            .set("sasl.password", password);
    }

    Ok(c)
}

/// 按配置构建 ClientConfig，支持 PLAINTEXT / SSL / SASL_PLAINTEXT / SASL_SSL
pub fn build_client_config(cfg: &ClusterConfig) -> AppResult<ClientConfig> {
    let mut c = base_client_config(cfg)?;
    // 浏览消息用手动 assign + seek，不加入任何消费组
    c.set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest");
    Ok(c)
}

/// 发送消息用的配置：不掺 consumer 属性，避免 librdkafka 打「配置项被忽略」告警
pub fn build_producer_config(cfg: &ClusterConfig) -> AppResult<ClientConfig> {
    let mut c = base_client_config(cfg)?;
    c.set("message.timeout.ms", "10000")
        .set("enable.idempotence", "true");
    Ok(c)
}

impl Cluster {
    pub fn new(cfg: &ClusterConfig, timeout: Duration) -> AppResult<Self> {
        let base = build_client_config(cfg)?;
        let consumer: BaseConsumer = base
            .create()
            .map_err(|e| AppError::Config(format!("集群 {} 创建 consumer 失败: {}", cfg.name, e)))?;
        let admin: AdminClient<DefaultClientContext> = base
            .create()
            .map_err(|e| AppError::Config(format!("集群 {} 创建 admin client 失败: {}", cfg.name, e)))?;
        Ok(Self {
            consumer: Arc::new(consumer),
            admin: Arc::new(admin),
            producer: OnceCell::new(),
            timeout,
        })
    }

    /// 惰性创建并复用 producer；首次发送时才连 broker
    pub fn producer(&self, cfg: &ClusterConfig) -> AppResult<&FutureProducer> {
        // OnceCell::get_or_try_init 需要 async，这里用同步路径：
        // 先读，未初始化时逐个尝试写入（并发下只有一个会成功）
        if let Some(p) = self.producer.get() {
            return Ok(p);
        }
        let built: FutureProducer = build_producer_config(cfg)?
            .create()
            .map_err(|e| AppError::Config(format!("集群 {} 创建 producer 失败: {}", cfg.name, e)))?;
        // set 失败说明别的请求已经写入，直接用已存在的那个
        let _ = self.producer.set(built);
        self.producer
            .get()
            .ok_or_else(|| AppError::Internal("producer 初始化失败".to_string()))
    }
}
