use std::env;
use std::path::Path;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub server: ServerConfig,
    #[serde(default)]
    pub clusters: Vec<ClusterConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_timeout")]
    pub request_timeout_secs: u64,
}

/// 集群连接配置。既用于配置文件种子，也用于页面端的增删改（JSON 落盘）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub name: String,
    pub bootstrap_servers: String,
    #[serde(default = "default_protocol")]
    pub security_protocol: String,
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
    #[serde(default = "default_log_level")]
    pub log_level: String,
}
fn default_listen() -> String {
    "0.0.0.0:7755".to_string()
}
fn default_timeout() -> u64 {
    15
}
fn default_protocol() -> String {
    "PLAINTEXT".to_string()
}
fn default_log_level() -> String {
    "3".to_string() // librdkafka: 0=disabled ... 3=warning, 6=info
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            request_timeout_secs: default_timeout(),
        }
    }
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("读取配置文件失败: {}", path.display()))?;
        let mut config: AppConfig =
            toml::from_str(&raw).with_context(|| format!("解析配置文件失败: {}", path.display()))?;

        for cluster in &mut config.clusters {
            cluster.expand_env()?;
        }
        // 允许 clusters 为空：集群配置可能已经全部搬到页面端（JSON 存储）里
        Ok(config)
    }

    /// 配置文件不存在时使用的默认配置：只用默认 server 参数，集群交给页面端管理
    pub fn default_without_file() -> Self {
        Self {
            server: ServerConfig::default(),
            clusters: Vec::new(),
        }
    }
}

impl ClusterConfig {
    /// 展开 ${ENV_VAR} 语法，密码不落盘到代码或配置明文
    fn expand_env(&mut self) -> Result<()> {
        fn expand(value: Option<String>, field: &str) -> Result<Option<String>> {
            let Some(v) = value else { return Ok(None) };
            if !v.starts_with("${") || !v.ends_with('}') {
                return Ok(Some(v));
            }
            let name = &v[2..v.len() - 1];
            let resolved = env::var(name)
                .with_context(|| format!("字段 {} 引用了未设置的环境变量 {}", field, name))?;
            Ok(Some(resolved))
        }
        self.sasl_password = expand(self.sasl_password.take(), "sasl_password")?;
        self.ssl_key_password = expand(self.ssl_key_password.take(), "ssl_key_password")?;
        Ok(())
    }
}
