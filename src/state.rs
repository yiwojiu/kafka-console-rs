use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;

use crate::config::{ClusterConfig, ServerConfig};
use crate::core::cluster::Cluster;
use crate::error::{AppError, AppResult};
use crate::store::ClusterStore;

pub struct AppState {
    pub server: ServerConfig,
    pub store: ClusterStore,
    /// 已建立的客户端连接，按集群名缓存；页面端改了配置就整体替换
    clusters: RwLock<HashMap<String, Arc<Cluster>>>,
}

impl AppState {
    pub async fn new(server: ServerConfig, store: ClusterStore) -> AppResult<Self> {
        let timeout = Duration::from_secs(server.request_timeout_secs);
        let mut map = HashMap::new();
        for cfg in store.list().await {
            match Cluster::new(&cfg, timeout) {
                Ok(c) => {
                    map.insert(cfg.name.clone(), Arc::new(c));
                }
                Err(e) => {
                    // 单个集群配置有问题不应该阻塞启动，页面上还能改好它
                    tracing::warn!(cluster = %cfg.name, error = %e, "集群初始化失败，已跳过");
                }
            }
        }
        Ok(Self {
            server,
            store,
            clusters: RwLock::new(map),
        })
    }

    fn timeout(&self) -> Duration {
        Duration::from_secs(self.server.request_timeout_secs)
    }

    pub async fn cluster(&self, name: &str) -> AppResult<Arc<Cluster>> {
        if let Some(c) = self.clusters.read().await.get(name) {
            return Ok(Arc::clone(c));
        }
        // 内存里没有（启动时创建失败或刚新增），按存储配置补建一次
        let cfg = self
            .store
            .get(name)
            .await
            .ok_or_else(|| AppError::ClusterNotFound(name.to_string()))?;
        let cluster = Arc::new(
            Cluster::new(&cfg, self.timeout())
                .map_err(|e| AppError::Config(e.to_string()))?,
        );
        self.clusters
            .write()
            .await
            .insert(name.to_string(), Arc::clone(&cluster));
        Ok(cluster)
    }

    pub async fn cluster_cfg(&self, name: &str) -> AppResult<ClusterConfig> {
        self.store
            .get(name)
            .await
            .ok_or_else(|| AppError::ClusterNotFound(name.to_string()))
    }

    /// 集群列表（脱敏，不返回密码）
    pub async fn cluster_meta(&self) -> Vec<serde_json::Value> {
        self.store.list().await.iter().map(public_view).collect()
    }

    /// 新增或更新集群：先验证配置能建出客户端，再落盘并替换内存中的连接
    pub async fn upsert_cluster(&self, cfg: ClusterConfig) -> AppResult<()> {
        let cluster = Arc::new(
            Cluster::new(&cfg, self.timeout()).map_err(|e| AppError::Config(e.to_string()))?,
        );
        self.store
            .upsert(cfg.clone())
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        self.clusters
            .write()
            .await
            .insert(cfg.name.clone(), cluster);
        Ok(())
    }

    pub async fn remove_cluster(&self, name: &str) -> AppResult<bool> {
        let removed = self
            .store
            .remove(name)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        if removed {
            self.clusters.write().await.remove(name);
        }
        Ok(removed)
    }

    /// 断开某个集群的客户端连接：从缓存里摘掉并 drop。
    /// Arc 引用归零后 librdkafka 会销毁 client，连带释放它的后台线程与 TCP 连接；
    /// 之后再用到这个集群会按存储配置重新建连。落盘的配置不受影响。
    pub async fn disconnect_cluster(&self, name: &str) -> bool {
        match self.clusters.write().await.remove(name) {
            Some(cluster) => {
                drop(cluster);
                true
            }
            None => false,
        }
    }
}

/// 返回给前端的集群信息：密码与私钥口令一律不回显，只标注是否已设置
pub fn public_view(c: &ClusterConfig) -> serde_json::Value {
    serde_json::json!({
        "name": c.name,
        "bootstrap_servers": c.bootstrap_servers,
        "security_protocol": c.security_protocol,
        "sasl_mechanism": c.sasl_mechanism,
        "sasl_username": c.sasl_username,
        "has_sasl_password": c.sasl_password.as_deref().is_some_and(|v| !v.is_empty()),
        "ssl_ca_location": c.ssl_ca_location,
        "ssl_certificate_location": c.ssl_certificate_location,
        "ssl_key_location": c.ssl_key_location,
        "has_ssl_key_password": c.ssl_key_password.as_deref().is_some_and(|v| !v.is_empty()),
        "log_level": c.log_level,
    })
}
