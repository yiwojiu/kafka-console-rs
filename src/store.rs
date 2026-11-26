//! 集群配置存储：以 JSON 文件落盘，页面端的增删改都写这里，重启后依然生效。

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use tokio::sync::RwLock;

use crate::config::ClusterConfig;

pub struct ClusterStore {
    path: PathBuf,
    items: RwLock<Vec<ClusterConfig>>,
}

impl ClusterStore {
    /// 打开存储：文件存在则读它；不存在则用 seed（配置文件里的集群）初始化并立即落盘。
    /// seed 只用于第一次迁移，之后配置文件里的 clusters 不再被读取。
    pub fn open(path: PathBuf, seed: Vec<ClusterConfig>) -> Result<Self> {
        let exists = path.exists();
        let items = if exists {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("读取集群存储失败: {}", path.display()))?;
            serde_json::from_str::<Vec<ClusterConfig>>(&raw)
                .with_context(|| format!("解析集群存储失败: {}", path.display()))?
        } else {
            seed
        };
        if !exists {
            save(&path, &items)?;
        }
        Ok(Self {
            path,
            items: RwLock::new(items),
        })
    }

    pub async fn list(&self) -> Vec<ClusterConfig> {
        self.items.read().await.clone()
    }

    pub async fn get(&self, name: &str) -> Option<ClusterConfig> {
        self.items
            .read()
            .await
            .iter()
            .find(|c| c.name == name)
            .cloned()
    }

    /// 新增或按 name 覆盖，成功后立即落盘
    pub async fn upsert(&self, cfg: ClusterConfig) -> Result<()> {
        let mut guard = self.items.write().await;
        match guard.iter_mut().find(|c| c.name == cfg.name) {
            Some(slot) => *slot = cfg,
            None => guard.push(cfg),
        }
        save(&self.path, &guard)
    }

    pub async fn remove(&self, name: &str) -> Result<bool> {
        let mut guard = self.items.write().await;
        let before = guard.len();
        guard.retain(|c| c.name != name);
        let removed = guard.len() != before;
        if removed {
            save(&self.path, &guard)?;
        }
        Ok(removed)
    }
}

/// 原子写入：先写临时文件再 rename，避免写一半进程退出把配置弄坏
fn save(path: &Path, items: &[ClusterConfig]) -> Result<()> {
    if let Some(dir) = path.parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir)
                .with_context(|| format!("创建配置目录失败: {}", dir.display()))?;
        }
    }
    let raw = serde_json::to_string_pretty(items).context("序列化集群配置失败")?;
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, raw).with_context(|| format!("写入临时文件失败: {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .with_context(|| format!("替换集群存储失败: {}", path.display()))?;
    Ok(())
}
