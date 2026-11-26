use std::env;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use axum::routing::{delete, get, post};
use axum::Router;
use tower_http::cors::CorsLayer;
use tower_http::services::ServeDir;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

mod config;
mod core;
mod error;
mod state;
mod store;
mod web;

use config::AppConfig;
use state::AppState;
use store::ClusterStore;
use web::handlers;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    // 配置文件只提供 server 参数和「首次导入」的集群种子；之后的集群以 JSON 存储为准
    let config_path = env::var("KAFKA_CONSOLE_CONFIG")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config/clusters.toml"));
    let config = if config_path.exists() {
        AppConfig::load(&config_path)?
    } else {
        tracing::info!(
            "未找到配置文件 {}，使用默认参数；集群请在页面右上角「集群管理」里添加",
            config_path.display()
        );
        AppConfig::default_without_file()
    };

    let store_path = env::var("KAFKA_CONSOLE_STORE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("config/clusters.json"));
    let store = ClusterStore::open(store_path.clone(), config.clusters.clone())?;
    if store.list().await.is_empty() {
        tracing::warn!("暂无可用集群，请在页面右上角「集群管理」里添加一个 Kafka 集群");
    }
    tracing::info!("集群配置存储: {}", store_path.display());

    // 监听地址可用环境变量覆盖，方便完全没有配置文件时直接跑起来
    let listen = env::var("KAFKA_CONSOLE_LISTEN")
        .unwrap_or_else(|_| config.server.listen.clone());
    let server = config.server.clone();
    let state = Arc::new(AppState::new(server, store).await?);

    let api = Router::new()
        .route("/health", get(handlers::health))
        .route(
            "/clusters",
            get(handlers::clusters).post(handlers::save_cluster),
        )
        .route("/clusters/test", post(handlers::test_cluster))
        .route("/clusters/{name}", delete(handlers::delete_cluster))
        .route(
            "/clusters/{name}/disconnect",
            post(handlers::disconnect_cluster),
        )
        .route("/overview", get(handlers::overview))
        .route("/brokers/{id}/configs", get(handlers::broker_configs))
        .route(
            "/messages",
            get(handlers::messages).post(handlers::produce),
        )
        .route("/topics", get(handlers::topics))
        .route(
            "/topics/{topic}",
            get(handlers::topic_detail).delete(handlers::delete_topic),
        )
        .route("/topics/{topic}/partitions", post(handlers::add_partitions))
        .route("/topics/{topic}/groups", get(handlers::topic_consumers))
        .route("/partitions", get(handlers::partitions))
        .route("/groups", get(handlers::groups))
        .route("/groups/{group}/lag", get(handlers::group_lag))
        .route(
            "/groups/{group}/offsets",
            post(handlers::reset_group_offsets),
        )
        .with_state(state);

    let app = Router::new()
        .nest("/api", api)
        .fallback_service(ServeDir::new("static"))
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive());

    let listener = tokio::net::TcpListener::bind(&listen)
        .await
        .map_err(|e| anyhow!("监听 {} 失败: {}", listen, e))?;
    tracing::info!("Kafka Console 已启动: http://{}", listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    tracing::info!("收到退出信号，正在关闭服务");
}
