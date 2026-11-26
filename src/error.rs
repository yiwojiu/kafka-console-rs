use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("配置错误: {0}")]
    Config(String),

    #[error("集群不存在: {0}")]
    ClusterNotFound(String),

    #[error("参数错误: {0}")]
    BadRequest(String),

    #[error("Kafka 错误: {0}")]
    Kafka(#[from] rdkafka::error::KafkaError),

    #[error("Kafka 操作失败: {0}")]
    AdminOp(String),

    #[error("内部错误: {0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let status = match &self {
            AppError::ClusterNotFound(_) => StatusCode::NOT_FOUND,
            AppError::BadRequest(_) | AppError::AdminOp(_) => StatusCode::BAD_REQUEST,
            AppError::Config(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::Kafka(_) | AppError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, Json(json!({ "error": self.to_string() }))).into_response()
    }
}

pub type AppResult<T> = Result<T, AppError>;
