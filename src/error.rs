#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Postcard error: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("Send error")]
    Send,
}
