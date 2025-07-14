pub mod websocket;
pub use websocket::Hub;

use crate::common::AppResult;

/// 初始化所有服务
pub async fn init_services() -> AppResult<()> {
    // 初始化WebSocket服务
    Hub::init();
    tracing::info!("WebSocket service initialized");

    Ok(())
}
