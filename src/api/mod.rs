use tokio_tungstenite::WebSocketStream;
use tokio::net::TcpStream;
use crate::{conf, service::websocket::Hub, util};

pub async fn handle_connection(socket: WebSocketStream<TcpStream>, api_key: String) {
    // 获取连接参数
    let peer_addr = socket.get_ref().peer_addr().ok();
    if let Some(addr) = peer_addr {
        log::debug!("New connection from: {}", addr);
        
        if api_key == conf::admin_key() {
            // 主服务器连接
            if let Err(e) = Hub::global().add_master(socket).await {
                log::error!("Failed to add master connection: {}", e);
            } else {
                log::debug!("Successfully added master connection");
            }
        } else {
            // 客户端连接
            if let Some(user_info) = util::get_user_info(&api_key).await {
                if let Err(e) = Hub::global().add_client(socket, user_info.clone()).await {
                    log::error!("Failed to add user connection: {}", e);
                } else {
                    log::debug!("Successfully added user: [{}], oId: [{}]", user_info.user_name, user_info.o_id);
                }
            } else {
                log::error!("Failed to get user info: {}", api_key);
            }
        }
    } else {
        log::error!("Failed to get peer address");
    }
} 