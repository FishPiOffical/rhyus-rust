use crate::{conf, service::websocket::Hub, util};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::time::SystemTime;
use tokio::net::TcpStream;
use tokio_tungstenite::WebSocketStream;

// 连接限流管理
struct ConnectionLimiter {
    // API Key限流 - 记录每个API Key在当前分钟内的连接次数
    api_key_counters: DashMap<String, u32>,
    // 全局限流 - 记录当前分钟内的全局连接次数
    global_counter: AtomicU32,
    // 当前分钟的时间戳(秒)
    current_minute: AtomicUsize,
}

impl ConnectionLimiter {
    fn new() -> Self {
        Self {
            api_key_counters: DashMap::new(),
            global_counter: AtomicU32::new(0),
            current_minute: AtomicUsize::new(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as usize
                    / 60,
            ),
        }
    }

    // 检查并更新API Key的连接计数
    fn check_api_key_limit(&self, api_key: &str) -> bool {
        self.update_minute_if_needed();

        let mut entry = self
            .api_key_counters
            .entry(api_key.to_string())
            .or_insert(0);
        let api_key_limit = conf::Settings::global()
            .websocket
            .api_key_conn_limit_per_minute;

        // 检查是否超过API Key限制
        if *entry >= api_key_limit {
            log::warn!(
                "API Key {} 每分钟连接数超过限制 ({})",
                api_key,
                api_key_limit
            );
            return false;
        }

        // 更新计数
        *entry += 1;
        true
    }

    // 检查并更新全局连接计数
    fn check_global_limit(&self) -> bool {
        self.update_minute_if_needed();

        let global_limit = conf::Settings::global()
            .websocket
            .global_conn_limit_per_minute;

        // 检查是否超过全局限制
        let current = self.global_counter.load(Ordering::Relaxed);
        if current >= global_limit {
            log::warn!("全局每分钟连接数超过限制 ({})", global_limit);
            return false;
        }

        // 更新计数
        self.global_counter.fetch_add(1, Ordering::Relaxed);
        true
    }

    // 检查并更新当前分钟
    fn update_minute_if_needed(&self) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as usize
            / 60;

        let current = self.current_minute.load(Ordering::Relaxed);

        // 如果分钟变更，重置所有计数器
        if now > current {
            // 尝试更新当前分钟，只有一个线程会成功
            if self
                .current_minute
                .compare_exchange(current, now, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                // 清空API Key计数器
                self.api_key_counters.clear();
                // 重置全局计数器
                self.global_counter.store(0, Ordering::Relaxed);
            }
        }
    }
}

// 全局单例
static CONNECTION_LIMITER: Lazy<ConnectionLimiter> = Lazy::new(ConnectionLimiter::new);

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
            if !CONNECTION_LIMITER.check_global_limit() {
                log::warn!("全局连接数限制，拒绝连接: {}", addr);
                return;
            }

            if !CONNECTION_LIMITER.check_api_key_limit(&api_key) {
                log::warn!(
                    "API Key连接数限制，拒绝连接: {}, API Key: {}",
                    addr,
                    api_key
                );
                return;
            }

            if let Some(user_info) = util::get_user_info(&api_key).await {
                if let Err(e) = Hub::global().add_client(socket, user_info.clone()).await {
                    log::error!("Failed to add user connection: {}", e);
                } else {
                    log::debug!(
                        "Successfully added user: [{}], oId: [{}]",
                        user_info.user_name,
                        user_info.o_id
                    );
                }
            } else {
                log::error!("Failed to get user info: {}", api_key);
            }
        }
    } else {
        log::error!("Failed to get peer address");
    }
}
