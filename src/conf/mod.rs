use serde::{Deserialize, Serialize};
use std::sync::OnceLock;
use std::fs;

// 全局配置实例
static CONFIG: OnceLock<Settings> = OnceLock::new();

#[derive(Debug, Deserialize, Serialize)]
pub struct Settings {
    pub server: ServerConfig,
    pub websocket: WebSocketConfig,
    pub log: LogConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
    pub heartbeat_interval: u64,  // 心跳间隔（秒）
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WebSocketConfig {
    pub master_url: String,      // 主服务器地址
    pub admin_key: String,       // 管理员密钥
    pub message_cache_size: usize, // 消息缓存大小
    pub max_sessions_per_user: usize, // 每个用户最大会话数
    pub message_send_delay_ms: u64, // 消息发送延迟（毫秒）
    pub queue_channel_capacity: usize, // 队列通知通道容量
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LogConfig {
    pub level: String,
}

impl Settings {
    pub fn global() -> &'static Settings {
        CONFIG.get_or_init(|| {
            Self::init().unwrap_or_else(|e| {
                panic!("Failed to initialize config: {}", e);
            })
        })
    }

    fn init() -> anyhow::Result<Self> {
        // 默认配置
        let default_config = Settings {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 10831,
                heartbeat_interval: 30,
            },
            websocket: WebSocketConfig {
                master_url: "http://localhost:8080".to_string(),
                admin_key: "123456".to_string(),
                message_cache_size: 1024,
                max_sessions_per_user: 10,
                message_send_delay_ms: 10, // 默认10毫秒延迟
                queue_channel_capacity: 100, // 默认队列通道容量
            },
            log: LogConfig {
                level: "info".to_string(),
            },
        };

        // 尝试读取配置文件
        let config = match fs::read_to_string("config.toml") {
            Ok(content) => {
                let config: Settings = toml::from_str(&content)?;
                config
            }
            Err(_) => {
                // 如果配置文件不存在，使用默认配置并创建配置文件
                let content = toml::to_string_pretty(&default_config)?;
                fs::write("config.toml", content)?;
                default_config
            }
        };

        Ok(config)
    }
}

pub fn server_address() -> String {
    let config = Settings::global();
    format!("{}:{}", config.server.host, config.server.port)
}

pub fn master_url() -> &'static str {
    &Settings::global().websocket.master_url
}

pub fn admin_key() -> &'static str {
    &Settings::global().websocket.admin_key
}

