use serde::{Deserialize, Serialize};
use std::fs;
use std::sync::OnceLock;

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
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WebSocketConfig {
    pub master_url: String,                 // 主服务器地址
    pub admin_key: String,                  // 管理员密钥
    pub message_cache_size: usize,          // 消息缓存大小
    pub max_sessions_per_user: usize,       // 每个用户最大会话数
    pub message_send_delay_ms: u64,         // 消息发送延迟（毫秒）
    pub queue_channel_capacity: usize,      // 队列通知通道容量
    pub task_process_interval_ms: u64,      // 任务处理间隔（毫秒）
    pub default_bandwidth_limit_kb: u64,    // 默认带宽限制 (KB/s)
    pub emergency_queue_ratio: f64,         // 紧急队列占用带宽比例 (0-1)
    pub normal_queue_ratio: f64,            // 普通队列占用带宽比例 (0-1)
    pub slow_queue_ratio: f64,              // 慢速队列占用带宽比例 (0-1)
    pub api_key_conn_limit_per_minute: u32, // 每个API Key每分钟最大连接数
    pub global_conn_limit_per_minute: u32,  // 全局每分钟最大连接数
    pub batch_size: usize,                  // 批量处理消息大小
    pub yield_after_clients: usize,         // 每处理N个客户端后让出CPU
    pub yield_sleep_ms: u64,                // 让出CPU时的睡眠时间(毫秒)
    pub batch_delay_base_ms: u64,           // 批次之间的基础延迟(毫秒)
    pub yield_after_tasks: usize,           // 每处理N个任务后让出CPU
    pub max_batch_delay_ms: u64,            // 批次延迟最大值(毫秒)
    pub max_batch_delay_small_ms: u64,      // 小批次延迟最大值(毫秒)
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
            },
            websocket: WebSocketConfig {
                master_url: "http://localhost:8080".to_string(),
                admin_key: "123456".to_string(),
                message_cache_size: 512,
                max_sessions_per_user: 10,
                message_send_delay_ms: 10,         // 默认10毫秒延迟
                queue_channel_capacity: 200,       // 默认队列通道容量
                task_process_interval_ms: 20,      // 默认每20毫秒处理一个任务
                default_bandwidth_limit_kb: 10000, // 默认10Mbps
                emergency_queue_ratio: 0.7,        // 紧急队列占70%带宽
                normal_queue_ratio: 0.25,          // 普通队列占25%带宽
                slow_queue_ratio: 0.05,            // 慢速队列占5%带宽
                api_key_conn_limit_per_minute: 10, // 每个API Key每分钟最大连接数
                global_conn_limit_per_minute: 120, // 全局每分钟最大连接数
                batch_size: 15,                    // 默认批处理大小为15
                yield_after_clients: 5,            // 每处理5个客户端后让出CPU
                yield_after_tasks: 10,             // 每处理10个任务后让出CPU
                yield_sleep_ms: 10,                // 让出CPU时的睡眠时间(毫秒)
                batch_delay_base_ms: 50,           // 每批次递增50毫秒延迟
                max_batch_delay_ms: 1000,          // 批次延迟最大值(毫秒)
                max_batch_delay_small_ms: 500,     // 小批次延迟最大值(毫秒)
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
