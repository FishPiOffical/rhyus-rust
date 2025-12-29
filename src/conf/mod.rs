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
    
    // 连接限流配置
    pub api_key_conn_limit_per_minute: u32, // 每个API Key每分钟最大连接数
    pub global_conn_limit_per_minute: u32,  // 全局每分钟最大连接数
    
    // 带宽控制配置
    pub default_bandwidth_limit_kb: u64,    // 默认带宽限制 (KB/s)
    pub emergency_queue_ratio: f64,         // 紧急队列占用带宽比例 (0-1)
    pub normal_queue_ratio: f64,            // 普通队列占用带宽比例 (0-1)
    pub slow_queue_ratio: f64,              // 慢速队列占用带宽比例 (0-1)
    
    // 消息频率处理配置
    pub frequency: FrequencyConfig,
    pub direct: DirectConfig,
    pub micro_batch: MicroBatchConfig,
    pub batch: BatchConfig,
    pub frequency_check_interval_ms: u64,   // 频率检查间隔
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FrequencyConfig {
    pub direct_mode_threshold: u64,         // 直接发送模式阈值 (msg/s)
    pub micro_batch_threshold: u64,         // 微批处理模式阈值 (msg/s)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DirectConfig {
    pub max_delay_ms: u64,                  // 最大延迟 (ms)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MicroBatchConfig {
    pub batch_size: usize,                  // 批量大小
    pub max_delay_ms: u64,                  // 最大延迟 (ms)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct BatchConfig {
    pub batch_size: usize,                  // 批量大小
    pub max_delay_ms: u64,                  // 最大延迟 (ms)
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
                
                // 连接限流配置
                api_key_conn_limit_per_minute: 10,  // 每个API Key每分钟最大连接数
                global_conn_limit_per_minute: 120,  // 全局每分钟最大连接数
                
                // 带宽控制配置
                default_bandwidth_limit_kb: 10000,  // 默认10Mbps
                emergency_queue_ratio: 0.7,         // 紧急队列占70%带宽
                normal_queue_ratio: 0.25,           // 普通队列占25%带宽
                slow_queue_ratio: 0.05,             // 慢速队列占5%带宽
                
                // 消息频率处理配置
                frequency: FrequencyConfig {
                    direct_mode_threshold: 10,       // 低于10msg/s使用直接发送
                    micro_batch_threshold: 100,      // 低于100msg/s使用微批处理
                },
                direct: DirectConfig {
                    max_delay_ms: 5,                 // 直接模式最大延迟5ms
                },
                micro_batch: MicroBatchConfig {
                    batch_size: 5,                   // 微批处理批量大小5
                    max_delay_ms: 10,                // 微批处理最大延迟10ms
                },
                batch: BatchConfig {
                    batch_size: 50,                  // 批量处理批量大小50
                    max_delay_ms: 30,                // 批量处理最大延迟30ms
                },
                frequency_check_interval_ms: 100,   // 每100ms检查一次频率
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
