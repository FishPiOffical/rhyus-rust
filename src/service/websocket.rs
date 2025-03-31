use std::time::{SystemTime, Duration};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::broadcast;
use tokio_tungstenite::{tungstenite::{Message, Error as WsError}, WebSocketStream};
use tokio::net::TcpStream;
use crate::{common::{AppResult, AppError}, model::UserInfo, util, conf::Settings};
use std::sync::Arc;
use once_cell::sync::Lazy;
use reqwest;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::collections::HashMap;

impl From<WsError> for AppError {
    fn from(err: WsError) -> Self {
        AppError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("WebSocket error: {}", err),
        ))
    }
}

// WebSocket消息
#[derive(Clone)]
struct WsMessage {
    data: String,
    delay: Option<Duration>,
    priority: MessagePriority,  // 新增消息优先级
}

// 消息优先级定义
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum MessagePriority {
    Slow = 0,        // 缓慢消息（例如：服务端定期发送的在线人数）
    Normal = 1,      // 普通消息（例如：聊天消息）
    Emergency = 2,   // 紧急消息（例如：红包、撤回等需要及时处理的消息）
}

impl WsMessage {
    fn new(data: impl Into<String>) -> Self {
        Self {
            data: data.into(),
            delay: None,
            priority: MessagePriority::Normal,  // 默认为普通优先级
        }
    }

    // fn with_delay(data: impl Into<String>, delay: Duration) -> Self {
    //     Self {
    //         data: data.into(),
    //         delay: Some(delay),
    //         priority: MessagePriority::Slow,  // 有延迟的通常是慢消息
    //     }
    // }
    
    // 紧急消息构造函数
    fn emergency(data: impl Into<String>) -> Self {
        Self {
            data: data.into(),
            delay: None,
            priority: MessagePriority::Emergency,
        }
    }
    
    // 慢速消息构造函数
    fn slow(data: impl Into<String>) -> Self {
        Self {
            data: data.into(),
            delay: Some(Duration::from_millis(100)),
            priority: MessagePriority::Slow,
        }
    }
}

// 活跃的主服务器连接
struct ActiveMaster {
    // tx: broadcast::Sender<WsMessage>,
    last_active: SystemTime,
}

// 活跃的客户端连接
struct ActiveClient {
    user_info: UserInfo,
    tx: broadcast::Sender<WsMessage>,
    last_active: SystemTime,
}

// 消息处理器
#[derive(Clone)]
struct MessageHandler {
    client_out_tx: broadcast::Sender<WsMessage>,
}

// 在 Hub 结构体前添加
#[allow(dead_code)]
#[derive(Debug)]
struct QueuedMessage {
    sender: String,
    content: String,
    timestamp: SystemTime,
}

// 发送任务定义
#[derive(Debug)]
#[allow(dead_code)]
struct SendTask {
    content: String,             // 消息内容
    client_addr: String,         // 目标客户端地址
    client_tx: broadcast::Sender<WsMessage>, // 目标客户端发送通道
    timestamp: SystemTime,       // 任务创建时间
    priority: MessagePriority,   // 消息优先级
    is_batch: bool,              // 是否为批量任务
    batch_clients: Option<Vec<(String, broadcast::Sender<WsMessage>)>>, // 批量客户端列表
}

// 消息队列统计
struct MsgQueueStats {
    total_msgs: u64,         // 收到的消息总数
    // processed_msgs: u64,     // 处理的消息总数
    queued_tasks: u64,       // 已排队的发送任务总数
    completed_tasks: u64,    // 已完成的发送任务总数
    queue_max_size: usize,   // 队列最大长度
    last_msg_time: SystemTime, // 最后处理消息的时间
    last_msg_content: String,  // 最后处理的消息内容
    connection_count: usize,   // 当前连接数量
    send_duration_ms: u64,     // 发送持续时间(毫秒)
    
    // 各优先级队列统计
    emergency_queue_size: usize, // 紧急队列当前大小
    normal_queue_size: usize,    // 普通队列当前大小
    slow_queue_size: usize,      // 慢速队列当前大小
    
    // 线程数量配置
    emergency_thread_count: usize, // 紧急队列处理线程数
    normal_thread_count: usize,    // 普通队列处理线程数
    slow_thread_count: usize,      // 慢速队列处理线程数
    
    // 各优先级消息统计
    emergency_msgs_total: u64,   // 紧急消息总数
    normal_msgs_total: u64,      // 普通消息总数
    slow_msgs_total: u64,        // 慢速消息总数
    
    // 带宽统计
    current_bandwidth_limit: u64,  // 当前带宽限制(kb/s)
}

impl MsgQueueStats {
    fn new() -> Self {
        Self {
            total_msgs: 0,
            // processed_msgs: 0,
            queued_tasks: 0,
            completed_tasks: 0,
            queue_max_size: 0,
            last_msg_time: SystemTime::now(),
            last_msg_content: String::new(),
            connection_count: 0,
            send_duration_ms: 0,
            
            emergency_queue_size: 0,
            normal_queue_size: 0,
            slow_queue_size: 0,
            
            emergency_thread_count: 3,
            normal_thread_count: 1,
            slow_thread_count: 2,
            
            emergency_msgs_total: 0,
            normal_msgs_total: 0,
            slow_msgs_total: 0,
            
            current_bandwidth_limit: 10000, // 默认10000kb/s
            // avg_msg_size_kb: 0.0,
        }
    }
    
    // // 更新统计信息
    // fn update_with_task(&mut self, task: &SendTask, content_len: usize) {
    //     self.total_msgs += 1;
    //     self.queued_tasks += 1;
        
    //     // 更新平均消息大小
    //     let msg_size_kb = (content_len as f64) / 1024.0;
    //     if self.avg_msg_size_kb == 0.0 {
    //         self.avg_msg_size_kb = msg_size_kb;
    //     } else {
    //         // 使用指数移动平均法更新平均值
    //         self.avg_msg_size_kb = 0.7 * self.avg_msg_size_kb + 0.3 * msg_size_kb;
    //     }
        
    //     // 根据优先级更新统计
    //     match task.priority {
    //         MessagePriority::Emergency => self.emergency_msgs_total += 1,
    //         MessagePriority::Normal => self.normal_msgs_total += 1,
    //         MessagePriority::Slow => self.slow_msgs_total += 1,
    //     }
    // }
    
    // 生成统计报告
    fn generate_report(&self) -> String {
        let now = SystemTime::now();
        let uptime = now.duration_since(self.last_msg_time).unwrap_or_default();
        
        let total_queue_size = self.emergency_queue_size + self.normal_queue_size + self.slow_queue_size;
        // let config = Settings::global();
        
        // 计算当前的带宽分配
        let emergency_bw = if total_queue_size > 0 {
            (self.emergency_queue_size as f64 / total_queue_size as f64) * 100.0
        } else { 0.0 };
        
        let normal_bw = if total_queue_size > 0 {
            (self.normal_queue_size as f64 / total_queue_size as f64) * 100.0
        } else { 0.0 };
        
        let slow_bw = if total_queue_size > 0 {
            (self.slow_queue_size as f64 / total_queue_size as f64) * 100.0
        } else { 0.0 };
        
        // 计算完成率，确保不超过100%
        let completion_rate = if self.total_msgs > 0 {
            ((self.completed_tasks as f64 / self.total_msgs as f64) * 100.0).min(100.0)
        } else { 
            0.0 
        };
        
        format!(
            "消息队列统计报告:\n\
             总消息数: {}, 已处理: {}, 完成率: {:.1}%\n\
             队列状态: 紧急队列[{}], 普通队列[{}], 慢速队列[{}], 总计[{}/{}]\n\
             队列占比: 紧急[{:.1}%], 普通[{:.1}%], 慢速[{:.1}%]\n\
             线程配置: 紧急[{}线程], 普通[{}线程], 慢速[{}线程]\n\
             带宽占比: 紧急[{:.1}%], 普通[{:.1}%], 慢速[{:.1}%], 总限制[{}kb/s]\n\
             消息类型: 紧急[{}], 普通[{}], 慢速[{}]\n\
             连接数: {}, 最后活动: {:.1}秒前",
            self.total_msgs,
            self.completed_tasks,
            completion_rate,
            self.emergency_queue_size,
            self.normal_queue_size,
            self.slow_queue_size,
            total_queue_size,
            self.queue_max_size,
            emergency_bw,
            normal_bw,
            slow_bw,
            self.emergency_thread_count,
            self.normal_thread_count,
            self.slow_thread_count,
            emergency_bw,
            normal_bw,
            slow_bw,
            self.current_bandwidth_limit,
            self.emergency_msgs_total,
            self.normal_msgs_total,
            self.slow_msgs_total,
            self.connection_count,
            uptime.as_secs_f64()
        )
    }
}

// WebSocket连接池
pub struct Hub {
    // 主服务器连接相关
    masters: Arc<DashMap<String, ActiveMaster>>,
    master_tx: broadcast::Sender<WsMessage>,
    message_handler: MessageHandler,

    // 客户端连接相关
    clients: Arc<DashMap<String, ActiveClient>>,
    
    // 在线用户信息
    online_users: Arc<DashMap<String, i32>>,
    all_online_users: Arc<tokio::sync::RwLock<String>>,
    client_online_users: Arc<tokio::sync::RwLock<String>>,

    // 消息队列相关 - 按优先级分离队列
    normal_queue: Arc<Mutex<VecDeque<SendTask>>>,  // 普通消息队列
    emergency_queue: Arc<Mutex<VecDeque<SendTask>>>,  // 紧急消息队列
    slow_queue: Arc<Mutex<VecDeque<SendTask>>>,  // 缓慢消息队列
    queue_sender: broadcast::Sender<()>,
    
    // 队列处理线程控制
    normal_thread_count: usize,   // 普通消息处理线程数(1)
    emergency_thread_count: usize, // 紧急消息处理线程数(3)
    slow_thread_count: usize,     // 慢速消息处理线程数(2)
    
    // 带宽管理
    bandwidth_limit: Arc<tokio::sync::RwLock<u64>>,  // 当前最大带宽限制 (kb/s)
    
    // 带宽控制器
    emergency_token_bucket: Arc<tokio::sync::Mutex<TokenBucket>>,  // 紧急消息带宽控制器
    normal_token_bucket: Arc<tokio::sync::Mutex<TokenBucket>>,     // 普通消息带宽控制器
    slow_token_bucket: Arc<tokio::sync::Mutex<TokenBucket>>,       // 慢速消息带宽控制器
    
    // 消息队列统计
    msg_stats: Arc<tokio::sync::RwLock<MsgQueueStats>>,
}

// 令牌桶算法实现带宽控制
#[derive(Debug)]
struct TokenBucket {
    capacity: u64,         // 桶容量 (kb)
    tokens: f64,           // 当前令牌数
    last_refill: SystemTime, // 上次填充时间
    refill_rate: f64,      // 填充速率 (kb/s)
    priority: MessagePriority, // 关联的优先级
}

impl TokenBucket {
    // 创建新的令牌桶
    fn new(capacity_kb: u64, rate_kb_per_sec: u64, priority: MessagePriority) -> Self {
        Self {
            capacity: capacity_kb,
            tokens: capacity_kb as f64, // 初始满桶
            last_refill: SystemTime::now(),
            refill_rate: rate_kb_per_sec as f64,
            priority,
        }
    }
    
    // 尝试消耗指定数量的令牌，返回需要等待的时间
    fn consume(&mut self, kb: u64) -> Duration {
        // 先填充令牌
        let now = SystemTime::now();
        let elapsed = now.duration_since(self.last_refill).unwrap_or_default();
        let elapsed_secs = elapsed.as_secs_f64();
        
        // 计算新增的令牌数
        let new_tokens = elapsed_secs * self.refill_rate;
        self.tokens = (self.tokens + new_tokens).min(self.capacity as f64);
        self.last_refill = now;
        
        // 计算需要等待的时间
        if (kb as f64) <= self.tokens {
            // 有足够的令牌，直接消耗
            self.tokens -= kb as f64;
            Duration::from_secs(0)
        } else {
            // 没有足够的令牌，计算需要等待的时间
            let missing_tokens = (kb as f64) - self.tokens;
            let wait_time_secs = missing_tokens / self.refill_rate;
            
            // 消耗所有可用令牌
            self.tokens = 0.0;
            
            // 返回需要等待的时间
            Duration::from_secs_f64(wait_time_secs)
        }
    }
    
    // 重置令牌桶速率
    fn reset_rate(&mut self, new_rate_kb_per_sec: u64) {
        // 先更新当前令牌数
        let now = SystemTime::now();
        let elapsed = now.duration_since(self.last_refill).unwrap_or_default();
        let elapsed_secs = elapsed.as_secs_f64();
        
        // 使用旧速率添加令牌
        let new_tokens = elapsed_secs * self.refill_rate;
        self.tokens = (self.tokens + new_tokens).min(self.capacity as f64);
        self.last_refill = now;
        
        // 更新速率
        self.refill_rate = new_rate_kb_per_sec as f64;
        
        log::debug!("{:?}优先级令牌桶速率已更新: {}kb/s", 
                  self.priority, new_rate_kb_per_sec);
    }
}

impl Hub {
    pub fn new() -> Self {
        let config = Settings::global();
        let message_cache_size = config.websocket.message_cache_size;
        
        let (master_tx, _) = broadcast::channel(message_cache_size);
        let (client_out_tx, _) = broadcast::channel(message_cache_size);
        let (queue_sender, _) = broadcast::channel(config.websocket.queue_channel_capacity);

        let message_handler = MessageHandler {
            client_out_tx,
        };
        
        // 总带宽限制
        let total_bandwidth = config.websocket.default_bandwidth_limit_kb;
        
        // 创建令牌桶
        let emergency_bucket = TokenBucket::new(
            total_bandwidth, 
            (total_bandwidth as f64 * config.websocket.emergency_queue_ratio) as u64,
            MessagePriority::Emergency
        );
        
        let normal_bucket = TokenBucket::new(
            total_bandwidth, 
            (total_bandwidth as f64 * config.websocket.normal_queue_ratio) as u64,
            MessagePriority::Normal
        );
        
        let slow_bucket = TokenBucket::new(
            total_bandwidth, 
            (total_bandwidth as f64 * config.websocket.slow_queue_ratio) as u64,
            MessagePriority::Slow
        );

        let hub = Self {
            masters: Arc::new(DashMap::new()),
            master_tx,
            message_handler,
            clients: Arc::new(DashMap::new()),
            online_users: Arc::new(DashMap::new()),
            all_online_users: Arc::new(tokio::sync::RwLock::new("[]".to_string())),
            client_online_users: Arc::new(tokio::sync::RwLock::new("{}".to_string())),
            normal_queue: Arc::new(Mutex::new(VecDeque::with_capacity(message_cache_size * 10))),
            emergency_queue: Arc::new(Mutex::new(VecDeque::with_capacity(message_cache_size * 3))),
            slow_queue: Arc::new(Mutex::new(VecDeque::with_capacity(message_cache_size * 5))),
            queue_sender,
            normal_thread_count: 1,   // 普通消息单线程处理
            emergency_thread_count: 3, // 紧急消息3线程处理 
            slow_thread_count: 2,     // 慢速消息2线程处理
            bandwidth_limit: Arc::new(tokio::sync::RwLock::new(total_bandwidth)),
            emergency_token_bucket: Arc::new(tokio::sync::Mutex::new(emergency_bucket)),
            normal_token_bucket: Arc::new(tokio::sync::Mutex::new(normal_bucket)),
            slow_token_bucket: Arc::new(tokio::sync::Mutex::new(slow_bucket)),
            msg_stats: Arc::new(tokio::sync::RwLock::new(MsgQueueStats::new())),
        };

        // 启动消息队列处理器
        hub.start_queue_processors();
        
        // 启动统计监控
        hub.start_msg_stats_monitor();
        
        // 启动带宽监控和动态调整
        hub.start_bandwidth_monitor();
        
        hub
    }

    // 清理不活跃的连接
    async fn clean_inactive_connections(&self, hours: u64) -> HashMap<String, u64> {
        let now = SystemTime::now();
        let inactive_threshold = Duration::from_secs(hours * 3600);
        let mut inactive_users = HashMap::new();
        
        // 收集不活跃的客户端连接
        self.clients.iter().for_each(|entry| {
            if let Ok(duration) = now.duration_since(entry.value().last_active) {
                if duration >= inactive_threshold {
                    let elapsed_hours = duration.as_secs() / 3600;
                    inactive_users.insert(
                        entry.value().user_info.user_name.clone(),
                        elapsed_hours
                    );
                }
            }
        });
        
        // 强制移除不活跃用户的所有连接
        for username in inactive_users.keys() {
            self.force_remove_user(&username);
        }
        
        // 更新在线用户列表
        if !inactive_users.is_empty() {
            self.update_online_users_list().await;
        }
        
        inactive_users
    }

    // 强制移除用户的所有连接
    pub fn force_remove_user(&self, username: &str) {
        // 收集需要移除的连接地址
        let mut addresses_to_remove = Vec::new();
        self.clients.iter().for_each(|entry| {
            if entry.value().user_info.user_name == username {
                addresses_to_remove.push(entry.key().clone());
            }
        });
        
        // 移除所有连接
        for addr in addresses_to_remove {
            self.clients.remove(&addr);
        }
        
        // 移除用户计数
        self.online_users.remove(username);
        
        // 通知主服务器用户离开
        let username = username.to_string();
        tokio::spawn(async move {
            if let Err(e) = util::post_message_to_master("leave", &username).await {
                log::error!("通知主服务器用户离开失败: {}", e);
            } else {
                log::debug!("已通知主服务器用户 {} 离开", username);
            }
        });
    }

    // 发送消息给指定用户
    fn send_to_user(&self, username: &str, msg: WsMessage) {
        let hub = self.clone();
        let msg_clone = msg.clone();
        let username = username.to_string();
        
        tokio::spawn(async move {
            // let mut failed_clients = Vec::new();
            // let mut sent = false;
            
            // 尝试向用户的所有连接发送消息
            for entry in hub.clients.iter() {
                let client = entry.value();
                if client.user_info.user_name == username {
                    if let Err(e) = client.tx.send(msg_clone.clone()) {
                        log::error!("向用户 {} 发送消息失败: {}", username, e);
                        // failed_clients.push(entry.key().clone());
                    }
                }
            }
            
            // // 移除发送失败的客户端
            // for key in failed_clients {
            //     hub.clients.remove(&key);
            // }
            
            // if !sent {
            //     log::error!("用户 {} 不在线或消息发送全部失败", username);
            // }
        });
    }

    // 广播消息给所有客户端 - 改为将发送任务加入优先级队列，使用批量处理
    fn broadcast_to_clients(&self, msg: WsMessage, exclude_user: Option<&str>) {
        let hub = self.clone();
        let content = msg.data.clone();
        let priority = msg.priority;
        
        // 首先收集客户端和获取队列大小，避免在异步块中使用MutexGuard
        let clients: Vec<_> = hub.clients.iter()
            .filter(|entry| {
                // 如果指定了排除用户，则过滤掉该用户的所有连接
                if let Some(exclude) = exclude_user {
                    entry.value().user_info.user_name != exclude
                } else {
                    true
                }
            })
            .map(|entry| (
                entry.key().clone(), 
                entry.value().tx.clone()
            ))
            .collect();
        
        let connection_count = clients.len();
        if connection_count == 0 {
            return;
        }
        
        // 使用批量处理以减少队列大小
        const BATCH_SIZE: usize = 50; // 每批50个客户端
        let batch_count = (connection_count + BATCH_SIZE - 1) / BATCH_SIZE;
        
        // 提前获取队列引用
        let queue = match priority {
            MessagePriority::Emergency => &hub.emergency_queue,
            MessagePriority::Normal => &hub.normal_queue,
            MessagePriority::Slow => &hub.slow_queue,
        };
        
        // 为所有客户端批量创建发送任务并加入队列
        let now = SystemTime::now();
        let mut queue_size = 0;
        
        // 在异步块外加锁，避免Send trait问题
        {
            let mut queue_guard = queue.lock().unwrap();
            
            // 分批处理
            for batch_idx in 0..batch_count {
                let start = batch_idx * BATCH_SIZE;
                let end = (start + BATCH_SIZE).min(connection_count);
                let client_batch = &clients[start..end];
                
                
                // 为此批次创建一个任务 - 保持原始消息内容不变
                let batch_task = SendTask {
                    content: content.clone(), // 保持原始消息内容不变
                    client_addr: format!("batch_{}_{}", batch_idx, now.elapsed().unwrap_or_default().as_nanos()),
                    client_tx: client_batch[0].1.clone(), // 使用第一个客户端的发送通道作为引用
                    timestamp: now,
                    priority,
                    is_batch: true,
                    batch_clients: Some(client_batch.to_vec()),
                };
                
                // 加入队列
                queue_guard.push_back(batch_task);
            }
            
            queue_size = queue_guard.len();
        }
        
        log::debug!("消息广播任务，优先级:{:?}，连接数:{}, 批次数:{}", 
                   priority, connection_count, batch_count);
        
        tokio::spawn(async move {
            // 更新统计信息
            {
                let mut stats = hub.msg_stats.write().await;
                stats.total_msgs += 1; // 只计为一条消息
                stats.queued_tasks += batch_count as u64; // 任务数是批次数
                stats.last_msg_time = now;
                stats.last_msg_content = content.clone();
                stats.connection_count = connection_count;
                
                // 更新优先级统计
                match priority {
                    MessagePriority::Emergency => stats.emergency_msgs_total += 1,
                    MessagePriority::Normal => stats.normal_msgs_total += 1,
                    MessagePriority::Slow => stats.slow_msgs_total += 1,
                }
                
                if queue_size > stats.queue_max_size {
                    stats.queue_max_size = queue_size;
                }
            }
            
            // 通知队列处理器
            let _ = hub.queue_sender.send(());
        });
    }

    // 启动消息队列处理器 - 处理不同优先级的队列
    fn start_queue_processors(&self) {
        // 订阅队列通知
        let hub = self.clone();
        let mut rx = self.queue_sender.subscribe();
        
        // 启动通知监听线程
        tokio::spawn(async move {
            loop {
                // 等待队列通知或定期检查
                tokio::select! {
                    result = rx.recv() => {
                        if result.is_ok() {
                            // 收到通知，更新队列统计
                            let emergency_size = hub.emergency_queue.lock().unwrap().len();
                            let normal_size = hub.normal_queue.lock().unwrap().len();
                            let slow_size = hub.slow_queue.lock().unwrap().len();
                            let total_size = emergency_size + normal_size + slow_size;
                            
                            // 更新队列大小统计
                            if total_size > 0 {
                                let mut stats = hub.msg_stats.write().await;
                                stats.emergency_queue_size = emergency_size;
                                stats.normal_queue_size = normal_size;
                                stats.slow_queue_size = slow_size;
                                
                                if total_size > stats.queue_max_size {
                                    stats.queue_max_size = total_size;
                                }
                            }
                            
                            // 记录调试日志
                            if emergency_size > 0 || normal_size > 0 || slow_size > 0 {
                                log::debug!("队列状态更新 - 紧急: {}, 普通: {}, 慢速: {}, 总计: {}",
                                    emergency_size, normal_size, slow_size, total_size);
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        // 定期检查队列，暂不执行额外操作
                    }
                }
            }
        });
        
        // 使用统一函数启动不同优先级的处理线程
        self.start_priority_processors(
            MessagePriority::Emergency, 
            self.emergency_thread_count, 
            self.emergency_queue.clone(), 
            1, // 使用基础处理间隔
            10 // 队列为空时休眠时间(ms)
        );
        
        self.start_priority_processors(
            MessagePriority::Normal, 
            self.normal_thread_count, 
            self.normal_queue.clone(),
            1, // 使用基础处理间隔
            20 // 队列为空时休眠时间(ms)
        );
        
        self.start_priority_processors(
            MessagePriority::Slow, 
            self.slow_thread_count, 
            self.slow_queue.clone(),
            2, // 慢速消息使用2倍处理间隔
            50 // 队列为空时休眠时间(ms)
        );
    }

    // 通用优先级处理器启动函数 - 减少代码重复
    fn start_priority_processors(
        &self,
        priority: MessagePriority,
        thread_count: usize,
        queue: Arc<Mutex<VecDeque<SendTask>>>,
        interval_factor: u64,
        empty_sleep_ms: u64
    ) {
        for _ in 0..thread_count {
            let hub = self.clone();
            let config = Settings::global();
            let queue = queue.clone();
            
            tokio::spawn(async move {
                // 处理间隔
                let process_interval = Duration::from_millis(
                    config.websocket.task_process_interval_ms * interval_factor
                );
                let mut last_process_time = SystemTime::now();
                
                loop {
                    // 控制处理速率
                    let elapsed = SystemTime::now().duration_since(last_process_time).unwrap_or_default();
                    if elapsed < process_interval {
                        tokio::time::sleep(process_interval - elapsed).await;
                    }
                    
                    // 批量处理任务 - 最多一次取出10个任务减少锁竞争
                    let tasks: Vec<SendTask> = {
                        let mut queue_guard = queue.lock().unwrap();
                        let count = queue_guard.len().min(10);
                        (0..count).filter_map(|_| queue_guard.pop_front()).collect()
                    };
                    
                    if !tasks.is_empty() {
                        // 处理获取的所有任务
                        for (i, task) in tasks.iter().enumerate() {
                            // 根据优先级创建消息，保持原始内容不变
                            let msg = match priority {
                                MessagePriority::Emergency => WsMessage::emergency(&task.content),
                                MessagePriority::Normal => WsMessage::new(&task.content),
                                MessagePriority::Slow => WsMessage::slow(&task.content),
                            };
                            
                            // 计算消息大小 (kb)
                            let msg_size_kb = (task.content.len() as f64 / 1024.0).ceil() as u64;
                            
                            // 获取对应优先级的令牌桶
                            let token_bucket = match priority {
                                MessagePriority::Emergency => &hub.emergency_token_bucket,
                                MessagePriority::Normal => &hub.normal_token_bucket,
                                MessagePriority::Slow => &hub.slow_token_bucket,
                            };
                            
                            // 获取带宽限制并消耗令牌
                            let wait_time = {
                                let mut bucket = token_bucket.lock().await;
                                bucket.consume(msg_size_kb)
                            };
                            
                            // 如果需要等待，则等待指定时间
                            if !wait_time.is_zero() {
                                tokio::time::sleep(wait_time).await;
                            }
                            
                            // 处理批量任务或单个任务
                            if task.is_batch && task.batch_clients.is_some() {
                                // 批量发送消息
                                let batch_clients = task.batch_clients.as_ref().unwrap();
                                let mut success_count = 0;
                                let mut fail_count = 0;
                                
                                for (client_addr, client_tx) in batch_clients {
                                    // 发送消息给此客户端
                                    match client_tx.send(msg.clone()) {
                                        Ok(_) => success_count += 1,
                                        Err(err) => {
                                            fail_count += 1;
                                            log::error!("批量任务: 发送{}消息给客户端失败: {} - {}", 
                                                      format!("{:?}", priority).to_lowercase(), 
                                                      client_addr, err);
                                        }
                                    }
                                    
                                    // 每发送10个客户端让出一次CPU时间
                                    if (success_count + fail_count) % 10 == 0 {
                                        tokio::task::yield_now().await;
                                    }
                                }
                                
                                // 更新统计信息
                                {
                                    let mut stats = hub.msg_stats.write().await;
                                    stats.completed_tasks += success_count as u64;
                                    stats.send_duration_ms += wait_time.as_millis() as u64;
                                }
                                
                                // 记录批量发送结果
                                log::debug!("批量发送完成: 成功={}, 失败={}, 总计={}, 优先级={:?}, 等待时间={}ms", 
                                          success_count, fail_count, batch_clients.len(),
                                          priority, wait_time.as_millis());
                                
                            } else {
                                // 单个发送消息
                                if let Err(err) = task.client_tx.send(msg.clone()) {
                                    log::error!("发送{}消息给客户端失败: {} - {}", 
                                              format!("{:?}", priority).to_lowercase(), 
                                              task.client_addr, err);
                                } else {
                                    // 更新统计信息
                                    {
                                        let mut stats = hub.msg_stats.write().await;
                                        stats.completed_tasks += 1;
                                        stats.send_duration_ms += wait_time.as_millis() as u64;
                                    }
                                }
                            }
                            
                            // 每处理5个任务让出一次CPU，防止长时间占用
                            if i % 5 == 4 {
                                tokio::task::yield_now().await;
                            }
                        }
                        
                        // 更新处理时间
                        last_process_time = SystemTime::now();
                    } else {
                        // 队列为空，短暂休眠
                        tokio::time::sleep(Duration::from_millis(empty_sleep_ms)).await;
                    }
                }
            });
        }
    }

    // 启动消息处理循环
    pub fn start_message_handlers(&self) {
        // 处理广播到客户端的消息
        let hub = self.clone();
        tokio::spawn(async move {
            let mut rx = hub.message_handler.client_out_tx.subscribe();
            while let Ok(msg) = rx.recv().await {
                hub.broadcast_to_clients(msg, None);
            }
        });
    }

    // 消息统计监控
    fn start_msg_stats_monitor(&self) {
        let hub = self.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1800));
            // 添加周期统计变量
            let mut last_total_msgs = 0u64;
            let mut last_completed_tasks = 0u64;
            
            loop {
                interval.tick().await;
                
                // 获取各队列大小 - 批量获取以减少锁竞争
                let (emergency_size, normal_size, slow_size) = {
                    let e_size = hub.emergency_queue.lock().unwrap().len();
                    let n_size = hub.normal_queue.lock().unwrap().len();
                    let s_size = hub.slow_queue.lock().unwrap().len();
                    (e_size, n_size, s_size)
                };
                
                // 获取连接数量
                let connection_count = hub.clients.len();
                
                // 获取带宽限制
                let bandwidth_limit = *hub.bandwidth_limit.read().await;
                
                // 更新统计信息
                let mut current_total_msgs = 0;
                let mut current_completed_tasks = 0;
                {
                    let mut stats = hub.msg_stats.write().await;
                    stats.connection_count = connection_count;
                    stats.emergency_queue_size = emergency_size;
                    stats.normal_queue_size = normal_size;
                    stats.slow_queue_size = slow_size;
                    stats.current_bandwidth_limit = bandwidth_limit;
                    
                    // 更新线程数量信息
                    stats.emergency_thread_count = hub.emergency_thread_count;
                    stats.normal_thread_count = hub.normal_thread_count;
                    stats.slow_thread_count = hub.slow_thread_count;
                    
                    let total_size = emergency_size + normal_size + slow_size;
                    if total_size > stats.queue_max_size {
                        stats.queue_max_size = total_size;
                    }
                    
                    // 保存当前累计值用于计算增量
                    current_total_msgs = stats.total_msgs;
                    current_completed_tasks = stats.completed_tasks;
                    
                    // 生成并记录统计报告
                    let report = stats.generate_report();
                    log::info!("{}", report);
                    
                    // 额外记录线程和队列处理信息
                    let total_threads = hub.emergency_thread_count + hub.normal_thread_count + hub.slow_thread_count;
                    log::info!("队列处理线程配置 - 紧急: {}线程({}%), 普通: {}线程({}%), 慢速: {}线程({}%), 总计: {}线程",
                        hub.emergency_thread_count,
                        (hub.emergency_thread_count as f64 / total_threads as f64 * 100.0).round(),
                        hub.normal_thread_count,
                        (hub.normal_thread_count as f64 / total_threads as f64 * 100.0).round(),
                        hub.slow_thread_count,
                        (hub.slow_thread_count as f64 / total_threads as f64 * 100.0).round(),
                        total_threads);
                    
                    // 队列负载统计 - 仅在有队列任务时记录
                    if total_size > 0 {
                        log::info!("队列负载统计 - 紧急: {:.1}个/线程, 普通: {:.1}个/线程, 慢速: {:.1}个/线程",
                            emergency_size as f64 / hub.emergency_thread_count.max(1) as f64,
                            normal_size as f64 / hub.normal_thread_count.max(1) as f64,
                            slow_size as f64 / hub.slow_thread_count.max(1) as f64);
                        
                        // 检查队列是否有异常积压
                        if emergency_size > 1000 || normal_size > 5000 || slow_size > 2000 {
                            log::warn!("检测到队列异常积压! 请检查系统性能或增加处理线程");
                        }
                    }
                }
                
                // 计算本周期的增量值
                let period_msgs = current_total_msgs.saturating_sub(last_total_msgs);
                let period_completed = current_completed_tasks.saturating_sub(last_completed_tasks);
                
                // 更新记录点
                last_total_msgs = current_total_msgs;
                last_completed_tasks = current_completed_tasks;
                
                // 输出周期统计
                let completion_rate = if period_msgs > 0 {
                    (period_completed as f64 / period_msgs as f64) * 100.0
                } else {
                    0.0
                };
                
                if period_msgs > 0 || period_completed > 0 {
                    log::info!("周期统计(30分钟) - 新增消息: {}, 已处理: {}, 完成率: {:.1}%",
                        period_msgs, period_completed, completion_rate.min(100.0));
                }
                
                // 主动让出一次CPU时间，减少长期任务对系统的影响
                tokio::task::yield_now().await;
            }
        });
    }

    // 带宽监控和动态调整
    fn start_bandwidth_monitor(&self) {
        let hub = self.clone();
        
        tokio::spawn(async move {
            // 检查间隔设置为5分钟
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            
            loop {
                interval.tick().await;
                
                // 获取当前总带宽限制
                let total_bandwidth = *hub.bandwidth_limit.read().await;
                let config = Settings::global();
                
                // 获取队列状态用于动态调整带宽分配
                let emergency_size = hub.emergency_queue.lock().unwrap().len();
                let normal_size = hub.normal_queue.lock().unwrap().len();
                let slow_size = hub.slow_queue.lock().unwrap().len();
                let total_size = emergency_size + normal_size + slow_size;
                
                // 根据队列情况动态调整各优先级的带宽分配
                if total_size > 0 {
                    // 基础分配比例
                    let mut emergency_ratio = config.websocket.emergency_queue_ratio;
                    let mut normal_ratio = config.websocket.normal_queue_ratio;
                    let mut slow_ratio = config.websocket.slow_queue_ratio;
                    
                    // 如果紧急队列有积压，增加紧急队列带宽占比
                    if emergency_size > 200 {
                        // 增加紧急队列带宽，减少其他队列带宽
                        emergency_ratio = (emergency_ratio * 1.5).min(0.8); // 最高占总带宽80%
                        normal_ratio = ((1.0 - emergency_ratio) * 0.8).max(0.1); // 最低10%
                        slow_ratio = (1.0 - emergency_ratio - normal_ratio).max(0.05); // 最低5%
                        
                        log::info!("检测到紧急队列积压({}), 动态调整带宽分配: 紧急[{:.1}%], 普通[{:.1}%], 慢速[{:.1}%]",
                                 emergency_size, emergency_ratio * 100.0, 
                                 normal_ratio * 100.0, slow_ratio * 100.0);
                    } 
                    // 如果普通队列有积压但紧急队列较少，增加普通队列带宽占比
                    else if normal_size > 500 && emergency_size < 50 {
                        normal_ratio = (normal_ratio * 1.3).min(0.7); // 最高占总带宽70%
                        emergency_ratio = ((1.0 - normal_ratio) * 0.5).max(0.2); // 最低20%
                        slow_ratio = (1.0 - emergency_ratio - normal_ratio).max(0.05); // 最低5%
                        
                        log::info!("检测到普通队列积压({}), 动态调整带宽分配: 紧急[{:.1}%], 普通[{:.1}%], 慢速[{:.1}%]",
                                 normal_size, emergency_ratio * 100.0, 
                                 normal_ratio * 100.0, slow_ratio * 100.0);
                    }
                    
                    // 更新令牌桶速率
                    {
                        let mut emergency_bucket = hub.emergency_token_bucket.lock().await;
                        emergency_bucket.reset_rate((total_bandwidth as f64 * emergency_ratio) as u64);
                    }
                    
                    {
                        let mut normal_bucket = hub.normal_token_bucket.lock().await;
                        normal_bucket.reset_rate((total_bandwidth as f64 * normal_ratio) as u64);
                    }
                    
                    {
                        let mut slow_bucket = hub.slow_token_bucket.lock().await;
                        slow_bucket.reset_rate((total_bandwidth as f64 * slow_ratio) as u64);
                    }
                }
            }
        });
    }

    // 添加客户端连接
    pub async fn add_client(&self, socket: WebSocketStream<TcpStream>, user_info: UserInfo) -> AppResult<()> {
        let addr = format!("{}_{}", user_info.user_name, SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos());
        
        let count = self.online_users.get(&user_info.user_name).map(|v| *v.value()).unwrap_or(0);
        let max_sessions = Settings::global().websocket.max_sessions_per_user;
        if count >= max_sessions as i32 {
            log::warn!("用户 {} 的连接数超过限制: {}/{}", user_info.user_name, count, max_sessions);
            self.force_remove_user(&user_info.user_name);
            return Ok(());
        }
        
        let (tx, _) = broadcast::channel(Settings::global().websocket.message_cache_size);
        
        let client = ActiveClient {
            user_info: user_info.clone(),
            tx: tx.clone(),
            last_active: SystemTime::now(),
        };

        // 先更新连接计数和用户列表
        let is_first_connection = count < 1;
        self.clients.insert(addr.clone(), client);
        self.online_users.insert(user_info.user_name.clone(), count + 1);
        
        // 异步处理用户加入通知和在线列表更新
        let hub = self.clone();
        let username = user_info.user_name.clone();
        tokio::spawn(async move {
            if is_first_connection {
                log::debug!("用户 {} 的第一个连接，通知主服务器用户加入", username);
                if let Err(e) = util::post_message_to_master("join", &username).await {
                    log::error!("通知主服务器用户加入失败: {}", e);
                } else {
                    log::debug!("已通知主服务器用户 {} 加入", username);
                }
                
                // 只在用户第一次连接时更新在线用户列表
                hub.update_online_users_list().await;
            }
        });

        log::debug!("用户 {} 的连接计数: {} (连接ID: {})", user_info.user_name, count + 1, addr);
        // log::debug!("当前在线用户总数: {}", self.online_users.len());

        // 异步发送在线用户列表给新连接的客户端
        let client_online_users = self.client_online_users.read().await.clone();
        let users_list = if !client_online_users.is_empty() {
            client_online_users
        } else {
            self.all_online_users.read().await.clone()
        };

        if users_list != "[]" {
            let hub = self.clone();
            let addr = addr.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if let Some(client) = hub.clients.get(&addr) {
                    match client.value().tx.send(WsMessage::new(users_list)) {
                        Ok(_) => log::debug!("成功发送在线用户列表到客户端"),
                        Err(e) => log::error!("发送在线用户列表失败: {}", e),
                    }
                }
            });
        }

        let (mut write, mut read) = socket.split();
        let hub = self.clone();
        let addr_clone = addr.clone();
        let username_clone = user_info.user_name.clone();
        let mut rx = tx.subscribe();
        
        // 处理发送消息的任务
        let send_task = tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                if let Some(delay) = msg.delay {
                    tokio::time::sleep(delay).await;
                }
                
                if let Err(e) = write.send(Message::Text(msg.data.clone())).await {
                    log::error!("发送消息到客户端失败 {} ({}): {}", username_clone, addr_clone, e);
                    
                    if e.to_string().contains("connection reset") || 
                       e.to_string().contains("broken pipe") || 
                       e.to_string().contains("closed") {
                        log::warn!("客户端连接已关闭，停止发送消息: {} ({})", username_clone, addr_clone);
                        break;
                    }
                }
            }
            
            log::debug!("客户端消息发送任务结束: {} ({})", username_clone, addr_clone);
        });
        
        // 处理接收消息的任务
        let hub_clone = hub.clone();
        let addr_clone2 = addr.clone();
        let username = user_info.user_name.clone();
        
        tokio::spawn(async move {
            while let Some(Ok(msg)) = read.next().await {
                match msg {
                    Message::Text(text) => {
                        if let Some(mut client) = hub_clone.clients.get_mut(&addr_clone2) {
                            client.value_mut().last_active = SystemTime::now();
                        }
                        
                        log::debug!("收到客户端消息: {} ({}): {}", username, addr_clone2, text);
                    }
                    Message::Close(_) => {
                        log::debug!("客户端主动断开连接: {} ({})", username, addr_clone2);
                        break;
                    }
                    _ => {
                        if let Some(mut client) = hub_clone.clients.get_mut(&addr_clone2) {
                            client.value_mut().last_active = SystemTime::now();
                        }
                        log::debug!("收到客户端其他类型消息: {} ({}): {:?}", username, addr_clone2, msg);
                    }
                }
            }
            
            // 异步处理连接断开
            let hub = hub_clone.clone();
            let username = username.clone();
            let addr = addr_clone2.clone();
            
            tokio::spawn(async move {
                // 先中止发送任务，确保不会继续向已断开的连接发送消息
                send_task.abort();
                
                // 移除客户端连接
                if hub.clients.remove(&addr).is_some() {
                    log::debug!("清理客户端连接: {} ({})", username, addr);
                }
                
                let count = hub.online_users.get(&username).map(|v| *v.value()).unwrap_or(0);
                if count <= 1 {
                    log::debug!("用户 {} 的最后一个连接断开，通知主服务器用户离开", username);
                    hub.online_users.remove(&username);
                    hub.force_remove_user(&username);
                    hub.update_online_users_list().await;
                } else {
                    hub.online_users.insert(username.clone(), count - 1);
                    log::debug!("用户 {} 的连接计数更新为: {}", username, count - 1);
                }
            });
        });
        
        Ok(())
    }

    pub fn global() -> &'static Hub {
        static INSTANCE: Lazy<Hub> = Lazy::new(Hub::new);
        &INSTANCE
    }

    pub fn init() {
        let hub = Self::global();
        // 启动消息处理循环
        hub.start_message_handlers();
    }

    // 更新在线用户列表
    async fn update_online_users_list(&self) {
        // 收集当前在线用户信息
        let mut online_users_map = std::collections::HashMap::new();
        self.clients.iter().for_each(|entry| {
            let user_info = entry.value().user_info.clone();
            online_users_map.insert(user_info.o_id.clone(), user_info);
        });
        
        let online_users: Vec<UserInfo> = online_users_map.into_values().collect();
        log::debug!("更新在线用户列表，当前在线用户数: {}", online_users.len());
        
        // 更新在线用户列表
        let users_json = json!(online_users).to_string();
        *self.all_online_users.write().await = users_json.clone();
        
    }

    // 处理主服务器消息
    async fn handle_master_message(&self, text: &str, socket: &mut WebSocketStream<TcpStream>) -> AppResult<()> {
        if text.contains(":::") {
            let parts: Vec<&str> = text.splitn(2, ":::").collect();
            if parts.len() == 2 && parts[0] == crate::conf::admin_key() {
                
                match parts[1] {
                    "hello" => {
                        if let Err(e) = socket.send(Message::Text("hello from rhyus-rust".to_string())).await {
                            log::error!("发送 hello 响应失败: {}", e);
                            return Err(e.into());
                        }
                    }
                    cmd if cmd == "clear" => {
                        // 清理不活跃用户（6小时以上）
                        let inactive_users = self.clean_inactive_connections(6).await;
                        
                        let response = if inactive_users.is_empty() {
                            json!({}).to_string()
                        } else {
                            json!(inactive_users).to_string()
                        };
                        
                        if let Err(e) = socket.send(Message::Text(response)).await {
                            log::error!("发送清理用户响应失败: {}", e);
                            return Err(e.into());
                        }
                        log::debug!("<------ master: clear")
                    }
                    "online" => {
                        // 直接收集所有连接的用户信息
                        let mut online_users_vec = Vec::new();
                        self.clients.iter().for_each(|entry| {
                            let user_info = entry.value().user_info.clone();
                            online_users_vec.push(json!({
                                "userName": user_info.user_name,
                                "userAvatarURL": user_info.user_avatar_url,
                                "homePage": format!("/member/{}", user_info.user_name)
                            }));
                        });
                        
                        // 直接构建 JSONArray 并返回
                        let users_json = match serde_json::to_string(&online_users_vec) {
                            Ok(json) => json,
                            Err(e) => {
                                log::error!("序列化在线用户列表失败: {}", e);
                                "[]".to_string()
                            }
                        };
                        
                        *self.all_online_users.write().await = users_json.clone();
                        
                        if let Err(e) = socket.send(Message::Text(users_json)).await {
                            log::error!("发送在线用户列表失败: {}", e);
                            return Err(e.into());
                        }
                    }
                    cmd if cmd.starts_with("tell") => {
                        let parts: Vec<&str> = cmd[5..].splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            let to = parts[0];
                            let content = parts[1];
                            // tell命令直接发送，不经过队列，不限制带宽
                            self.send_to_user(to, WsMessage::new(content));
                            socket.send(Message::Text("OK".to_string())).await?;
                        }
                    }
                    cmd if cmd.starts_with("msg") => {
                        let parts: Vec<&str> = cmd[4..].splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            let sender = parts[0];
                            let content = parts[1];
                            
                            self.send_to_user(sender, WsMessage::new(content));
                            
                            self.broadcast_to_clients(WsMessage::new(content), Some(sender));
                        } else {
                            log::error!("msg 命令参数错误: {}", cmd);
                        }
                    }
                    cmd if cmd.starts_with("all") => {
                        let content = &cmd[4..];
                        if !content.is_empty() {
                            // 紧急消息，使用紧急优先级
                            self.broadcast_to_clients(WsMessage::emergency(content), None);
                            socket.send(Message::Text("OK".to_string())).await?;
                        } else {
                            log::error!("all 命令参数错误: {}", cmd);
                        }
                    }
                    cmd if cmd.starts_with("push") => {
                        let content = &cmd[5..];
                        
                        // 保存服务器推送的数据
                        *self.client_online_users.write().await = content.to_string();
                        
                        // 解析在线用户列表并更新本地计数
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(content) {
                            if let Some(users) = json.get("users") {
                                if let Some(users_array) = users.as_array() {
                                    // 清空当前在线用户计数
                                    self.online_users.clear();
                                    
                                    // 更新在线用户计数
                                    for user in users_array {
                                        if let Some(username) = user.get("userName").and_then(|v| v.as_str()) {
                                            // 统计每个用户的连接数
                                            let count = self.clients.iter()
                                                .filter(|entry| entry.value().user_info.user_name == username)
                                                .count() as i32;
                                            if count > 0 {
                                                self.online_users.insert(username.to_string(), count);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
                        socket.send(Message::Text("OK".to_string())).await?;
                        // 异步广播给所有客户端，使用普通优先级
                        let content = content.to_string();
                        let hub = self.clone();
                        tokio::spawn(async move {
                            let _ = hub.message_handler.client_out_tx.send(WsMessage::new(content));
                        });
                    }
                    cmd if cmd.starts_with("slow") => {
                        let content = &cmd[5..];
                        // 慢速消息，使用慢速优先级，延迟为100毫秒
                        self.broadcast_to_clients(WsMessage::slow(content), None);
                        socket.send(Message::Text("OK".to_string())).await?;
                    }
                    cmd if cmd.starts_with("kick") => {
                        let username = &cmd[5..];
                        if !username.is_empty() {
                            // 踢出用户
                            self.clients.retain(|_, client| {
                                client.user_info.user_name != username
                            });
                            self.force_remove_user(username);
                            socket.send(Message::Text("OK".to_string())).await?;
                        } else {
                            log::error!("kick 命令参数错误: {}", cmd);
                        }
                    }
                    _ => {
                        log::error!("未知命令: {}", parts[1]);
                    }
                }
            } else {
                log::error!("无效的消息格式: {}", text);
            }
        }
        Ok(())
    }

    // 添加主服务器连接
    pub async fn add_master(&self, mut socket: WebSocketStream<TcpStream>) -> AppResult<()> {
        let addr = format!("master_{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs());
        
        self.masters.clear();
        let master = ActiveMaster { 
            // tx: self.master_tx.clone(), 
            last_active: SystemTime::now(),
        };
        self.masters.insert(addr.clone(), master);

        // 处理主服务器连接
        let hub = self.clone();
        tokio::spawn(async move {
            let addr_clone = addr.clone();
            
            // 处理消息循环
            while let Some(result) = socket.next().await {
                match result {
                    Ok(msg) => {
                        match msg {
                            Message::Text(text) => {
                                if let Some(mut master) = hub.masters.get_mut(&addr_clone) {
                                    master.last_active = SystemTime::now();
                                }
                                
                                if let Err(e) = hub.handle_master_message(&text, &mut socket).await {
                                    log::error!("处理主服务器消息失败: {}", e);
                                }
                            }
                            Message::Close(_) => {
                                break;
                            }
                            _ => {}
                        }
                    }
                    Err(e) => {
                        log::error!("主服务器连接错误: {}", e);
                        break;
                    }
                }
            }
        });

        Ok(())
    }

    // 向主服务器发送消息
    pub async fn send_to_master(&self, message: &str) -> AppResult<()> {
        // 直接通过HTTP发送消息到主服务器
        let url = format!("{}/chat-room/node/push", crate::conf::master_url());
        log::debug!("发送请求到主服务器: URL={}, 请求体={}", url, message);
        
        let client = reqwest::Client::new();
        let response = client.post(&url)
            .header("Content-Type", "application/json")
            .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36")
            .body(message.to_string())
            .send()
            .await
            .map_err(|e| crate::common::AppError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("发送消息到主服务器失败: {}", e),
            )))?;
            
        if response.status().is_success() {
            let body = response.text().await.unwrap_or_default();
            log::debug!("消息已成功发送到主服务器，响应: {}", body);
            Ok(())
        } else {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            log::error!("发送消息到主服务器失败: HTTP {} - {}", status, body);
            Err(crate::common::AppError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("主服务器返回错误状态码: {} - {}", status, body),
            )))
        }
    }
}

impl Clone for Hub {
    fn clone(&self) -> Self {
        Self {
            masters: self.masters.clone(),
            master_tx: self.master_tx.clone(),
            message_handler: self.message_handler.clone(),
            clients: self.clients.clone(),
            online_users: self.online_users.clone(),
            all_online_users: self.all_online_users.clone(),
            client_online_users: self.client_online_users.clone(),
            normal_queue: self.normal_queue.clone(),
            emergency_queue: self.emergency_queue.clone(),
            slow_queue: self.slow_queue.clone(),
            queue_sender: self.queue_sender.clone(),
            normal_thread_count: self.normal_thread_count,
            emergency_thread_count: self.emergency_thread_count,
            slow_thread_count: self.slow_thread_count,
            bandwidth_limit: self.bandwidth_limit.clone(),
            emergency_token_bucket: self.emergency_token_bucket.clone(),
            normal_token_bucket: self.normal_token_bucket.clone(),
            slow_token_bucket: self.slow_token_bucket.clone(),
            msg_stats: self.msg_stats.clone(),
        }
    }
} 