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
}

impl WsMessage {
    fn new(data: impl Into<String>) -> Self {
        Self {
            data: data.into(),
            delay: None,
        }
    }

    fn with_delay(data: impl Into<String>, delay: Duration) -> Self {
        Self {
            data: data.into(),
            delay: Some(delay),
        }
    }
}

// 活跃的主服务器连接
struct ActiveMaster {
    tx: broadcast::Sender<WsMessage>,
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

// 消息队列统计
struct MsgQueueStats {
    total_msgs: u64,         // 收到的消息总数
    processed_msgs: u64,     // 处理的消息总数
    broadcast_msgs: u64,     // 广播出去的消息总数
    queue_max_size: usize,   // 队列最大长度
    last_msg_time: SystemTime, // 最后处理消息的时间
    last_msg_content: String,  // 最后处理的消息内容
    connection_count: usize,   // 当前连接数量
    send_duration_ms: u64,     // 发送持续时间(毫秒)
}

impl MsgQueueStats {
    fn new() -> Self {
        Self {
            total_msgs: 0,
            processed_msgs: 0,
            broadcast_msgs: 0,
            queue_max_size: 0,
            last_msg_time: SystemTime::now(),
            last_msg_content: String::new(),
            connection_count: 0,
            send_duration_ms: 0,
        }
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

    // 消息队列相关
    message_queue: Arc<Mutex<VecDeque<QueuedMessage>>>,
    queue_sender: broadcast::Sender<()>,
    
    // 消息队列统计
    msg_stats: Arc<tokio::sync::RwLock<MsgQueueStats>>,
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

        let hub = Self {
            masters: Arc::new(DashMap::new()),
            master_tx,
            message_handler,
            clients: Arc::new(DashMap::new()),
            online_users: Arc::new(DashMap::new()),
            all_online_users: Arc::new(tokio::sync::RwLock::new("[]".to_string())),
            client_online_users: Arc::new(tokio::sync::RwLock::new("{}".to_string())),
            message_queue: Arc::new(Mutex::new(VecDeque::with_capacity(message_cache_size / 2))),
            queue_sender,
            msg_stats: Arc::new(tokio::sync::RwLock::new(MsgQueueStats::new())),
        };

        // 启动心跳检测和清理任务
        let hub_clone = hub.clone();
        let heartbeat_interval = config.server.heartbeat_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(heartbeat_interval));
            loop {
                interval.tick().await;
                hub_clone.check_and_clean().await;
            }
        });

        // 启动消息队列处理器
        hub.start_queue_processor();
        
        hub.start_msg_stats_monitor();
        
        hub
    }

    async fn check_and_clean(&self) {
        let ping_message = WsMessage::new("{\"ping\":\"pong\"}");
        
        // 只发送心跳消息并记录活跃连接数
        let mut master_num = 0;
        let mut client_num = 0;
        
        // 主服务器心跳
        self.masters.iter().for_each(|master| {
            let _ = master.value().tx.send(ping_message.clone());
            master_num += 1;
        });

        // 客户端心跳
        self.clients.iter().for_each(|client| {
            let _ = client.value().tx.send(ping_message.clone());
            client_num += 1;
        });

        log::debug!("active master: {}, active client: {}", master_num, client_num);
    }

    // pub fn remove_user_session(&self, username: &str) {
    //     if let Some(mut count) = self.online_users.get_mut(username) {
    //         *count -= 1;
    //         if *count <= 0 {
    //             self.online_users.remove(username);
    //             // 用户的最后一个连接断开，通知主服务器用户离开
    //             let username = username.to_string();
    //             tokio::spawn(async move {
    //                 if let Err(e) = util::post_message_to_master("leave", &username).await {
    //                     log::error!("通知主服务器用户离开失败: {}", e);
    //                 } else {
    //                     log::debug!("已通知主服务器用户 {} 离开", username);
    //                 }
    //             });
    //         }
    //     }
    // }

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
            let mut failed_clients = Vec::new();
            // let mut sent = false;
            
            // 尝试向用户的所有连接发送消息
            for entry in hub.clients.iter() {
                let client = entry.value();
                if client.user_info.user_name == username {
                    if let Err(e) = client.tx.send(msg_clone.clone()) {
                        log::error!("向用户 {} 发送消息失败: {}", username, e);
                        failed_clients.push(entry.key().clone());
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

    // 广播消息给所有客户端
    fn broadcast_to_clients(&self, msg: WsMessage) {
        let hub = self.clone();
        let msg_clone = msg.clone();
        
        tokio::spawn(async move {
            let mut sent_count = 0;
            let mut failed_clients = Vec::new();
            
            // 向所有客户端广播消息
            for entry in hub.clients.iter() {
                if let Err(e) = entry.value().tx.send(msg_clone.clone()) {
                    log::error!("向客户端 {} 广播消息失败: {}", entry.key(), e);
                    failed_clients.push(entry.key().clone());
                } else {
                    sent_count += 1;
                }
            }
            
            // // 移除发送失败的客户端
            // for key in failed_clients {
            //     hub.clients.remove(&key);
            // }
            
            if hub.clients.len() > 0 && sent_count == 0 {
                log::error!("广播消息全部失败，总客户端数: {}", hub.clients.len());
            }
        });
    }

    // 启动消息队列处理器
    fn start_queue_processor(&self) {
        let hub = self.clone();
        let mut rx = self.queue_sender.subscribe();
        
        tokio::spawn(async move {
            loop {
                // 等待队列通知
                if rx.recv().await.is_ok() {
                    // 检查队列中是否有消息
                    loop {
                        let has_message = {
                            let queue = hub.message_queue.lock().unwrap();
                            !queue.is_empty()
                        };
                        
                        if !has_message {
                            break;
                        }
                        
                        let message = {
                            let mut queue = hub.message_queue.lock().unwrap();
                            queue.pop_front()
                        };
                        
                        if let Some(msg) = message {
                            // 收集所有客户端连接
                            let clients: Vec<_> = hub.clients.iter()
                                .map(|entry| (
                                    entry.key().clone(), 
                                    entry.value().user_info.user_name.clone(),
                                    entry.value().tx.clone()
                                ))
                                .collect();
                            
                            let connection_count = clients.len();
                            let start_time = SystemTime::now();
                            
                            // 更新统计信息
                            {
                                let mut stats = hub.msg_stats.write().await;
                                stats.processed_msgs += 1;
                                stats.broadcast_msgs += connection_count as u64;
                                stats.last_msg_time = start_time;
                                stats.last_msg_content = msg.content.clone();
                                stats.connection_count = connection_count;
                            }
                            
                            log::debug!("开始发送消息给 {} 个连接", connection_count);
                            
                            let sender = &msg.sender;
                            let mut success_count = 0;
                            let mut failed_clients = Vec::new();
                            
                            for (addr, username, tx) in clients {
                                if &username == sender {
                                    continue;
                                }
                                
                                if let Err(err) = tx.send(WsMessage::new(&msg.content)) {
                                    log::error!("发送消息给客户端失败: {} - {}", username, err);
                                    failed_clients.push((addr, username));
                                } else {
                                    success_count += 1;
                                    
                                    let delay_ms = crate::conf::Settings::global().websocket.message_send_delay_ms;
                                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                                }
                            }
                            
                            // // 移除失败的客户端
                            // for (addr, username) in failed_clients {
                            //     if hub.clients.remove(&addr).is_some() {
                            //         hub.remove_user_session(&username);
                            //     }
                            // }
                            
                            // 计算发送持续时间
                            let duration = SystemTime::now().duration_since(start_time).unwrap_or_default();
                            let duration_ms = duration.as_millis() as u64;
                            
                            // 更新发送时间统计
                            {
                                let mut stats = hub.msg_stats.write().await;
                                stats.send_duration_ms = duration_ms;
                            }
                            
                            log::debug!("消息发送完成，成功发送给 {} 个连接，耗时 {}ms", success_count, duration_ms);
                        }
                    }
                }
            }
        });
    }

    // 启动消息处理循环
    pub fn start_message_handlers(&self) {
        // 处理广播到客户端的消息
        let hub = self.clone();
        tokio::spawn(async move {
            let mut rx = hub.message_handler.client_out_tx.subscribe();
            while let Ok(msg) = rx.recv().await {
                hub.broadcast_to_clients(msg);
            }
        });
    }

    // 消息统计监控
    fn start_msg_stats_monitor(&self) {
        let hub = self.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                
                // 获取当前队列大小
                let queue_size = {
                    let queue = hub.message_queue.lock().unwrap();
                    queue.len()
                };
                
                // 获取连接数量
                let connection_count = hub.clients.len();
                
                // 获取统计信息
                let stats = hub.msg_stats.read().await;
                
                // 计算平均每条消息的广播数
                let avg_broadcast = if stats.processed_msgs > 0 {
                    stats.broadcast_msgs as f64 / stats.processed_msgs as f64
                } else {
                    0.0
                };
                
                log::debug!("消息队列统计 - 收到消息: {}, 处理消息: {}, 广播消息: {}, 平均广播: {:.2}", 
                    stats.total_msgs, stats.processed_msgs, stats.broadcast_msgs, avg_broadcast);
                
                log::debug!("连接统计 - 当前连接数: {}, 最后发送耗时: {}ms", 
                    connection_count, stats.send_duration_ms);
                
                log::debug!("队列状态 - 最大长度: {}, 当前长度: {}", stats.queue_max_size, queue_size);
                
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
                        log::warn!("收到客户端未知消息类型");
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
        // 启动心跳检测
        hub.start_heartbeat();
    }

    // 启动心跳检测
    fn start_heartbeat(&self) {
        let hub = self.clone();
        let heartbeat_interval = Settings::global().server.heartbeat_interval;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(heartbeat_interval));
            loop {
                interval.tick().await;
                
                let heartbeat_msg = WsMessage::new("{\"type\":\"heartbeat\"}");
                hub.clients.iter().for_each(|entry| {
                    if let Err(e) = entry.value().tx.send(heartbeat_msg.clone()) {
                        log::debug!("发送心跳消息失败: {} ({}): {}", 
                            entry.value().user_info.user_name, entry.key(), e);
                    }
                });
                
                log::debug!("当前在线客户端数量: {}", hub.clients.len());
            }
        });
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
            let parts: Vec<&str> = text.split(":::").collect();
            if parts.len() == 2 && parts[0] == crate::conf::admin_key() {
                
                match parts[1] {
                    "hello" => {
                        if let Err(e) = socket.send(Message::Text("hello from rhyus-rust".to_string())).await {
                            log::error!("发送 hello 响应失败: {}", e);
                            return Err(e.into());
                        }
                    }
                    cmd if cmd == "clear" => {
                        let mut inactive_users = std::collections::HashMap::new();
                        let now = SystemTime::now();
                        
                        // 收集不活跃用户
                        self.clients.iter().for_each(|client| {
                            if let Ok(duration) = now.duration_since(client.value().last_active) {
                                let hours = duration.as_secs() / 3600; // 转换为小时
                                if hours >= 6 {
                                    inactive_users.insert(
                                        client.value().user_info.user_name.clone(),
                                        hours
                                    );
                                }
                            }
                        });
                        
                        // 强制移除不活跃用户的所有连接
                        for username in inactive_users.keys() {
                            self.force_remove_user(username);
                        }
                        
                        // 更新在线用户列表
                        if !inactive_users.is_empty() {
                            self.update_online_users_list().await;
                        }
                        
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
                            self.send_to_user(to, WsMessage::new(content));
                            socket.send(Message::Text("OK".to_string())).await?;
                        }
                    }
                    cmd if cmd.starts_with("msg") => {
                        let parts: Vec<&str> = cmd[4..].splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            let sender = parts[0];
                            let content = parts[1];
                            
                            let mut sender_found = false;
                            for entry in self.clients.iter() {
                                if entry.value().user_info.user_name == sender {
                                    if let Err(err) = entry.value().tx.send(WsMessage::new(content)) {
                                        log::error!("直接发送消息给发送者失败: {} - {}", sender, err);
                                    } else {
                                        sender_found = true;
                                        log::debug!("已直接发送消息给发送者: {}", sender);
                                    }
                                    break;
                                }
                            }
                            
                            if !sender_found {
                                log::debug!("发送者 {} 不在本节点", sender);
                            }
                            
                            // 将消息添加到队列
                            let queue_size = {
                                let mut queue = self.message_queue.lock().unwrap();
                                queue.push_back(QueuedMessage {
                                    sender: sender.to_string(),
                                    content: content.to_string(),
                                    timestamp: SystemTime::now(),
                                });
                                queue.len()
                            };
                            
                            // 更新统计信息
                            {
                                let mut stats = self.msg_stats.write().await;
                                stats.total_msgs += 1;
                                if queue_size > stats.queue_max_size {
                                    stats.queue_max_size = queue_size;
                                }
                            }
                            
                            // 通知队列处理器
                            let _ = self.queue_sender.send(());
                            
                        } else {
                            log::error!("msg 命令参数错误: {}", cmd);
                        }
                    }
                    cmd if cmd.starts_with("all") => {
                        let content = &cmd[4..];
                        if !content.is_empty() {
                            self.broadcast_to_clients(WsMessage::new(content));
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
                        // 异步广播给所有客户端
                        let content = content.to_string();
                        let hub = self.clone();
                        tokio::spawn(async move {
                            let _ = hub.message_handler.client_out_tx.send(WsMessage::new(content));
                        });
                    }
                    cmd if cmd.starts_with("slow") => {
                        let content = &cmd[5..];
                        self.broadcast_to_clients(WsMessage::with_delay(content.to_string(), Duration::from_millis(100)));
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
            tx: self.master_tx.clone(), 
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
            message_queue: self.message_queue.clone(),
            queue_sender: self.queue_sender.clone(),
            msg_stats: self.msg_stats.clone(),
        }
    }
} 