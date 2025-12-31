use crate::{
    common::{AppError, AppResult},
    conf::Settings,
    model::UserInfo,
    util,
};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use std::sync::{RwLock, atomic::{AtomicU8, AtomicU64, Ordering}};
use reqwest;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, Instant};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, Semaphore};
use tokio_tungstenite::{
    tungstenite::{Error as WsError, Message},
    WebSocketStream,
};
use std::sync::atomic::Ordering::Acquire;
use std::time::UNIX_EPOCH;
use std::sync::atomic::Ordering::Release;
use std::sync::atomic::Ordering::Relaxed;

impl From<WsError> for AppError {
    fn from(err: WsError) -> Self {
        AppError::Io(std::io::Error::other(format!("WebSocket error: {}", err)))
    }
}

/// 消息优先级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    Emergency = 2,
}

/// WebSocket消息
#[derive(Debug, Clone)]
pub struct WsMessage {
    pub content: String,
    pub priority: MessagePriority,
    pub delay: Option<Duration>,
    pub size_bytes: usize,
    pub excluded_user: Option<String>,
}

unsafe impl Send for WsMessage {}
unsafe impl Sync for WsMessage {}

impl WsMessage {
    pub fn new(content: impl Into<String>) -> Self {
        let content = content.into();
        let size_bytes = content.len();
        Self {
            content,
            priority: MessagePriority::Normal,
            delay: None,
            size_bytes,
            excluded_user: None,
        }
    }

    pub fn with_priority(content: impl Into<String>, priority: MessagePriority) -> Self {
        let content = content.into();
        let size_bytes = content.len();
        Self {
            content,
            priority,
            delay: None,
            size_bytes,
            excluded_user: None,
        }
    }

    pub fn emergency(content: impl Into<String>) -> Self {
        Self::with_priority(content, MessagePriority::Emergency)
    }

    pub fn low_with_delay(content: impl Into<String>, delay: Duration) -> Self {
        let content = content.into();
        let size_bytes = content.len();
        Self {
            content,
            priority: MessagePriority::Low,
            delay: Some(delay),
            size_bytes,
            excluded_user: None,
        }
    }
}

/// 原子令牌桶
#[derive(Debug)]
struct AtomicTokenBucket {
    tokens_fp: AtomicU64,
    capacity_fp: u64,
    refill_rate_fp: u64,
    last_refill: AtomicU64,
}

impl AtomicTokenBucket {
    fn new(capacity: f64, refill_rate_per_sec: f64) -> Self {
        let capacity_fp = (capacity * 1000.0) as u64;
        let refill_rate_fp = (refill_rate_per_sec * 1000.0 / 1000.0) as u64;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        Self {
            tokens_fp: AtomicU64::new(capacity_fp),
            capacity_fp,
            refill_rate_fp,
            last_refill: AtomicU64::new(now_ms),
        }
    }

    fn try_consume(&self, tokens_needed: f64) -> bool {
        let tokens_needed_fp = (tokens_needed * 1000.0) as u64;
        self.try_refill();
        let current = self.tokens_fp.load(Acquire);
        if current >= tokens_needed_fp {
            self.tokens_fp.compare_exchange(current, current - tokens_needed_fp, Release, Relaxed).is_ok()
        } else {
            false
        }
    }

    fn try_refill(&self) {
        let now_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64;
        let last_ms = self.last_refill.load(Relaxed);
        if now_ms - last_ms >= 10 {
            let elapsed_ms = now_ms - last_ms;
            let new_tokens = elapsed_ms * self.refill_rate_fp;
            let current = self.tokens_fp.load(Relaxed);
            let updated = (current + new_tokens).min(self.capacity_fp);
            if self.tokens_fp.compare_exchange(current, updated, Release, Relaxed).is_ok() {
                self.last_refill.store(now_ms, Release);
            }
        }
    }

    async fn consume(&self, tokens_needed: f64) {
        loop {
            if self.try_consume(tokens_needed) {
                return;
            }
            let deficit_fp = (tokens_needed * 1000.0) as u64 - self.tokens_fp.load(Relaxed);
            let wait_ms = deficit_fp / self.refill_rate_fp.max(1);
            tokio::time::sleep(Duration::from_millis(wait_ms.clamp(1, 100))).await;
        }
    }

    async fn consume_batch(&self, requests: &[(f64, MessagePriority)]) -> Vec<bool> {
        let mut results = vec![false; requests.len()];
        for (i, &(tokens_needed, _)) in requests.iter().enumerate() {
            if self.try_consume(tokens_needed) {
                results[i] = true;
            }
        }
        for (i, &(tokens_needed, priority)) in requests.iter().enumerate() {
            if !results[i] {
                if priority == MessagePriority::Emergency {
                    self.consume(tokens_needed).await;
                    results[i] = true;
                } else {
                    results[i] = tokio::time::timeout(Duration::from_millis(50), self.consume(tokens_needed)).await.is_ok();
                }
            }
        }
        results
    }
}

/// 客户端连接信息
#[derive(Debug, Clone)]
struct ClientConnection {
    user_info: UserInfo,
    sender: broadcast::Sender<WsMessage>,
    last_active: SystemTime,
    connection_id: u64, // 添加连接ID用于精确标识
    // token_bucket: Arc<tokio::sync::Mutex<TokenBucket>>, // 每个连接的速率限制
    // message_count: Arc<AtomicU64>, // 消息计数
}

impl ClientConnection {
    fn new(user_info: UserInfo, connection_id: u64) -> Self {
        let config = Settings::global();
        let (sender, _) = broadcast::channel(config.websocket.message_cache_size);

        Self {
            user_info,
            sender,
            last_active: SystemTime::now(),
            connection_id,
        }
    }

    fn update_activity(&mut self) {
        self.last_active = SystemTime::now();
    }

    fn send_message(&self, message: &WsMessage) -> Result<usize, broadcast::error::SendError<WsMessage>> {
        self.sender.send(message.clone())
    }
}

/// 高效的环形缓冲区连接管理器
#[derive(Debug, Clone)]
struct ConnectionRing {
    connections: Vec<Option<ClientConnection>>,
    capacity: usize,
    count: usize,
    next_slot: usize,
}

impl ConnectionRing {
    fn new(capacity: usize) -> Self {
        Self {
            connections: vec![None; capacity],
            capacity,
            count: 0,
            next_slot: 0,
        }
    }

    /// 添加新连接，如果满了则替换最旧的连接
    fn add_connection(&mut self, connection: ClientConnection) -> Option<u64> {
        if self.count < self.capacity {
            // 还有空位，直接添加
            while self.connections[self.next_slot].is_some() {
                self.next_slot = (self.next_slot + 1) % self.capacity;
            }
            self.connections[self.next_slot] = Some(connection);
            self.count += 1;
            None
        } else {
            // 满了，替换最旧的连接
            let old_connection = self.connections[self.next_slot].replace(connection);
            self.next_slot = (self.next_slot + 1) % self.capacity;
            old_connection.map(|conn| conn.connection_id)
        }
    }

    /// 根据连接ID移除连接
    fn remove_connection(&mut self, connection_id: u64) -> bool {
        for slot in &mut self.connections {
            if let Some(conn) = slot {
                if conn.connection_id == connection_id {
                    *slot = None;
                    self.count = self.count.saturating_sub(1);
                    return true;
                }
            }
        }
        false
    }

    /// 获取活跃连接数
    fn active_count(&self) -> usize {
        self.count
    }

    /// 更新所有连接的活跃时间
    fn update_all_activity(&mut self) {
        for conn in self.connections.iter_mut().flatten() {
            conn.update_activity();
        }
    }

    /// 获取所有活跃连接的引用
    fn active_connections(&self) -> impl Iterator<Item = &ClientConnection> {
        self.connections.iter().filter_map(|slot| slot.as_ref())
    }

    /// 获取第一个连接的引用
    fn first(&self) -> Option<&ClientConnection> {
        self.active_connections().next()
    }
}

impl Default for ConnectionRing {
    fn default() -> Self {
        Self::new(8) // 默认每个用户最多8个连接
    }
}

pub struct Hub {
    /// 活跃的客户端连接
    clients: DashMap<String, ConnectionRing>,
    
    /// 在线用户计数
    online_users: DashMap<String, usize>,

    /// 缓存在线用户列表
    cached_online_list: RwLock<String>,
    
    /// 分优先级的广播通道
    emergency_sender: broadcast::Sender<WsMessage>,
    normal_sender: broadcast::Sender<WsMessage>,
    low_sender: broadcast::Sender<WsMessage>,
    
    /// 连接ID生成器
    connection_counter: AtomicU64,
    
    /// 全局资源控制
    global_semaphore: Arc<Semaphore>, // 限制并发处理数
    
    /// 消息频率监控器
    frequency_monitor: Arc<MessageFrequencyMonitor>,
    
    /// 消息缓冲区
    micro_batch_buffer: Arc<tokio::sync::Mutex<MessageBuffer>>,
    batch_buffer: Arc<tokio::sync::Mutex<MessageBuffer>>,
    
    /// 全局广播速率控制 - 使用原子令牌桶
    normal_token_bucket: Arc<AtomicTokenBucket>, 
    low_token_bucket: Arc<AtomicTokenBucket>,
}

impl Hub {
    pub fn new() -> Self {
        let config = Settings::global();
        let channel_size = config.websocket.message_cache_size;
        
        // 根据配置的带宽比例分配通道容量
        let emergency_size = (channel_size as f64 * config.websocket.emergency_queue_ratio) as usize;
        let normal_size = (channel_size as f64 * config.websocket.normal_queue_ratio) as usize;  
        let low_size = (channel_size as f64 * config.websocket.slow_queue_ratio) as usize;
        
        let (emergency_sender, _) = broadcast::channel(emergency_size.max(64)); // 最小64
        let (normal_sender, _) = broadcast::channel(normal_size.max(128));      // 最小128
        let (low_sender, _) = broadcast::channel(low_size.max(32));             // 最小32
        
        // 全局并发限制：最多1000个并发连接处理
        let global_semaphore = Arc::new(Semaphore::new(1000));
        
        // 创建全局广播的原子令牌桶
        let total_bandwidth = config.websocket.default_bandwidth_limit_kb as f64;
        let normal_bucket = AtomicTokenBucket::new(
            total_bandwidth * 2.0,
            total_bandwidth * config.websocket.normal_queue_ratio  
        );
        let low_bucket = AtomicTokenBucket::new(
            total_bandwidth * 2.0,
            total_bandwidth * config.websocket.slow_queue_ratio
        );
        
        // 创建频率监控器
        let frequency_monitor = Arc::new(MessageFrequencyMonitor::new());
        
        // 创建消息缓冲区
        let micro_batch_buffer = Arc::new(tokio::sync::Mutex::new(MessageBuffer::new(
            config.websocket.micro_batch.batch_size,
            config.websocket.micro_batch.max_delay_ms,
        )));
        let batch_buffer = Arc::new(tokio::sync::Mutex::new(MessageBuffer::new(
            config.websocket.batch.batch_size,
            config.websocket.batch.max_delay_ms,
        )));
        
        let hub = Self {
            clients: DashMap::new(),
            online_users: DashMap::new(),
            cached_online_list: RwLock::new("[]".to_string()),
            emergency_sender,
            normal_sender,
            low_sender,
            connection_counter: AtomicU64::new(1),
            global_semaphore,
            frequency_monitor,
            micro_batch_buffer,
            batch_buffer,
            normal_token_bucket: Arc::new(normal_bucket),
            low_token_bucket: Arc::new(low_bucket),
        };
        
        // 启动频率监控
        hub.start_frequency_monitoring();
        
        hub
    }

    /// 获取全局实例
    pub fn global() -> &'static Self {
        use std::sync::OnceLock;
        static HUB: OnceLock<Hub> = OnceLock::new();
        HUB.get_or_init(Self::new)
    }

    /// 初始化WebSocket服务
    pub fn init() {
        // 确保Hub单例被初始化
        let _ = Self::global();
        
        let config = Settings::global();
        log::info!("WebSocket Hub initialized");
        log::info!("带宽限制配置:");
        log::info!("  - 默认带宽限制: {} KB/s", config.websocket.default_bandwidth_limit_kb);
        log::info!("  - 紧急消息比例: {:.1}%", config.websocket.emergency_queue_ratio * 100.0);
        log::info!("  - 普通消息比例: {:.1}%", config.websocket.normal_queue_ratio * 100.0);
        log::info!("  - 低优先级比例: {:.1}%", config.websocket.slow_queue_ratio * 100.0);
        log::info!("  - 每连接令牌桶容量: {:.0} 令牌", config.websocket.default_bandwidth_limit_kb as f64 * 2.0);
    }

    /// 批量消费令牌
    async fn consume_tokens_batch(
        normal_bucket: &AtomicTokenBucket,
        low_bucket: &AtomicTokenBucket,
        messages: &[WsMessage],
    ) -> (Vec<bool>, Vec<bool>) {
        let mut normal_requests = Vec::new();
        let mut low_requests = Vec::new();
        for msg in messages {
            if msg.priority != MessagePriority::Emergency {
                let size_kb = (msg.size_bytes as f64 / 1024.0).max(1.0);
                match msg.priority {
                    MessagePriority::Normal => normal_requests.push((size_kb, msg.priority)),
                    MessagePriority::Low => low_requests.push((size_kb, msg.priority)),
                    _ => {}
                }
            }
        }
        tokio::join!(
            async { if normal_requests.is_empty() { vec![] } else { normal_bucket.consume_batch(&normal_requests).await } },
            async { if low_requests.is_empty() { vec![] } else { low_bucket.consume_batch(&low_requests).await } }
        )
    }
    
    /// 发送消息到对应通道
    fn send_messages_to_channels(
        emergency_sender: &broadcast::Sender<WsMessage>,
        normal_sender: &broadcast::Sender<WsMessage>,
        low_sender: &broadcast::Sender<WsMessage>,
        messages: Vec<WsMessage>,
    ) {
        for msg in messages {
            let sender = match msg.priority {
                MessagePriority::Emergency => emergency_sender,
                MessagePriority::Normal => normal_sender,
                MessagePriority::Low => low_sender,
            };
            if let Err(e) = sender.send(msg) {
                log::error!("发送消息失败: {}", e);
            }
        }
    }
    
    /// 刷新缓冲区并发送
    async fn flush_buffer_with_tokens(
        buffer: &Arc<tokio::sync::Mutex<MessageBuffer>>,
        normal_bucket: &AtomicTokenBucket,
        low_bucket: &AtomicTokenBucket,
        emergency_sender: &broadcast::Sender<WsMessage>,
        normal_sender: &broadcast::Sender<WsMessage>,
        low_sender: &broadcast::Sender<WsMessage>,
    ) {
        let mut buf = buffer.lock().await;
        if buf.should_flush() {
            let messages = buf.flush();
            drop(buf);
            if !messages.is_empty() {
                let (normal_results, low_results) = Self::consume_tokens_batch(normal_bucket, low_bucket, &messages).await;
                log::debug!("批量令牌消费: Normal={}/{}, Low={}/{}",
                    normal_results.iter().filter(|&&r| r).count(), normal_results.len(),
                    low_results.iter().filter(|&&r| r).count(), low_results.len());
                Self::send_messages_to_channels(emergency_sender, normal_sender, low_sender, messages);
            }
        }
    }

    /// 添加客户端连接
    pub async fn add_client(&self, socket: WebSocketStream<TcpStream>, user_info: UserInfo) -> AppResult<()> {
        // 获取全局资源许可
        let _permit = self.global_semaphore.acquire().await.map_err(|_| {
            AppError::Io(std::io::Error::other("无法接受新连接"))
        })?;

        let username = user_info.user_name.clone();
        let config = Settings::global();
        
        // 检查连接数限制
        let connection_count = self.get_user_connection_count(&username);
        if connection_count >= config.websocket.max_sessions_per_user {
            log::warn!("用户 {} 连接数超限 ({}/{})", username, connection_count, config.websocket.max_sessions_per_user);
            return Ok(());
        }

        // 生成唯一连接ID
        let connection_id = self.connection_counter.fetch_add(1, Ordering::Relaxed);
        let connection = ClientConnection::new(user_info.clone(), connection_id);
        
        // 订阅不同优先级的消息通道
        let mut emergency_receiver = self.emergency_sender.subscribe();
        let mut normal_receiver = self.normal_sender.subscribe(); 
        let mut low_receiver = self.low_sender.subscribe();
        
        // 保存个人消息发送器的克隆，避免被移动
        let personal_sender = connection.sender.clone();
        
        // 添加连接到RingBuffer
        let mut ring = self.clients.entry(username.clone()).or_default();
        let replaced_id = ring.add_connection(connection);
        
        if let Some(old_id) = replaced_id {
            log::warn!("用户 {} 连接已满，替换旧连接 ID: {}", username, old_id);
        }
        
        // 更新在线用户计数
        let new_count = ring.active_count();
        drop(ring); // 释放锁
        self.online_users.insert(username.clone(), new_count);

        // 在用户连接建立后，发送缓存的在线用户列表
        let cached_list = self.cached_online_list.read().unwrap().clone();
        if !cached_list.is_empty() && cached_list != "[]" {
            let personal_sender = personal_sender.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if let Err(e) = personal_sender.send(WsMessage::new(cached_list)) {
                    log::error!("发送在线用户列表失败: {}", e);
                }
            });
        }

        // 如果是首次连接，通知主服务器
        if connection_count == 0 {
            self.notify_master_user_join(&username).await;
        }

        // 分离socket为读写两部分
        let (mut sink, mut stream) = socket.split();
        
        // 现在从保存的sender创建接收器
        let mut personal_receiver = personal_sender.subscribe();
        
        log::debug!("用户 {} 连接已建立 ({}/{})", username, new_count, config.websocket.max_sessions_per_user);

        // 管理所有异步操作
        loop {
            tokio::select! {
                // 处理接收到的WebSocket消息
                msg_result = stream.next() => {
                    match msg_result {
                        Some(Ok(msg)) => {
                            match msg {
                                Message::Text(text) => {
                                    self.update_client_activity(&username);
                                    log::debug!("收到用户 {} 消息: {}", username, text);
                                }
                                Message::Close(_) => {
                                    log::debug!("用户 {} 主动断开连接", username);
                                    break;
                                }
                                _ => {
                                    self.update_client_activity(&username);
                                }
                            }
                        }
                        Some(Err(e)) => {
                            log::debug!("用户 {} 连接错误: {}", username, e);
                            break;
                        }
                        None => {
                            log::debug!("用户 {} 流结束", username);
                            break;
                        }
                    }
                }
                // 处理个人消息
                msg = personal_receiver.recv() => {
                    match msg {
                        Ok(ws_msg) => {
                            if let Err(e) = Self::send_message_to_sink(&mut sink, &ws_msg).await {
                                log::error!("发送个人消息失败 {}: {}", username, e);
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            log::debug!("个人消息通道关闭: {}", username);
                            break;
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            log::warn!("用户 {} 个人消息滞后，跳过 {} 条", username, n);
                        }
                    }
                }
                // 处理紧急消息 (优先级最高)
                msg = emergency_receiver.recv() => {
                    match msg {
                        Ok(ws_msg) => {
                            // 检查是否应该排除当前用户
                            if let Some(excluded) = &ws_msg.excluded_user {
                                if excluded == &username {
                                    continue; // 跳过被排除的用户
                                }
                            }
                            
                            if let Err(e) = Self::send_message_to_sink(&mut sink, &ws_msg).await {
                                log::error!("发送紧急消息失败 {}: {}", username, e);
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            log::debug!("紧急消息通道关闭: {}", username);
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            log::warn!("用户 {} 紧急消息滞后，跳过 {} 条", username, n);
                        }
                    }
                }
                // 处理普通消息
                msg = normal_receiver.recv() => {
                    match msg {
                        Ok(ws_msg) => {
                            // 检查是否应该排除当前用户
                            if let Some(excluded) = &ws_msg.excluded_user {
                                if excluded == &username {
                                    continue; // 跳过被排除的用户
                                }
                            }
                            
                            if let Err(e) = Self::send_message_to_sink(&mut sink, &ws_msg).await {
                                log::error!("发送普通消息失败 {}: {}", username, e);
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            log::debug!("普通消息通道关闭: {}", username);
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            log::warn!("用户 {} 普通消息滞后，跳过 {} 条", username, n);
                        }
                    }
                }
                // 处理低优先级消息  
                msg = low_receiver.recv() => {
                    match msg {
                        Ok(ws_msg) => {
                            // 检查是否应该排除当前用户
                            if let Some(excluded) = &ws_msg.excluded_user {
                                if excluded == &username {
                                    continue; // 跳过被排除的用户
                                }
                            }
                            
                            if let Err(e) = Self::send_message_to_sink(&mut sink, &ws_msg).await {
                                log::error!("发送低优先级消息失败 {}: {}", username, e);
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            log::debug!("低优先级消息通道关闭: {}", username);
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            log::warn!("用户 {} 低优先级消息滞后，跳过 {} 条", username, n);
                        }
                    }
                }
            }
        }

        // 结构化清理
        self.remove_client_connection(&username, connection_id).await;
        log::debug!("用户 {} 连接处理完成 (ID: {})", username, connection_id);
        
        Ok(())
    }

    /// 发送消息到sink
    async fn send_message_to_sink(
        sink: &mut futures::stream::SplitSink<WebSocketStream<TcpStream>, Message>,
        ws_msg: &WsMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // 如果有延迟，先等待
        if let Some(delay) = ws_msg.delay {
            tokio::time::sleep(delay).await;
        }

        sink.send(Message::Text(ws_msg.content.clone())).await?;
        Ok(())
    }

    /// 更新客户端活跃时间
    fn update_client_activity(&self, username: &str) {
        if let Some(mut connections) = self.clients.get_mut(username) {
            // 更新所有连接的活跃时间
            connections.update_all_activity();
        }
    }

    /// 获取用户连接数
    fn get_user_connection_count(&self, username: &str) -> usize {
        self.clients.get(username)
            .map(|conns| conns.active_count())
            .unwrap_or(0)
    }

    /// 移除客户端连接
    async fn remove_client_connection(&self, username: &str, connection_id: u64) {
        let should_notify_leave = if let Some(mut connections) = self.clients.get_mut(username) {
            // 根据连接ID精确移除
            connections.remove_connection(connection_id);
            
            let remaining = connections.active_count();
            if remaining == 0 {
                drop(connections); // 释放锁
                self.clients.remove(username);
                self.online_users.remove(username);
                true
            } else {
                self.online_users.insert(username.to_string(), remaining);
                false
            }
        } else {
            false
        };

        if should_notify_leave {
            self.notify_master_user_leave(username).await;
            // self.broadcast_online_users().await;
        }

        log::debug!("用户 {} 连接已断开 (ID: {})", username, connection_id);
    }

    /// 发送消息给特定用户
    pub fn send_to_user(&self, username: &str, message: WsMessage) {
        if let Some(connections) = self.clients.get(username) {
            for connection in connections.active_connections() {
                let _ = connection.send_message(&message);
            }
        } else {
            log::debug!("用户 {} 不在线", username);
        }
    }

    /// 广播消息给所有客户端
    pub async fn broadcast_message(&self, message: WsMessage) {
        let online_count = self.online_users.len() as u64;
        self.frequency_monitor.record_message_load(online_count.max(1));
        
        if message.priority == MessagePriority::Emergency {
            if let Err(e) = self.emergency_sender.send(message) {
                log::error!("紧急消息发送失败: {}", e);
            }
            return;
        }
        
        match self.frequency_monitor.get_current_mode() {
            ProcessingMode::Direct => {
                let size_kb = (message.size_bytes as f64 / 1024.0).max(1.0);
                let bucket = match message.priority {
                    MessagePriority::Normal => &self.normal_token_bucket,
                    MessagePriority::Low => &self.low_token_bucket,
                    _ => return,
                };
                bucket.consume(size_kb).await;
                let sender = match message.priority {
                    MessagePriority::Normal => &self.normal_sender,
                    MessagePriority::Low => &self.low_sender,
                    _ => return,
                };
                if let Err(e) = sender.send(message) {
                    log::error!("直接发送失败: {}", e);
                }
            }
            ProcessingMode::MicroBatch => {
                Self::flush_buffer_with_tokens(
                    &self.micro_batch_buffer,
                    &self.normal_token_bucket,
                    &self.low_token_bucket,
                    &self.emergency_sender,
                    &self.normal_sender,
                    &self.low_sender,
                ).await;
            }
            ProcessingMode::Batch => {
                Self::flush_buffer_with_tokens(
                    &self.batch_buffer,
                    &self.normal_token_bucket,
                    &self.low_token_bucket,
                    &self.emergency_sender,
                    &self.normal_sender,
                    &self.low_sender,
                ).await;
            }
        }
    }


    /// 广播消息给除指定用户外的所有客户端  
    pub fn broadcast_message_except(&self, message: WsMessage, exclude_username: &str) {
        // 创建带排除信息的消息
        let mut excluded_message = message.clone();
        excluded_message.excluded_user = Some(exclude_username.to_string());
        
        // 使用统一的全局广播通道
        let sender = match excluded_message.priority {
            MessagePriority::Emergency => &self.emergency_sender,
            MessagePriority::Normal => &self.normal_sender,
            MessagePriority::Low => &self.low_sender,
        };
        
        if let Err(e) = sender.send(excluded_message) {
            log::error!("广播排除消息失败: {}", e);
        }
    }

    // /// 获取在线用户列表
    // pub fn get_online_users(&self) -> Vec<UserInfo> {
    //     let mut users = Vec::new();
    //     for entry in self.clients.iter() {
    //         if let Some(connection) = entry.value().first() {
    //             users.push(connection.user_info.clone());
    //         }
    //     }
    //     users
    // }

    /// 获取在线用户JSON字符串
    pub fn get_online_users_json(&self) -> String {
        let online_users: Vec<serde_json::Value> = self
            .clients
            .iter()
            .filter_map(|entry| entry.value().first().map(|conn| conn.user_info.clone()))
            .map(|user_info| {
                json!({
                    "userName": user_info.user_name,
                    "userAvatarURL": user_info.user_avatar_url,
                    "homePage": format!("/member/{}", user_info.user_name)
                })
            })
            .collect();
        
        serde_json::to_string(&online_users).unwrap_or_else(|e| {
            log::error!("序列化在线用户列表失败: {}", e);
            "[]".to_string()
        })
    }

    /// 清理不活跃连接
    pub async fn cleanup_inactive_connections(&self, hours: u64) -> HashMap<String, u64> {
        let now = SystemTime::now();
        let threshold = Duration::from_secs(hours * 3600);
        let mut inactive_users = HashMap::new();
        
        let mut users_to_remove = Vec::new();
        
        for entry in self.clients.iter() {
            let username = entry.key();
            let connections = entry.value();
            
            // 检查是否所有连接都不活跃
            let all_inactive = connections.active_connections().all(|conn| {
                let elapsed = now.duration_since(conn.last_active).unwrap_or(Duration::ZERO);
                if elapsed >= threshold {
                    let hours = elapsed.as_secs() / 3600;
                    inactive_users.insert(username.clone(), hours);
                    true
                } else {
                    false
                }
            });
            
            if all_inactive {
                users_to_remove.push(username.clone());
            }
        }
        
        // 移除不活跃用户
        for username in users_to_remove {
            self.clients.remove(&username);
            self.online_users.remove(&username);
            self.notify_master_user_leave(&username).await;
            log::info!("移除不活跃用户: {}", username);
        }
        
        inactive_users
    }

    /// 强制移除用户的所有连接
    pub fn force_remove_user(&self, username: &str) {
        self.clients.remove(username);
        self.online_users.remove(username);
        
        // 异步通知主服务器
        let username = username.to_string();
        tokio::spawn(async move {
            if let Err(e) = util::post_message_to_master("leave", &username).await {
                log::error!("通知主服务器用户离开失败: {}", e);
            } else {
                log::debug!("已通知主服务器用户 {} 离开", username);
            }
        });
    }

    /// 通知主服务器用户加入
    async fn notify_master_user_join(&self, username: &str) {
        if let Err(e) = util::post_message_to_master("join", username).await {
            log::error!("通知主服务器用户 {} 加入失败: {}", username, e);
        } else {
            log::debug!("已通知主服务器用户 {} 加入", username);
        }
    }

    /// 通知主服务器用户离开
    async fn notify_master_user_leave(&self, username: &str) {
        if let Err(e) = util::post_message_to_master("leave", username).await {
            log::error!("通知主服务器用户 {} 离开失败: {}", username, e);
        } else {
            log::debug!("已通知主服务器用户 {} 离开", username);
        }
    }

    /// 处理主服务器消息
    pub async fn handle_master_message(&self, text: &str) -> AppResult<String> {
        if !text.contains(":::") {
            return Ok("Invalid format".to_string());
        }

        let parts: Vec<&str> = text.splitn(2, ":::").collect();
        if parts.len() != 2 || parts[0] != crate::conf::admin_key() {
            return Ok("Invalid auth".to_string());
        }

        let command = parts[1];
        match command {
            "hello" => Ok("hello from rhyus-rust".to_string()),
            
            "online" => Ok(self.get_online_users_json()),
            
            "clear" => {
                let inactive_users = self.cleanup_inactive_connections(6).await;
                let response = if inactive_users.is_empty() {
                    json!({}).to_string()
                } else {
                    json!(inactive_users).to_string()
                };
                Ok(response)
            }
            
            cmd if cmd.starts_with("tell ") => {
                let parts: Vec<&str> = cmd[5..].splitn(2, ' ').collect();
                if parts.len() == 2 {
                    let (username, message) = (parts[0], parts[1]);
                    self.send_to_user(username, WsMessage::new(message));
                    Ok("OK".to_string())
                } else {
                    Ok("Invalid tell format".to_string())
                }
            }
            
            cmd if cmd.starts_with("all ") => {
                let message = &cmd[4..];
                let msg = WsMessage::emergency(message);
                // 使用带速率控制的广播方法
                self.broadcast_message(msg).await;
                Ok("OK".to_string())
            }
            
            cmd if cmd.starts_with("msg ") => {
                let parts: Vec<&str> = cmd[4..].splitn(2, ' ').collect();
                if parts.len() == 2 {
                    let (sender, message) = (parts[0], parts[1]);
                    let ws_message = WsMessage::new(message);
                    
                    // 先发给发送者自己
                    self.send_to_user(sender, ws_message.clone());
                    
                    // 再广播给其他用户
                    self.broadcast_message_except(ws_message, sender);
                    Ok("OK".to_string())
                } else {
                    Ok("Invalid msg format".to_string())
                }
            }
            
            cmd if cmd.starts_with("push ") => {
                let content = &cmd[5..];
                *self.cached_online_list.write().unwrap() = content.to_string();
                let msg = WsMessage::emergency(content);
                self.broadcast_message(msg).await;
                Ok("OK".to_string())
            }
            
            cmd if cmd.starts_with("slow ") => {
                let content = &cmd[5..];
                let msg = WsMessage::low_with_delay(content, Duration::from_millis(100));
                self.broadcast_message(msg).await;
                Ok("OK".to_string())
            }
            
            cmd if cmd.starts_with("kick ") => {
                let username = &cmd[5..];
                if !username.is_empty() {
                    self.force_remove_user(username);
                    Ok("OK".to_string())
                } else {
                    Ok("Invalid kick format".to_string())
                }
            }
            
            _ => Ok("Unknown command".to_string()),
        }
    }

    /// 添加主服务器连接
    pub async fn add_master(&self, socket: WebSocketStream<TcpStream>) -> AppResult<()> {
        let (mut sink, mut stream) = socket.split();
        
        while let Some(result) = stream.next().await {
            match result {
                Ok(Message::Text(text)) => {
                    match self.handle_master_message(&text).await {
                        Ok(response) => {
                            if let Err(e) = sink.send(Message::Text(response)).await {
                                log::error!("发送响应失败: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            log::error!("处理主服务器消息失败: {}", e);
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    // log::info!("主服务器断开连接");
                    break;
                }
                Err(e) => {
                    log::error!("主服务器连接错误: {}", e);
                    break;
                }
                _ => {}
            }
        }
        
        Ok(())
    }

    /// 发送消息到主服务器
    pub async fn send_to_master(&self, message: &str) -> AppResult<()> {
        // 直接通过HTTP发送消息到主服务器
        let url = format!("{}/chat-room/node/push", crate::conf::master_url());
        log::debug!("发送请求到主服务器: URL={}, 请求体={}", url, message);

        let client = reqwest::ClientBuilder::new()
            .danger_accept_invalid_certs(true)
            .build()
            .map_err(|e| {
                crate::common::AppError::Io(std::io::Error::other(
                    format!("创建HTTP客户端失败: {}", e),
                ))
            })?;
            
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
            let (status, body) = (response.status(), response.text().await.unwrap_or_default());
            log::error!("发送消息到主服务器失败: HTTP {} - {}", status, body);
            Err(crate::common::AppError::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("主服务器返回错误状态码: {} - {}", status, body),
            )))
        }
    }

    /// 启动频率监控
    fn start_frequency_monitoring(&self) {
        let frequency_monitor = self.frequency_monitor.clone();
        let micro_batch_buffer = self.micro_batch_buffer.clone();
        let batch_buffer = self.batch_buffer.clone();
        let normal_token_bucket = self.normal_token_bucket.clone();
        let low_token_bucket = self.low_token_bucket.clone();
        let emergency_sender = self.emergency_sender.clone();
        let normal_sender = self.normal_sender.clone();
        let low_sender = self.low_sender.clone();
        
        // 启动频率检查任务（每100ms检查一次）
        {
            let frequency_monitor = frequency_monitor.clone();
            let micro_batch_buffer = micro_batch_buffer.clone();
            let batch_buffer = batch_buffer.clone();
            let normal_token_bucket = normal_token_bucket.clone();
            let low_token_bucket = low_token_bucket.clone();
            let emergency_sender = emergency_sender.clone();
            let normal_sender = normal_sender.clone();
            let low_sender = low_sender.clone();
            
            tokio::spawn(async move {
                let config = Settings::global();
                let mut interval = tokio::time::interval(Duration::from_millis(
                    config.websocket.frequency_check_interval_ms
                ));
                
                loop {
                    interval.tick().await;
                    
                    // 更新处理模式
                    frequency_monitor.update_mode(&config.websocket).await;
                    
                    // 检查并刷新缓冲区
                    Self::check_and_flush_buffers_simple(
                        &micro_batch_buffer,
                        &batch_buffer,
                        &normal_token_bucket,
                        &low_token_bucket,
                        &emergency_sender,
                        &normal_sender,
                        &low_sender,
                    ).await;
                }
            });
        }
        
        // 启动计数器重置任务（每1秒重置一次）
        tokio::spawn(async move {
            let mut reset_interval = tokio::time::interval(Duration::from_secs(1));
            
            loop {
                reset_interval.tick().await;
                frequency_monitor.reset_counters();
            }
        });
    }

    /// 缓冲区刷新
    async fn check_and_flush_buffers_simple(
        micro_batch_buffer: &Arc<tokio::sync::Mutex<MessageBuffer>>,
        batch_buffer: &Arc<tokio::sync::Mutex<MessageBuffer>>,
        normal_token_bucket: &Arc<AtomicTokenBucket>,
        low_token_bucket: &Arc<AtomicTokenBucket>,
        emergency_sender: &broadcast::Sender<WsMessage>,
        normal_sender: &broadcast::Sender<WsMessage>,
        low_sender: &broadcast::Sender<WsMessage>,
    ) {
        Self::flush_buffer_with_tokens(micro_batch_buffer, normal_token_bucket, low_token_bucket, emergency_sender, normal_sender, low_sender).await;
        Self::flush_buffer_with_tokens(batch_buffer, normal_token_bucket, low_token_bucket, emergency_sender, normal_sender, low_sender).await;
    }
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

/// 消息处理模式
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ProcessingMode {
    Direct,     // 直接发送模式
    MicroBatch, // 微批处理模式
    Batch,      // 批量处理模式
}

/// 消息频率监控器
#[derive(Debug)]
struct MessageFrequencyMonitor {
    messages_1s: AtomicU64,       // 过去1秒实际消息负载
    current_mode: AtomicU8,       // 当前处理模式 (0=直接, 1=微批, 2=批量)
}

impl MessageFrequencyMonitor {
    fn new() -> Self {
        Self {
            messages_1s: AtomicU64::new(0),
            current_mode: AtomicU8::new(0), // 默认直接模式
        }
    }

    /// 记录新消息（考虑实际负载）
    fn record_message_load(&self, load: u64) {
        self.messages_1s.fetch_add(load, Ordering::Relaxed);
    }

    /// 获取当前处理模式
    fn get_current_mode(&self) -> ProcessingMode {
        let mode = self.current_mode.load(Ordering::Relaxed);
        match mode {
            0 => ProcessingMode::Direct,
            1 => ProcessingMode::MicroBatch,
            _ => ProcessingMode::Batch,
        }
    }

    /// 更新处理模式（基于配置的阈值）
    async fn update_mode(&self, config: &crate::conf::WebSocketConfig) {
        let load_1s = self.messages_1s.load(Ordering::Relaxed);
        
        let new_mode = if load_1s < config.frequency.direct_mode_threshold {
            ProcessingMode::Direct
        } else if load_1s < config.frequency.micro_batch_threshold {
            ProcessingMode::MicroBatch
        } else {
            ProcessingMode::Batch
        };

        let mode_value = match new_mode {
            ProcessingMode::Direct => 0,
            ProcessingMode::MicroBatch => 1,
            ProcessingMode::Batch => 2,
        };

        self.current_mode.store(mode_value, Ordering::Relaxed);
        
        log::debug!("系统负载: {}实际msg/s -> 模式: {:?}", load_1s, new_mode);
    }

    /// 重置消息计数（每秒调用）
    fn reset_counters(&self) {
        self.messages_1s.store(0, Ordering::Relaxed);
    }
}

/// 消息批处理缓冲区
#[derive(Debug)]
struct MessageBuffer {
    messages: Vec<WsMessage>,
    first_message_time: Option<Instant>,
    max_size: usize,
    max_delay_ms: u64,
}

impl MessageBuffer {
    fn new(max_size: usize, max_delay_ms: u64) -> Self {
        Self {
            messages: Vec::with_capacity(max_size),
            first_message_time: None,
            max_size,
            max_delay_ms,
        }
    }

    /// 添加消息到缓冲区
    fn push(&mut self, message: WsMessage) {
        if self.messages.is_empty() {
            self.first_message_time = Some(Instant::now());
        }
        self.messages.push(message);
    }

    /// 检查是否应该刷新缓冲区
    fn should_flush(&self) -> bool {
        if self.messages.is_empty() {
            return false;
        }

        // 达到最大大小
        if self.messages.len() >= self.max_size {
            return true;
        }

        // 达到最大延迟
        if let Some(first_time) = self.first_message_time {
            let elapsed = first_time.elapsed().as_millis() as u64;
            if elapsed >= self.max_delay_ms {
                return true;
            }
        }

        false
    }

    /// 清空缓冲区并返回消息
    fn flush(&mut self) -> Vec<WsMessage> {
        self.first_message_time = None;
        std::mem::take(&mut self.messages)
    }

    // /// 获取缓冲区大小
    // fn len(&self) -> usize {
    //     self.messages.len()
    // }

    // /// 检查是否为空
    // fn is_empty(&self) -> bool {
    //     self.messages.is_empty()
    // }
}
