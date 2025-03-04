use std::time::{SystemTime, Duration};
use dashmap::DashMap;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio::sync::broadcast;
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
use tokio::net::TcpStream;
use crate::{common::AppResult, model::UserInfo, util, conf::Settings};
use std::sync::Arc;
use once_cell::sync::Lazy;
use reqwest;



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
    master_in_tx: broadcast::Sender<WsMessage>,
    client_out_tx: broadcast::Sender<WsMessage>,
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
}

impl Hub {
    pub fn new() -> Self {
        let config = Settings::global();
        let message_cache_size = config.websocket.message_cache_size;
        
        let (master_tx, _) = broadcast::channel(message_cache_size);
        let (master_in_tx, _) = broadcast::channel(message_cache_size);
        let (client_out_tx, _) = broadcast::channel(message_cache_size);

        let message_handler = MessageHandler {
            master_in_tx,
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

        hub
    }

    async fn check_and_clean(&self) {
        let ping_message = WsMessage::new("{\"ping\":\"pong\"}");
        
        // 清理超时连接（6小时无响应）
        let timeout = Duration::from_secs(6 * 60 * 60);
        let now = SystemTime::now();
        
        // 清理主服务器连接
        let mut master_num = 0;
        self.masters.retain(|_, master| {
            if let Ok(duration) = now.duration_since(master.last_active) {
                if duration > timeout {
                    return false;
                }
            }
            let _ = master.tx.send(ping_message.clone());
            master_num += 1;
            true
        });

        // 清理客户端连接
        let mut client_num = 0;
        let mut kicked_users = Vec::new();
        self.clients.retain(|_, client| {
            if let Ok(duration) = now.duration_since(client.last_active) {
                if duration > timeout {
                    kicked_users.push(client.user_info.user_name.clone());
                    return false;
                }
            }
            let _ = client.tx.send(ping_message.clone());
            client_num += 1;
            true
        });

        // 更新在线用户计数并通知主服务器
        let mut need_update_list = false;
        for user in kicked_users {
            if let Some(mut count) = self.online_users.get_mut(&user) {
                *count -= 1;
                if *count <= 0 {
                    // 用户的最后一个连接断开，通知主服务器用户离开
                    log::debug!("用户 {} 的最后一个连接超时断开，通知主服务器用户离开", user);
                    let username = user.clone();  // 克隆用户名，避免移动
                    tokio::spawn(async move {
                        if let Err(e) = util::post_message_to_master("leave", &username).await {
                            log::error!("通知主服务器用户离开失败: {}", e);
                        }
                    });
                    self.online_users.remove(&user);
                    need_update_list = true;
                }
            }
        }

        // 如果有用户被踢出，更新在线用户列表
        if need_update_list {
            let hub_clone = self.clone();
            tokio::spawn(async move {
                hub_clone.update_online_users_list().await;
            });
        }

        log::debug!("active master: {}, active client: {}", master_num, client_num);
    }

    pub fn remove_user_session(&self, username: &str) {
        if let Some(mut count) = self.online_users.get_mut(username) {
            *count -= 1;
            if *count <= 0 {
                self.online_users.remove(username);
            }
        }
    }

    // 发送消息给指定用户
    fn send_to_user(&self, username: &str, msg: WsMessage) {
        let mut sent = false;
        
        self.clients.iter().for_each(|entry| {
            if entry.value().user_info.user_name == username {
                match entry.value().tx.send(msg.clone()) {
                    Ok(_) => {
                        sent = true;
                        log::debug!("消息已发送到用户 {}: {}", username, msg.data);
                    }
                    Err(err) => {
                        log::error!("发送消息到 {} 失败: {}", username, err);
                    }
                }
            }
        });
        
        if !sent {
            log::debug!("用户 {} 未找到或消息未发送", username);
        }
    }

    // 广播消息给除指定用户外的所有客户端
    fn broadcast_except_user(&self, username: &str, msg: WsMessage) {
        let mut sent_count = 0;
        
        self.clients.iter().for_each(|entry| {
            if entry.value().user_info.user_name != username {
                match entry.value().tx.send(msg.clone()) {
                    Ok(_) => {
                        sent_count += 1;
                    }
                    Err(err) => {
                        log::error!("发送消息失败 (除 {} 外): {} - {}", 
                            username, entry.value().user_info.user_name, err);
                    }
                }
            }
        });
        
        log::debug!("广播消息已发送给 {} 个客户端 (除 {} 外)", sent_count, username);
    }
    
    // 广播消息给所有客户端
    fn broadcast_to_clients(&self, msg: WsMessage) {
        let mut sent_count = 0;
        
        self.clients.iter().for_each(|entry| {
            match entry.value().tx.send(msg.clone()) {
                Ok(_) => {
                    sent_count += 1;
                }
                Err(err) => {
                    log::error!("广播消息到 {} 失败: {}", 
                        entry.value().user_info.user_name, err);
                }
            }
        });
        
        log::debug!("广播消息已发送给 {} 个客户端", sent_count);
    }

    // 处理主服务器消息
    async fn handle_master_message(&self, text: &str) {
        log::debug!(" <--- master: {}", text);
        
        if text.contains(":::") {
            let parts: Vec<&str> = text.split(":::").collect();
            if parts.len() == 2 && parts[0] == crate::conf::admin_key() {
                match parts[1] {
                    "hello" => {
                        let _ = self.message_handler.master_in_tx.send(WsMessage::new("hello from rhyus-rust"));
                    }
                    "online" => {
                        // 获取本地维护的在线用户列表
                        let online_users = self.all_online_users.read().await.clone();
                        if online_users.is_empty() || online_users == "[]" {
                            // 如果没有缓存的用户列表，则生成一个新的
                            let mut online_users_map = std::collections::HashMap::new();
                            let mut num = 0;
                            self.clients.iter().for_each(|entry| {
                                let user_info = entry.value().user_info.clone();
                                online_users_map.insert(user_info.o_id.clone(), user_info);
                                num += 1;
                            });
                            log::debug!("当前在线客户端数量: {}", num);
                            
                            let online_users_vec: Vec<UserInfo> = online_users_map.into_values().collect();
                            log::debug!("去重后的在线用户数量: {}", online_users_vec.len());
                            
                            // 直接响应用户列表数组
                            let users_json = json!(online_users_vec).to_string();
                            log::debug!("响应主服务器 online 命令，发送用户列表: {}", users_json);
                            let _ = self.message_handler.master_in_tx.send(WsMessage::new(users_json.clone()));
                            
                            // 更新本地缓存
                            *self.all_online_users.write().await = users_json;
                        } else {
                            // 直接发送缓存的用户列表
                            log::debug!("发送缓存的在线用户列表: {}", online_users);
                            let _ = self.message_handler.master_in_tx.send(WsMessage::new(online_users.clone()));
                        }
                    }
                    cmd if cmd.starts_with("tell") => {
                        let parts: Vec<&str> = cmd[5..].splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            let to = parts[0];
                            let content = parts[1];
                            self.send_to_user(to, WsMessage::new(content));
                            if !self.masters.is_empty() {
                                let _ = self.message_handler.master_in_tx.send(WsMessage::new("OK"));
                            }
                        }
                    }
                    cmd if cmd.starts_with("msg") => {
                        let parts: Vec<&str> = cmd[4..].splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            let sender = parts[0];
                            let content = parts[1];
                            // 先发给发送者
                            self.send_to_user(sender, WsMessage::new(content));
                            // 再发给其他人
                            self.broadcast_except_user(sender, WsMessage::with_delay(content, Duration::from_millis(10)));
                            if !self.masters.is_empty() {
                                let _ = self.message_handler.master_in_tx.send(WsMessage::new("OK"));
                            }
                        } else {
                            log::error!("msg 命令参数错误: {}", cmd);
                        }
                    }
                    cmd if cmd.starts_with("all") => {
                        let content = &cmd[4..];
                        self.broadcast_to_clients(WsMessage::with_delay(content, Duration::from_millis(10)));
                        if !self.masters.is_empty() {
                            let _ = self.message_handler.master_in_tx.send(WsMessage::new("OK"));
                        }
                    }
                    cmd if cmd.starts_with("push") => {
                        let content = &cmd[5..];
                        log::debug!("收到主服务器push列表: {}", content);
                        
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
                                            self.online_users.insert(username.to_string(), 1);
                                        }
                                    }
                                    
                                    log::debug!("更新在线用户列表，当前在线用户数: {}", users_array.len());
                                }
                            }
                        }
                        
                        // 直接广播给所有客户端
                        let _ = self.message_handler.client_out_tx.send(WsMessage::new(content.to_string()));

                        if !self.masters.is_empty() {
                            let _ = self.message_handler.master_in_tx.send(WsMessage::new("OK"));
                        }
                    }
                    cmd if cmd.starts_with("kick") => {
                        let username = &cmd[5..];
                        let before_count = self.clients.len();
                        self.clients.retain(|_, client| {
                            client.user_info.user_name != username
                        });
                        self.remove_user_session(username);
                        log::debug!("踢出用户 {} 后，客户端数量从 {} 变为 {}", username, before_count, self.clients.len());
                        if !self.masters.is_empty() {
                            let _ = self.message_handler.master_in_tx.send(WsMessage::new("OK"));
                        }
                    }
                    cmd if cmd.starts_with("slow") => {
                        let content = &cmd[5..];
                        self.broadcast_to_clients(WsMessage::with_delay(content.to_string(), Duration::from_millis(100)));
                        if !self.masters.is_empty() {
                            let _ = self.message_handler.master_in_tx.send(WsMessage::new("OK"));
                        }
                    }
                    cmd if cmd == "clear" => {
                        let mut cleared_users = Vec::new();
                        let now = SystemTime::now();
                        self.clients.retain(|_, client| {
                            if let Ok(duration) = now.duration_since(client.last_active) {
                                if duration > Duration::from_secs(6 * 60 * 60) {
                                    cleared_users.push(client.user_info.user_name.clone());
                                    return false;
                                }
                            }
                            true
                        });
                        
                        // 更新用户计数
                        for username in &cleared_users {
                            self.remove_user_session(username);
                        }
                        
                        if !self.masters.is_empty() {
                            let _ = self.message_handler.master_in_tx.send(WsMessage::new(json!(cleared_users).to_string()));
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // 添加主服务器连接
    pub async fn add_master(&self, mut socket: WebSocketStream<TcpStream>) -> AppResult<()> {
        let addr = format!("master_{}", SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs());
        
        // 清理旧连接
        self.masters.clear();
        let master = ActiveMaster { 
            tx: self.master_tx.clone(), 
            last_active: SystemTime::now() 
        };
        self.masters.insert(addr.clone(), master);

        // 立即发送本地维护的用户列表作为初始响应
        let online_users = self.all_online_users.read().await.clone();
        if online_users.is_empty() || online_users == "[]" {
            // 如果没有用户列表，发送一个空数组
            if let Err(e) = socket.send(Message::Text("[]".to_string())).await {
                log::error!("无法发送初始用户列表到主服务器: {}", e);
            } else {
                log::debug!("已发送空用户列表到主服务器");
            }
        } else {
            // 发送现有的用户列表
            if let Err(e) = socket.send(Message::Text(online_users)).await {
                log::error!("无法发送用户列表到主服务器: {}", e);
            } else {
                log::debug!("已发送用户列表到主服务器");
            }
        }

        // 处理主服务器连接
        let hub = self.clone();
        tokio::spawn(async move {
            let addr_clone = addr.clone();
            
            // 处理消息循环
            while let Some(Ok(msg)) = socket.next().await {
                match msg {
                    Message::Text(text) => {
                        if let Some(mut master) = hub.masters.get_mut(&addr_clone) {
                            master.last_active = SystemTime::now();
                        }
                        
                        // 特殊处理hello消息
                        if text.contains(":::hello") {
                            log::debug!("收到主服务器hello消息，立即响应");
                            if let Err(e) = socket.send(Message::Text("hello from rhyus-rust".to_string())).await {
                                log::error!("无法响应主服务器hello消息: {}", e);
                            }
                        } else {
                            let _ = hub.message_handler.master_in_tx.send(WsMessage::new(text));
                        }
                    }
                    Message::Close(_) => {
                        log::debug!("主服务器断开连接: {}", addr_clone);
                        break;
                    }
                    _ => {
                        log::warn!("收到主服务器未知消息类型");
                    }
                }
            }
        });

        Ok(())
    }

    // 启动消息处理循环
    pub fn start_message_handlers(&self) {
        // 处理主服务器入站消息
        let hub = self.clone();
        tokio::spawn(async move {
            let mut rx = hub.message_handler.master_in_tx.subscribe();
            while let Ok(msg) = rx.recv().await {
                let hub_clone = hub.clone();
                tokio::spawn(async move {
                    hub_clone.handle_master_message(&msg.data).await;
                });
            }
        });

        // 处理广播到客户端的消息
        let hub = self.clone();
        tokio::spawn(async move {
            let mut rx = hub.message_handler.client_out_tx.subscribe();
            while let Ok(msg) = rx.recv().await {
                hub.broadcast_to_clients(msg);
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

        self.clients.insert(addr.clone(), client);

        if count < 1 {
            log::debug!("用户 {} 的第一个连接，通知主服务器用户加入", user_info.user_name);
            if let Err(e) = util::post_message_to_master("join", &user_info.user_name).await {
                log::error!("通知主服务器用户加入失败: {}", e);
            } else {
                log::debug!("已通知主服务器用户 {} 加入", user_info.user_name);
            }
        }
        self.online_users.insert(user_info.user_name.clone(), count + 1);
        log::debug!("用户 {} 的连接计数: {} (连接ID: {})", user_info.user_name, count + 1, addr);
        log::debug!("当前在线用户总数: {}", self.online_users.len());

        self.update_online_users_list().await;

        // 获取在线用户列表并发送给新连接的客户端
        let client_online_users = self.client_online_users.read().await.clone();
        
        // 直接发送给新连接的客户端
        if !client_online_users.is_empty() {
            log::debug!("向新连接的客户端发送在线用户列表: {}", client_online_users);
            let online_message = WsMessage::with_delay(client_online_users, Duration::from_secs(3));
            let _ = self.message_handler.client_out_tx.send(online_message);
        } else {
            log::debug!("没有可用的在线用户列表发送给新客户端");
        }

        let (mut write, mut read) = socket.split();
        
        let hub = self.clone();
        let addr_clone = addr.clone();
        let username_clone = user_info.user_name.clone();
        
        let mut rx = tx.subscribe();
        
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
            
            if hub_clone.clients.remove(&addr_clone2).is_some() {
                log::debug!("清理客户端连接: {} ({})", username, addr_clone2);
            }
            
            let count = hub_clone.online_users.get(&username).map(|v| *v.value()).unwrap_or(0);
            if count <= 1 {
                log::debug!("用户 {} 的最后一个连接断开，通知主服务器用户离开", username);
                if let Err(e) = util::post_message_to_master("leave", &username).await {
                    log::error!("通知主服务器用户离开失败: {}", e);
                }
                hub_clone.online_users.remove(&username);
                
                hub_clone.update_online_users_list().await;
            } else {
                hub_clone.online_users.insert(username.clone(), count - 1);
                log::debug!("用户 {} 的连接计数更新为: {}", username, count - 1);
            }
            
            send_task.abort();
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
        }
    }
} 