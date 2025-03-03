use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClientType {
    Web,
    PC,
    Mobile,
    Windows,
    MacOS,
    IOS,
    Android,
    IDEA,
    Chrome,
    Edge,
    VSCode,
    Python,
    Golang,
    IceNet,
    ElvesOnline,
    Dart,
    Bird,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatSource {
    pub client: String,
    pub version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRoomMessage {
    #[serde(rename = "oid")]
    pub id: String,
    #[serde(rename = "userOid")]
    pub user_id: i32,
    #[serde(rename = "userName")]
    pub user_name: String,
    #[serde(rename = "userNickname")]
    pub nickname: String,
    #[serde(rename = "avatar_url")]
    pub avatar_url: String,
    #[serde(rename = "sys_metal")]
    pub sys_metal: Vec<MetalItem>,
    pub via: ChatSource,
    pub content: String,
    pub md: String,
    #[serde(rename = "red_packet")]
    pub red_packet: Option<RedPacketMessage>,
    pub weather: Option<WeatherMsg>,
    pub music: Option<MusicMsg>,
    pub unknown: Option<serde_json::Value>,
    pub time: String,
    #[serde(rename = "type")]
    pub msg_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChatContentType {
    #[serde(rename = "md")]
    Markdown,
    #[serde(rename = "html")]
    HTML,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChatMessageType {
    Context,
    Before,
    After,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChatRoomMessageType {
    #[serde(rename = "online")]
    Online,
    #[serde(rename = "discussChanged")]
    DiscussChanged,
    #[serde(rename = "revoke")]
    Revoke,
    #[serde(rename = "msg")]
    Msg,
    #[serde(rename = "redPacket")]
    RedPacket,
    #[serde(rename = "redPacketStatus")]
    RedPacketStatus,
    #[serde(rename = "barrager")]
    Barrager,
    #[serde(rename = "customMessage")]
    Custom,
    #[serde(rename = "weather")]
    Weather,
    #[serde(rename = "music")]
    Music,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRoomData {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub online: Option<Vec<OnlineInfo>>,
    pub discuss: Option<String>,
    pub revoke: Option<String>,
    pub msg: Option<ChatRoomMessage>,
    pub status: Option<RedPacketStatusMsg>,
    pub barrager: Option<BarragerMsg>,
    pub custom: Option<String>,
    pub unknown: Option<serde_json::Value>,
}

// 相关类型的空结构体
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetalItem {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedPacketMessage {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WeatherMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MusicMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnlineInfo {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedPacketStatusMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarragerMsg {}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRoomNode {
    pub node: String,
    pub name: String,
    pub online: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRoomNodeInfo {
    pub recommend: ChatRoomNode,
    pub available: ChatRoomNode,
} 