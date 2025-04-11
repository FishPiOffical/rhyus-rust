use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserInfo {
    #[serde(rename = "oId")]
    pub o_id: String,
    #[serde(rename = "userName")]
    pub user_name: String,
    #[serde(rename = "userNickname")]
    pub user_nickname: String,
    #[serde(rename = "userURL")]
    pub user_url: String,
    #[serde(rename = "userCity")]
    pub user_city: String,
    #[serde(rename = "userIntro")]
    pub user_intro: String,
    #[serde(rename = "userNo")]
    pub user_no: String,
    #[serde(rename = "userOnlineFlag")]
    pub user_online_flag: bool,
    #[serde(rename = "userPoint")]
    pub user_point: i32,
    #[serde(rename = "userRole")]
    pub user_role: String,
    #[serde(rename = "userAppRole")]
    pub user_app_role: String,
    #[serde(rename = "userAvatarURL")]
    pub user_avatar_url: String,
    #[serde(rename = "cardBg")]
    pub card_bg: String,
    #[serde(rename = "followingUserCount")]
    pub following_user_count: i32,
    #[serde(rename = "followerCount")]
    pub follower_count: i32,
    #[serde(rename = "onlineMinute")]
    pub online_minute: i32,
    #[serde(default)]
    pub can_follow: String,
    #[serde(default)]
    pub all_metal_owned: String,
    #[serde(rename = "sysMetal")]
    pub sys_metal: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Result<T> {
    pub code: i32,
    pub msg: String,
    pub data: T,
    #[serde(default)]
    pub time: Option<String>,
}
