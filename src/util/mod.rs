use serde::Serialize;
use std::time::Duration;
use crate::common::{AppResult, AppError};
use crate::model::{UserInfo, Result as ApiResult};

const USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36";
const REQUEST_TIMEOUT: u64 = 5;

static CLIENT: once_cell::sync::Lazy<reqwest::Client> = once_cell::sync::Lazy::new(|| {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(REQUEST_TIMEOUT))
        .build()
        .unwrap_or_else(|_| {
            log::warn!("创建HTTP客户端失败，使用默认配置");
            reqwest::Client::new()
        })
});

pub async fn get_user_info(api_key: &str) -> Option<UserInfo> {
    let url = format!("{}/api/user?apiKey={}", crate::conf::master_url(), api_key);
    
    match fetch_user_info(&url).await {
        Ok(user_info) => Some(user_info),
        Err(e) => {
            log::error!("获取用户信息失败: {}", e);
            None
        }
    }
}

async fn fetch_user_info(url: &str) -> AppResult<UserInfo> {
    let response = CLIENT.get(url)
        .header("User-Agent", USER_AGENT)
        .send()
        .await
        .map_err(|e| {
            log::error!("发送请求失败: {}", e);
            AppError::Request(e)
        })?;
    
    let status = response.status();
    if !status.is_success() {
        log::error!("获取用户信息失败, HTTP状态码: {}", status);
        return Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::Other, 
            format!("HTTP请求失败: {}", status)
        )));
    }
    
    let api_result = response.json::<ApiResult<UserInfo>>().await
        .map_err(|e| {
            log::error!("解析API响应失败: {}", e);
            AppError::Request(e)
        })?;
    
    // 检查API响应状态码
    if api_result.code != 0 {
        log::error!("API调用失败: 代码={}, 消息={}", api_result.code, api_result.msg);
        return Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            format!("API错误: {}", api_result.msg)
        )));
    }
    
    Ok(api_result.data)
}

#[derive(Debug, Serialize)]
struct ChatroomNodePush {
    msg: String,
    data: String,
    #[serde(rename = "adminKey")]
    admin_key: String,
}

pub async fn post_message_to_master(action: &str, username: &str) -> AppResult<()> {
    let request_data = ChatroomNodePush {
        msg: action.to_string(),
        data: username.to_string(),
        admin_key: crate::conf::admin_key().to_string(),
    };

    let message = serde_json::to_string(&request_data)
        .map_err(|e| AppError::SerdeJson(e))?;
    
    crate::service::Hub::global().send_to_master(&message).await
} 