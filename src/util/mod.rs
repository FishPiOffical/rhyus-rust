use serde::Serialize;
use crate::common::{AppResult, AppError};
use crate::model::UserInfo;


static CLIENT: once_cell::sync::Lazy<reqwest::Client> = once_cell::sync::Lazy::new(reqwest::Client::new);

pub async fn get_user_info(api_key: &str) -> Option<UserInfo> {
    let url = format!("{}/api/user?apiKey={}", crate::conf::master_url(), api_key);
    
    match CLIENT.get(&url)
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36")
        .send()
        .await {
        Ok(response) => {
            let status = response.status();
            if status.is_success() {
                match response.json::<serde_json::Value>().await {
                    Ok(result) => {
                        if let Some(data) = result.get("data") {
                            match serde_json::from_value::<UserInfo>(data.clone()) {
                                Ok(user_info) => {
                                    Some(user_info)
                                }
                                Err(e) => {
                                    log::error!("Failed to parse user info: {}", e);
                                    None
                                }
                            }
                        } else {
                            log::error!("No data field in response");
                            None
                        }
                    }
                    Err(e) => {
                        log::error!("Failed to parse response as JSON: {}", e);
                        None
                    }
                }
            } else {
                log::error!("Failed to get user info: {}", status);
                None
            }
        }
        Err(e) => {
            log::error!("Failed to send request: {}", e);
            None
        }
    }
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