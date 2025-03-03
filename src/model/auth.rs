use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Auth {
    pub user_id: String,
    pub token: String,
    pub created_at: SystemTime,
    pub expires_at: SystemTime,
    pub is_valid: bool,
}

impl Auth {
    pub fn new(user_id: String, token: String, expires_in: u64) -> Self {
        let now = SystemTime::now();
        let expires_at = now + std::time::Duration::from_secs(expires_in);
        Self {
            user_id,
            token,
            created_at: now,
            expires_at,
            is_valid: true,
        }
    }

    pub fn is_expired(&self) -> bool {
        SystemTime::now() > self.expires_at
    }

    pub fn invalidate(&mut self) {
        self.is_valid = false;
    }
} 