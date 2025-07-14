use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::handshake::server::{Request, Response},
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use url::Url;

mod api;
mod common;
mod conf;
mod model;
mod service;
mod util;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 初始化配置
    let config = conf::Settings::global();

    // 初始化日志
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(&config.log.level))
        .with(tracing_subscriber::fmt::layer())
        .init();

    // 初始化服务
    service::init_services().await?;

    let addr = SocketAddr::from_str(&conf::server_address())?;
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("server starting on {}", addr);

    // 处理连接
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            let api_key = Arc::new(Mutex::new(String::new()));
            let api_key_clone = Arc::clone(&api_key);

            let callback = move |req: &Request, response: Response| {
                // 从请求中获取URL和查询参数
                let uri = req.uri().to_string();

                // 解析URL参数
                let full_uri = format!("ws://127.0.0.1:10831{}", uri);
                if let Ok(url) = Url::parse(&full_uri) {
                    if let Some(key) = url
                        .query_pairs()
                        .find(|(key, _)| key == "apiKey")
                        .map(|(_, value)| value.into_owned())
                    {
                        if let Ok(mut guard) = api_key_clone.lock() {
                            *guard = key;
                        }
                    }
                }
                Ok(response)
            };

            if let Ok(socket) = accept_hdr_async(stream, callback).await {
                let key = api_key.lock().unwrap().clone();
                api::handle_connection(socket, key).await;
            }
        });
    }

    Ok(())
}
