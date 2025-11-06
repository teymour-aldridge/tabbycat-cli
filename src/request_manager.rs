use std::{
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use reqwest::StatusCode;

/// Manages a set of HTTP requests.
#[derive(Clone)]
pub struct RequestManager {
    pub client: reqwest::Client,
    authorization: String,
    backoff_secs: std::sync::Arc<AtomicU64>,
}

impl RequestManager {
    pub fn new(authorization: &str) -> Self {
        let client = reqwest::Client::builder()
            .build()
            .expect("Failed to build reqwest client");

        Self {
            client,
            authorization: format!("Token {}", authorization),
            backoff_secs: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn send_request(
        &self,
        get_request: impl Fn() -> reqwest::Request,
    ) -> reqwest::Response {
        let mut timeout = None;

        let secs = self.backoff_secs.load(std::sync::atomic::Ordering::SeqCst);
        if secs > 0 {
            tokio::time::sleep(Duration::from_secs(secs)).await;
        }

        loop {
            let mut req = (get_request)();
            req.headers_mut().insert(
                "Authorization",
                reqwest::header::HeaderValue::from_str(&self.authorization)
                    .expect("Invalid authorization header"),
            );
            let res = self.client.execute(req.try_clone().unwrap()).await.unwrap();

            if res.status().is_success() {
                self.backoff_secs
                    .store(0, std::sync::atomic::Ordering::SeqCst);

                return res;
            }

            if matches!(res.status(), StatusCode::TOO_MANY_REQUESTS) {
                let wait = timeout.unwrap_or(0.5f32);

                if wait >= 0.95 {
                    self.backoff_secs
                        .store(wait.round() as u64, std::sync::atomic::Ordering::SeqCst);
                }

                timeout = Some(wait * 2.0);
                tokio::time::sleep(Duration::from_secs_f32(wait)).await;
            } else {
                tracing::error!(
                    "{} \n {} \n {} \n {:?}",
                    req.url(),
                    res.status(),
                    res.text().await.unwrap(),
                    req.body()
                        .map(|body| String::from_utf8_lossy(body.as_bytes().unwrap()))
                );
                // todo: log specific problems with the request
                panic!("Encountered unexpected request failure.")
            }
        }
    }
}
