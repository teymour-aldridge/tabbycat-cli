use std::process::exit;

use serde::de::DeserializeOwned;

pub async fn json_of_resp<T: DeserializeOwned>(res: reqwest::Response) -> T {
    if !res.status().is_success() {
        tracing::error!("Response error: {}", res.text().await.unwrap());
        exit(1)
    }

    let text = res.text().await.unwrap();

    match serde_json::from_str(&text) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(
                "Error processing response from Tabbycat API: {e}.

                ------ DATA ------
                {text}"
            );
            exit(1)
        }
    }
}
