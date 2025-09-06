use std::process::exit;

use serde::de::DeserializeOwned;

pub fn json_of_resp<T: DeserializeOwned>(res: attohttpc::Response) -> T {
    if !res.is_success() {
        tracing::error!("Response error: {}", res.text_utf8().unwrap());
        exit(1)
    }

    match res.json() {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(
                "Error processing response from Tabbycat API: {e}.

                ------ DATA ------
                {}",
                res.text_utf8().unwrap()
            );
            exit(1)
        }
    }
}
