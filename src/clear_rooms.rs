use serde_json::json;
use tracing::{Level, error, span};

use crate::Auth;

pub fn do_clear_room_urls(auth: Auth) {
    let mut rooms: Vec<tabbycat_api::types::Venue> = attohttpc::get(format!(
        "{}/api/v1/tournaments/{}/venues",
        auth.tabbycat_url, auth.tournament_slug
    ))
    .header("Authorization", format!("Token {}", auth.api_key))
    .send()
    .unwrap()
    .json()
    .unwrap();

    let span = span!(Level::INFO, "clear_room_urls");
    let _guard = span.enter();

    for (i, room) in rooms.clone().into_iter().enumerate() {
        let response = attohttpc::patch(room.url.clone())
            .header("Authorization", format!("Token {}", auth.api_key))
            .json(&json!({
                "external_url": ""
            }))
            .unwrap()
            .send()
            .unwrap();

        if !response.is_success() {
            error!(
                "Failed to clear room URL for room {}: {} {}",
                room.id,
                response.status(),
                response.text_utf8().unwrap()
            );
            panic!("Failed to clear room URL");
        }

        let room: tabbycat_api::types::Venue = response.json().unwrap();

        tracing::info!("Cleared room {} URL", room.id);

        rooms[i] = room;
    }
}
