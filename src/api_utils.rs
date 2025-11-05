use tabbycat_api::types::RoundPairing;

use crate::{Auth, dispatch_req::json_of_resp, request_manager::RequestManager};

pub async fn get_rounds(
    Auth {
        tabbycat_url,
        tournament_slug,
        api_key: _,
    }: &Auth,
    manager: RequestManager,
) -> Vec<tabbycat_api::types::Round> {
    let api_addr = format!("{tabbycat_url}/api/v1");

    let base_url = format!("{api_addr}/tournaments/{tournament_slug}/rounds");
    let resp = manager
        .send_request(|| manager.client.get(&base_url).build().unwrap())
        .await;

    resp.json().await.unwrap()
}

pub async fn get_teams(
    Auth {
        tabbycat_url,
        tournament_slug,
        api_key: _,
    }: &Auth,
    manager: RequestManager,
) -> Vec<tabbycat_api::types::Team> {
    let api_addr = format!("{tabbycat_url}/api/v1");

    let base_url = format!("{api_addr}/tournaments/{tournament_slug}/teams");
    let resp = manager
        .send_request(|| manager.client.get(&base_url).build().unwrap())
        .await;

    resp.json().await.unwrap()
}

pub async fn get_judges(
    Auth {
        tabbycat_url,
        tournament_slug,
        api_key: _,
    }: &Auth,
    manager: RequestManager,
) -> Vec<tabbycat_api::types::Adjudicator> {
    let api_addr = format!("{tabbycat_url}/api/v1");

    let base_url = format!("{api_addr}/tournaments/{tournament_slug}/adjudicators");
    let resp = manager
        .send_request(|| manager.client.get(&base_url).build().unwrap())
        .await;

    resp.json().await.unwrap()
}

pub async fn get_round(
    round: &str,
    auth: &Auth,
    manager: RequestManager,
) -> tabbycat_api::types::Round {
    let rounds = get_rounds(auth, manager.clone()).await;
    let round = rounds
        .iter()
        .find(|r| {
            r.abbreviation.as_str().eq_ignore_ascii_case(round)
                || r.name.as_str().eq_ignore_ascii_case(round)
        })
        .expect("the round you specified does not exist");
    round.clone()
}

pub async fn pairings_of_round(
    auth: &Auth,
    round: &tabbycat_api::types::Round,
    manager: RequestManager,
) -> Vec<RoundPairing> {
    let resp = manager
        .send_request(|| {
            manager
                .client
                .get(&round.links.pairing)
                .header("Authorization", format!("Token {}", auth.api_key))
                .build()
                .unwrap()
        })
        .await;

    json_of_resp(resp).await
}

pub async fn get_institutions(
    auth: &Auth,
    manager: RequestManager,
) -> Vec<tabbycat_api::types::PerTournamentInstitution> {
    let resp = manager
        .send_request(|| {
            manager
                .client
                .get(format!("{}/api/v1/institutions", auth.tabbycat_url))
                .build()
                .unwrap()
        })
        .await;

    resp.json().await.unwrap()
}
