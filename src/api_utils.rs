use tabbycat_api::types::RoundPairing;

use crate::{Auth, dispatch_req::json_of_resp};

pub fn get_rounds(
    Auth {
        tabbycat_url,
        tournament_slug,
        api_key,
    }: &Auth,
) -> Vec<tabbycat_api::types::Round> {
    let api_addr = format!("{}/api/v1", tabbycat_url);

    let base_url = format!("{api_addr}/tournaments/{}/rounds", tournament_slug);
    let resp = attohttpc::get(&base_url)
        .header("Authorization", format!("Token {}", api_key))
        .send()
        .unwrap();
    if !resp.is_success() {
        panic!(
            "error {base_url} {:?} {}",
            resp.status(),
            resp.text_utf8().unwrap()
        );
    }
    resp.json().unwrap()
}

pub fn get_teams(
    Auth {
        tabbycat_url,
        tournament_slug,
        api_key,
    }: &Auth,
) -> Vec<tabbycat_api::types::Team> {
    let api_addr = format!("{}/api/v1", tabbycat_url);

    let base_url = format!("{api_addr}/tournaments/{tournament_slug}/teams");
    let resp = attohttpc::get(&base_url)
        .header("Authorization", format!("Token {}", api_key))
        .send()
        .unwrap();

    if !resp.is_success() {
        panic!(
            "error {base_url} {:?} {}",
            resp.status(),
            resp.text_utf8().unwrap()
        );
    }

    resp.json().unwrap()
}

pub fn get_judges(
    Auth {
        tabbycat_url,
        tournament_slug,
        api_key,
    }: &Auth,
) -> Vec<tabbycat_api::types::Adjudicator> {
    let api_addr = format!("{}/api/v1", tabbycat_url);

    let base_url = format!("{api_addr}/tournaments/{tournament_slug}/adjudicators");
    let resp = attohttpc::get(&base_url)
        .header("Authorization", format!("Token {}", api_key))
        .send()
        .unwrap();

    if !resp.is_success() {
        panic!(
            "error {base_url} {:?} {}",
            resp.status(),
            resp.text_utf8().unwrap()
        );
    }

    resp.json().unwrap()
}

pub fn get_round(round: &str, auth: &Auth) -> tabbycat_api::types::Round {
    let rounds = get_rounds(auth);
    let round = rounds
        .iter()
        .find(|r| {
            r.abbreviation.as_str().to_ascii_lowercase() == round.to_ascii_lowercase()
                || r.name.as_str().to_ascii_lowercase() == round.to_ascii_lowercase()
        })
        .expect("the round you specified does not exist");
    round.clone()
}

pub fn pairings_of_round(auth: &Auth, round: &tabbycat_api::types::Round) -> Vec<RoundPairing> {
    let pairings: Vec<tabbycat_api::types::RoundPairing> = json_of_resp(
        attohttpc::get(&round.links.pairing)
            .header("Authorization", format!("Token {}", auth.api_key))
            .send()
            .unwrap(),
    );
    pairings
}
