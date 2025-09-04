use crate::Auth;

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
