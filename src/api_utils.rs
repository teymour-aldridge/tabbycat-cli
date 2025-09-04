pub fn get_rounds(api_addr: &str, slug: &str, api_key: &str) -> Vec<tabbycat_api::types::Round> {
    let api_addr = format!("{}/api/v1", api_addr);

    let base_url = format!("{api_addr}/tournaments/{}/rounds", slug);
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

pub fn get_teams(api_addr: &str, slug: &str, api_key: &str) -> Vec<tabbycat_api::types::Team> {
    let api_addr = format!("{}/api/v1", api_addr);

    let base_url = format!("{api_addr}/tournaments/{slug}/teams");
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
