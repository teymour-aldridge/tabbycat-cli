pub fn get_rounds(api_addr: &str, slug: &str, api_key: &str) -> Vec<tabbycat_api::types::Round> {
    let api_addr = format!("{}/api/v1", api_addr);

    let resp = attohttpc::get(format!("{api_addr}/tournaments/{}/rounds", slug))
        .header("Authorization", format!("Token {}", api_key))
        .send()
        .unwrap();
    if !resp.is_success() {
        panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
    }
    resp.json().unwrap()
}

pub fn get_teams(api_addr: &str, slug: &str, api_key: &str) -> Vec<tabbycat_api::types::Team> {
    let api_addr = format!("{}/api/v1", api_addr);

    attohttpc::get(format!("{api_addr}/tournaments/{}/teams", slug))
        .header("Authorization", format!("Token {}", api_key))
        .send()
        .unwrap()
        .json()
        .unwrap()
}
