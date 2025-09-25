use tracing::{Level, info, span};

use crate::Auth;

/// Adds conflicts that Tabbycat often fails to create. These can be missing
/// (for example) if a team's institution is added using the edit database
/// interface, which will not create the team-institution conflict correctly.
pub fn do_make_sensible_conflicts(auth: Auth) {
    let resp = attohttpc::get(format!(
        "{}/api/v1/tournaments/{}/teams",
        auth.tabbycat_url, auth.tournament_slug
    ))
    .header("Authorization", format!("Token {}", auth.api_key))
    .send();

    if let Err(e) = &resp {
        dbg!(e);
        panic!("Failed to fetch teams: {e:?}");
    }
    let resp = resp.unwrap();

    if !resp.is_success() {
        dbg!(&resp);
        panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
    }

    let mut teams: Vec<tabbycat_api::types::Team> = resp.json().unwrap();

    for team in teams.clone() {
        let adding_team_conflict = span!(Level::INFO, "sensible_conflict", team = team.long_name);
        let _adding_team_guard = adding_team_conflict.enter();

        if let Some(inst) = team.institution
            && !team.institution_conflicts.contains(&inst)
        {
            let mut conflicts = team.institution_conflicts.clone();
            conflicts.push(inst);
            let patched_team: tabbycat_api::types::Team = attohttpc::patch(team.url)
                .header("Authorization", format!("Token {}", auth.api_key))
                .json(&serde_json::json!({
                    "institution_conflicts": conflicts
                }))
                .unwrap()
                .send()
                .unwrap()
                .json()
                .unwrap();
            let original_team = teams
                .iter_mut()
                .find(|team| team.url == patched_team.url)
                .unwrap();
            let name = patched_team.short_name.clone();
            *original_team = patched_team;

            info!("Clashed team {} against its own institution.", name);
        }
    }

    let resp = attohttpc::get(format!(
        "{}/api/v1/tournaments/{}/adjudicators",
        auth.tabbycat_url, auth.tournament_slug
    ))
    .header("Authorization", format!("Token {}", auth.api_key))
    .send()
    .unwrap();
    if !resp.is_success() {
        panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
    }
    let mut judges: Vec<tabbycat_api::types::Adjudicator> = resp.json().unwrap();

    for judge in judges.clone() {
        let adding_judge_conflict = span!(Level::INFO, "sensible_conflict", judge = judge.name);
        let _adding_judge_guard = adding_judge_conflict.enter();

        if let Some(inst) = judge.institution
            && !judge.institution_conflicts.contains(&inst)
        {
            let mut t = judge.team_conflicts;
            t.push(inst);
            let adj: tabbycat_api::types::Adjudicator = attohttpc::patch(judge.url)
                .header("Authorization", format!("Token {}", auth.api_key))
                .json(&serde_json::json!({
                    "institution_conflicts": t
                }))
                .unwrap()
                .send()
                .unwrap()
                .json()
                .unwrap();
            let judge = judges
                .iter_mut()
                .find(|judge| judge.url == adj.url)
                .unwrap();
            let name = adj.name.clone();
            *judge = adj;

            info!("Clashed adj {} against their own institution", name);
        } else {
            info!(
                "Adjudicator {} is already clashed against their own institution",
                judge.name,
            )
        }
    }
}
