use serde_json::json;
use tabbycat_api::types::DebateAdjudicator;

use crate::{
    Auth,
    api_utils::{get_judges, get_round, get_teams, pairings_of_round},
};

enum Kind {
    Judge(tabbycat_api::types::Adjudicator),
    Team(tabbycat_api::types::Team),
}

impl Kind {
    fn url(&self) -> &str {
        match self {
            Kind::Judge(adjudicator) => &adjudicator.url,
            Kind::Team(team) => &team.url,
        }
    }
}

fn kind(
    a: &str,
    teams: &[tabbycat_api::types::Team],
    judges: &[tabbycat_api::types::Adjudicator],
) -> Kind {
    if let Some(team) = teams.iter().find(|team| {
        team.long_name.to_lowercase().trim().to_string() == a.to_lowercase().trim().to_string()
            || team.short_name.to_lowercase().trim() == a.to_lowercase().trim()
    }) {
        Kind::Team(team.clone())
    } else if let Some(judge) = judges.iter().find(|judge| {
        judge.name.to_lowercase().trim() == a.to_lowercase().trim()
            || judge.id.to_string().trim() == a.to_lowercase().trim()
    }) {
        Kind::Judge(judge.clone())
    } else {
        println!("Error: {a} is not a team or judge!");
        std::process::exit(1);
    }
}

pub fn swap(round: &str, a: &str, b: &str, auth: Auth) {
    let teams = get_teams(&auth);
    let judges = get_judges(&auth);

    let round = get_round(round, &auth);
    let pairings = pairings_of_round(&auth, &round);

    let a = (kind)(a, &teams, &judges);
    let b = (kind)(b, &teams, &judges);

    if a.url() == b.url() {
        println!("Can't swap two identical objects.");
        std::process::exit(1);
    }

    match (a, b) {
        (Kind::Judge(adj1), Kind::Judge(adj2)) => {
            let mut pairing_a = get_adj_pairing(&pairings, adj1.clone()).clone();
            let mut pairing_b = get_adj_pairing(&pairings, adj2.clone()).clone();

            if pairing_a.url == pairing_b.url {
                let a_loc = get_adj_ref(&adj1.url, &mut pairing_a);
                *a_loc = "tmp".to_string();

                let b_loc = get_adj_ref(&adj2.url, &mut pairing_a);
                *b_loc = adj1.url;

                let tmp_loc = get_adj_ref("tmp", &mut pairing_a);
                *tmp_loc = adj2.url;

                patch_adjudicators_in_pairing(&auth, &pairing_a);
            } else {
                let a_loc = get_adj_ref(&adj1.url, &mut pairing_a);
                let b_loc = get_adj_ref(&adj2.url, &mut pairing_b);
                *a_loc = adj2.url;
                *b_loc = adj1.url;
                patch_adjudicators_in_pairing(&auth, &pairing_a);
                patch_adjudicators_in_pairing(&auth, &pairing_b);
            }
        }
        (Kind::Judge(_), Kind::Team(_)) | (Kind::Team(_), Kind::Judge(_)) => {
            println!("Cannot swap judges and teams on the draw!");
            std::process::exit(1);
        }
        (Kind::Team(team1), Kind::Team(team2)) => {
            let mut pairings = pairings;
            replace_team_url(&mut pairings, &team1.url, "tmp");
            replace_team_url(&mut pairings, &team2.url, &team1.url);
            replace_team_url(&mut pairings, "tmp", &team2.url);
            let pairing_a = pairing_of_team(&pairings, &team1.url);
            let pairing_b = pairing_of_team(&pairings, &team2.url);

            if pairing_a.url != pairing_b.url {
                patch_teams_in_pairing(&auth, pairing_a);
                patch_teams_in_pairing(&auth, pairing_b);
            } else {
                patch_teams_in_pairing(&auth, pairing_a);
            }
        }
    };
}

fn pairing_of_team<'r>(
    pairings: &'r [tabbycat_api::types::RoundPairing],
    team_url: &str,
) -> &'r tabbycat_api::types::RoundPairing {
    pairings
        .iter()
        .find(|pairing| pairing.teams.iter().any(|team| team.team == team_url))
        .unwrap()
}

fn replace_team_url(
    pairings: &mut [tabbycat_api::types::RoundPairing],
    team_url: &str,
    new_team_url: &str,
) {
    for pairing in pairings {
        for team in &mut pairing.teams {
            if team.team == team_url {
                team.team = new_team_url.to_string();
            }
        }
    }
}

fn patch_teams_in_pairing(auth: &Auth, pairing_a: &tabbycat_api::types::RoundPairing) {
    attohttpc::patch(pairing_a.url.clone())
        .header("Authorization", format!("Token {}", auth.api_key))
        .json(&json! ({
            "teams": pairing_a.teams.clone()
        }))
        .unwrap()
        .send()
        .unwrap();
}

fn patch_adjudicators_in_pairing(auth: &Auth, pairing_a: &tabbycat_api::types::RoundPairing) {
    attohttpc::patch(pairing_a.url.clone())
        .header("Authorization", format!("Token {}", auth.api_key))
        .json(&json! ({
            "adjudicators": {
                "chair": pairing_a.adjudicators.as_ref().unwrap().chair.clone(),
                "panellists": pairing_a.adjudicators.as_ref().unwrap().panellists.clone(),
                "trainees": pairing_a.adjudicators.as_ref().unwrap().trainees.clone()
            }
        }))
        .unwrap()
        .send()
        .unwrap();
}

fn get_adj_ref<'r>(
    adj_id: &str,
    pairing: &'r mut tabbycat_api::types::RoundPairing,
) -> &'r mut String {
    let mut a_loc = None;

    let adjs = pairing.adjudicators.as_mut().unwrap();
    match &mut adjs.chair {
        Some(adj) if adj == adj_id => {
            a_loc = Some(adj);
        }
        _ => (),
    }
    adjs.panellists.iter_mut().for_each(|p| {
        if *p == adj_id {
            a_loc = Some(p);
        }
    });
    adjs.trainees.iter_mut().for_each(|p| {
        if *p == adj_id {
            a_loc = Some(p);
        }
    });
    a_loc.unwrap()
}

fn get_adj_pairing(
    pairings: &[tabbycat_api::types::RoundPairing],
    adj1: tabbycat_api::types::Adjudicator,
) -> &tabbycat_api::types::RoundPairing {
    pairings
        .iter()
        .find(|pairing| {
            pairing
                .adjudicators
                .as_ref()
                .map(|adjs| {
                    adjs.chair.as_ref() == Some(&adj1.url)
                        || adjs.panellists.iter().any(|p| p == &adj1.url)
                        || adjs.trainees.iter().any(|p| p == &adj1.url)
                })
                .unwrap_or(false)
        })
        .unwrap_or_else(|| {
            println!("Adjudicator `{}` is not on the draw", adj1.name);
            std::process::exit(1);
        })
}

enum Role {
    C,
    P,
    T,
}

pub fn alloc(round: &str, to: &str, a: &str, role: &str, auth: Auth) {
    let to = match to.parse::<i64>() {
        Ok(t) => t,
        Err(_) => {
            println!("Please provide an integer room!");
            std::process::exit(1);
        }
    };

    let role = match role.to_lowercase().as_str() {
        "c" | "chair" => Role::C,

        "p" | "panellist" => Role::P,
        "t" | "trainee" => Role::T,
        _ => {
            println!("Role should be one of `c`/`chair`, `p`/`pannelist`, `t`/`trainee`");
            std::process::exit(1);
        }
    };

    let teams = get_teams(&auth);
    let judges = get_judges(&auth);

    let round = get_round(round, &auth);
    let pairings = pairings_of_round(&auth, &round);

    let judge = match kind(a, &teams, &judges) {
        Kind::Judge(adjudicator) => adjudicator,
        Kind::Team(_) => {
            println!("Error: can only assign judges to panels!");
            std::process::exit(1);
        }
    };

    match pairings.iter().find(|pairing| pairing.id == to) {
        Some(pairing) => {
            let mut pairing = pairing.clone();
            if pairing.adjudicators.is_none() {
                pairing.adjudicators = Some(DebateAdjudicator {
                    chair: None,
                    panellists: vec![],
                    trainees: vec![],
                });
            }
            match role {
                Role::C => pairing.adjudicators.as_mut().unwrap().chair = Some(judge.url),
                Role::P => pairing
                    .adjudicators
                    .as_mut()
                    .unwrap()
                    .panellists
                    .push(judge.url),
                Role::T => pairing
                    .adjudicators
                    .as_mut()
                    .unwrap()
                    .trainees
                    .push(judge.url),
            }
            patch_adjudicators_in_pairing(&auth, &pairing);
        }
        None => {
            println!("Error: pairing ID provided was invalid");
            std::process::exit(1);
        }
    }
}

pub fn remove(round: &str, a: &str, auth: Auth) {
    let teams = get_teams(&auth);
    let judges = get_judges(&auth);

    let round = get_round(round, &auth);
    let pairings = pairings_of_round(&auth, &round);

    let judge = match kind(a, &teams, &judges) {
        Kind::Judge(adjudicator) => adjudicator,
        Kind::Team(_) => {
            println!("Error: can only assign judges to panels!");
            std::process::exit(1);
        }
    };

    let pairing = get_adj_pairing(&pairings, judge.clone());

    let mut pairing = pairing.clone();

    if pairing.adjudicators.as_mut().unwrap().chair == Some(judge.url.clone()) {
        pairing.adjudicators.as_mut().unwrap().chair = None;
    }

    pairing
        .adjudicators
        .as_mut()
        .unwrap()
        .panellists
        .retain(|t| *t != judge.url);

    pairing
        .adjudicators
        .as_mut()
        .unwrap()
        .trainees
        .retain(|t| *t != judge.url);

    patch_adjudicators_in_pairing(&auth, &pairing);
}
