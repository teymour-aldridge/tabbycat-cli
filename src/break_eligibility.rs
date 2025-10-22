use std::collections::{HashMap, HashSet};

use serde_json::json;
use tracing::{Level, error, info, span};

use crate::Auth;

/// Computes whether each team should be break eligible according to the rules
/// of the specified format.
pub fn do_compute_break_eligibility(auth: Auth, format: String) {
    let break_categories: Vec<tabbycat_api::types::BreakCategory> = attohttpc::get(format!(
        "{}/api/v1/tournaments/{}/break-categories",
        auth.tabbycat_url, auth.tournament_slug
    ))
    .header("Authorization", format!("Token {}", auth.api_key))
    .send()
    .unwrap()
    .json()
    .unwrap();
    let teams: Vec<tabbycat_api::types::Team> = attohttpc::get(format!(
        "{}/api/v1/tournaments/{}/teams",
        auth.tabbycat_url, auth.tournament_slug
    ))
    .header("Authorization", format!("Token {}", auth.api_key))
    .send()
    .unwrap()
    .json()
    .unwrap();
    let speaker_categories: Vec<tabbycat_api::types::SpeakerCategory> = attohttpc::get(format!(
        "{}/api/v1/tournaments/{}/speaker-categories",
        auth.tabbycat_url, auth.tournament_slug
    ))
    .header("Authorization", format!("Token {}", auth.api_key))
    .send()
    .unwrap()
    .json()
    .unwrap();

    let span = span!(Level::INFO, "break_eligibility");
    let _guard = span.enter();

    let mut map = HashMap::new();

    let open = break_categories
        .iter()
        .find(|cat| cat.name.to_ascii_lowercase().contains("open"))
        .unwrap();

    for break_cat in &break_categories {
        if break_cat.name.to_ascii_lowercase().contains("open") {
            continue;
        }
        let speaker_cat = speaker_categories
            .iter()
            .find(|s| s.name.to_ascii_lowercase() == break_cat.name.to_ascii_lowercase())
            .unwrap_or_else(|| {
                panic!("no matching category found for {}", break_cat.name.as_str())
            });
        map.insert(speaker_cat.url.clone(), break_cat.url.clone());
    }

    let mut team_breaking_counts = HashMap::new();

    for team in &teams {
        let mut n_breaking_per_category: HashMap<String, usize> = HashMap::new();

        for speaker in &team.speakers {
            for category in &speaker.categories {
                let break_cat = map.get(category).unwrap();

                n_breaking_per_category
                    .entry(break_cat.clone())
                    .and_modify(|count| *count += 1)
                    .or_insert(1);
            }
        }

        team_breaking_counts.insert(team.url.clone(), n_breaking_per_category);
    }

    let c = format.to_ascii_lowercase();
    if c == "wsdc" {
        // todo: handle EFL gracefully if it doesn't exist (warn user, and then
        // compute break categories without it)
        let esl = break_categories
            .iter()
            .find(|cat| cat.name.to_ascii_lowercase().contains("esl"))
            .unwrap();
        let efl = break_categories
            .iter()
            .find(|cat| cat.name.to_ascii_lowercase().contains("efl"))
            .unwrap();

        for (team_url, breaking_counts) in team_breaking_counts {
            let team = teams.iter().find(|t| t.url == team_url).unwrap();
            let mut break_cats = HashSet::new();

            for category in &break_categories {
                let count = breaking_counts.get(&category.url).unwrap_or(&0);
                if *count >= team.speakers.len().saturating_sub(1) {
                    break_cats.insert(category.url.clone());
                }
            }

            let breaks_esl = {
                breaking_counts.get(&esl.url).unwrap_or(&0)
                    + breaking_counts.get(&efl.url).unwrap_or(&0)
                    >= team.speakers.len().saturating_sub(1)
            };

            if breaks_esl {
                break_cats.insert(esl.url.clone());
            } else {
                break_cats.remove(&esl.url.clone());
            }

            break_cats.insert(open.url.clone());

            attohttpc::patch(&team_url)
                .header("Authorization", format!("Token {}", auth.api_key))
                .json(&json!({
                    "break_categories": break_cats
                }))
                .unwrap()
                .send()
                .unwrap();
            info!(
                "Set team {} break eligibility to {:?}",
                team.short_name,
                break_cats
                    .iter()
                    .map(|cat| {
                        break_categories
                            .iter()
                            .find(|c| &c.url == cat)
                            .unwrap()
                            .name
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            );
        }
    } else if c == "bp" {
        // todo: test this
        let esl = break_categories
            .iter()
            .find(|cat| cat.name.to_ascii_lowercase().contains("esl"))
            .unwrap();
        let efl = break_categories
            .iter()
            .find(|cat| cat.name.to_ascii_lowercase().contains("efl"));

        for (team_url, breaking_counts) in team_breaking_counts {
            let team = teams.iter().find(|t| t.url == team_url).unwrap();
            let mut break_cats = HashSet::new();

            for category in &break_categories {
                let count = breaking_counts.get(&category.url).unwrap_or(&0);
                if *count == team.speakers.len() {
                    break_cats.insert(category.url.clone());
                }
            }

            let breaks_esl = {
                breaking_counts.get(&esl.url).unwrap_or(&0)
                    + efl
                        .map(|efl| breaking_counts.get(&efl.url))
                        .flatten()
                        .unwrap_or(&0)
                    == team.speakers.len()
            };

            if breaks_esl {
                break_cats.insert(esl.url.clone());
            } else {
                break_cats.remove(&esl.url.clone());
            }

            break_cats.insert(open.url.clone());

            attohttpc::patch(&team_url)
                .header("Authorization", format!("Token {}", auth.api_key))
                .json(&json!({
                    "break_categories": break_cats
                }))
                .unwrap()
                .send()
                .unwrap();
            info!(
                "Set team {} break eligibility to {:?}",
                team.short_name,
                break_cats
                    .iter()
                    .map(|cat| {
                        break_categories
                            .iter()
                            .find(|c| &c.url == cat)
                            .unwrap()
                            .name
                            .to_string()
                    })
                    .collect::<Vec<_>>()
            );
        }
    } else {
        error!("Unrecognised format {}", c)
    }
}
