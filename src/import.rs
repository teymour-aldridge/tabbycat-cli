use std::{
    collections::{HashMap, HashSet},
    process::exit,
};

use itertools::Itertools;
use serde::{
    Deserialize, Deserializer,
    de::{self, Unexpected},
};
use serde_json::json;
use tabbycat_api::types::{BreakCategory, SpeakerCategory, Team};
use tracing::{Level, debug, error, info, span};

use crate::{
    Auth, Import,
    api_utils::{get_rounds, get_teams},
    merge, open_csv_file,
};

#[derive(Deserialize, Debug, Clone)]
pub struct InstitutionRow {
    pub region: Option<String>,
    // TODO: warn when this is >20 characters (Tabbycat currently applies
    // this restriction) to aid with debugging
    pub short_code: String,
    pub full_name: String,
}

fn ret_false() -> bool {
    false
}

fn tags_deserialize<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let str_sequence = String::deserialize(deserializer)?;
    Ok(str_sequence
        .split(',')
        .map(|item| item.to_owned())
        .filter(|item| !item.is_empty())
        .collect())
}

fn bool_from_str<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    match String::deserialize(deserializer)?.to_lowercase().trim() {
        "t" | "true" | "1" | "on" | "y" | "yes" => Ok(true),
        "f" | "false" | "0" | "off" | "n" | "no" | "" => Ok(false),
        other => Err(de::Error::invalid_value(
            Unexpected::Str(other),
            &"Must be truthy (t, true, 1, on, y, yes) or falsey (f, false, 0, off, n, no)",
        )),
    }
}

fn not_true() -> bool {
    false
}

// todo: team institution clashes
#[derive(Deserialize, Debug, Clone)]
pub struct TeamRow {
    pub full_name: String,
    /// If not supplied, we truncate the full name.
    pub short_name: Option<String>,
    #[serde(deserialize_with = "tags_deserialize", default = "Vec::new")]
    pub categories: Vec<String>,
    pub code_name: Option<String>,
    pub institution: Option<String>,
    pub seed: Option<u32>,
    pub emoji: Option<String>,
    #[serde(deserialize_with = "bool_from_str", default = "not_true")]
    pub use_institution_prefix: bool,
    #[serde(flatten, deserialize_with = "deserialize_fields_to_vec")]
    pub speakers: Vec<Speaker>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Clash {
    pub object_1: String,
    pub object_2: String,
}

fn deserialize_fields_to_vec<'de, D>(deserializer: D) -> Result<Vec<Speaker>, D::Error>
where
    D: Deserializer<'de>,
{
    let map: HashMap<String, String> = HashMap::deserialize(deserializer)?;
    let speaker_buckets = {
        let mut buckets: HashMap<u8, HashMap<String, String>> = HashMap::new();
        for (key, value) in map.iter() {
            if let Some(iter) = key.strip_prefix("speaker") {
                // todo: good error messages
                let mut iter = iter.split('_');
                let number = iter.next().unwrap().trim().parse::<u8>().unwrap();
                let field_name = iter.next().unwrap();
                buckets
                    .entry(number)
                    .and_modify(|map| {
                        map.insert(field_name.to_string(), value.clone());
                    })
                    .or_insert({
                        let mut t = HashMap::new();

                        t.insert(field_name.to_string(), value.clone());
                        t
                    });
            }
        }
        buckets
    };

    Ok(speaker_buckets
        .into_iter()
        .sorted_by_key(|(t, _)| *t)
        .filter_map(|(_, map)| {
            if map.values().all(|key| key.trim().is_empty()) {
                None
            } else {
                Some(Speaker {
                    name: map.get("name").cloned().expect("error: missing name!"),
                    categories: map
                        .get("categories")
                        .cloned()
                        .map(|t| {
                            t.split(',')
                                .map(|x| x.to_string())
                                .filter(|t| !t.trim().is_empty())
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or(vec![]),
                    email: map.get("email").cloned(),
                    phone: map.get("phone").cloned(),
                    anonymous: map
                        .get("anonymous")
                        .cloned()
                        .map(|t| t.eq_ignore_ascii_case("true"))
                        .unwrap_or(false),
                    code_name: map.get("code_name").cloned(),
                    url_key: map.get("url_key").cloned(),
                    gender: map.get("gender").map(|gender| {
                        if gender.to_lowercase() == "male" {
                            "M"
                        } else if gender.to_lowercase() == "female" {
                            "F"
                        } else if gender.to_lowercase() == "other" {
                            "O"
                        } else {
                            gender
                        }
                        .to_string()
                    }),
                    pronoun: map.get("pronoun").cloned(),
                })
            }
        })
        .collect::<Vec<_>>())
}

#[derive(Deserialize, Debug, Clone)]
pub struct Speaker {
    pub name: String,
    pub categories: Vec<String>,
    pub email: Option<String>,
    pub phone: Option<String>,
    pub anonymous: bool,
    pub code_name: Option<String>,
    pub url_key: Option<String>,
    // todo: validate correct
    pub gender: Option<String>,
    // todo: validate length
    pub pronoun: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct JudgeRow {
    pub name: String,
    pub institution: Option<String>,
    #[serde(deserialize_with = "tags_deserialize", default = "Vec::new")]
    pub institution_clashes: Vec<String>,
    pub email: Option<String>,
    #[serde(deserialize_with = "bool_from_str", default = "ret_false")]
    pub is_ca: bool,
    #[serde(deserialize_with = "bool_from_str", default = "ret_false")]
    pub is_ia: bool,
    pub base_score: Option<f64>,
    #[serde(deserialize_with = "tags_deserialize", default = "Vec::new")]
    pub availability: Vec<String>,
}

pub fn do_import(auth: Auth, import: Import) {
    let institutions_csv = open_csv_file(import.institutions_csv.clone(), true);
    let teams_csv = open_csv_file(import.teams_csv.clone(), true);
    let judges_csv = open_csv_file(import.judges_csv.clone(), true);
    let clashes_csv = open_csv_file(import.clashes_csv.clone(), false);

    let api_addr = format!("{}/api/v1", auth.tabbycat_url);

    let mut speaker_categories: Vec<tabbycat_api::types::SpeakerCategory> = {
        let base_url = format!(
            "{api_addr}/tournaments/{}/speaker-categories",
            auth.tournament_slug
        );
        let resp = attohttpc::get(&base_url)
            .header("Authorization", format!("Token {}", auth.api_key))
            .send()
            .unwrap();

        if !resp.is_success() {
            panic!(
                "url: {base_url} error {:?}
                \n \n
                RESPONSE \n
                -----------------------------------------------
                {}",
                resp.status(),
                resp.text_utf8().unwrap()
            );
        }

        resp.json().unwrap()
    };

    let mut break_categories: Vec<tabbycat_api::types::BreakCategory> = {
        let resource_loc = format!(
            "{api_addr}/tournaments/{}/break-categories",
            auth.tournament_slug
        );
        let resp = attohttpc::get(&resource_loc)
            .header("Authorization", format!("Token {}", auth.api_key))
            .send()
            .unwrap();

        if !resp.is_success() {
            error!(
                "Failed to fetch break categories: url={resource_loc}, status = {:?}, body = {}",
                resp.status(),
                resp.text_utf8().unwrap()
            );
            panic!("Failed to fetch break categories");
        }

        resp.json().unwrap()
    };

    let mut institutions: Vec<tabbycat_api::types::PerTournamentInstitution> = {
        let resp = attohttpc::get(format!("{api_addr}/institutions"))
            .header("Authorization", format!("Token {}", auth.api_key))
            .send()
            .unwrap();

        if !resp.is_success() {
            error!(
                "Failed to fetch institutions: status = {:?}, body = {}",
                resp.status(),
                resp.text_utf8().unwrap()
            );
            panic!("Failed to fetch institutions");
        }

        resp.json().unwrap()
    };

    let mut speakers: Vec<tabbycat_api::types::Speaker> = {
        let resp = attohttpc::get(format!(
            "{api_addr}/tournaments/{}/speakers",
            auth.tournament_slug
        ))
        .header("Authorization", format!("Token {}", auth.api_key))
        .send()
        .unwrap();

        if !resp.is_success() {
            error!(
                "Failed to fetch speakers: status = {:?}, body = {}",
                resp.status(),
                resp.text_utf8().unwrap()
            );
            panic!("Failed to fetch speakers");
        }

        resp.json().unwrap()
    };

    let mut teams = get_teams(&auth);

    let rounds = get_rounds(&auth);

    let resp = attohttpc::get(format!(
        "{api_addr}/tournaments/{}/adjudicators",
        auth.tournament_slug
    ))
    .header("Authorization", format!("Token {}", auth.api_key))
    .send()
    .unwrap();
    if !resp.is_success() {
        panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
    }
    let mut judges: Vec<tabbycat_api::types::Adjudicator> = resp.json().unwrap();

    if let Some(mut institutions_csv) = institutions_csv {
        let headers = institutions_csv.headers().unwrap().clone();
        let institutions_span = span!(Level::INFO, "importing institutions");
        let _institutions_guard = institutions_span.enter();

        for institution2import in institutions_csv.records() {
            let institution2import = institution2import.unwrap();

            let institution: InstitutionRow =
                institution2import.deserialize(Some(&headers)).unwrap();

            if !institutions.iter().any(|cmp| {
                cmp.name.as_str() == institution.full_name
                    || cmp.code.as_str() == institution.short_code
            }) {
                let response = attohttpc::post(format!("{api_addr}/institutions"))
                    .header("Authorization", format!("Token {}", auth.api_key))
                    .json(&serde_json::json!({
                        "region": institution.region,
                        "name": institution.full_name,
                        "code": institution.short_code
                    }))
                    .unwrap()
                    .send()
                    .unwrap();
                if !response.is_success() {
                    panic!("error: {}", response.text_utf8().unwrap());
                }
                let inst: tabbycat_api::types::PerTournamentInstitution = response.json().unwrap();
                info!(
                    "Institution {} added to Tabbycat, id is {}",
                    inst.name.as_str(),
                    inst.id
                );
                institutions.push(inst);
            } else {
                info!(
                    "Institution {} already exists, not inserting",
                    institution.full_name
                );
            }
        }
    } else {
        info!("No institutions were provided to import.")
    }

    if let Some(mut judges_csv) = judges_csv {
        let headers = judges_csv.headers().unwrap().clone();
        let judges_span = span!(Level::INFO, "importing judges");
        let _judges_guard = judges_span.enter();

        for judge2import in judges_csv.records() {
            let judge2import = judge2import.unwrap();
            let judge2import: JudgeRow = judge2import.deserialize(Some(&headers)).unwrap();

            if !judges.iter().any(|judge| judge.name == judge2import.name) {
                let judge_inst_conflicts = institutions
                    .iter()
                    .filter(|inst_from_api| {
                        judge2import
                            .institution_clashes
                            .iter()
                            .any(|inst_judge_clashes| {
                                inst_from_api.name.as_str() == inst_judge_clashes
                                    || inst_from_api.code.as_str() == inst_judge_clashes
                            })
                    })
                    .map(|inst| inst.url.clone())
                    .collect::<Vec<_>>();

                // todo: have a debug mode which logs debug output to a file

                let inst_url = institutions
                    .iter()
                    .find(|api_inst| {
                        Some(api_inst.name.as_str().to_string()) == judge2import.institution
                            || Some(api_inst.code.as_str().to_string()) == judge2import.institution
                    })
                    .map(|inst| inst.url.clone());

                if judge2import.institution.is_some() {
                    assert!(
                        inst_url.is_some(),
                        "error: {:?} {:?}",
                        judge2import.institution,
                        institutions
                    );
                }

                let mut payload = serde_json::json!({
                    "name": judge2import.name,
                    "institution": inst_url,
                    "institution_conflicts": judge_inst_conflicts,
                    "email": judge2import.email,
                    "team_conflicts": [],
                    "adjudicator_conflicts": [],
                    "independent": judge2import.is_ia,
                    "adj_core": judge2import.is_ca,
                });

                if let Some(base_score) = judge2import.base_score {
                    tracing::trace!("base score {base_score}");
                    merge(&mut payload, &json!({"base_score": base_score}));
                }

                tracing::trace!("data for request is: {payload:?}");

                let resp = attohttpc::post(format!(
                    "{api_addr}/tournaments/{}/adjudicators",
                    auth.tournament_slug
                ))
                .header("Authorization", format!("Token {}", auth.api_key))
                .json(&payload)
                .unwrap()
                .send()
                .unwrap();
                if !resp.is_success() {
                    error!("error");
                    panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
                }

                let judge: tabbycat_api::types::Adjudicator = resp.json().unwrap();
                info!("Created judge {} with id {}", judge.name, judge.id);
                judges.push(judge.clone());

                // TODO: there should be a way to opt-out of setting this (or
                // at least specify the default)
                let norm = judge2import
                    .availability
                    .iter()
                    .map(|availability| availability.to_ascii_lowercase())
                    .collect::<HashSet<_>>();
                for api_round in &rounds {
                    let (available, method, url) = if norm
                        .contains(&api_round.abbreviation.to_ascii_lowercase())
                        || norm.contains(&api_round.name.to_ascii_lowercase())
                    {
                        (
                            "available",
                            attohttpc::Method::PUT,
                            format!(
                                "{api_addr}/tournaments/{}/rounds/{}/availabilities",
                                auth.tournament_slug, api_round.seq
                            ),
                        )
                    } else {
                        (
                            "unavailable",
                            attohttpc::Method::POST,
                            format!(
                                "{api_addr}/tournaments/{}/rounds/{}/availabilities",
                                auth.tournament_slug, api_round.seq
                            ),
                        )
                    };

                    let resp = attohttpc::RequestBuilder::new(method, &url)
                        .header("Authorization", format!("Token {}", auth.api_key))
                        .json(&json!([judge.url]))
                        .unwrap()
                        .send()
                        .unwrap();

                    if !resp.is_success() {
                        error!(
                            "Failed to mark judge {} as {available} for round {}: {} {}",
                            judge2import.name,
                            api_round.name.as_str(),
                            resp.status(),
                            resp.text_utf8().unwrap()
                        );
                        panic!("Failed to mark judge as {available}");
                    } else {
                        info!(
                            "Marked judge {} as {available} for round {}",
                            judge2import.name,
                            api_round.name.as_str()
                        );
                    }
                }
            } else {
                info!(
                    "Judge {} already exists, therefore not creating a record \
                    for this judge.",
                    judge2import.name
                );
            }
        }
    } else {
        info!("No judges were provided to import.")
    }

    if let Some(mut teams_csv) = teams_csv {
        let headers = teams_csv.headers().unwrap().clone();
        let teams_span = span!(Level::INFO, "importing teams");
        let _teams_guard = teams_span.enter();

        for team2import in teams_csv.records() {
            let team2import = team2import.unwrap();
            let team2import: TeamRow = team2import.deserialize(Some(&headers)).unwrap();

            let inst_of_team2_import = institutions.iter().find(|api_inst| {
                Some(api_inst.name.as_str().to_lowercase())
                    == team2import.institution.as_ref().map(|t| t.to_lowercase())
                    || Some(api_inst.code.as_str().to_lowercase())
                        == team2import.institution.as_ref().map(|t| t.to_lowercase())
            });

            let team_url = if let Some(team) = teams.iter().find(|team| {
                let (long_prefix, short_prefix) =
                    if team2import.use_institution_prefix || import.use_institution_prefix {
                        if let Some(inst) = inst_of_team2_import {
                            (
                                format!("{} ", inst.name.as_str()),
                                format!("{} ", inst.code.as_str()),
                            )
                        } else {
                            (String::new(), String::new())
                        }
                    } else {
                        (String::new(), String::new())
                    };

                team.long_name == format!("{long_prefix}{}", team2import.full_name.trim())
                    || Some(format!("{short_prefix}{}", team.short_name.as_str()).as_str())
                        == team2import.short_name.as_ref().map(|t| t.trim())
                    || team.code_name.clone().map(|t| t.as_str().to_string())
                        == team2import.code_name.as_ref().map(|t| t.trim().to_string())
            }) {
                info!(
                    "Team {} already exists, therefore not creating a record \
                    for this team.",
                    team2import.full_name
                );
                team.url.clone()
            } else {
                let inst = inst_of_team2_import.map(|inst| inst.url.clone());

                if team2import.institution.is_some() {
                    if inst.is_none() {
                        error!(
                            "Team {} belongs to institution {:?}, however, no \
                            corresponding institution was defined in {}.",
                            team2import.full_name,
                            team2import.institution.unwrap(),
                            import.institutions_csv.as_ref().unwrap()
                        );
                    }
                    assert!(inst.is_some());
                }

                let break_category_urls = {
                    let category_and_optionally_url = team2import
                        .categories
                        .iter()
                        .map(|team2_import_category_name| {
                            assert!(!team2_import_category_name.is_empty());
                            (
                                team2_import_category_name,
                                break_categories
                                    .iter()
                                    .find(|api_cat| {
                                        api_cat
                                            .slug
                                            .as_str()
                                            .eq_ignore_ascii_case(team2_import_category_name.trim())
                                    })
                                    .cloned(),
                            )
                        })
                        .collect::<Vec<_>>();

                    category_and_optionally_url
                        .into_iter()
                        .map(|(name, api_category)| match api_category {
                            Some(t) => t.clone(),
                            None => {
                                let seq = break_categories.len() + 1;
                                let resp = attohttpc::post(format!(
                                    "{api_addr}/tournaments/{}/break-categories",
                                    auth.tournament_slug
                                ))
                                .header("Authorization", format!("Token {}", auth.api_key))
                                .header("content-type", "application/json")
                                .json(&serde_json::json!({
                                    "name": name,
                                    "slug": name.to_ascii_lowercase(),
                                    "seq": seq,
                                    "break_size": 4,
                                    "is_general": false,
                                    "priority": 1
                                }))
                                .unwrap()
                                .send()
                                .unwrap();

                                if !resp.is_success() {
                                    panic!(
                                        "error when creating category {name}\n
                                        {:?} {}",
                                        resp.status(),
                                        resp.text_utf8().unwrap()
                                    );
                                }

                                let category: BreakCategory = resp.json().unwrap();
                                break_categories.push(category.clone());
                                category
                            }
                        })
                        .map(|t| t.url.clone())
                        .collect::<Vec<_>>()
                };

                let mut payload = {
                    serde_json::json!({
                        "institution": inst,
                        "reference": team2import.full_name,
                        "seed": team2import.seed,
                        "emoji": team2import.emoji,
                        "use_institution_prefix":
                            // TODO: document this behaviour
                            import.use_institution_prefix
                            || team2import.use_institution_prefix,
                        "break_categories": break_category_urls,
                        // note: we don't add speakers here!
                    })
                };

                if let Some(code_name) = team2import.code_name {
                    merge(&mut payload, &json!({"code_name": code_name}));
                }

                if let Some(short_name) = team2import.short_name {
                    merge(&mut payload, &json!({"short_reference": short_name}));
                }

                let resp = attohttpc::post(format!(
                    "{api_addr}/tournaments/{}/teams",
                    auth.tournament_slug
                ))
                .header("Authorization", format!("Token {}", auth.api_key))
                .header("content-type", "application/json")
                .json(&payload)
                .unwrap()
                .send()
                .unwrap();
                if !resp.is_success() {
                    panic!(
                        "error (team is {}) {:?} {} \n {:#?}",
                        team2import.full_name,
                        resp.status(),
                        resp.text_utf8().unwrap(),
                        teams
                    );
                }
                let team: Team = resp.json().unwrap();
                info!(
                    "Created team {} with id {} (institution: {:?})",
                    team.long_name, team.id, inst
                );
                let url = team.url.clone();
                teams.push(team.clone());
                url
            };

            let team_span = span!(Level::INFO, "team", team_name = team2import.full_name);
            let _team_guard = team_span.enter();
            for speaker2import in team2import.speakers {
                if !speakers.iter().any(|speaker| {
                    speaker.name.trim() == speaker2import.name.trim()
                        || speaker
                            .url_key
                            .clone()
                            .map(|key| Some(key.as_str().to_string()) == speaker2import.url_key)
                            .unwrap_or(false)
                }) {
                    let speaker_category_urls = {
                        let mut ret = Vec::new();
                        for speaker2import_cat in speaker2import.categories {
                            let speaker2import_cat = speaker2import_cat.trim();
                            let category_from_tabbycat = speaker_categories
                                .iter()
                                .find(|api_cat| {
                                    api_cat.slug.as_str().to_ascii_lowercase().trim()
                                        == speaker2import_cat.to_ascii_lowercase()
                                })
                                .cloned();

                            match category_from_tabbycat {
                                Some(t) => ret.push(t.clone().url),
                                None => {
                                    let seq = speaker_categories.len() + 1;
                                    let resp = attohttpc::post(format!(
                                        "{api_addr}/tournaments/{}/speaker-categories",
                                        auth.tournament_slug
                                    ))
                                    .header("Authorization", format!("Token {}", auth.api_key))
                                    .header("content-type", "application/json")
                                    .json(&serde_json::json!({
                                        "name": speaker2import_cat,
                                        "slug": speaker2import_cat,
                                        "seq": seq
                                    }))
                                    .unwrap()
                                    .send()
                                    .unwrap();
                                    if !resp.is_success() {
                                        panic!(
                                            "Error: request failed, (note: \
                                            response body is {}) \n
                                            category: {speaker2import_cat} \n
                                            ",
                                            resp.text_utf8().unwrap()
                                        )
                                    }
                                    let category: SpeakerCategory = resp.json().unwrap();
                                    speaker_categories.push(category.clone());
                                    ret.push(category.url);
                                }
                            }
                        }
                        ret
                    };

                    let mut payload = json!({
                        "name": speaker2import.name,
                        "team": team_url,
                        "categories": speaker_category_urls,
                        "email": speaker2import.email,
                        "anonymous": speaker2import.anonymous,
                    });

                    if let Some(code_name) = speaker2import.code_name {
                        merge(
                            &mut payload,
                            &json!({
                                "code_name": code_name,
                            }),
                        );
                    }

                    if let Some(phone) = speaker2import.phone {
                        merge(
                            &mut payload,
                            &json!({
                                "phone": phone,
                            }),
                        )
                    }

                    if let Some(gender) = speaker2import.gender {
                        merge(
                            &mut payload,
                            &json!({
                                "gender": gender,
                            }),
                        )
                    }

                    if let Some(pronoun) = speaker2import.pronoun {
                        merge(
                            &mut payload,
                            &json!({
                                "pronoun": pronoun,
                            }),
                        )
                    }

                    let resp = attohttpc::post(format!(
                        "{api_addr}/tournaments/{}/speakers",
                        auth.tournament_slug
                    ))
                    .header("Authorization", format!("Token {}", auth.api_key))
                    .header("Content-Type", "application/json")
                    .json(&payload)
                    .unwrap()
                    .send()
                    .unwrap();

                    // TODO: we can format the JSON error messages in a more
                    // human-friendly way
                    if !resp.is_success() {
                        panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
                    }

                    let speaker: tabbycat_api::types::Speaker = resp.json().unwrap();
                    info!("Created speaker {} with id {}", speaker.name, speaker.id);
                    speakers.push(speaker.clone());
                    let team = teams
                        .iter_mut()
                        .find(|team| team.url == speaker.team)
                        .unwrap();
                    *team = attohttpc::get(team.url.clone())
                        .header("Authorization", format!("Token {}", auth.api_key))
                        .header("Content-Type", "application/json")
                        .json(&payload)
                        .unwrap()
                        .send()
                        .unwrap()
                        .json()
                        .unwrap();
                } else {
                    info!(
                        "Speaker {} already exists, therefore not creating a \
                        record for this speaker.",
                        speaker2import.name
                    );
                }
            }
        }
    } else {
        info!("No teams were provided to import.")
    }

    if let Some(mut clashes_csv) = clashes_csv {
        for clash2import in clashes_csv.records() {
            let clash2import = clash2import.unwrap();
            let clash2import: Clash = clash2import.deserialize(None).unwrap();

            let adding_clash_span = span!(
                Level::INFO,
                "clash",
                a = clash2import.object_1,
                b = clash2import.object_2
            );
            let _adding_clash_guard = adding_clash_span.enter();

            pub enum ClashKind {
                Adj(tabbycat_api::types::Adjudicator),
                Team(tabbycat_api::types::Team),
                Inst(tabbycat_api::types::PerTournamentInstitution),
            }

            fn find_obj(
                key: &str,
                teams: &[Team],
                judges: &[tabbycat_api::types::Adjudicator],
                institutions: &[tabbycat_api::types::PerTournamentInstitution],
            ) -> Option<ClashKind> {
                for inst in institutions {
                    if inst.name.as_str().eq_ignore_ascii_case(key)
                        || inst.code.as_str().eq_ignore_ascii_case(key)
                    {
                        return Some(ClashKind::Inst(inst.clone()));
                    }
                }

                for judge in judges {
                    if judge.name.eq_ignore_ascii_case(key) {
                        debug!("Resolved {key} as judge {} due to name match.", judge.name);

                        return Some(ClashKind::Adj(judge.clone()));
                    }
                }

                for team in teams {
                    if team.long_name.eq_ignore_ascii_case(key)
                        || team.short_name.eq_ignore_ascii_case(key)
                    {
                        debug!(
                            "Resolved {key} as team {} due to name match.",
                            team.long_name
                        );
                        return Some(ClashKind::Team(team.clone()));
                    }

                    if team
                        .speakers
                        .iter()
                        .any(|speaker| speaker.name.eq_ignore_ascii_case(key))
                    {
                        debug!(
                            "Resolved {key} as team {} as provided key matched \
                             the speaker name.",
                            team.long_name
                        );
                        return Some(ClashKind::Team(team.clone()));
                    }
                }

                None
            }

            if clash2import
                .object_1
                .eq_ignore_ascii_case(&clash2import.object_2)
            {
                error!(
                    "You have attempted to clash someone against themself: {} and {}",
                    clash2import.object_1, clash2import.object_2
                );
            }

            let a = find_obj(&clash2import.object_1, &teams, &judges, &institutions)
                .unwrap_or_else(|| {
                    panic!(
                        "error: no judge, team name, or speaker found matching {}",
                        clash2import.object_1
                    )
                });
            let b = find_obj(&clash2import.object_2, &teams, &judges, &institutions)
                .unwrap_or_else(|| {
                    panic!(
                        "error: no judge, team name, or speaker found matching {}",
                        clash2import.object_2
                    )
                });

            match (a, b) {
                (ClashKind::Adj(a), ClashKind::Inst(inst))
                | (ClashKind::Inst(inst), ClashKind::Adj(a)) => {
                    if !a.institution_conflicts.contains(&inst.url) {
                        let mut t = a.institution_conflicts;
                        t.push(inst.url);
                        let resp = attohttpc::patch(a.url)
                            .header("Authorization", format!("Token {}", auth.api_key))
                            .json(&serde_json::json!({
                                "institution_conflicts": t
                            }))
                            .unwrap()
                            .send()
                            .unwrap();

                        if !resp.is_success() {
                            error!(
                                "Failed to patch adjudicator: {} {}",
                                resp.status(),
                                resp.text_utf8().unwrap()
                            );
                            panic!("Failed to patch adjudicator institution conflicts");
                        }

                        let adj: tabbycat_api::types::Adjudicator = resp.json().unwrap();
                        let judge = judges
                            .iter_mut()
                            .find(|judge| judge.url == adj.url)
                            .unwrap();
                        let name = adj.name.clone();
                        *judge = adj;

                        info!("Clashed adj {} against inst {}", name, inst.code.as_str());
                    } else {
                        info!(
                            "Adjudicator {} is already clashed against institution {}",
                            a.name,
                            inst.name.as_str()
                        )
                    }
                }
                (ClashKind::Team(t), ClashKind::Inst(inst))
                | (ClashKind::Inst(inst), ClashKind::Team(t)) => {
                    if !t.institution_conflicts.contains(&inst.url) {
                        let mut conflicts = t.institution_conflicts;
                        conflicts.push(inst.url);
                        let patched_team: tabbycat_api::types::Team = attohttpc::patch(t.url)
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

                        info!("Clashed team {} against inst {}", name, inst.code.as_str());
                    } else {
                        info!(
                            "Team {} is already clashed against institution {}",
                            t.short_name,
                            inst.name.as_str()
                        )
                    }
                }
                (ClashKind::Adj(a), ClashKind::Adj(b)) => {
                    if !a.adjudicator_conflicts.contains(&b.url) {
                        let mut t = a.adjudicator_conflicts;
                        t.push(b.url);
                        let adj: tabbycat_api::types::Adjudicator = attohttpc::patch(a.url)
                            .header("Authorization", format!("Token {}", auth.api_key))
                            .json(&serde_json::json!({
                                "adjudicator_conflicts": t
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

                        info!("Clashed adj {} against adj {}", name, b.name);
                    } else {
                        info!("Adj {} is already clashed against adj {}", a.name, b.name)
                    }
                }
                (ClashKind::Adj(adj), ClashKind::Team(team))
                | (ClashKind::Team(team), ClashKind::Adj(adj)) => {
                    if !adj.team_conflicts.contains(&team.url) {
                        let mut t = adj.team_conflicts;
                        t.push(team.url);
                        let adj: tabbycat_api::types::Adjudicator = attohttpc::patch(adj.url)
                            .header("Authorization", format!("Token {}", auth.api_key))
                            .json(&serde_json::json!({
                                "team_conflicts": t
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
                        info!("Clashed adj {} against team {}", name, team.short_name);
                    } else {
                        info!(
                            "Adj {} is already clashed against team {}",
                            adj.name, team.short_name
                        );
                    }
                }
                (ClashKind::Team(_), ClashKind::Team(_)) => {
                    error!(
                        "You have tried to add a conflict between two \
                                 teams, which is not supported!"
                    );
                    exit(1)
                }
                (ClashKind::Inst(_), ClashKind::Inst(_)) => {
                    error!(
                        "You have tried to add a conflict between two \
                         institutions, which is not supported!"
                    );
                    exit(1)
                }
            }
        }
    }
}
