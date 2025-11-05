use std::{
    collections::{HashMap, HashSet},
    process::exit,
    sync::Arc,
};
use tokio::task::JoinSet;

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
    api_utils::{get_institutions, get_judges, get_rounds, get_teams},
    merge, open_csv_file,
    request_manager::RequestManager,
};

#[derive(Deserialize, Debug, Clone)]
pub struct InstitutionRow {
    pub region: Option<String>,
    // TODO: warn when this is >20 characters (Tabbycat currently applies
    // this restriction) to aid with debugging
    pub short_code: String,
    pub full_name: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RoomRow {
    #[serde(deserialize_with = "tags_deserialize", default = "Vec::new")]
    pub categories: Vec<String>,
    pub external_url: Option<String>,
    pub barcode: Option<String>,
    pub name: String,
    pub priority: i64,
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

pub async fn do_import(auth: Auth, import: Import) {
    tracing::info!(
        "Running import with these parameters: overwrite={}",
        import.overwrite
    );

    let institutions_csv = open_csv_file(import.institutions_csv.clone(), true);
    let teams_csv = open_csv_file(import.teams_csv.clone(), true);
    let judges_csv = open_csv_file(import.judges_csv.clone(), true);
    let clashes_csv = open_csv_file(import.clashes_csv.clone(), false);
    let rooms_csv = open_csv_file(import.rooms.clone(), true);

    let api_addr = format!("{}/api/v1", auth.tabbycat_url);

    let request_manager = RequestManager::new(&auth.api_key);

    let compute_speaker_categories = async {
        let speaker_categories: Vec<tabbycat_api::types::SpeakerCategory> = {
            let resp = request_manager
                .send_request(|| {
                    let base_url = format!(
                        "{api_addr}/tournaments/{}/speaker-categories",
                        auth.tournament_slug
                    );

                    request_manager.client.get(base_url).build().unwrap()
                })
                .await;

            resp.json().await.unwrap()
        };

        speaker_categories
    };

    let break_categories = async {
        let resp = request_manager
            .send_request(|| {
                let resource_loc = format!(
                    "{api_addr}/tournaments/{}/break-categories",
                    auth.tournament_slug
                );

                request_manager.client.get(resource_loc).build().unwrap()
            })
            .await;

        let break_categories: Vec<tabbycat_api::types::BreakCategory> = resp.json().await.unwrap();

        break_categories
    };

    let institutions = get_institutions(&auth, request_manager.clone());

    let speakers = async {
        let resp = request_manager
            .send_request(|| {
                let url = format!("{api_addr}/tournaments/{}/speakers", auth.tournament_slug);
                request_manager.client.get(url).build().unwrap()
            })
            .await;

        if !resp.status().is_success() {
            error!(
                "Failed to fetch speakers: status = {:?}, body = {}",
                resp.status(),
                resp.text().await.unwrap()
            );
            panic!("Failed to fetch speakers");
        }

        let speakers: Vec<tabbycat_api::types::Speaker> = resp.json().await.unwrap();
        speakers
    };

    let teams = get_teams(&auth, request_manager.clone());

    let rounds = get_rounds(&auth, request_manager.clone());

    let (speaker_categories, break_categories, mut institutions, mut speakers, mut teams, rounds) = tokio::join!(
        compute_speaker_categories,
        break_categories,
        institutions,
        speakers,
        teams,
        rounds
    );

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

    if import.overwrite {
        // todo: could track all objects which have a matching item in the
        // spreadsheet and then delete those which don't

        let _overwriting_span = span!(Level::INFO, "overwriting");

        let _delete_judges = {
            let mut join_set = JoinSet::new();

            for judge in judges.iter() {
                let request_manager = request_manager.clone();
                let judge_name = judge.name.clone();
                let judge_url = judge.url.clone();

                join_set.spawn(async move {
                    info!("Deleting judge {}", judge_name);
                    request_manager
                        .send_request(|| {
                            request_manager
                                .client
                                .delete(judge_url.clone())
                                .build()
                                .unwrap()
                        })
                        .await;
                });
            }

            while let Some(result) = join_set.join_next().await {
                if let Err(err) = result {
                    error!("Error occurred while deleting a judge: {:?}", err);
                    panic!("failed to delete judge");
                }
            }
        };

        let _delete_teams = {
            {
                let mut join_set = JoinSet::new();

                for team in &teams {
                    let team_url = team.url.clone();
                    let team_name = team.short_name.clone();

                    let manager = request_manager.clone();
                    join_set.spawn(async move {
                        manager
                            .send_request(|| {
                                info!("Deleting team {}", team_name);
                                let resp = manager.client.delete(team_url.clone()).build().unwrap();
                                resp
                            })
                            .await;
                    });
                }

                while let Some(result) = join_set.join_next().await {
                    if let Err(err) = result {
                        error!("Error occurred while deleting a team: {:?}", err);
                        panic!("failed to delete team");
                    }
                }
            }
        };

        let _delete_institutions = {
            let mut join_set = JoinSet::new();

            for institution in &institutions {
                let institution_name = institution.name.clone();
                let institution_url = institution.url.clone();
                let request_manager = request_manager.clone();

                join_set.spawn(async move {
                    info!("Deleting institution {}", institution_name.as_str());

                    let resp = request_manager
                        .send_request(|| {
                            request_manager
                                .client
                                .delete(institution_url.clone())
                                .build()
                                .unwrap()
                        })
                        .await;

                    if !resp.status().is_success() {
                        error!(
                            "Could not delete institution {}: {} {}",
                            institution_name.as_str(),
                            resp.status(),
                            resp.text().await.unwrap()
                        );
                        panic!("failed to delete!");
                    }
                });
            }

            while let Some(result) = join_set.join_next().await {
                if let Err(err) = result {
                    error!("Error occurred while deleting an institution: {:?}", err);
                    panic!("failed to delete institutions");
                }
            }
        };

        judges.clear();
        teams.clear();
        institutions.clear();
        speakers.clear();
    }

    let institutions = if let Some(mut institutions_csv) = institutions_csv {
        let headers = Arc::new(institutions_csv.headers().unwrap().clone());
        let institutions_span = span!(Level::INFO, "importing institutions");
        let _institutions_guard = institutions_span.enter();

        let institutions = Arc::new(tokio::sync::Mutex::new(institutions));

        // note: institutions need to be processed sequentially to avoid
        // running into Tabbycat bugs (!)
        for institution2import in institutions_csv.records() {
            let api_addr = api_addr.clone();
            let headers = headers.clone();
            let request_manager = request_manager.clone();
            let institutions = institutions.clone();
            let institution2import = institution2import.unwrap();

            let institution: InstitutionRow =
                institution2import.deserialize(Some(&headers)).unwrap();

            if !institutions.lock().await.iter().any(|cmp| {
                cmp.name.as_str() == institution.full_name
                    || cmp.code.as_str() == institution.short_code
            }) {
                let response = request_manager
                    .clone()
                    .send_request(|| {
                        request_manager
                            .client
                            .post(format!("{api_addr}/institutions"))
                            .json(&serde_json::json!({
                                "region": institution.region,
                                "name": institution.full_name,
                                "code": institution.short_code
                            }))
                            .build()
                            .unwrap()
                    })
                    .await;
                if !response.status().is_success() {
                    panic!("error: {}", response.text().await.unwrap());
                }
                let inst: tabbycat_api::types::PerTournamentInstitution =
                    response.json().await.unwrap();
                info!(
                    "Institution {} added to Tabbycat, id is {}",
                    inst.name.as_str(),
                    inst.id
                );
                institutions.clone().lock().await.push(inst);
            } else {
                info!(
                    "Institution {} already exists, not inserting",
                    institution.full_name
                );
            }
        }

        institutions.clone().lock().await.clone()
    } else {
        info!("No institutions were provided to import.");
        institutions
    };

    if let Some(mut rooms_csv) = rooms_csv {
        let rooms_span = span!(Level::INFO, "importing rooms");
        let _rooms_guard = rooms_span.enter();
        let headers = rooms_csv.headers().unwrap().clone();

        let mut categories = HashMap::new();

        tracing::info!("starting rooms import");

        for room2import in rooms_csv.records() {
            tracing::info!("adding room");

            let room2import = room2import.unwrap();
            let room2import: RoomRow = room2import.deserialize(Some(&headers)).unwrap();

            let res = request_manager
                .send_request(|| {
                    request_manager
                        .client
                        .post(format!(
                            "{}/tournaments/{}/venues",
                            api_addr, auth.tournament_slug
                        ))
                        .json(&json!({
                            "categories": [],
                            "name": room2import.name,
                            "priority": room2import.priority
                        }))
                        .build()
                        .unwrap()
                })
                .await;

            let room: tabbycat_api::types::Venue = res.json().await.unwrap();
            for cat in room2import.categories {
                categories
                    .entry(cat)
                    .and_modify(|cat: &mut Vec<_>| {
                        cat.push(room.url.clone());
                    })
                    .or_insert({
                        let mut v = Vec::new();
                        v.push(room.url.clone());
                        v
                    });
            }
        }

        for (key, values) in categories {
            let res = request_manager
                .send_request(|| {
                    request_manager
                        .client
                        .post(format!(
                            "{}/tournaments/{}/venue-categories",
                            api_addr, auth.tournament_slug
                        ))
                        .json(&json!({
                            "venues": values,
                            "name": key,
                            "display_in_venue_name": "P"
                        }))
                        .build()
                        .unwrap()
                })
                .await;

            if !res.status().is_success() {
                error!(
                    "Failed to create venue category '{}': status = {:?}, body = {}",
                    key,
                    res.status(),
                    res.text()
                        .await
                        .unwrap_or_else(|_| "Unable to fetch response body".to_string())
                );
                panic!("Failed to create venue category");
            }
        }
    };

    let mut judges = if let Some(mut judges_csv) = judges_csv {
        let headers = Arc::new(judges_csv.headers().unwrap().clone());
        let judges_span = span!(Level::INFO, "importing judges");
        let _judges_guard = judges_span.enter();

        let mut join_set = JoinSet::new();

        let judges = Arc::new(tokio::sync::Mutex::new(judges.clone()));
        let institutions = Arc::new(institutions.clone());
        let rounds = Arc::new(rounds);

        for judge2import in judges_csv.records() {
            let api_addr = api_addr.clone();
            let headers = headers.clone();
            let request_manager = request_manager.clone();
            let judges = judges.clone();
            let institutions = institutions.clone();
            let rounds = rounds.clone();
            let auth = auth.clone();
            let import = import.clone();

            join_set.spawn(async move {
                let judge2import = judge2import.unwrap();
                let judge2import: JudgeRow = judge2import.deserialize(Some(&headers)).unwrap();

                if !judges
                    .lock()
                    .await
                    .iter()
                    .any(|judge| judge.name == judge2import.name)
                {
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
                                || Some(api_inst.code.as_str().to_string())
                                    == judge2import.institution
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

                    let resp = request_manager
                        .send_request(|| {
                            request_manager
                                .client
                                .post(format!(
                                    "{api_addr}/tournaments/{}/adjudicators",
                                    auth.tournament_slug
                                ))
                                .json(&payload)
                                .build()
                                .unwrap()
                        })
                        .await;
                    if !resp.status().is_success() {
                        error!("error");
                        panic!("error {:?} {}", resp.status(), resp.text().await.unwrap());
                    }

                    let judge: tabbycat_api::types::Adjudicator = resp.json().await.unwrap();
                    info!("Created judge {} with id {}", judge.name, judge.id);
                    judges.lock().await.push(judge.clone());

                    // TODO: there should be a way to opt-out of setting this (or
                    // at least specify the default)
                    if import.set_availability {
                        let norm = judge2import
                            .availability
                            .iter()
                            .map(|availability| availability.to_ascii_lowercase())
                            .collect::<HashSet<_>>();
                        for api_round in rounds.iter() {
                            let (available, method, url) = if norm
                                .contains(&api_round.abbreviation.to_ascii_lowercase())
                                || norm.contains(&api_round.name.to_ascii_lowercase())
                            {
                                (
                                    "available",
                                    "PUT",
                                    format!(
                                        "{api_addr}/tournaments/{}/rounds/{}/availabilities",
                                        auth.tournament_slug, api_round.seq
                                    ),
                                )
                            } else {
                                (
                                    "unavailable",
                                    "POST",
                                    format!(
                                        "{api_addr}/tournaments/{}/rounds/{}/availabilities",
                                        auth.tournament_slug, api_round.seq
                                    ),
                                )
                            };

                            let resp = request_manager
                                .send_request(|| {
                                    let req = if method == "PUT" {
                                        request_manager.client.put(&url)
                                    } else {
                                        request_manager.client.post(&url)
                                    };
                                    req.json(&json!([judge.url])).build().unwrap()
                                })
                                .await;

                            if !resp.status().is_success() {
                                error!(
                                    "Failed to mark judge {} as {available} for round {}: {} {}",
                                    judge2import.name,
                                    api_round.name.as_str(),
                                    resp.status(),
                                    resp.text().await.unwrap()
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
                    }
                } else {
                    info!(
                        "Judge {} already exists, therefore not creating a record \
                        for this judge.",
                        judge2import.name
                    );
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(err) = result {
                error!("Error occurred while importing a judge: {:?}", err);
                panic!("Failed to import judge");
            }
        }

        let judges = judges.lock().await.clone();
        judges
    } else {
        info!("No judges were provided to import.");
        judges
    };

    let (mut teams, _, _, _) = if let Some(mut teams_csv) = teams_csv {
        let headers = Arc::new(teams_csv.headers().unwrap().clone());
        let teams_span = span!(Level::INFO, "importing teams");
        let _teams_guard = teams_span.enter();

        let mut join_set = JoinSet::new();

        let teams = Arc::new(tokio::sync::Mutex::new(teams.clone()));
        let speakers = Arc::new(tokio::sync::Mutex::new(speakers));
        let break_categories = Arc::new(tokio::sync::Mutex::new(break_categories));
        let speaker_categories = Arc::new(tokio::sync::Mutex::new(speaker_categories));
        let institutions = Arc::new(institutions.clone());

        for team2import in teams_csv.records() {
            let api_addr = api_addr.clone();
            let headers = headers.clone();
            let request_manager = request_manager.clone();
            let teams = teams.clone();
            let speakers = speakers.clone();
            let break_categories = break_categories.clone();
            let speaker_categories = speaker_categories.clone();
            let institutions = institutions.clone();
            let auth = auth.clone();
            let import = import.clone();

            join_set.spawn(async move {
                let team2import = team2import.unwrap();
                let team2import: TeamRow = team2import.deserialize(Some(&headers)).unwrap();

                let inst_of_team2_import = institutions.iter().find(|api_inst| {
                    Some(api_inst.name.as_str().to_lowercase())
                        == team2import.institution.as_ref().map(|t| t.to_lowercase())
                        || Some(api_inst.code.as_str().to_lowercase())
                            == team2import.institution.as_ref().map(|t| t.to_lowercase())
                });

                let teams_lock = teams.lock().await;
                let team_url = if let Some(team) = teams_lock.iter().find(|team| {
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
                    drop(teams_lock);
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
                        let mut break_categories_lock = break_categories.lock().await;
                        let category_and_optionally_url = team2import
                            .categories
                            .iter()
                            .map(|team2_import_category_name| {
                                assert!(!team2_import_category_name.is_empty());
                                (
                                    team2_import_category_name,
                                    break_categories_lock
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

                        let mut result = Vec::new();
                        for (name, api_category) in category_and_optionally_url {
                            match api_category {
                                Some(t) => result.push(t.url.clone()),
                                None => {
                                    let seq = break_categories_lock.len() + 1;
                                    let resp = request_manager
                                        .send_request(|| {
                                            request_manager
                                                .client
                                                .post(format!(
                                                    "{api_addr}/tournaments/{}/break-categories",
                                                    auth.tournament_slug
                                                ))
                                                .json(&serde_json::json!({
                                                    "name": name,
                                                    "slug": name.to_ascii_lowercase(),
                                                    "seq": seq,
                                                    "break_size": 4,
                                                    "is_general": false,
                                                    "priority": 1
                                                }))
                                                .build()
                                                .unwrap()
                                        })
                                        .await;

                                    if !resp.status().is_success() {
                                        panic!(
                                            "error when creating category {name}\n
                                            {:?} {}",
                                            resp.status(),
                                            resp.text().await.unwrap()
                                        );
                                    }

                                    let category: BreakCategory = resp.json().await.unwrap();
                                    result.push(category.url.clone());
                                    break_categories_lock.push(category);
                                }
                            }
                        }
                        result
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

                    let resp = request_manager
                        .send_request(|| {
                            request_manager
                                .client
                                .post(format!(
                                    "{api_addr}/tournaments/{}/teams",
                                    auth.tournament_slug
                                ))
                                .json(&payload)
                                .build()
                                .unwrap()
                        })
                        .await;
                    if !resp.status().is_success() {
                        panic!(
                            "error (team is {}) {:?} {} \n {:#?}",
                            team2import.full_name,
                            resp.status(),
                            resp.text().await.unwrap(),
                            teams.lock().await
                        );
                    }
                    let team: Team = resp.json().await.unwrap();
                    info!(
                        "Created team {} with id {} (institution: {:?})",
                        team.long_name, team.id, inst
                    );
                    let url = team.url.clone();
                    teams.lock().await.push(team.clone());
                    url
                };

                let team_span = span!(Level::INFO, "team", team_name = team2import.full_name);
                let _team_guard = team_span.enter();
                for speaker2import in team2import.speakers {
                    let speakers_lock = speakers.lock().await;
                    if !speakers_lock.iter().any(|speaker| {
                        speaker.name.trim() == speaker2import.name.trim()
                            || speaker
                                .url_key
                                .clone()
                                .map(|key| Some(key.as_str().to_string()) == speaker2import.url_key)
                                .unwrap_or(false)
                    }) {
                        drop(speakers_lock);
                        let speaker_category_urls = {
                            let mut speaker_categories_lock = speaker_categories.lock().await;
                            let mut ret = Vec::new();
                            for speaker2import_cat in speaker2import.categories {
                                let speaker2import_cat = speaker2import_cat.trim();
                                let category_from_tabbycat = speaker_categories_lock
                                    .iter()
                                    .find(|api_cat| {
                                        api_cat.slug.as_str().to_ascii_lowercase().trim()
                                            == speaker2import_cat.to_ascii_lowercase()
                                    })
                                    .cloned();

                                match category_from_tabbycat {
                                    Some(t) => ret.push(t.clone().url),
                                    None => {
                                        let seq = speaker_categories_lock.len() + 1;
                                        let resp = request_manager
                                            .send_request(|| {
                                                request_manager
                                                    .client
                                                    .post(format!(
                                                        "{api_addr}/tournaments/{}/speaker-categories",
                                                        auth.tournament_slug
                                                    ))
                                                    .json(&serde_json::json!({
                                                        "name": speaker2import_cat,
                                                        "slug": speaker2import_cat,
                                                        "seq": seq
                                                    }))
                                                    .build()
                                                    .unwrap()
                                            })
                                            .await;
                                        if !resp.status().is_success() {
                                            panic!(
                                                "Error: request failed, (note: \
                                                response body is {}) \n
                                                category: {speaker2import_cat} \n
                                                ",
                                                resp.text().await.unwrap()
                                            )
                                        }
                                        let category: SpeakerCategory = resp.json().await.unwrap();
                                        ret.push(category.url.clone());
                                        speaker_categories_lock.push(category);
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

                        let resp = request_manager
                            .send_request(|| {
                                request_manager
                                    .client
                                    .post(format!(
                                        "{api_addr}/tournaments/{}/speakers",
                                        auth.tournament_slug
                                    ))
                                    .json(&payload)
                                    .build()
                                    .unwrap()
                            })
                            .await;

                        // TODO: we can format the JSON error messages in a more
                        // human-friendly way
                        if !resp.status().is_success() {
                            panic!("error {:?} {}", resp.status(), resp.text().await.unwrap());
                        }

                        let speaker: tabbycat_api::types::Speaker = resp.json().await.unwrap();
                        info!("Created speaker {} with id {}", speaker.name, speaker.id);
                        speakers.lock().await.push(speaker.clone());
                        let mut teams_lock = teams.lock().await;
                        let team = teams_lock
                            .iter_mut()
                            .find(|team| team.url == speaker.team)
                            .unwrap();
                        let updated_team_resp = request_manager
                            .send_request(|| {
                                request_manager
                                    .client
                                    .get(team.url.clone())
                                    .build()
                                    .unwrap()
                            })
                            .await;
                        *team = updated_team_resp.json().await.unwrap();
                    } else {
                        info!(
                            "Speaker {} already exists, therefore not creating a \
                            record for this speaker.",
                            speaker2import.name
                        );
                    }
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(err) = result {
                error!("Error occurred while importing a team: {:?}", err);
                panic!("Failed to import team");
            }
        }

        let teams = teams.lock().await.clone();
        let speakers = speakers.lock().await.clone();
        let break_categories = break_categories.lock().await.clone();
        let speaker_categories = speaker_categories.lock().await.clone();
        (teams, speakers, break_categories, speaker_categories)
    } else {
        info!("No teams were provided to import.");
        (teams, speakers, break_categories, speaker_categories)
    };

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

            add_clash(&auth, &institutions, &mut teams, &mut judges, clash2import);
        }
    }
}

pub async fn add_clash_cmd(a: &str, b: &str, auth: &Auth) {
    let request_manager = RequestManager::new(&auth.api_key);

    let (mut teams, mut judges, mut institutions) = tokio::join!(
        get_teams(&auth, request_manager.clone()),
        get_judges(&auth, request_manager.clone()),
        get_institutions(&auth, request_manager.clone())
    );

    add_clash(
        auth,
        &mut institutions,
        &mut teams,
        &mut judges,
        Clash {
            object_1: a.into(),
            object_2: b.into(),
        },
    );
}

fn add_clash(
    auth: &Auth,
    institutions: &Vec<tabbycat_api::types::PerTournamentInstitution>,
    teams: &mut Vec<Team>,
    judges: &mut Vec<tabbycat_api::types::Adjudicator>,
    clash2import: Clash,
) {
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
            if team.long_name.eq_ignore_ascii_case(key) || team.short_name.eq_ignore_ascii_case(key)
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
                    team.clone().long_name
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

    let a =
        find_obj(&clash2import.object_1, &*teams, &*judges, institutions).unwrap_or_else(|| {
            panic!(
                "error: no judge, team name, or speaker found matching {}",
                clash2import.object_1
            )
        });
    let b =
        find_obj(&clash2import.object_2, &*teams, &*judges, institutions).unwrap_or_else(|| {
            panic!(
                "error: no judge, team name, or speaker found matching {}",
                clash2import.object_2
            )
        });

    match (a, b) {
        (ClashKind::Adj(a), ClashKind::Inst(inst)) | (ClashKind::Inst(inst), ClashKind::Adj(a)) => {
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
