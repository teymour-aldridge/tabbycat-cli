use std::process::exit;

use clap::Parser;
use csv::Trim;
use serde_json::{Value, json};
use tabbycat_api::types::{BreakCategory, SpeakerCategory, Team};
use tracing::{Level, debug, error, info, span};
use types::InstitutionRow;

/// A program to import data into Tabbycat from a spreadsheet.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Path of the CSV file containing the institutions.
    #[arg(long)]
    institutions_csv: Option<String>,

    #[arg(long)]
    /// Path of the CSV file containing the judges.
    judges_csv: Option<String>,

    #[arg(long)]
    /// Path of the CSV file containing the teams.
    teams_csv: Option<String>,

    #[arg(long)]
    clashes_csv: Option<String>,

    #[arg(long)]
    /// The URL of the Tabbycat instance.
    tabbycat_url: String,

    #[arg(long)]
    /// The tournament slug.
    tournament: String,

    #[arg(long)]
    /// Whether teams should use be prefixed with the name of their institution
    /// by default.
    ///
    /// Note: if you specify a value in the `use_institutional_prefix` column
    /// (if this column is supplied) of the teams CSV file, those values will
    /// take precedence over this flag.
    #[clap(default_value_t = false)]
    use_institution_prefix: bool,

    #[arg(long)]
    /// An API key for the Tabbycat instance.
    api_key: String,

    #[arg(long)]
    /// Create "sensible" conflicts (currently, add conflicts between a speaker/
    /// judge and their own institution).
    #[clap(default_value_t = false)]
    make_sensible_conflicts: bool,

    #[arg(long)]
    #[clap(default_value_t = false)]
    clear_room_urls: bool,
}

mod types {
    use std::collections::HashMap;

    use itertools::Itertools;
    use serde::{Deserialize, Deserializer};

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

    // todo: team institution clashes
    #[derive(Deserialize, Debug, Clone)]
    pub struct TeamRow {
        pub full_name: String,
        /// May be supplied: if not we just truncate the full name
        pub short_name: Option<String>,
        #[serde(deserialize_with = "tags_deserialize", default = "Vec::new")]
        pub categories: Vec<String>,
        pub code_name: Option<String>,
        pub institution: Option<String>,
        pub seed: Option<u32>,
        pub emoji: Option<String>,
        pub use_institution_prefix: Option<bool>,
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
                if key.starts_with("speaker") {
                    // todo: good error messages
                    let mut iter = key["speaker".len()..].split('_');
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
                            .map(|t| t.to_ascii_lowercase() == "true")
                            .unwrap_or(false),
                        code_name: map.get("code_name").cloned(),
                        url_key: map.get("url_key").cloned(),
                        gender: map.get("gender").cloned(),
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
        pub institution_clashes: Option<Vec<String>>,
        // todo: add these later
        // pub team_conflicts: Option<Vec<String>>,
        // todo: add these later
        // pub judge_conflicts: Option<Vec<String>>,
        pub email: Option<String>,
        #[serde(default = "ret_false")]
        pub is_ca: bool,
        #[serde(default = "ret_false")]
        pub is_ia: bool,
        pub base_score: Option<f64>,
    }
}

fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "tabbycat_import=debug,none");
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_target(false)
        .with_ansi(true)
        .init();

    info!("Starting");

    let args = Args::parse();

    let institutions_csv = open_csv_file(args.institutions_csv.clone());
    let teams_csv = open_csv_file(args.teams_csv.clone());
    let judges_csv = open_csv_file(args.judges_csv.clone());
    let clashes_csv = open_csv_file(args.clashes_csv.clone());

    let api_addr = format!("{}/api/v1", args.tabbycat_url);

    let mut speaker_categories: Vec<tabbycat_api::types::SpeakerCategory> =
        attohttpc::get(format!(
            "{api_addr}/tournaments/{}/speaker-categories",
            args.tournament
        ))
        .header("Authorization", format!("Token {}", args.api_key))
        .send()
        .unwrap()
        .json()
        .unwrap();

    let mut break_categories: Vec<tabbycat_api::types::BreakCategory> = attohttpc::get(format!(
        "{api_addr}/tournaments/{}/break-categories",
        args.tournament
    ))
    .header("Authorization", format!("Token {}", args.api_key))
    .send()
    .unwrap()
    .json()
    .unwrap();

    let mut institutions: Vec<tabbycat_api::types::PerTournamentInstitution> =
        attohttpc::get(format!("{api_addr}/institutions"))
            .header("Authorization", format!("Token {}", args.api_key))
            .send()
            .unwrap()
            .json()
            .unwrap();

    let mut speakers: Vec<tabbycat_api::types::Speaker> = attohttpc::get(format!(
        "{api_addr}/tournaments/{}/speakers",
        args.tournament
    ))
    .header("Authorization", format!("Token {}", args.api_key))
    .send()
    .unwrap()
    .json()
    .unwrap();

    let mut teams: Vec<tabbycat_api::types::Team> =
        attohttpc::get(format!("{api_addr}/tournaments/{}/teams", args.tournament))
            .header("Authorization", format!("Token {}", args.api_key))
            .send()
            .unwrap()
            .json()
            .unwrap();

    let mut rooms: Vec<tabbycat_api::types::Venue> =
        attohttpc::get(format!("{api_addr}/tournaments/{}/venues", args.tournament))
            .header("Authorization", format!("Token {}", args.api_key))
            .send()
            .unwrap()
            .json()
            .unwrap();

    let resp = attohttpc::get(format!(
        "{api_addr}/tournaments/{}/adjudicators",
        args.tournament
    ))
    .header("Authorization", format!("Token {}", args.api_key))
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

            if institutions
                .iter()
                .find(|cmp| {
                    cmp.name.as_str() == institution.full_name
                        || cmp.code.as_str() == institution.short_code
                })
                .is_none()
            {
                let response = attohttpc::post(format!("{api_addr}/institutions"))
                    .header("Authorization", format!("Token {}", args.api_key))
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
            let judge2import: crate::types::JudgeRow =
                judge2import.deserialize(Some(&headers)).unwrap();

            if judges
                .iter()
                .find(|judge| judge.name == judge2import.name)
                .is_none()
            {
                let judge_inst_conflicts = institutions
                    .iter()
                    .filter(|inst_from_api| {
                        judge2import
                            .institution_clashes
                            .as_ref()
                            .map(|clashes| {
                                clashes.iter().any(|inst_judge_clashes| {
                                    inst_from_api.name.as_str() == inst_judge_clashes
                                        || inst_from_api.code.as_str() == inst_judge_clashes
                                })
                            })
                            .unwrap_or(false)
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
                    args.tournament
                ))
                .header("Authorization", format!("Token {}", args.api_key))
                .json(&payload)
                .unwrap()
                .send()
                .unwrap();
                if !resp.is_success() {
                    panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
                }

                let judge: tabbycat_api::types::Adjudicator = resp.json().unwrap();
                info!("Created judge {} with id {}", judge.name, judge.id);
                judges.push(judge);
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
            let team2import: crate::types::TeamRow =
                team2import.deserialize(Some(&headers)).unwrap();

            if teams
                .iter()
                .find(|team| {
                    team.long_name == team2import.full_name.trim()
                        || Some(team.short_name.as_str())
                            == team2import.short_name.as_ref().map(|t| t.trim())
                        || team.code_name.clone().map(|t| t.as_str().to_string())
                            == team2import.code_name.as_ref().map(|t| t.trim().to_string())
                })
                .is_none()
            {
                let inst = institutions
                    .iter()
                    .find(|api_inst| {
                        Some(api_inst.name.as_str().to_string()) == team2import.institution
                            || Some(api_inst.code.as_str().to_string()) == team2import.institution
                    })
                    .map(|t| t.url.clone());

                if team2import.institution.is_some() {
                    if !inst.is_some() {
                        error!(
                            "Team {} belongs to institution {:?}, however, no \
                            corresponding institution was defined in {}.",
                            team2import.full_name,
                            team2import.institution.unwrap(),
                            args.institutions_csv.as_ref().unwrap()
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
                                        api_cat.slug.as_str() == team2_import_category_name.trim()
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
                                    args.tournament
                                ))
                                .header("Authorization", format!("Token {}", args.api_key))
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
                            if let Some(val) = team2import.use_institution_prefix {
                                val
                            } else {
                                args.use_institution_prefix
                            },
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

                let resp =
                    attohttpc::post(format!("{api_addr}/tournaments/{}/teams", args.tournament))
                        .header("Authorization", format!("Token {}", args.api_key))
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
                info!("Created team {} with id {}", team.long_name, team.id);
                teams.push(team.clone());
            } else {
                info!(
                    "Team {} already exists, therefore not creating a record \
                    for this team.",
                    team2import.full_name
                );
            }

            let team_span = span!(Level::INFO, "team", team_name = team2import.full_name);
            let _team_guard = team_span.enter();
            for speaker2import in team2import.speakers {
                if speakers
                    .iter()
                    .find(|speaker| {
                        speaker.name.trim() == speaker2import.name.trim()
                            || speaker
                                .url_key
                                .clone()
                                .map(|key| Some(key.as_str().to_string()) == speaker2import.url_key)
                                .unwrap_or(false)
                    })
                    .is_none()
                {
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
                                        args.tournament
                                    ))
                                    .header("Authorization", format!("Token {}", args.api_key))
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
                        "team": teams
                            .iter()
                            .find(|team| team.long_name == team2import.full_name.trim())
                            .map(|t| t.url.clone())
                            .expect(&format!("expected to find matching team for speaker ({}) \n {:#?}", team2import.full_name, teams)),
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
                        args.tournament
                    ))
                    .header("Authorization", format!("Token {}", args.api_key))
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
                    speakers.push(speaker);
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
            let clash2import: crate::types::Clash = clash2import.deserialize(None).unwrap();

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
                    if inst.name.as_str().to_ascii_lowercase() == key.to_ascii_lowercase()
                        || inst.code.as_str().to_ascii_lowercase() == key.to_ascii_lowercase()
                    {
                        return Some(ClashKind::Inst(inst.clone()));
                    }
                }

                for judge in judges {
                    if judge.name.to_ascii_lowercase() == key.to_ascii_lowercase() {
                        debug!("Resolved {key} as judge {} due to name match.", judge.name);

                        return Some(ClashKind::Adj(judge.clone()));
                    }
                }

                for team in teams {
                    if team.long_name.to_ascii_lowercase() == key.to_ascii_lowercase()
                        || team.short_name.to_ascii_lowercase() == key.to_ascii_lowercase()
                    {
                        debug!(
                            "Resolved {key} as team {} due to name match.",
                            team.long_name
                        );
                        return Some(ClashKind::Team(team.clone()));
                    }

                    if team.speakers.iter().any(|speaker| {
                        speaker.name.to_ascii_lowercase() == key.to_ascii_lowercase()
                    }) {
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

            if clash2import.object_1.to_ascii_lowercase()
                == clash2import.object_2.to_ascii_lowercase()
            {
                error!(
                    "You have attempted to clash someone against themself: {} and {}",
                    clash2import.object_1, clash2import.object_2
                );
            }

            let a =
                find_obj(&clash2import.object_1, &teams, &judges, &institutions).expect(&format!(
                    "error: no judge, team name, or speaker found matching {}",
                    clash2import.object_1
                ));
            let b =
                find_obj(&clash2import.object_2, &teams, &judges, &institutions).expect(&format!(
                    "error: no judge, team name, or speaker found matching {}",
                    clash2import.object_2
                ));

            match (a, b) {
                (ClashKind::Adj(a), ClashKind::Inst(inst))
                | (ClashKind::Inst(inst), ClashKind::Adj(a)) => {
                    if !a.institution_conflicts.contains(&inst.url) {
                        let mut t = a.institution_conflicts;
                        t.push(inst.url);
                        let resp = attohttpc::patch(a.url)
                            .header("Authorization", format!("Token {}", args.api_key))
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
                            .header("Authorization", format!("Token {}", args.api_key))
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
                            .header("Authorization", format!("Token {}", args.api_key))
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
                            .header("Authorization", format!("Token {}", args.api_key))
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

    if args.make_sensible_conflicts {
        for team in teams.clone() {
            let adding_team_conflict =
                span!(Level::INFO, "sensible_conflict", team = team.long_name);
            let _adding_team_guard = adding_team_conflict.enter();

            if let Some(inst) = team.institution
                && !team.institution_conflicts.contains(&inst)
            {
                let mut conflicts = team.institution_conflicts.clone();
                conflicts.push(inst);
                let patched_team: tabbycat_api::types::Team = attohttpc::patch(team.url)
                    .header("Authorization", format!("Token {}", args.api_key))
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

        for judge in judges.clone() {
            let adding_judge_conflict = span!(Level::INFO, "sensible_conflict", judge = judge.name);
            let _adding_judge_guard = adding_judge_conflict.enter();

            if let Some(inst) = judge.institution
                && !judge.institution_conflicts.contains(&inst)
            {
                let mut t = judge.team_conflicts;
                t.push(inst);
                let adj: tabbycat_api::types::Adjudicator = attohttpc::patch(judge.url)
                    .header("Authorization", format!("Token {}", args.api_key))
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

    if args.clear_room_urls {
        for (i, room) in rooms.clone().into_iter().enumerate() {
            let response = attohttpc::patch(room.url.clone())
                .header("Authorization", format!("Token {}", args.api_key))
                .json(&json!({
                    "external_url": ""
                }))
                .unwrap()
                .send()
                .unwrap();

            if !response.is_success() {
                error!(
                    "Failed to clear room URL for room {}: {} {}",
                    room.id,
                    response.status(),
                    response.text_utf8().unwrap()
                );
                panic!("Failed to clear room URL");
            }

            let room: tabbycat_api::types::Venue = response.json().unwrap();

            tracing::info!("Cleared room {} URL", room.id);

            rooms[i] = room;
        }
    }
}

fn open_csv_file(file_path: Option<String>) -> Option<csv::Reader<std::fs::File>> {
    file_path.map(|path| {
        let file = std::fs::File::open(path).unwrap();
        csv::ReaderBuilder::new().trim(Trim::All).from_reader(file)
    })
}

fn merge(a: &mut Value, b: &Value) {
    match (a, b) {
        (Value::Object(a), Value::Object(b)) => {
            for (k, v) in b {
                merge(a.entry(k.clone()).or_insert(Value::Null), v);
            }
        }
        (a, b) => *a = b.clone(),
    }
}
