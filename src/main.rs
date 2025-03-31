use clap::Parser;
use serde_json::{Value, json};
use tabbycat_api::types::{BreakCategory, SpeakerCategory, Team};
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
}

mod types {
    use std::collections::HashMap;

    use itertools::Itertools;
    use serde::{Deserialize, Deserializer};

    #[derive(Deserialize, Debug, Clone)]
    pub struct InstitutionRow {
        pub region: Option<String>,
        // list of the rooms to which this institution should be constrained
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
            .map(|(_, map)| Speaker {
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
    }
}

fn main() {
    let args = Args::parse();

    let (institutions_csv, teams_csv, judges_csv) = {
        let inst = if let Some(institutions_csv) = args.institutions_csv {
            let institutions = std::fs::File::open(institutions_csv).unwrap();
            let rdr = csv::Reader::from_reader(institutions);
            Some(rdr)
        } else {
            None
        };
        let teams = if let Some(teams_csv) = args.teams_csv {
            let institutions = std::fs::File::open(teams_csv).unwrap();
            let rdr = csv::Reader::from_reader(institutions);
            Some(rdr)
        } else {
            None
        };
        let judges = if let Some(judges_csv) = args.judges_csv {
            let institutions = std::fs::File::open(judges_csv).unwrap();
            let rdr = csv::Reader::from_reader(institutions);
            Some(rdr)
        } else {
            None
        };

        (inst, teams, judges)
    };

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
                let inst: tabbycat_api::types::PerTournamentInstitution =
                    attohttpc::post(format!("{api_addr}/institutions"))
                        .header("Authorization", format!("Token {}", args.api_key))
                        .json(&serde_json::json!({
                            "region": institution.region,
                            "name": institution.full_name,
                            "code": institution.short_code
                        }))
                        .unwrap()
                        .send()
                        .unwrap()
                        .json()
                        .unwrap();
                println!(
                    "Institution {} added to Tabbycat, id is {}",
                    inst.name.as_str(),
                    inst.id
                );
                institutions.push(inst);
            } else {
                println!(
                    "Institution {} already exists, not inserting",
                    institution.full_name
                );
            }
        }
    } else {
        println!("No institutions were provided to import, therefore skipping.")
    }

    if let Some(mut judges_csv) = judges_csv {
        let headers = judges_csv.headers().unwrap().clone();

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

                let resp = attohttpc::post(format!(
                    "{api_addr}/tournaments/{}/adjudicators",
                    args.tournament
                ))
                .header("Authorization", format!("Token {}", args.api_key))
                .json(&serde_json::json!({
                    "name": judge2import.name,
                    "institution": inst_url,
                    "institution_conflicts": judge_inst_conflicts,
                    "email": judge2import.email,
                    "team_conflicts": [],
                    "adjudicator_conflicts": [],
                    "independent": judge2import.is_ia,
                    "adj_core": judge2import.is_ca
                }))
                .unwrap()
                .send()
                .unwrap();
                if !resp.is_success() {
                    panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
                }

                let judge: tabbycat_api::types::Adjudicator = resp.json().unwrap();
                println!("Created judge {} with id {}", judge.name, judge.id);
                judges.push(judge);
            } else {
                println!(
                    "Judge {} already exists, not inserting (NOTE: this means
                     that data will not be updated if you have changed it: to
                     do that you must delete the judge on the Tabbycat instance
                     and then run this command again).",
                    judge2import.name
                );
            }
        }
    }

    if let Some(mut teams_csv) = teams_csv {
        let headers = teams_csv.headers().unwrap().clone();

        for team2import in teams_csv.records() {
            let team2import = team2import.unwrap();
            let team2import: crate::types::TeamRow =
                team2import.deserialize(Some(&headers)).unwrap();

            if teams
                .iter()
                .find(|team| {
                    team.long_name == team2import.full_name
                        || Some(&team.short_name) == team2import.short_name.as_ref()
                        || team.code_name.clone().map(|t| t.as_str().to_string())
                            == team2import.code_name
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
                    assert!(inst.is_some());
                }

                let break_category_urls = {
                    let category_and_optionally_url = team2import
                        .categories
                        .iter()
                        .map(|team2_import_category_name| {
                            (
                                team2_import_category_name,
                                break_categories
                                    .iter()
                                    .find(|api_cat| {
                                        api_cat.slug.as_str() == team2_import_category_name
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
                                let category: BreakCategory = attohttpc::post(format!(
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
                                .unwrap()
                                .json()
                                .unwrap();
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
                    panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
                }
                let team: Team = resp.json().unwrap();
                println!("Created team {} with id {}", team.long_name, team.id);
                teams.push(team.clone());
            } else {
                println!(
                    "Team {} already exists, not inserting (NOTE: this means
                     that data will not be updated if you have changed it: to
                     do that you must delete the judge on the Tabbycat instance
                     and then run this command again).",
                    team2import.full_name
                );
            }

            for speaker2import in team2import.speakers {
                if speakers
                    .iter()
                    .find(|speaker| {
                        speaker.name == speaker2import.name
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
                            let category_from_tabbycat = speaker_categories
                                .iter()
                                .find(|api_cat| {
                                    api_cat.slug.as_str().to_ascii_lowercase()
                                        == speaker2import_cat.to_ascii_lowercase()
                                })
                                .cloned();

                            match category_from_tabbycat {
                                Some(t) => ret.push(t.clone()),
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
                                             response body is {})",
                                            resp.text_utf8().unwrap()
                                        )
                                    }
                                    let category: SpeakerCategory = resp.json().unwrap();
                                    speaker_categories.push(category.clone());
                                    ret.push(category);
                                }
                            }
                        }
                        ret
                    };

                    let mut payload = json!({
                        "name": speaker2import.name,
                        "team": teams.iter().find(|team| {team.long_name == team2import.full_name}).map(|t| t.url.clone()).unwrap(),
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

                    if !resp.is_success() {
                        panic!("error {:?} {}", resp.status(), resp.text_utf8().unwrap());
                    }

                    let speaker: tabbycat_api::types::Speaker = resp.json().unwrap();
                    println!("Created speaker {} with id {}", speaker.name, speaker.id);
                    speakers.push(speaker);
                } else {
                    println!(
                        "Speaker {} already exists, not inserting (NOTE: this means
                         that data will not be updated if you have changed it: to
                         do that you must delete the judge on the Tabbycat instance
                         and then run this command again).",
                        speaker2import.name
                    );
                }
            }
        }
    }
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
