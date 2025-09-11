use std::{fs::File, io::BufReader};

use itertools::Itertools;
use tabbycat_api::types::RoundPairing;
use tracing::info;

use crate::{Auth, api_utils::get_rounds, dispatch_req::json_of_resp};

pub fn save_panels(round: &str, to: &str, auth: Auth) {
    let round = get_round(round, &auth);

    let pairings: Vec<tabbycat_api::types::RoundPairing> = json_of_resp(
        attohttpc::get(&round.links.pairing)
            .header("Authorization", format!("Token {}", auth.api_key))
            .send()
            .unwrap(),
    );

    std::fs::write(to, serde_json::to_string(&pairings).unwrap()).unwrap();

    info!("Successfully wrote current draw to `{}`.", to)
}

#[cfg(test)]
#[test]
fn test_deserialize() {
    serde_json::from_str::<tabbycat_api::types::DebateTeam>(
        r#"
        {
          "team": "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/teams/280811",
          "side": "og",
          "flags": []
        }
        "#,
    )
    .unwrap();

    serde_json::from_str::<Vec<tabbycat_api::types::RoundPairing>>(
        r#"
        [
          {
            "id": 472224,
            "url": "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/rounds/12/pairings/472224",
            "venue": "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/venues/129947",
            "teams": [
              {
                "team": "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/teams/280811",
                "side": "og",
                "flags": []
              },
              {
                "team": "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/teams/280828",
                "side": "oo",
                "flags": []
              },
              {
                "team": "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/teams/280964",
                "side": "cg",
                "flags": []
              },
              {
                "team": "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/teams/280813",
                "side": "co",
                "flags": []
              }
            ],
            "adjudicators": {
              "chair": "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/adjudicators/978116",
              "panellists": [
                "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/adjudicators/978150",
                "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/adjudicators/978127",
                "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/adjudicators/978133",
                "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/adjudicators/978163"
              ],
              "trainees": []
            },
            "barcode": null,
            "_links": {
              "ballots": "https://tokyoiv2025.calicotab.com/api/v1/tournaments/tokyoiv2025/rounds/12/pairings/472224/ballots"
            },
            "sides_confirmed": true
          }
        ]
        "#,
    ).unwrap();
}

pub fn restore_panels(round: &str, to: &str, auth: Auth) {
    let round = get_round(round, &auth);

    let old_draw: Vec<tabbycat_api::types::RoundPairing> =
        serde_json::from_reader(BufReader::new(File::open(to).unwrap())).unwrap();

    let live_pairings: Vec<tabbycat_api::types::RoundPairing> =
        attohttpc::get(&round.links.pairing)
            .header("Authorization", format!("Token {}", auth.api_key))
            .send()
            .unwrap()
            .json::<Vec<tabbycat_api::types::RoundPairing>>()
            .unwrap()
            .into_iter()
            .sorted_by_key(|k| k.room_rank.unwrap_or(i32::MAX))
            .collect();

    for (i, room) in old_draw
        .iter()
        .sorted_by_key(|r| r.room_rank.unwrap_or(i32::MAX))
        .enumerate()
        // If the number of rooms decreases, the panel which was previously
        // judging the lowest-ranked teams will be dropped (these judges should
        // then be re-allocated).
        .take(live_pairings.len())
    {
        let corresponding_room = &live_pairings[i];

        attohttpc::post(&corresponding_room.url)
            .header("Authorization", format!("Token {}", auth.api_key))
            .json(&RoundPairing {
                adjudicators: room.adjudicators.clone(),
                ..corresponding_room.clone()
            })
            .unwrap()
            .send()
            .unwrap();
    }

    info!("Restored previous panels.")
}

fn get_round(round: &str, auth: &Auth) -> tabbycat_api::types::Round {
    let rounds = get_rounds(auth);
    let round = rounds
        .iter()
        .find(|r| {
            r.abbreviation.as_str().to_ascii_lowercase() == round.to_ascii_lowercase()
                || r.name.as_str().to_ascii_lowercase() == round.to_ascii_lowercase()
        })
        .expect("the round you specified does not exist");
    round.clone()
}
