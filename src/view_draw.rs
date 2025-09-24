use std::process::exit;

use comfy_table::{Cell, Table, modifiers::UTF8_ROUND_CORNERS, presets::UTF8_FULL};

use crate::{
    Auth,
    api_utils::{get_judges, get_round, get_teams},
    dispatch_req::json_of_resp,
};

pub fn view_draw(round: &String, auth: Auth) {
    let round = get_round(round, &auth);

    let teams_in_debate: tabbycat_api::types::Preference = json_of_resp(
        attohttpc::get(format!(
            "{}/api/v1/tournaments/{}/preferences/{}",
            auth.tabbycat_url, auth.tournament_slug, "debate_rules__teams_in_debate"
        ))
        .header("Authorization", format!("Token {}", auth.api_key))
        .send()
        .unwrap(),
    );
    let teams_in_debate = teams_in_debate.value.as_i64().unwrap();

    let pairings: Vec<tabbycat_api::types::RoundPairing> = json_of_resp(
        attohttpc::get(&round.links.pairing)
            .header("Authorization", format!("Token {}", auth.api_key))
            .send()
            .unwrap(),
    );

    let teams = get_teams(&auth);

    let name_of_team = |url: &str| -> String {
        teams
            .iter()
            .find(|team| team.url == url)
            .unwrap()
            .short_name
            .clone()
    };

    let judges = get_judges(&auth);

    let name_of_judge = |url: &str| -> String {
        judges
            .iter()
            .find(|team| team.url == url)
            .unwrap()
            .name
            .clone()
    };

    if pairings.is_empty() {
        println!("No draw for this round");

        return;
    }

    let headers = {
        let mut headers = Vec::new();
        headers.push("Nb");
        if teams_in_debate == 2 {
            headers.push("Prop");
            headers.push("Opp");
        } else if teams_in_debate == 4 {
            headers.push("OG");
            headers.push("OO");
            headers.push("CG");
            headers.push("CO");
        } else {
            println!("Error: bad number of teams (should be 2 or 4, not {teams_in_debate})!");
            exit(1);
        }
        headers.push("Panel");
        headers
    };

    let mut table = Table::new();

    table
        .load_preset(UTF8_FULL)
        .set_content_arrangement(comfy_table::ContentArrangement::Dynamic)
        .apply_modifier(UTF8_ROUND_CORNERS)
        .set_header(headers)
        .set_width(80);

    for pairing in pairings {
        let mut cells = Vec::new();

        cells.push(if matches!(pairing.sides_confirmed, Some(false) | None) {
            Cell::new("Sides not confirmed!".to_string()).bg(comfy_table::Color::Yellow)
        } else {
            Cell::new(String::new())
        });

        let exists_by = pairing.teams.iter().any(|team| {
            matches!(
                team.side,
                Some(tabbycat_api::types::DebateTeamSide::Variant1(
                    tabbycat_api::types::DebateTeamSideVariant1::Bye
                ))
            )
        });

        if exists_by {
            cells.push(Cell::new((name_of_team)(&pairing.teams[0].team)))
        } else {
            for _ in 0..teams_in_debate {
                cells.push(Cell::new(String::new()));
            }

            for team in pairing.teams {
                match team.side {
                    Some(tabbycat_api::types::DebateTeamSide::Variant1(side)) => {
                        cells[1 + match side {
                            tabbycat_api::types::DebateTeamSideVariant1::Aff => 0,
                            tabbycat_api::types::DebateTeamSideVariant1::Neg => 1,
                            tabbycat_api::types::DebateTeamSideVariant1::Cg => 2,
                            tabbycat_api::types::DebateTeamSideVariant1::Co => 3,
                            tabbycat_api::types::DebateTeamSideVariant1::Bye => unreachable!(),
                        }] = Cell::new((name_of_team)(&team.team));
                    }
                    _ => unreachable!(),
                }
            }
        }

        let mut judge_cell_contents = String::new();
        if let Some(judges) = pairing.adjudicators {
            let mut prev = false;
            if let Some(chair) = judges.chair {
                let judge = (name_of_judge)(&chair);
                judge_cell_contents += &format!("{judge} (c)");
                prev = true;
            }
            for panelist in judges.panellists {
                if prev {
                    judge_cell_contents += "\n";
                }
                judge_cell_contents += &format!("{}", (name_of_judge)(&panelist));
            }
            for trainee in judges.trainees {
                if prev {
                    judge_cell_contents += "\n";
                }
                judge_cell_contents += &format!("{} (t)", (name_of_judge)(&trainee));
            }
        }
        cells.push(Cell::new(judge_cell_contents));

        table.add_row(cells);
    }

    println!("{table}");
}
