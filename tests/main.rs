use std::collections::HashMap;
use std::env;
use std::process::Command;

use serde::Serialize;
use serde_json::json;

#[derive(Serialize, serde::Deserialize, Clone)]
pub struct Auth {
    tabbycat_url: String,
    tournament_slug: String,
    api_key: String,
}

#[test]
fn test_tabbycat_setup() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_target(false)
        .with_ansi(true)
        .init();

    Command::new("git")
        .args([
            "clone",
            "--depth=1",
            "https://github.com/tabbycatDebate/tabbycat",
        ])
        .status()
        .expect("Failed to clone repository");

    env::set_current_dir("tabbycat").expect("Failed to change directory");
    Command::new("docker")
        .args(["compose", "-f", "docker-compose.yml", "up", "--detach"])
        .status()
        .expect("Failed to start docker containers");

    loop {
        match attohttpc::get("http://localhost:8000").send() {
            Ok(res) if res.status().is_success() || res.status().is_redirection() => break,
            _ => (),
        }
    }

    if std::env::var("CI") == Err(std::env::VarError::NotPresent) {
        Command::new("docker")
            .args([
                "compose",
                "run",
                "web",
                "python",
                "tabbycat/manage.py",
                "reset_db",
                "--no-input",
            ])
            .status()
            .expect("Failed to reset database");

        tracing::trace!("Finished reset_db");

        Command::new("docker")
            .args([
                "compose",
                "run",
                "web",
                "python",
                "tabbycat/manage.py",
                "migrate",
                "--no-input",
            ])
            .status()
            .expect("Failed to reset database");
    }

    let mut env_vars = HashMap::new();
    env_vars.insert("DJANGO_SUPERUSER_PASSWORD".to_string(), "test".to_string());
    env_vars.insert(
        "DJANGO_SUPERUSER_EMAIL".to_string(),
        "email@example.com".to_string(),
    );
    env_vars.insert("DJANGO_SUPERUSER_USERNAME".to_string(), "user".to_string());

    Command::new("docker")
        .args([
            "compose",
            "run",
            "--env",
            &format!(
                "DJANGO_SUPERUSER_PASSWORD={}",
                env_vars["DJANGO_SUPERUSER_PASSWORD"]
            ),
            "--env",
            &format!(
                "DJANGO_SUPERUSER_EMAIL={}",
                env_vars["DJANGO_SUPERUSER_EMAIL"]
            ),
            "--env",
            &format!(
                "DJANGO_SUPERUSER_USERNAME={}",
                env_vars["DJANGO_SUPERUSER_USERNAME"]
            ),
            "web",
            "python",
            "tabbycat/manage.py",
            "createsuperuser",
            "--noinput",
        ])
        .status()
        .expect("Failed to create superuser");

    let output = Command::new("docker")
        .args([
            "compose",
            "run",
            "web",
            "python",
            "tabbycat/manage.py",
            "dumpdata",
            "authtoken.token",
        ])
        .output()
        .expect("Failed to get auth token");

    let stdout = String::from_utf8(output.stdout).unwrap();
    let json: serde_json::Value =
        serde_json::from_str(stdout.lines().last().unwrap().trim()).unwrap();
    let api_key = json[0]["pk"].as_str().unwrap();

    let home_dir = dirs::home_dir().expect("Could not determine home directory");
    let auth_path = home_dir.join(".tabbycat");

    std::fs::write(
        auth_path,
        toml::to_string(&Auth {
            tabbycat_url: "http://localhost:8000".to_string(),
            tournament_slug: "bp88team".to_string(),
            api_key: api_key.to_string(),
        })
        .unwrap(),
    )
    .unwrap();

    attohttpc::post("http://localhost:8000/api/v1/tournaments")
        .header("Authorization", format!("Token {api_key}"))
        .json(&json!({
            "name": "bp88team",
            "slug": "bp88team"
        }))
        .unwrap()
        .send()
        .unwrap();

    env::set_current_dir("..").expect("Failed to change back to original directory");
    Command::new("cargo")
        .args(["install", "--path", "."])
        .status()
        .expect("Failed to install package");

    let _do_initial_import = {
        Command::new("tabbycat")
            .args([
                "import",
                "--judges-csv",
                "data/judges.csv",
                "--teams-csv",
                "data/teams.csv",
                "--institutions-csv",
                "data/institutions.csv",
                "--clashes-csv",
                "data/clashes.csv",
            ])
            .status()
            .expect("Failed to import data");

        let teams: Vec<tabbycat_api::types::Team> =
            attohttpc::get("http://localhost:8000/api/v1/tournaments/bp88team/teams")
                .header("Authorization", format!("Token {api_key}"))
                .send()
                .unwrap()
                .json()
                .unwrap();
        assert_eq!(teams.len(), 88);

        let speakers: Vec<tabbycat_api::types::Speaker> =
            attohttpc::get("http://localhost:8000/api/v1/tournaments/bp88team/speakers")
                .header("Authorization", format!("Token {api_key}"))
                .send()
                .unwrap()
                .json()
                .unwrap();
        assert_eq!(speakers.len(), 88 * 2);

        let judges: Vec<tabbycat_api::types::Adjudicator> =
            attohttpc::get("http://localhost:8000/api/v1/tournaments/bp88team/adjudicators")
                .header("Authorization", format!("Token {api_key}"))
                .send()
                .unwrap()
                .json()
                .unwrap();
        assert_eq!(judges.len(), 80);
    };

    let _overwrite = {
        attohttpc::post("http://localhost:8000/api/v1/tournaments/bp88team/teams")
            .json(&json!({
                "short_reference": "ET",
                "reference": "Extra team"
            }))
            .unwrap()
            .header("Authorization", format!("Token {api_key}"))
            .send()
            .unwrap();

        let teams: Vec<tabbycat_api::types::Team> =
            attohttpc::get("http://localhost:8000/api/v1/tournaments/bp88team/teams")
                .header("Authorization", format!("Token {api_key}"))
                .send()
                .unwrap()
                .json()
                .unwrap();
        assert_eq!(teams.len(), 89);

        Command::new("tabbycat")
            .args([
                "import",
                "--judges-csv",
                "data/judges.csv",
                "--teams-csv",
                "data/teams.csv",
                "--institutions-csv",
                "data/institutions.csv",
                "--clashes-csv",
                "data/clashes.csv",
                "--overwrite",
            ])
            .status()
            .expect("Failed to import data");

        let teams: Vec<tabbycat_api::types::Team> =
            attohttpc::get("http://localhost:8000/api/v1/tournaments/bp88team/teams")
                .header("Authorization", format!("Token {api_key}"))
                .send()
                .unwrap()
                .json()
                .unwrap();
        assert_eq!(teams.len(), 88);

        let speakers: Vec<tabbycat_api::types::Speaker> =
            attohttpc::get("http://localhost:8000/api/v1/tournaments/bp88team/speakers")
                .header("Authorization", format!("Token {api_key}"))
                .send()
                .unwrap()
                .json()
                .unwrap();
        assert_eq!(speakers.len(), 88 * 2);

        let judges: Vec<tabbycat_api::types::Adjudicator> =
            attohttpc::get("http://localhost:8000/api/v1/tournaments/bp88team/adjudicators")
                .header("Authorization", format!("Token {api_key}"))
                .send()
                .unwrap()
                .json()
                .unwrap();
        assert_eq!(judges.len(), 80);
    };
}
