pub mod api_utils;
pub mod break_eligibility;
pub mod clear_rooms;
pub mod dispatch_req;
pub mod edit_draw;
pub mod import;
pub mod request_manager;
pub mod save_panels;
pub mod sensible;
pub mod view_draw;

use std::process::exit;

use clap::{Parser, Subcommand};
use csv::Trim;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, info};
use url::Url;

use crate::{
    break_eligibility::do_compute_break_eligibility,
    clear_rooms::do_clear_room_urls,
    import::do_import,
    request_manager::RequestManager,
    save_panels::{restore_panels, save_panels},
    sensible::do_make_sensible_conflicts,
    view_draw::view_draw,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand, Clone)]
pub enum Command {
    /// Set the current tournament. After running this, you will be prompted for
    /// the Tabbycat instance's URL, the tournament slug and an API key.
    Set,
    /// Import teams from a spreadsheet (CSV file).
    Import(Import),
    /// Create missing conflicts that Tabbycat often doesn't add.
    MakeSensibleConflicts,
    /// Remove URLs from all rooms.
    ClearRoomUrls,
    /// Compute break eligibility (currently the only supported format is
    /// "wsdc").
    ///
    /// The available presets are
    /// - wsdc: this will set a team as eligible to break in a given category,
    ///   provided that n-1 speakers (where n = number of speakers on a
    ///   team) are break eligible. The esl category is special-cased,
    ///   and efl speakers are also counted when determining eligibility
    ///   in this category.
    ComputeBreakEligibility {
        format: String,
    },
    SaveAllocs {
        to: String,
        round: String,
    },
    RestoreAllocs {
        to: String,
        round: String,
    },
    ViewDraw {
        round: String,
    },
    /// Swap two entities (either two teams, or two judges) on the draw.
    DrawSwap {
        round: String,
        a: String,
        b: String,
    },
    /// Add a judge to the draw for a given round.
    AddJudge {
        round: String,
        judge: String,
        room_id: String,
        role: String,
    },
    RemoveJudge {
        round: String,
        judge: String,
    },
    Clash {
        a: String,
        b: String,
    },
}

#[derive(Debug, Parser, Clone)]
pub struct Import {
    /// Path of the CSV file containing the institutions.
    #[arg(long, alias = "institutions")]
    institutions_csv: Option<String>,

    #[arg(long, alias = "judges")]
    /// Path of the CSV file containing the judges.
    judges_csv: Option<String>,

    #[arg(long, alias = "teams")]
    /// Path of the CSV file containing the teams.
    teams_csv: Option<String>,

    #[arg(long, alias = "clashes")]
    clashes_csv: Option<String>,

    #[arg(long, alias = "rooms_csv")]
    rooms: Option<String>,

    #[arg(long)]
    /// Whether teams should use be prefixed with the name of their institution
    /// by default.
    ///
    /// Note: if you specify a value in the `use_institutional_prefix` column
    /// (if this column is supplied) of the teams CSV file, those values will
    /// take precedence over this flag.
    #[clap(default_value_t = false)]
    use_institution_prefix: bool,
    /// Whether existing data should be overwitten. This is UNSAFE if you have
    /// already sent private URLs (the old private URLs will be invalid for the
    /// new team objects) or imported institutions/teams/speakers/judges from
    /// elsewhere.
    #[arg(long)]
    #[clap(default_value_t = false)]
    overwrite: bool,
    #[arg(long)]
    #[clap(default_value_t = false)]
    set_availability: bool,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Auth {
    tabbycat_url: String,
    tournament_slug: String,
    api_key: String,
}

fn load_credentials() -> Auth {
    use dirs;
    use std::fs;
    use toml;

    let home_dir = dirs::home_dir().expect("Could not determine home directory");
    let auth_path = home_dir.join(".tabbycat");

    let auth_toml = match fs::read_to_string(&auth_path) {
        Ok(t) => t,
        Err(_) => {
            error!("Please run `tabbycat set` and provide your tournament's details first.");
            exit(1)
        }
    };

    match toml::from_str(&auth_toml) {
        Ok(t) => t,
        Err(_) => {
            error!(
                "Your ~/.tabbycat file is malformed (you may need to run `tabbycat set` again to fix this)."
            );
            exit(1)
        }
    }
}

#[tokio::main]
async fn main() {
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    if std::env::var("RUST_LOG").is_err() {
        unsafe {
            std::env::set_var("RUST_LOG", "tabbycat=debug,none");
        }
    }

    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_timer(tracing_subscriber::fmt::time::uptime())
        .with_target(false)
        .with_ansi(true)
        .init();

    let args = Args::parse();

    match args.command {
        Command::Set => {
            use rpassword::read_password;
            use std::io::{self, Write};

            let tabbycat_url = loop {
                print!("Enter Tabbycat URL (e.g. https://wudc2025.calicotab.com): ");
                io::stdout().flush().unwrap();
                let mut tabbycat_url = String::new();
                io::stdin().read_line(&mut tabbycat_url).unwrap();
                let tabbycat_url = tabbycat_url.trim().to_string();
                if let Ok(url) = tabbycat_url.parse::<Url>() {
                    break url.as_str().trim_end_matches('/').to_string();
                } else {
                    error!("Invalid Tabbycat URL provided!");
                }
            };

            print!("Enter tournament slug: ");
            io::stdout().flush().unwrap();
            let mut tournament = String::new();
            io::stdin().read_line(&mut tournament).unwrap();
            let tournament = tournament.trim().to_string();

            print!("Enter API key: ");
            io::stdout().flush().unwrap();
            let api_key = read_password().unwrap();

            if api_key.chars().any(char::is_whitespace) {
                panic!("Your API key should not contain spaces.");
            }

            let auth = Auth {
                tabbycat_url,
                tournament_slug: tournament,
                api_key,
            };

            let home_dir = dirs::home_dir().expect("Could not determine home directory");
            let auth_path = home_dir.join(".tabbycat");

            let auth_json = toml::to_string_pretty(&auth).expect("Failed to serialize Auth");
            std::fs::write(&auth_path, auth_json).expect("Failed to write Auth to ~/.tabbycat");

            info!("Tabbycat credentials saved to {}", auth_path.display());
        }
        Command::Import(import) => {
            let auth = load_credentials();
            do_import(auth, import).await;
        }
        Command::MakeSensibleConflicts => {
            let auth = load_credentials();
            do_make_sensible_conflicts(auth);
        }
        Command::ClearRoomUrls => {
            let auth = load_credentials();
            do_clear_room_urls(auth);
        }
        Command::ComputeBreakEligibility { format } => {
            let auth = load_credentials();
            do_compute_break_eligibility(auth, format);
        }
        Command::SaveAllocs { to, round } => {
            let auth = load_credentials();
            save_panels(&round, &to, auth).await;
        }
        Command::RestoreAllocs { to, round } => {
            let auth = load_credentials();
            restore_panels(&round, &to, auth).await;
        }
        Command::ViewDraw { round } => {
            let auth = load_credentials();

            view_draw(&round, auth).await;
        }
        Command::DrawSwap { round, a, b } => {
            let auth = load_credentials();

            edit_draw::swap(&round, &a, &b, auth).await;
        }
        Command::AddJudge {
            round,
            room_id,
            judge,
            role,
        } => {
            let auth = load_credentials();

            edit_draw::alloc(&round, &room_id, &judge, &role, auth).await;
        }
        Command::RemoveJudge { round, judge } => {
            let auth = load_credentials();

            edit_draw::remove(&round, &judge, auth).await;
        }
        Command::Clash { a, b } => {
            let auth = load_credentials();
            import::add_clash_cmd(&a, &b, &auth, RequestManager::new(&auth.api_key)).await
        }
    }
}

fn open_csv_file(file_path: Option<String>, headers: bool) -> Option<csv::Reader<std::fs::File>> {
    file_path.map(|path| {
        let file = std::fs::File::open(path).unwrap();
        csv::ReaderBuilder::new()
            .has_headers(headers)
            .trim(Trim::All)
            .from_reader(file)
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
