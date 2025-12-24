use std::process::exit;

use crate::{
    Auth,
    api_utils::{get_feedback_questions, get_feedbacks, get_judges, get_teams},
    request_manager::RequestManager,
};

pub async fn export(auth: Auth, format: &str, output: &str) {
    match format {
        "csv" => {
            export_feedback_csv(auth, output).await;
        }
        "sqlite" => {
            export_feedback_db(auth, output).await;
        }
        _ => {
            tracing::error!("Invalid format `{}` expected either csv or sqlite", format);
            exit(1);
        }
    }
}

struct FeedbackData {
    feedbacks: Vec<tabbycat_api::types::Feedback>,
    judges: Vec<tabbycat_api::types::Adjudicator>,
    teams: Vec<tabbycat_api::types::Team>,
    feedback_questions: Vec<tabbycat_api::types::FeedbackQuestion>,
}

async fn fetch_feedback_data(auth: &Auth) -> FeedbackData {
    let manager = RequestManager::new(&auth.api_key);

    let feedbacks = get_feedbacks(auth, manager.clone()).await;
    let judges = get_judges(auth, manager.clone()).await;
    let teams = get_teams(auth, manager.clone()).await;
    let feedback_questions = get_feedback_questions(auth, manager.clone()).await;

    FeedbackData {
        feedbacks,
        judges,
        teams,
        feedback_questions,
    }
}

pub async fn export_feedback_csv(auth: Auth, output: &str) {
    let data = fetch_feedback_data(&auth).await;

    let mut writer = csv::Writer::from_path(output).unwrap();

    let mut header = vec![
        "feedback_id".to_string(),
        "source".to_string(),
        "source_kind".to_string(),
        "target".to_string(),
    ];

    for question in data.feedback_questions.iter() {
        header.push(format!("question_{}", question.reference.to_string()));
    }

    writer.write_record(&header).unwrap();

    for (feedback_idx, feedback) in data.feedbacks.iter().enumerate() {
        let mut record = vec![
            feedback_idx.to_string(),
            if feedback.source.contains("/team") {
                data.teams
                    .iter()
                    .find(|team| team.url == feedback.source)
                    .cloned()
                    .unwrap()
                    .long_name
            } else {
                data.judges
                    .iter()
                    .find(|team| team.url == feedback.source)
                    .cloned()
                    .unwrap()
                    .name
            },
            if feedback.source.contains("/team") {
                "team"
            } else {
                "judge"
            }
            .to_string(),
            data.judges
                .iter()
                .find(|judge| judge.url == feedback.adjudicator)
                .cloned()
                .unwrap()
                .name,
        ];

        for question in &data.feedback_questions {
            let qna = feedback.answers.iter().find(|a| a.question == question.url);

            if let Some(qna) = qna {
                record.push(match &qna.answer {
                    tabbycat_api::types::FeedbackAnswerAnswer::Variant0(x) => x.to_string(),
                    tabbycat_api::types::FeedbackAnswerAnswer::Variant1(x) => x.to_string(),
                    tabbycat_api::types::FeedbackAnswerAnswer::Variant2(x) => x.clone(),
                    tabbycat_api::types::FeedbackAnswerAnswer::Variant3(items) => {
                        format!("\"{}\"", items.join(","))
                    }
                });
            } else {
                record.push(String::new());
            }
        }

        writer.write_record(&record).unwrap();
    }

    writer.flush().unwrap();
    tracing::info!("Saved all feedback into CSV file {}", output);
}

pub async fn export_feedback_db(auth: Auth, output: &str) {
    let data = fetch_feedback_data(&auth).await;

    let database = rusqlite::Connection::open(output).unwrap();

    database
        .execute_batch(
            r#"
        create table if not exists judges (
            id integer not null primary key,
            url text not null unique,
            name text not null
        );

        create table if not exists teams (
            id integer not null primary key,
            url text not null unique,
            name text not null
        );

        create table if not exists questions (
            id integer not null primary key,
            url text not null unique,
            title text not null
        );

        create table if not exists feedbacks (
            id integer not null primary key,
            source text not null,
            -- always targets a judge
            target integer not null
        );

        create table if not exists feedback_answers (
            feedback_id integer not null references feedbacks (id),
            question text not null references questions (url),
            answer text not null
        );
        "#,
        )
        .unwrap();

    for judge in data.judges {
        database
            .execute(
                "insert into judges (url, name) values (?, ?);",
                (judge.url, judge.name),
            )
            .unwrap();
    }

    for team in data.teams {
        database
            .execute(
                "insert into teams (url, name) values (?, ?);",
                (team.url, team.long_name),
            )
            .unwrap();
    }

    for question in data.feedback_questions {
        database
            .execute(
                "insert into questions (url, title) values (?, ?);",
                (question.url, question.text.to_string()),
            )
            .unwrap();
    }

    for feedback in data.feedbacks {
        let id = database
            .query_one(
                "insert into feedbacks (source, target) values (?, ?) returning id;",
                (feedback.source, feedback.adjudicator),
                |row| row.get::<_, i64>(0),
            )
            .unwrap();

        for qna in feedback.answers {
            database
                .execute(
                    "insert into feedback_answers (feedback_id, question, answer) values (?, ?, ?)",
                    (
                        id,
                        qna.question,
                        serde_json::to_string(&qna.answer).unwrap(),
                    ),
                )
                .unwrap();
        }
    }

    tracing::info!("Saved all feedback into database {}", output);
}
