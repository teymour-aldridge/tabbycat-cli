use crate::{
    Auth,
    api_utils::{get_feedback_questions, get_feedbacks, get_judges, get_teams},
    request_manager::RequestManager,
};

pub async fn export_feedback_db(auth: Auth, output: &str) {
    let manager = RequestManager::new(&auth.api_key);

    let feedbacks = get_feedbacks(&auth, manager.clone()).await;
    let judges = get_judges(&auth, manager.clone()).await;
    let teams = get_teams(&auth, manager.clone()).await;
    let feedback_questions = get_feedback_questions(&auth, manager.clone()).await;

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

    for judge in judges {
        database
            .execute(
                "insert into judges (url, name) values (?, ?);",
                (judge.url, judge.name),
            )
            .unwrap();
    }

    for team in teams {
        database
            .execute(
                "insert into teams (url, name) values (?, ?);",
                (team.url, team.long_name),
            )
            .unwrap();
    }

    for question in feedback_questions {
        database
            .execute(
                "insert into questions (url, title) values (?, ?);",
                (question.url, question.text.to_string()),
            )
            .unwrap();
    }

    for feedback in feedbacks {
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
