use diesel::prelude::*;
use diesel::sql_types::{BigInt, Integer, Nullable, Text, Timestamptz};
use diesel::PgConnection;

use crate::models::{Job, DlqJob, ArchivedJob, JobView, QueueStatusCount, TableStats};
use crate::schema::{job_queue, job_queue_dlq, job_queue_archive};

pub fn set_statement_timeout(conn: &mut PgConnection) {
    diesel::sql_query("SET statement_timeout = '5s'")
        .execute(conn)
        .ok();
}

pub fn get_queue_status_counts(conn: &mut PgConnection) -> Vec<QueueStatusCount> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        queue: String,
        #[diesel(sql_type = Text)]
        status: String,
        #[diesel(sql_type = BigInt)]
        count: i64,
    }

    let rows = diesel::sql_query(
        "SELECT queue, status, COUNT(*)::bigint AS count FROM job_queue GROUP BY queue, status ORDER BY queue, status",
    )
    .load::<Row>(conn)
    .unwrap_or_default();

    rows.into_iter()
        .map(|r| QueueStatusCount {
            queue: r.queue,
            status: r.status,
            count: r.count,
        })
        .collect()
}

pub fn get_dlq_counts(conn: &mut PgConnection) -> Vec<QueueStatusCount> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        queue: String,
        #[diesel(sql_type = Text)]
        status: String,
        #[diesel(sql_type = BigInt)]
        count: i64,
    }

    let rows = diesel::sql_query(
        "SELECT queue, status, COUNT(*)::bigint AS count FROM job_queue_dlq GROUP BY queue, status ORDER BY queue, status",
    )
    .load::<Row>(conn)
    .unwrap_or_default();

    rows.into_iter()
        .map(|r| QueueStatusCount {
            queue: r.queue,
            status: r.status,
            count: r.count,
        })
        .collect()
}

pub fn get_archive_counts(conn: &mut PgConnection) -> Vec<QueueStatusCount> {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = Text)]
        queue: String,
        #[diesel(sql_type = Text)]
        status: String,
        #[diesel(sql_type = BigInt)]
        count: i64,
    }

    let rows = diesel::sql_query(
        "SELECT queue, status, COUNT(*)::bigint AS count FROM job_queue_archive GROUP BY queue, status ORDER BY queue, status",
    )
    .load::<Row>(conn)
    .unwrap_or_default();

    rows.into_iter()
        .map(|r| QueueStatusCount {
            queue: r.queue,
            status: r.status,
            count: r.count,
        })
        .collect()
}

pub fn get_table_stats(conn: &mut PgConnection) -> TableStats {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = BigInt)]
        dead_tuples: i64,
        #[diesel(sql_type = BigInt)]
        live_tuples: i64,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        last_vacuum: Option<chrono::NaiveDateTime>,
        #[diesel(sql_type = Nullable<Timestamptz>)]
        last_autovacuum: Option<chrono::NaiveDateTime>,
        #[diesel(sql_type = BigInt)]
        total_inserts: i64,
        #[diesel(sql_type = BigInt)]
        total_updates: i64,
        #[diesel(sql_type = BigInt)]
        total_deletes: i64,
    }

    diesel::sql_query(
        "SELECT
            n_dead_tup AS dead_tuples,
            n_live_tup AS live_tuples,
            last_vacuum,
            last_autovacuum,
            n_tup_ins AS total_inserts,
            n_tup_upd AS total_updates,
            n_tup_del AS total_deletes
         FROM pg_stat_user_tables
         WHERE relname = 'job_queue'",
    )
    .get_result::<Row>(conn)
    .map(|r| TableStats {
        dead_tuples: r.dead_tuples,
        live_tuples: r.live_tuples,
        last_vacuum: r.last_vacuum,
        last_autovacuum: r.last_autovacuum,
        total_inserts: r.total_inserts,
        total_updates: r.total_updates,
        total_deletes: r.total_deletes,
    })
    .unwrap_or_default()
}

pub fn get_distinct_queues(conn: &mut PgConnection) -> Vec<String> {
    let q: Vec<String> = job_queue::table
        .select(job_queue::queue)
        .distinct()
        .load(conn)
        .unwrap_or_default();
    let dlq: Vec<String> = job_queue_dlq::table
        .select(job_queue_dlq::queue)
        .distinct()
        .load(conn)
        .unwrap_or_default();
    let archive: Vec<String> = job_queue_archive::table
        .select(job_queue_archive::queue)
        .distinct()
        .load(conn)
        .unwrap_or_default();
    let mut all: Vec<String> = q
        .into_iter()
        .chain(dlq)
        .chain(archive)
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    all.sort();
    all
}

#[derive(QueryableByName)]
struct JobRow {
    #[diesel(sql_type = diesel::sql_types::Uuid)]
    id: uuid::Uuid,
    #[diesel(sql_type = Nullable<Text>)]
    fingerprint: Option<String>,
    #[diesel(sql_type = Nullable<Text>)]
    unique_key: Option<String>,
    #[diesel(sql_type = Text)]
    queue: String,
    #[diesel(sql_type = Nullable<diesel::sql_types::Jsonb>)]
    job_data: Option<serde_json::Value>,
    #[diesel(sql_type = Text)]
    status: String,
    #[diesel(sql_type = Timestamptz)]
    created_at: chrono::NaiveDateTime,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    run_at: Option<chrono::NaiveDateTime>,
    #[diesel(sql_type = Nullable<Timestamptz>)]
    updated_at: Option<chrono::NaiveDateTime>,
    #[diesel(sql_type = Integer)]
    attempt: i32,
    #[diesel(sql_type = Integer)]
    max_attempts: i32,
    #[diesel(sql_type = Integer)]
    reprocess_count: i32,
}

fn job_row_to_view(row: JobRow, source: &str) -> JobView {
    JobView {
        id: row.id,
        fingerprint: row.fingerprint,
        unique_key: row.unique_key,
        queue: row.queue,
        job_data: row.job_data,
        status: row.status,
        created_at: row.created_at,
        run_at: row.run_at,
        updated_at: row.updated_at,
        attempt: row.attempt,
        max_attempts: row.max_attempts,
        reprocess_count: row.reprocess_count,
        source: source.to_string(),
    }
}

pub fn get_jobs(
    conn: &mut PgConnection,
    queue_name: &str,
    status_filter: Option<&str>,
    page: i64,
    per_page: i64,
    source: &str,
) -> Vec<JobView> {
    let table_name = match source {
        "dlq" => "job_queue_dlq",
        "archive" => "job_queue_archive",
        _ => "job_queue",
    };

    let offset = (page - 1) * per_page;

    let filter = match (queue_name, status_filter) {
        ("", None) => String::new(),
        ("", Some(_)) => format!("WHERE status = $1"),
        (_, None) => format!("WHERE queue = $1"),
        (_, Some(_)) => format!("WHERE queue = $1 AND status = $2"),
    };

    let sql = format!(
        "SELECT id, fingerprint, unique_key, queue, job_data, status, created_at, run_at, updated_at, \
         attempt, max_attempts, reprocess_count \
         FROM {} {} \
         ORDER BY created_at DESC LIMIT {} OFFSET {}",
        table_name,
        filter,
        match (queue_name, status_filter) {
            ("", None) => "$1",
            ("", Some(_)) => "$2",
            (_, None) => "$2",
            (_, Some(_)) => "$3",
        },
        match (queue_name, status_filter) {
            ("", None) => "$2",
            ("", Some(_)) => "$3",
            (_, None) => "$3",
            (_, Some(_)) => "$4",
        },
    );

    let rows = match (queue_name, status_filter) {
        ("", None) => diesel::sql_query(sql)
            .bind::<BigInt, _>(per_page)
            .bind::<BigInt, _>(offset)
            .load::<JobRow>(conn),
        ("", Some(status)) => diesel::sql_query(sql)
            .bind::<Text, _>(status)
            .bind::<BigInt, _>(per_page)
            .bind::<BigInt, _>(offset)
            .load::<JobRow>(conn),
        (q, None) => diesel::sql_query(sql)
            .bind::<Text, _>(q)
            .bind::<BigInt, _>(per_page)
            .bind::<BigInt, _>(offset)
            .load::<JobRow>(conn),
        (q, Some(status)) => diesel::sql_query(sql)
            .bind::<Text, _>(q)
            .bind::<Text, _>(status)
            .bind::<BigInt, _>(per_page)
            .bind::<BigInt, _>(offset)
            .load::<JobRow>(conn),
    };

    rows.unwrap_or_default()
        .into_iter()
        .map(|r| job_row_to_view(r, source))
        .collect()
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

pub fn count_jobs(
    conn: &mut PgConnection,
    queue_name: &str,
    status_filter: Option<&str>,
    source: &str,
) -> i64 {
    let table_name = match source {
        "dlq" => "job_queue_dlq",
        "archive" => "job_queue_archive",
        _ => "job_queue",
    };

    let filter = match (queue_name, status_filter) {
        ("", None) => String::new(),
        ("", Some(_)) => format!("WHERE status = $1"),
        (_, None) => format!("WHERE queue = $1"),
        (_, Some(_)) => format!("WHERE queue = $1 AND status = $2"),
    };

    let sql = format!(
        "SELECT COUNT(*)::bigint AS count FROM {} {}",
        table_name, filter
    );

    let result = match (queue_name, status_filter) {
        ("", None) => diesel::sql_query(sql)
            .get_result::<CountRow>(conn),
        ("", Some(status)) => diesel::sql_query(sql)
            .bind::<Text, _>(status)
            .get_result::<CountRow>(conn),
        (q, None) => diesel::sql_query(sql)
            .bind::<Text, _>(q)
            .get_result::<CountRow>(conn),
        (q, Some(status)) => diesel::sql_query(sql)
            .bind::<Text, _>(q)
            .bind::<Text, _>(status)
            .get_result::<CountRow>(conn),
    };

    result.map(|r| r.count).unwrap_or(0)
}

pub fn get_job(conn: &mut PgConnection, id: uuid::Uuid, source: &str) -> Option<JobView> {
    match source {
        "dlq" => {
            let job: Option<DlqJob> = job_queue_dlq::table
                .find(id)
                .first(conn)
                .ok();
            job.map(Into::into)
        }
        "archive" => {
            let job: Option<ArchivedJob> = job_queue_archive::table
                .find(id)
                .first(conn)
                .ok();
            job.map(Into::into)
        }
        _ => {
            let job: Option<Job> = job_queue::table.find(id).first(conn).ok();
            job.map(Into::into)
        }
    }
}

pub fn find_job_anywhere(conn: &mut PgConnection, id: uuid::Uuid) -> Option<JobView> {
    if let Some(job) = job_queue::table.find(id).first::<Job>(conn).ok() {
        return Some(job.into());
    }
    if let Some(job) = job_queue_dlq::table.find(id).first::<DlqJob>(conn).ok() {
        return Some(job.into());
    }
    if let Some(job) = job_queue_archive::table.find(id).first::<ArchivedJob>(conn).ok() {
        return Some(job.into());
    }
    None
}

pub fn restart_job(conn: &mut PgConnection, id: uuid::Uuid) -> Result<(), String> {
    let now = chrono::Utc::now().naive_utc();
    let affected = diesel::update(job_queue::table.filter(job_queue::id.eq(id)))
        .set((
            job_queue::status.eq("pending"),
            job_queue::attempt.eq(0),
            job_queue::run_at.eq(now),
            job_queue::updated_at.eq(now),
        ))
        .execute(conn)
        .map_err(|e| e.to_string())?;

    if affected == 0 {
        return Err("Job not found or not in queue".into());
    }
    Ok(())
}

pub fn requeue_from_dlq(conn: &mut PgConnection, id: uuid::Uuid) -> Result<(), String> {
    let now = chrono::Utc::now().naive_utc();
    let affected = diesel::sql_query(
        "WITH moved AS (
            DELETE FROM job_queue_dlq WHERE id = $1 RETURNING
                id, fingerprint, unique_key, queue, job_data, created_at,
                max_attempts, reprocess_count
        )
        INSERT INTO job_queue (id, fingerprint, unique_key, queue, job_data, status, created_at, run_at, updated_at, attempt, max_attempts, reprocess_count)
        SELECT id, fingerprint, unique_key, queue, job_data, 'pending', created_at, $2, $3, 0, max_attempts, reprocess_count + 1
        FROM moved",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<Timestamptz, _>(now)
    .bind::<Timestamptz, _>(now)
    .execute(conn)
    .map_err(|e| e.to_string())?;

    if affected == 0 {
        return Err("Job not found in DLQ".into());
    }
    Ok(())
}

pub fn requeue_from_archive(conn: &mut PgConnection, id: uuid::Uuid) -> Result<(), String> {
    let now = chrono::Utc::now().naive_utc();
    let affected = diesel::sql_query(
        "WITH moved AS (
            DELETE FROM job_queue_archive WHERE id = $1 RETURNING
                id, fingerprint, unique_key, queue, job_data, created_at,
                max_attempts, reprocess_count
        )
        INSERT INTO job_queue (id, fingerprint, unique_key, queue, job_data, status, created_at, run_at, updated_at, attempt, max_attempts, reprocess_count)
        SELECT id, fingerprint, unique_key, queue, job_data, 'pending', created_at, $2, $3, 0, max_attempts, reprocess_count + 1
        FROM moved",
    )
    .bind::<diesel::sql_types::Uuid, _>(id)
    .bind::<Timestamptz, _>(now)
    .bind::<Timestamptz, _>(now)
    .execute(conn)
    .map_err(|e| e.to_string())?;

    if affected == 0 {
        return Err("Job not found in archive".into());
    }
    Ok(())
}
