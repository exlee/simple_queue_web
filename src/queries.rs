use diesel::prelude::*;
use diesel::PgConnection;

use crate::models::{Job, DlqJob, ArchivedJob, JobView, QueueStatusCount, TableStats};
use crate::schema::{job_queue, job_queue_dlq, job_queue_archive};

pub fn set_statement_timeout(conn: &mut PgConnection) {
    diesel::sql_query("SET statement_timeout = '5s'")
        .execute(conn)
        .ok();
}

pub fn get_queue_status_counts(conn: &mut PgConnection) -> Vec<QueueStatusCount> {
    job_queue::table
        .group_by((job_queue::queue, job_queue::status))
        .select((job_queue::queue, job_queue::status, diesel::dsl::count_star()))
        .order_by((job_queue::queue, job_queue::status))
        .load(conn)
        .unwrap_or_default()
        .into_iter()
        .map(|(queue, status, count)| QueueStatusCount { queue, status, count })
        .collect()
}

pub fn get_dlq_counts(conn: &mut PgConnection) -> Vec<QueueStatusCount> {
    job_queue_dlq::table
        .group_by((job_queue_dlq::queue, job_queue_dlq::status))
        .select((job_queue_dlq::queue, job_queue_dlq::status, diesel::dsl::count_star()))
        .order_by((job_queue_dlq::queue, job_queue_dlq::status))
        .load(conn)
        .unwrap_or_default()
        .into_iter()
        .map(|(queue, status, count)| QueueStatusCount { queue, status, count })
        .collect()
}

pub fn get_archive_counts(conn: &mut PgConnection) -> Vec<QueueStatusCount> {
    job_queue_archive::table
        .group_by((job_queue_archive::queue, job_queue_archive::status))
        .select((job_queue_archive::queue, job_queue_archive::status, diesel::dsl::count_star()))
        .order_by((job_queue_archive::queue, job_queue_archive::status))
        .load(conn)
        .unwrap_or_default()
        .into_iter()
        .map(|(queue, status, count)| QueueStatusCount { queue, status, count })
        .collect()
}

pub fn get_table_stats(conn: &mut PgConnection) -> TableStats {
    #[derive(QueryableByName)]
    struct Row {
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        dead_tuples: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        live_tuples: i64,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        last_vacuum: Option<chrono::NaiveDateTime>,
        #[diesel(sql_type = diesel::sql_types::Nullable<diesel::sql_types::Timestamptz>)]
        last_autovacuum: Option<chrono::NaiveDateTime>,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        total_inserts: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
        total_updates: i64,
        #[diesel(sql_type = diesel::sql_types::BigInt)]
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

pub fn get_jobs(
    conn: &mut PgConnection,
    queue_name: &str,
    status_filter: Option<&str>,
    page: i64,
    per_page: i64,
    source: &str,
) -> Vec<JobView> {
    let offset = (page - 1) * per_page;

    let jobs: Vec<JobView> = match source {
        "dlq" => {
            let mut query = job_queue_dlq::table
                .order_by(job_queue_dlq::created_at.desc())
                .limit(per_page)
                .offset(offset)
                .into_boxed();
            if !queue_name.is_empty() {
                query = query.filter(job_queue_dlq::queue.eq(queue_name));
            }
            if let Some(status) = status_filter {
                query = query.filter(job_queue_dlq::status.eq(status));
            }
            query.load::<DlqJob>(conn).unwrap_or_default()
                .into_iter().map(Into::into).collect()
        }
        "archive" => {
            let mut query = job_queue_archive::table
                .order_by(job_queue_archive::created_at.desc())
                .limit(per_page)
                .offset(offset)
                .into_boxed();
            if !queue_name.is_empty() {
                query = query.filter(job_queue_archive::queue.eq(queue_name));
            }
            if let Some(status) = status_filter {
                query = query.filter(job_queue_archive::status.eq(status));
            }
            query.load::<ArchivedJob>(conn).unwrap_or_default()
                .into_iter().map(Into::into).collect()
        }
        _ => {
            let mut query = job_queue::table
                .order_by(job_queue::created_at.desc())
                .limit(per_page)
                .offset(offset)
                .into_boxed();
            if !queue_name.is_empty() {
                query = query.filter(job_queue::queue.eq(queue_name));
            }
            if let Some(status) = status_filter {
                query = query.filter(job_queue::status.eq(status));
            }
            query.load::<Job>(conn).unwrap_or_default()
                .into_iter().map(Into::into).collect()
        }
    };

    jobs
}

pub fn count_jobs(
    conn: &mut PgConnection,
    queue_name: &str,
    status_filter: Option<&str>,
    source: &str,
) -> i64 {
    let count: i64 = match source {
        "dlq" => {
            let mut query = job_queue_dlq::table.count().into_boxed();
            if !queue_name.is_empty() {
                query = query.filter(job_queue_dlq::queue.eq(queue_name));
            }
            if let Some(status) = status_filter {
                query = query.filter(job_queue_dlq::status.eq(status));
            }
            query.first(conn).unwrap_or(0)
        }
        "archive" => {
            let mut query = job_queue_archive::table.count().into_boxed();
            if !queue_name.is_empty() {
                query = query.filter(job_queue_archive::queue.eq(queue_name));
            }
            if let Some(status) = status_filter {
                query = query.filter(job_queue_archive::status.eq(status));
            }
            query.first(conn).unwrap_or(0)
        }
        _ => {
            let mut query = job_queue::table.count().into_boxed();
            if !queue_name.is_empty() {
                query = query.filter(job_queue::queue.eq(queue_name));
            }
            if let Some(status) = status_filter {
                query = query.filter(job_queue::status.eq(status));
            }
            query.first(conn).unwrap_or(0)
        }
    };

    count
}

pub fn get_job(conn: &mut PgConnection, id: uuid::Uuid, source: &str) -> Option<JobView> {
    match source {
        "dlq" => job_queue_dlq::table
            .find(id)
            .first::<DlqJob>(conn)
            .ok()
            .map(Into::into),
        "archive" => job_queue_archive::table
            .find(id)
            .first::<ArchivedJob>(conn)
            .ok()
            .map(Into::into),
        _ => job_queue::table
            .find(id)
            .first::<Job>(conn)
            .ok()
            .map(Into::into),
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
    .bind::<diesel::sql_types::Timestamptz, _>(now)
    .bind::<diesel::sql_types::Timestamptz, _>(now)
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
    .bind::<diesel::sql_types::Timestamptz, _>(now)
    .bind::<diesel::sql_types::Timestamptz, _>(now)
    .execute(conn)
    .map_err(|e| e.to_string())?;

    if affected == 0 {
        return Err("Job not found in archive".into());
    }
    Ok(())
}
