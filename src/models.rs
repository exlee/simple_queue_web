use chrono::NaiveDateTime;
use diesel::Queryable;
use diesel::Selectable;
use serde::Serialize;
use uuid::Uuid;

use crate::schema::{job_queue, job_queue_archive, job_queue_dlq};

#[derive(Debug, Queryable, Selectable, Serialize, Clone)]
#[diesel(table_name = job_queue)]
pub struct Job {
    pub id: Uuid,
    pub fingerprint: Option<String>,
    pub unique_key: Option<String>,
    pub queue: String,
    pub job_data: Option<serde_json::Value>,
    pub status: String,
    pub created_at: NaiveDateTime,
    pub run_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
    pub completed_at: Option<NaiveDateTime>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub reprocess_count: i32,
}

#[derive(Debug, Queryable, Selectable, Serialize, Clone)]
#[diesel(table_name = job_queue_dlq)]
pub struct DlqJob {
    pub id: Uuid,
    pub fingerprint: Option<String>,
    pub unique_key: Option<String>,
    pub queue: String,
    pub job_data: Option<serde_json::Value>,
    pub status: String,
    pub created_at: NaiveDateTime,
    pub run_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
    pub completed_at: Option<NaiveDateTime>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub reprocess_count: i32,
}

#[derive(Debug, Queryable, Selectable, Serialize, Clone)]
#[diesel(table_name = job_queue_archive)]
pub struct ArchivedJob {
    pub id: Uuid,
    pub fingerprint: Option<String>,
    pub unique_key: Option<String>,
    pub queue: String,
    pub job_data: Option<serde_json::Value>,
    pub status: String,
    pub created_at: NaiveDateTime,
    pub run_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
    pub completed_at: Option<NaiveDateTime>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub reprocess_count: i32,
}

#[derive(Debug, Serialize, Clone)]
pub struct QueueStatusCount {
    pub queue: String,
    pub status: String,
    pub count: i64,
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct TableStats {
    pub dead_tuples: i64,
    pub live_tuples: i64,
    pub last_vacuum: Option<NaiveDateTime>,
    pub last_autovacuum: Option<NaiveDateTime>,
    pub total_inserts: i64,
    pub total_updates: i64,
    pub total_deletes: i64,
}

#[derive(Debug, Serialize, Clone)]
pub struct JobView {
    pub id: Uuid,
    pub fingerprint: Option<String>,
    pub unique_key: Option<String>,
    pub queue: String,
    pub job_data: Option<serde_json::Value>,
    pub status: String,
    pub created_at: NaiveDateTime,
    pub run_at: Option<NaiveDateTime>,
    pub updated_at: Option<NaiveDateTime>,
    pub completed_at: Option<NaiveDateTime>,
    pub attempt: i32,
    pub max_attempts: i32,
    pub reprocess_count: i32,
    pub source: String,
    #[serde(skip)]
    pub short_id: String,
    #[serde(skip)]
    pub created_at_fmt: Option<String>,
    #[serde(skip)]
    pub run_at_date: String,
    #[serde(skip)]
    pub run_at_time: String,
    #[serde(skip)]
    pub updated_at_date: String,
    #[serde(skip)]
    pub updated_at_time: String,
    #[serde(skip)]
    pub completed_at_fmt: Option<String>,
    #[serde(skip)]
    pub runtime: String,
    #[serde(skip)]
    pub job_data_pretty: String,
}

impl From<Job> for JobView {
    fn from(j: Job) -> Self {
        Self {
            id: j.id,
            fingerprint: j.fingerprint,
            unique_key: j.unique_key,
            queue: j.queue,
            job_data: j.job_data,
            status: j.status,
            created_at: j.created_at,
            run_at: j.run_at,
            updated_at: j.updated_at,
            completed_at: j.completed_at,
            attempt: j.attempt,
            max_attempts: j.max_attempts,
            reprocess_count: j.reprocess_count,
            source: "queue".into(),
            short_id: j.id.to_string()[..8].to_string(),
            created_at_fmt: None,
            run_at_date: String::new(),
            run_at_time: String::new(),
            updated_at_date: String::new(),
            updated_at_time: String::new(),
            completed_at_fmt: None,
            runtime: String::new(),
            job_data_pretty: String::new(),
        }
    }
}

impl From<DlqJob> for JobView {
    fn from(j: DlqJob) -> Self {
        Self {
            id: j.id,
            fingerprint: j.fingerprint,
            unique_key: j.unique_key,
            queue: j.queue,
            job_data: j.job_data,
            status: j.status,
            created_at: j.created_at,
            run_at: j.run_at,
            updated_at: j.updated_at,
            completed_at: j.completed_at,
            attempt: j.attempt,
            max_attempts: j.max_attempts,
            reprocess_count: j.reprocess_count,
            source: "dlq".into(),
            short_id: j.id.to_string()[..8].to_string(),
            created_at_fmt: None,
            run_at_date: String::new(),
            run_at_time: String::new(),
            updated_at_date: String::new(),
            updated_at_time: String::new(),
            completed_at_fmt: None,
            runtime: String::new(),
            job_data_pretty: String::new(),
        }
    }
}

impl From<ArchivedJob> for JobView {
    fn from(j: ArchivedJob) -> Self {
        Self {
            id: j.id,
            fingerprint: j.fingerprint,
            unique_key: j.unique_key,
            queue: j.queue,
            job_data: j.job_data,
            status: j.status,
            created_at: j.created_at,
            run_at: j.run_at,
            updated_at: j.updated_at,
            completed_at: j.completed_at,
            attempt: j.attempt,
            max_attempts: j.max_attempts,
            reprocess_count: j.reprocess_count,
            source: "archive".into(),
            short_id: j.id.to_string()[..8].to_string(),
            created_at_fmt: None,
            run_at_date: String::new(),
            run_at_time: String::new(),
            updated_at_date: String::new(),
            updated_at_time: String::new(),
            completed_at_fmt: None,
            runtime: String::new(),
            job_data_pretty: String::new(),
        }
    }
}
