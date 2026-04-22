mod db;
mod handlers;
mod models;
mod queries;
mod schema;
#[cfg(test)]
mod test_helpers;

use axum::{
    Router,
    routing::{get, post},
};
use handlers::AppState;
use tower_http::trace::TraceLayer;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "simple_queue_web=debug,tower_http=debug".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let database_url = args.get(1).cloned().unwrap_or_else(|| {
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set or passed as first argument")
    });

    let pool = db::establish_pool(&database_url);
    let state = AppState { pool };

    let app = Router::new()
        .route("/", get(handlers::index))
        .route("/dashboard", get(handlers::dashboard))
        .route("/queues/browse", get(handlers::queue_browse))
        .route("/jobs/{id}", get(handlers::job_inspect))
        .route("/jobs/{id}/restart", post(handlers::restart_job))
        .route("/jobs/{id}/cancel", post(handlers::cancel_job))
        .route("/jobs/{id}/reschedule", post(handlers::reschedule_job))
        .route("/jobs/{id}/requeue", post(handlers::requeue_job))
        .route("/api/dashboard/poll", get(handlers::api_dashboard_poll))
        .route("/api/queues/poll", get(handlers::api_queue_poll))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001")
        .await
        .expect("Failed to bind to 0.0.0.0:3001");

    tracing::info!("Queue Manager running on http://0.0.0.0:3001");
    axum::serve(listener, app).await.unwrap();
}

#[cfg(test)]
mod tests {
    use crate::schema::{job_queue, job_queue_archive, job_queue_dlq};
    use crate::test_helpers::*;
    use diesel::prelude::*;

    #[test]
    fn test_get_queue_status_counts() {
        let mut conn = establish_test_connection();

        insert_test_job(&mut conn, "test-queue", "pending", 0, 3, 0);
        insert_test_job(&mut conn, "test-queue", "pending", 0, 3, 0);
        insert_test_job(&mut conn, "test-queue", "running", 1, 3, 0);
        insert_test_job(&mut conn, "other-queue", "completed", 3, 3, 0);

        let counts = crate::queries::get_queue_status_counts(&mut conn);

        let test_pending = counts
            .iter()
            .find(|c| c.queue == "test-queue" && c.status == "pending")
            .expect("should find test-queue pending");
        assert_eq!(test_pending.count, 2);

        let test_running = counts
            .iter()
            .find(|c| c.queue == "test-queue" && c.status == "running")
            .expect("should find test-queue running");
        assert_eq!(test_running.count, 1);

        let other_completed = counts
            .iter()
            .find(|c| c.queue == "other-queue" && c.status == "completed")
            .expect("should find other-queue completed");
        assert_eq!(other_completed.count, 1);
    }

    #[test]
    fn test_get_queue_status_counts_empty() {
        let mut conn = establish_test_connection();

        let counts = crate::queries::get_queue_status_counts(&mut conn);

        let before = existing_job_queue_count(&mut conn);
        assert_eq!(counts.iter().map(|c| c.count as i64).sum::<i64>(), before);
    }

    #[test]
    fn test_get_distinct_queues() {
        let mut conn = establish_test_connection();

        let before = crate::queries::get_distinct_queues(&mut conn).len();

        insert_test_job(&mut conn, "alpha-tq-unique", "pending", 0, 3, 0);
        insert_test_job(&mut conn, "beta-tq-unique", "pending", 0, 3, 0);
        insert_test_dlq_job(&mut conn, "alpha-tq-unique", "failed", 3, 3, 0);
        insert_test_archive_job(&mut conn, "gamma-tq-unique", "completed", 3, 3, 0);

        let queues = crate::queries::get_distinct_queues(&mut conn);

        assert!(queues.contains(&"alpha-tq-unique".to_string()));
        assert!(queues.contains(&"beta-tq-unique".to_string()));
        assert!(queues.contains(&"gamma-tq-unique".to_string()));
        assert_eq!(queues.len(), before + 3);
    }

    #[test]
    fn test_get_distinct_queues_dedupes() {
        let mut conn = establish_test_connection();

        let before = crate::queries::get_distinct_queues(&mut conn).len();

        insert_test_job(&mut conn, "same-tq-unique", "pending", 0, 3, 0);
        insert_test_job(&mut conn, "same-tq-unique", "running", 1, 3, 0);
        insert_test_dlq_job(&mut conn, "same-tq-unique", "failed", 3, 3, 0);

        let queues = crate::queries::get_distinct_queues(&mut conn);

        assert!(queues.contains(&"same-tq-unique".to_string()));
        assert_eq!(queues.len(), before + 1);
    }

    #[test]
    fn test_get_dlq_counts() {
        let mut conn = establish_test_connection();

        insert_test_dlq_job(&mut conn, "failed-queue", "failed", 3, 3, 0);
        insert_test_dlq_job(&mut conn, "failed-queue", "failed", 3, 3, 0);

        let counts = crate::queries::get_dlq_counts(&mut conn);

        let failed = counts
            .iter()
            .find(|c| c.queue == "failed-queue" && c.status == "failed")
            .expect("should find failed-queue failed");
        assert_eq!(failed.count, 2);
    }

    #[test]
    fn test_get_archive_counts() {
        let mut conn = establish_test_connection();

        insert_test_archive_job(&mut conn, "old-queue", "completed", 3, 3, 1);
        insert_test_archive_job(&mut conn, "old-queue", "discarded", 1, 3, 2);

        let counts = crate::queries::get_archive_counts(&mut conn);

        assert_eq!(counts.len(), 2);
        let completed = counts
            .iter()
            .find(|c| c.queue == "old-queue" && c.status == "completed")
            .expect("should find completed");
        assert_eq!(completed.count, 1);
    }

    #[test]
    fn test_get_table_stats() {
        let mut conn = establish_test_connection();

        let stats = crate::queries::get_table_stats(&mut conn);

        assert!(stats.live_tuples >= 0);
        assert!(stats.dead_tuples >= 0);
    }

    #[test]
    fn test_get_jobs_queue_source() {
        let mut conn = establish_test_connection();

        insert_test_job(&mut conn, "browse-q", "pending", 0, 3, 0);
        insert_test_job(&mut conn, "browse-q", "running", 1, 3, 0);
        insert_test_job(&mut conn, "browse-q", "pending", 0, 3, 0);

        let all = crate::queries::get_jobs(
            &mut conn,
            "browse-q",
            None,
            1,
            50,
            "queue",
            "created_at",
            "desc",
        );
        assert_eq!(all.len(), 3);

        let pending = crate::queries::get_jobs(
            &mut conn,
            "browse-q",
            Some("pending"),
            1,
            50,
            "queue",
            "created_at",
            "desc",
        );
        assert_eq!(pending.len(), 2);

        let running = crate::queries::get_jobs(
            &mut conn,
            "browse-q",
            Some("running"),
            1,
            50,
            "queue",
            "created_at",
            "desc",
        );
        assert_eq!(running.len(), 1);
    }

    #[test]
    fn test_get_jobs_dlq_source() {
        let mut conn = establish_test_connection();

        insert_test_dlq_job(&mut conn, "dlq-tq-unique", "failed", 3, 3, 0);
        insert_test_dlq_job(&mut conn, "dlq-tq-unique", "failed", 3, 3, 0);

        let jobs = crate::queries::get_jobs(
            &mut conn,
            "dlq-tq-unique",
            None,
            1,
            50,
            "dlq",
            "created_at",
            "desc",
        );
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].source, "dlq");
    }

    #[test]
    fn test_get_jobs_archive_source() {
        let mut conn = establish_test_connection();

        insert_test_archive_job(&mut conn, "archive-tq-unique", "completed", 3, 3, 1);

        let jobs = crate::queries::get_jobs(
            &mut conn,
            "archive-tq-unique",
            None,
            1,
            50,
            "archive",
            "created_at",
            "desc",
        );
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].source, "archive");
    }

    #[test]
    fn test_get_jobs_pagination() {
        let mut conn = establish_test_connection();

        for i in 0..5 {
            insert_test_job(&mut conn, "page-q", "pending", 0, 3, i);
        }

        let page1 = crate::queries::get_jobs(
            &mut conn,
            "page-q",
            None,
            1,
            2,
            "queue",
            "created_at",
            "desc",
        );
        assert_eq!(page1.len(), 2);

        let page2 = crate::queries::get_jobs(
            &mut conn,
            "page-q",
            None,
            2,
            2,
            "queue",
            "created_at",
            "desc",
        );
        assert_eq!(page2.len(), 2);

        let page3 = crate::queries::get_jobs(
            &mut conn,
            "page-q",
            None,
            3,
            2,
            "queue",
            "created_at",
            "desc",
        );
        assert_eq!(page3.len(), 1);
    }

    #[test]
    fn test_get_jobs_empty_filters() {
        let mut conn = establish_test_connection();

        let before = existing_job_queue_count(&mut conn) as usize;

        let jobs =
            crate::queries::get_jobs(&mut conn, "", None, 1, 50, "queue", "created_at", "desc");
        assert_eq!(jobs.len(), 50.min(before));
    }

    #[test]
    fn test_count_jobs() {
        let mut conn = establish_test_connection();

        let baseline = crate::queries::count_jobs(&mut conn, "count-tq-unique", None, "queue");

        insert_test_job(&mut conn, "count-tq-unique", "pending", 0, 3, 0);
        insert_test_job(&mut conn, "count-tq-unique", "pending", 0, 3, 0);
        insert_test_job(&mut conn, "count-tq-unique", "running", 1, 3, 0);

        let all_count_q = crate::queries::count_jobs(&mut conn, "count-tq-unique", None, "queue");
        assert_eq!(all_count_q, baseline + 3);

        let pending =
            crate::queries::count_jobs(&mut conn, "count-tq-unique", Some("pending"), "queue");
        assert_eq!(pending, 2);

        let no_match =
            crate::queries::count_jobs(&mut conn, "count-tq-unique", Some("completed"), "queue");
        assert_eq!(no_match, 0);
    }

    #[test]
    fn test_count_jobs_dlq() {
        let mut conn = establish_test_connection();

        insert_test_dlq_job(&mut conn, "dlq-count", "failed", 3, 3, 0);
        insert_test_dlq_job(&mut conn, "dlq-count", "failed", 3, 3, 0);

        let count = crate::queries::count_jobs(&mut conn, "dlq-count", None, "dlq");
        assert_eq!(count, 2);
    }

    #[test]
    fn test_count_jobs_archive() {
        let mut conn = establish_test_connection();

        insert_test_archive_job(&mut conn, "archive-count", "completed", 3, 3, 1);

        let count = crate::queries::count_jobs(&mut conn, "archive-count", None, "archive");
        assert_eq!(count, 1);
    }

    #[test]
    fn test_get_job_queue() {
        let mut conn = establish_test_connection();

        let id = insert_test_job(&mut conn, "inspect-q", "pending", 0, 3, 0);

        let job = crate::queries::get_job(&mut conn, id, "queue");
        assert!(job.is_some());
        let job = job.unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.queue, "inspect-q");
        assert_eq!(job.status, "pending");
        assert_eq!(job.attempt, 0);
        assert_eq!(job.max_attempts, 3);
        assert_eq!(job.source, "queue");
    }

    #[test]
    fn test_get_job_dlq() {
        let mut conn = establish_test_connection();

        let id = insert_test_dlq_job(&mut conn, "dlq-inspect", "failed", 3, 3, 0);

        let job = crate::queries::get_job(&mut conn, id, "dlq");
        assert!(job.is_some());
        assert_eq!(job.unwrap().source, "dlq");
    }

    #[test]
    fn test_get_job_archive() {
        let mut conn = establish_test_connection();

        let id = insert_test_archive_job(&mut conn, "archive-inspect", "completed", 3, 3, 1);

        let job = crate::queries::get_job(&mut conn, id, "archive");
        assert!(job.is_some());
        assert_eq!(job.unwrap().source, "archive");
    }

    #[test]
    fn test_get_job_not_found() {
        let mut conn = establish_test_connection();

        let job = crate::queries::get_job(&mut conn, uuid::Uuid::new_v4(), "queue");
        assert!(job.is_none());
    }

    #[test]
    fn test_get_job_wrong_source() {
        let mut conn = establish_test_connection();

        let id = insert_test_job(&mut conn, "wrong-src", "pending", 0, 3, 0);

        let job = crate::queries::get_job(&mut conn, id, "dlq");
        assert!(job.is_none());
    }

    #[test]
    fn test_find_job_anywhere_queue() {
        let mut conn = establish_test_connection();

        let id = insert_test_job(&mut conn, "auto-q", "running", 1, 3, 0);

        let job = crate::queries::find_job_anywhere(&mut conn, id);
        assert!(job.is_some());
        assert_eq!(job.unwrap().source, "queue");
    }

    #[test]
    fn test_find_job_anywhere_dlq() {
        let mut conn = establish_test_connection();

        let id = insert_test_dlq_job(&mut conn, "auto-dlq", "failed", 3, 3, 0);

        let job = crate::queries::find_job_anywhere(&mut conn, id);
        assert!(job.is_some());
        assert_eq!(job.unwrap().source, "dlq");
    }

    #[test]
    fn test_find_job_anywhere_archive() {
        let mut conn = establish_test_connection();

        let id = insert_test_archive_job(&mut conn, "auto-arch", "completed", 3, 3, 1);

        let job = crate::queries::find_job_anywhere(&mut conn, id);
        assert!(job.is_some());
        assert_eq!(job.unwrap().source, "archive");
    }

    #[test]
    fn test_find_job_anywhere_not_found() {
        let mut conn = establish_test_connection();

        let job = crate::queries::find_job_anywhere(&mut conn, uuid::Uuid::new_v4());
        assert!(job.is_none());
    }

    #[test]
    fn test_restart_job() {
        let mut conn = establish_test_connection();

        let id = insert_test_job(&mut conn, "restart-q", "running", 2, 3, 1);

        crate::queries::restart_job(&mut conn, id).expect("restart should succeed");

        let job: crate::models::Job = job_queue::table.find(id).first(&mut conn).unwrap();
        assert_eq!(job.status, "pending");
        assert_eq!(job.attempt, 0);
        assert!(job.run_at.is_some());
        assert!(job.updated_at.is_some());
        assert_eq!(job.reprocess_count, 1);
    }

    #[test]
    fn test_restart_job_not_found() {
        let mut conn = establish_test_connection();

        let result = crate::queries::restart_job(&mut conn, uuid::Uuid::new_v4());
        assert!(result.is_err());
    }

    #[test]
    fn test_requeue_from_dlq() {
        let mut conn = establish_test_connection();

        let id = insert_test_dlq_job(&mut conn, "requeue-dlq", "failed", 3, 3, 2);

        crate::queries::requeue_from_dlq(&mut conn, id).expect("requeue from dlq should succeed");

        let dlq_exists: bool = diesel::select(diesel::dsl::exists(
            job_queue_dlq::table.filter(job_queue_dlq::id.eq(id)),
        ))
        .get_result(&mut conn)
        .unwrap();
        assert!(!dlq_exists, "job should be removed from DLQ");

        let job: crate::models::Job = job_queue::table.find(id).first(&mut conn).unwrap();
        assert_eq!(job.status, "pending");
        assert_eq!(job.queue, "requeue-dlq");
        assert_eq!(job.attempt, 0);
        assert_eq!(job.max_attempts, 3);
        assert_eq!(job.reprocess_count, 3);
        assert!(job.run_at.is_some());
        assert!(job.updated_at.is_some());
    }

    #[test]
    fn test_requeue_from_dlq_not_found() {
        let mut conn = establish_test_connection();

        let result = crate::queries::requeue_from_dlq(&mut conn, uuid::Uuid::new_v4());
        assert!(result.is_err());
    }

    #[test]
    fn test_requeue_from_archive() {
        let mut conn = establish_test_connection();

        let id = insert_test_archive_job(&mut conn, "requeue-arch", "discarded", 1, 3, 5);

        crate::queries::requeue_from_archive(&mut conn, id)
            .expect("requeue from archive should succeed");

        let archive_exists: bool = diesel::select(diesel::dsl::exists(
            job_queue_archive::table.filter(job_queue_archive::id.eq(id)),
        ))
        .get_result(&mut conn)
        .unwrap();
        assert!(!archive_exists, "job should be removed from archive");

        let job: crate::models::Job = job_queue::table.find(id).first(&mut conn).unwrap();
        assert_eq!(job.status, "pending");
        assert_eq!(job.queue, "requeue-arch");
        assert_eq!(job.attempt, 0);
        assert_eq!(job.reprocess_count, 6);
        assert!(job.run_at.is_some());
        assert!(job.updated_at.is_some());
    }

    #[test]
    fn test_requeue_from_archive_not_found() {
        let mut conn = establish_test_connection();

        let result = crate::queries::requeue_from_archive(&mut conn, uuid::Uuid::new_v4());
        assert!(result.is_err());
    }

    #[test]
    fn test_job_view_fields() {
        let mut conn = establish_test_connection();

        let id = insert_test_job(&mut conn, "fields-q", "pending", 0, 5, 3);

        let job = crate::queries::get_job(&mut conn, id, "queue").unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.queue, "fields-q");
        assert_eq!(job.status, "pending");
        assert_eq!(job.attempt, 0);
        assert_eq!(job.max_attempts, 5);
        assert_eq!(job.reprocess_count, 3);
        assert!(job.fingerprint.is_none());
        assert!(job.unique_key.is_none());
        assert!(job.job_data.is_none());
        assert!(job.created_at.and_utc().timestamp() > 0);
    }

    #[test]
    fn test_cancel_job() {
        let mut conn = establish_test_connection();

        let id = insert_test_job(&mut conn, "cancel-q", "pending", 0, 3, 0);

        crate::queries::cancel_job(&mut conn, id).expect("cancel should succeed");

        let job: crate::models::Job = job_queue::table.find(id).first(&mut conn).unwrap();
        assert_eq!(job.status, "cancelled");
        assert!(job.updated_at.is_some());
    }

    #[test]
    fn test_cancel_job_not_found() {
        let mut conn = establish_test_connection();

        let result = crate::queries::cancel_job(&mut conn, uuid::Uuid::new_v4());
        assert!(result.is_err());
    }

    #[test]
    fn test_reschedule_job() {
        let mut conn = establish_test_connection();

        let id = insert_test_job(&mut conn, "resched-q", "pending", 0, 3, 0);
        let new_run_at = chrono::DateTime::from_timestamp(9999999999, 0)
            .unwrap()
            .naive_utc();

        crate::queries::reschedule_job(&mut conn, id, new_run_at)
            .expect("reschedule should succeed");

        let job: crate::models::Job = job_queue::table.find(id).first(&mut conn).unwrap();
        assert!(job.run_at.is_some());
        assert_eq!(job.run_at.unwrap().and_utc().timestamp(), 9999999999);
        assert_eq!(job.status, "pending");
        assert!(job.updated_at.is_some());
    }

    #[test]
    fn test_reschedule_job_not_found() {
        let mut conn = establish_test_connection();

        let result = crate::queries::reschedule_job(
            &mut conn,
            uuid::Uuid::new_v4(),
            chrono::Utc::now().naive_utc(),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_get_jobs_sort_by_status() {
        let mut conn = establish_test_connection();

        insert_test_job(&mut conn, "sort-q", "running", 1, 3, 0);
        insert_test_job(&mut conn, "sort-q", "pending", 0, 3, 0);
        insert_test_job(&mut conn, "sort-q", "completed", 3, 3, 0);

        let asc =
            crate::queries::get_jobs(&mut conn, "sort-q", None, 1, 50, "queue", "status", "asc");
        assert_eq!(asc[0].status, "completed");
        assert_eq!(asc[1].status, "pending");
        assert_eq!(asc[2].status, "running");

        let desc =
            crate::queries::get_jobs(&mut conn, "sort-q", None, 1, 50, "queue", "status", "desc");
        assert_eq!(desc[0].status, "running");
        assert_eq!(desc[1].status, "pending");
        assert_eq!(desc[2].status, "completed");
    }

    #[test]
    fn test_get_jobs_sort_by_attempt() {
        let mut conn = establish_test_connection();

        let _id1 = insert_test_job(&mut conn, "attempt-sort-q", "pending", 2, 3, 0);
        let _id2 = insert_test_job(&mut conn, "attempt-sort-q", "pending", 0, 3, 0);
        let _id3 = insert_test_job(&mut conn, "attempt-sort-q", "pending", 1, 3, 0);

        let asc = crate::queries::get_jobs(
            &mut conn,
            "attempt-sort-q",
            None,
            1,
            50,
            "queue",
            "attempt",
            "asc",
        );
        assert_eq!(asc[0].attempt, 0);
        assert_eq!(asc[1].attempt, 1);
        assert_eq!(asc[2].attempt, 2);
    }
}
