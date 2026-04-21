use askama::Template;
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Redirect},
};
use serde::Deserialize;

use crate::db::PgPool;
use crate::queries;

#[derive(Clone)]
pub struct AppState {
    pub pool: PgPool,
}

#[derive(Deserialize)]
pub struct QueueBrowseParams {
    pub queue: Option<String>,
    pub status: Option<String>,
    pub source: Option<String>,
    pub page: Option<i64>,
    pub sort_by: Option<String>,
    pub sort_dir: Option<String>,
}

fn with_conn<F, R>(pool: &PgPool, f: F) -> Result<R, String>
where
    F: FnOnce(&mut diesel::PgConnection) -> Result<R, String>,
{
    tokio::task::block_in_place(|| {
        let mut conn = pool.get().map_err(|e| e.to_string())?;
        queries::set_statement_timeout(&mut conn);
        f(&mut conn)
    })
}

#[derive(serde::Serialize)]
pub struct DashboardData {
    pub total_pending: i64,
    pub total_running: i64,
    pub total_completed: i64,
    pub total_failed: i64,
    pub total_dlq: i64,
    pub total_archive: i64,
    pub queue_breakdown: Vec<QueueBreakdown>,
    pub dlq_counts: Vec<QueueStatusCount>,
    pub dead_tuples: i64,
    pub live_tuples: i64,
    pub last_vacuum: String,
    pub last_autovacuum: String,
    pub total_inserts: i64,
    pub total_updates: i64,
    pub total_deletes: i64,
}

#[derive(serde::Serialize, Clone)]
pub struct QueueBreakdown {
    pub queue: String,
    pub pending: i64,
    pub running: i64,
    pub completed: i64,
    pub failed: i64,
    pub total: i64,
}

#[derive(serde::Serialize)]
pub struct QueueStatusCount {
    pub queue: String,
    pub status: String,
    pub count: i64,
}

#[derive(serde::Serialize)]
pub struct JobTableData {
    pub jobs: Vec<crate::models::JobView>,
    pub selected_queue: String,
    pub selected_source: String,
    pub page: i64,
    pub total: i64,
    pub total_pages: i64,
    pub show_from: i64,
    pub show_to: i64,
    pub is_queue_source: bool,
    pub is_dlq_source: bool,
    pub is_archive_source: bool,
    pub sel_pending: bool,
    pub sel_running: bool,
    pub sel_completed: bool,
    pub sel_failed: bool,
    pub sort_by: String,
    pub sort_dir: String,
    pub sort_link_created_at: String,
    pub sort_link_status: String,
    pub sort_link_attempt: String,
    pub sort_link_reprocess_count: String,
    pub sort_link_run_at: String,
    pub sort_link_updated_at: String,
}

fn compute_dashboard_data(
    queue_counts: &[crate::models::QueueStatusCount],
    dlq_counts: &[crate::models::QueueStatusCount],
    archive_counts: &[crate::models::QueueStatusCount],
    stats: &crate::models::TableStats,
) -> DashboardData {
    let total_pending: i64 = queue_counts.iter().filter(|c| c.status == "pending").map(|c| c.count).sum();
    let total_running: i64 = queue_counts.iter().filter(|c| c.status == "running").map(|c| c.count).sum();
    let total_completed: i64 = queue_counts.iter().filter(|c| c.status == "completed").map(|c| c.count).sum();
    let total_failed: i64 = queue_counts.iter().filter(|c| c.status == "failed").map(|c| c.count).sum();
    let total_dlq: i64 = dlq_counts.iter().map(|c| c.count).sum();
    let total_archive: i64 = archive_counts.iter().map(|c| c.count).sum();

    let queue_names: std::collections::HashSet<&str> = queue_counts.iter().map(|c| c.queue.as_str()).collect();
    let mut sorted_queues: Vec<&str> = queue_names.into_iter().collect();
    sorted_queues.sort();

    let queue_breakdown: Vec<QueueBreakdown> = sorted_queues
        .into_iter()
        .map(|q| {
            let pending: i64 = queue_counts.iter().filter(|c| c.queue == q && c.status == "pending").map(|c| c.count).sum();
            let running: i64 = queue_counts.iter().filter(|c| c.queue == q && c.status == "running").map(|c| c.count).sum();
            let completed: i64 = queue_counts.iter().filter(|c| c.queue == q && c.status == "completed").map(|c| c.count).sum();
            let failed: i64 = queue_counts.iter().filter(|c| c.queue == q && c.status == "failed").map(|c| c.count).sum();
            QueueBreakdown {
                queue: q.to_string(),
                pending,
                running,
                completed,
                failed,
                total: pending + running + completed + failed,
            }
        })
        .collect();

    DashboardData {
        total_pending,
        total_running,
        total_completed,
        total_failed,
        total_dlq,
        total_archive,
        queue_breakdown,
        dlq_counts: dlq_counts.iter().map(|c| QueueStatusCount {
            queue: c.queue.clone(),
            status: c.status.clone(),
            count: c.count,
        }).collect(),
        dead_tuples: stats.dead_tuples,
        live_tuples: stats.live_tuples,
        last_vacuum: stats.last_vacuum.map(|t| t.to_string()).unwrap_or_else(|| "Never".into()),
        last_autovacuum: stats.last_autovacuum.map(|t| t.to_string()).unwrap_or_else(|| "Never".into()),
        total_inserts: stats.total_inserts,
        total_updates: stats.total_updates,
        total_deletes: stats.total_deletes,
    }
}

fn fmt_millis(dt: &chrono::NaiveDateTime) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()
}

fn fmt_millis_opt(dt: &Option<chrono::NaiveDateTime>) -> String {
    match dt {
        Some(d) => fmt_millis(d),
        None => "-".to_string(),
    }
}

fn sort_link(current_by: &str, col: &str, current_dir: &str, queue: &str, source: &str, page: i64) -> String {
    let dir = if current_by == col && current_dir == "asc" { "desc" } else { "asc" };
    let arrow = if current_by == col {
        if current_dir == "asc" { " \u{2191}" } else { " \u{2193}" }
    } else { "" };
    format!("/queues/browse?queue={}&source={}&page={}&sort_by={}&sort_dir={}{}", queue, source, page, col, dir, arrow)
}

fn build_job_table_data(
    jobs: Vec<crate::models::JobView>,
    selected_queue: &str,
    selected_status: Option<&str>,
    selected_source: &str,
    page: i64,
    total: i64,
    sort_by: &str,
    sort_dir: &str,
) -> JobTableData {
    let total_pages = if total > 0 { (total + 50 - 1) / 50 } else { 1 };
    let show_from = if total > 0 { (page - 1) * 50 + 1 } else { 0 };
    let show_to = if page * 50 < total { page * 50 } else { total };
    let status_str = selected_status.unwrap_or("");

    let mut jobs = jobs;
    for job in &mut jobs {
        job.created_at_fmt = Some(fmt_millis(&job.created_at));
        job.run_at_fmt = fmt_millis_opt(&job.run_at);
        job.updated_at_fmt = fmt_millis_opt(&job.updated_at);
    }

    JobTableData {
        jobs,
        selected_queue: selected_queue.to_string(),
        selected_source: selected_source.to_string(),
        page,
        total,
        total_pages,
        show_from,
        show_to,
        is_queue_source: selected_source == "queue",
        is_dlq_source: selected_source == "dlq",
        is_archive_source: selected_source == "archive",
        sel_pending: status_str == "pending",
        sel_running: status_str == "running",
        sel_completed: status_str == "completed",
        sel_failed: status_str == "failed",
        sort_by: sort_by.to_string(),
        sort_dir: sort_dir.to_string(),
        sort_link_created_at: sort_link(sort_by, "created_at", sort_dir, selected_queue, selected_source, page),
        sort_link_status: sort_link(sort_by, "status", sort_dir, selected_queue, selected_source, page),
        sort_link_attempt: sort_link(sort_by, "attempt", sort_dir, selected_queue, selected_source, page),
        sort_link_reprocess_count: sort_link(sort_by, "reprocess_count", sort_dir, selected_queue, selected_source, page),
        sort_link_run_at: sort_link(sort_by, "run_at", sort_dir, selected_queue, selected_source, page),
        sort_link_updated_at: sort_link(sort_by, "updated_at", sort_dir, selected_queue, selected_source, page),
    }
}

pub async fn index() -> impl IntoResponse {
    Redirect::to("/dashboard")
}

pub async fn dashboard(State(state): State<AppState>) -> Result<Html<String>, StatusCode> {
    let (queue_counts, dlq_counts, archive_counts, stats) =
        with_conn(&state.pool, |conn| {
            let qc = queries::get_queue_status_counts(conn);
            let dc = queries::get_dlq_counts(conn);
            let ac = queries::get_archive_counts(conn);
            let st = queries::get_table_stats(conn);
            Ok::<_, String>((qc, dc, ac, st))
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let data = compute_dashboard_data(&queue_counts, &dlq_counts, &archive_counts, &stats);

    let html = templates::DashboardTemplate { data: &data }
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Html(html))
}

pub async fn queue_browse(
    State(state): State<AppState>,
    Query(params): Query<QueueBrowseParams>,
) -> Result<Html<String>, StatusCode> {
    let queue_name = params.queue.clone().unwrap_or_default();
    let status = params.status.clone();
    let source = params.source.as_deref().unwrap_or("queue");
    let page = params.page.unwrap_or(1);
    let sort_by = params.sort_by.as_deref().unwrap_or("created_at");
    let sort_dir = params.sort_dir.as_deref().unwrap_or("desc");

    let (queues, jobs, total) = with_conn(&state.pool, |conn| {
        let qs = queries::get_distinct_queues(conn);
        let js = queries::get_jobs(conn, &queue_name, status.as_deref(), page, 50, source, sort_by, sort_dir);
        let t = queries::count_jobs(conn, &queue_name, status.as_deref(), source);
        Ok::<_, String>((qs, js, t))
    })
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let job_table = build_job_table_data(jobs, &queue_name, status.as_deref(), source, page, total, sort_by, sort_dir);

    let html = templates::QueueTemplate {
        queues: queues.iter().map(|q| (q.as_str(), q == &queue_name)).collect::<Vec<_>>(),
        t: &job_table,
    }
    .render()
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Html(html))
}

pub async fn job_inspect(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Html<String>, StatusCode> {
    let source = params.get("source").cloned().unwrap_or_else(|| "queue".into());

    let job = with_conn(&state.pool, |conn| {
        let job = if source == "auto" {
            queries::find_job_anywhere(conn, id)
        } else {
            queries::get_job(conn, id, &source)
        };
        Ok::<_, String>(job)
    })
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let job = job.ok_or(StatusCode::NOT_FOUND)?;

    let html = templates::JobTemplate { job: &job }
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Html(html))
}

#[derive(Deserialize)]
pub struct ActionParams {
    pub source: Option<String>,
    pub queue: Option<String>,
    pub page: Option<i64>,
}

#[derive(Deserialize)]
pub struct RescheduleParams {
    pub run_at: String,
    pub queue: Option<String>,
    pub page: Option<i64>,
}

pub async fn restart_job(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
    Query(params): Query<ActionParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let queue = params.queue.clone();
    let page = params.page;

    with_conn(&state.pool, |conn| queries::restart_job(conn, id))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut redirect = format!("/queues/browse?queue={}&source=queue", queue.unwrap_or_default());
    if let Some(p) = page {
        redirect.push_str(&format!("&page={}", p));
    }
    Ok::<_, StatusCode>(Redirect::to(&redirect))
}

pub async fn requeue_job(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
    Query(params): Query<ActionParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let source = params.source.clone().unwrap_or_else(|| "dlq".into());
    let queue = params.queue.clone();
    let page = params.page;

    let source_for_url = source.clone();
    with_conn(&state.pool, |conn| match source.as_str() {
        "dlq" => queries::requeue_from_dlq(conn, id),
        "archive" => queries::requeue_from_archive(conn, id),
        _ => Err("Invalid source".into()),
    })
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut redirect = format!(
        "/queues/browse?queue={}&source={}",
        queue.unwrap_or_default(),
        source_for_url
    );
    if let Some(p) = page {
        redirect.push_str(&format!("&page={}", p));
    }
    Ok::<_, StatusCode>(Redirect::to(&redirect))
}

pub async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
    Query(params): Query<ActionParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let queue = params.queue.clone();
    let page = params.page;

    with_conn(&state.pool, |conn| queries::cancel_job(conn, id))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut redirect = format!("/queues/browse?queue={}&source=queue", queue.unwrap_or_default());
    if let Some(p) = page {
        redirect.push_str(&format!("&page={}", p));
    }
    Ok::<_, StatusCode>(Redirect::to(&redirect))
}

pub async fn reschedule_job(
    State(state): State<AppState>,
    Path(id): Path<uuid::Uuid>,
    Query(params): Query<RescheduleParams>,
) -> Result<impl IntoResponse, StatusCode> {
    let run_at = chrono::NaiveDateTime::parse_from_str(&params.run_at, "%Y-%m-%dT%H:%M")
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let queue = params.queue.clone();
    let page = params.page;

    with_conn(&state.pool, |conn| queries::reschedule_job(conn, id, run_at))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut redirect = format!("/queues/browse?queue={}&source=queue", queue.unwrap_or_default());
    if let Some(p) = page {
        redirect.push_str(&format!("&page={}", p));
    }
    Ok::<_, StatusCode>(Redirect::to(&redirect))
}

pub async fn api_dashboard_poll(
    State(state): State<AppState>,
) -> Result<Html<String>, StatusCode> {
    let (queue_counts, dlq_counts, archive_counts, stats) =
        with_conn(&state.pool, |conn| {
            let qc = queries::get_queue_status_counts(conn);
            let dc = queries::get_dlq_counts(conn);
            let ac = queries::get_archive_counts(conn);
            let st = queries::get_table_stats(conn);
            Ok::<_, String>((qc, dc, ac, st))
        })
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let data = compute_dashboard_data(&queue_counts, &dlq_counts, &archive_counts, &stats);

    let counters_html = templates::partials::CountersTemplate { data: &data }
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let stats_html = templates::partials::TableStatsTemplate { data: &data }
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let body = format!(
        "selector #counters\n{}\n\nselector #table-stats\n{}",
        counters_html, stats_html
    );

    Ok(Html(body))
}

pub async fn api_queue_poll(
    State(state): State<AppState>,
    Query(params): Query<QueueBrowseParams>,
) -> Result<Html<String>, StatusCode> {
    let queue_name = params.queue.clone().unwrap_or_default();
    let status = params.status.clone();
    let source = params.source.as_deref().unwrap_or("queue");
    let page = params.page.unwrap_or(1);
    let sort_by = params.sort_by.as_deref().unwrap_or("created_at");
    let sort_dir = params.sort_dir.as_deref().unwrap_or("desc");

    let (jobs, total) = with_conn(&state.pool, |conn| {
        let js = queries::get_jobs(conn, &queue_name, status.as_deref(), page, 50, source, sort_by, sort_dir);
        let t = queries::count_jobs(conn, &queue_name, status.as_deref(), source);
        Ok::<_, String>((js, t))
    })
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let job_table = build_job_table_data(jobs, &queue_name, status.as_deref(), source, page, total, sort_by, sort_dir);

    let jobs_html = templates::partials::JobTableTemplate { t: &job_table }
        .render()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let body = format!("selector #job-table-wrapper\n{}", jobs_html);

    Ok(Html(body))
}

mod templates {
    use askama::Template;
    use super::{DashboardData, JobTableData};

    pub mod partials {
        use askama::Template;
        use super::{DashboardData, JobTableData};

        #[derive(Template)]
        #[template(path = "partials/counters.html")]
        pub struct CountersTemplate<'a> {
            pub data: &'a DashboardData,
        }

        #[derive(Template)]
        #[template(path = "partials/table_stats.html")]
        pub struct TableStatsTemplate<'a> {
            pub data: &'a DashboardData,
        }

        #[derive(Template)]
        #[template(path = "partials/job_table.html")]
        pub struct JobTableTemplate<'a> {
            pub t: &'a JobTableData,
        }
    }

    #[derive(Template)]
    #[template(path = "dashboard.html")]
    pub struct DashboardTemplate<'a> {
        pub data: &'a DashboardData,
    }

    #[derive(Template)]
    #[template(path = "queue.html")]
    pub struct QueueTemplate<'a> {
        pub queues: Vec<(&'a str, bool)>,
        pub t: &'a JobTableData,
    }

    #[derive(Template)]
    #[template(path = "job.html")]
    pub struct JobTemplate<'a> {
        pub job: &'a crate::models::JobView,
    }
}
