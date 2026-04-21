mod db;
mod handlers;
mod models;
mod queries;
mod schema;

use axum::{
    routing::{get, post},
    Router,
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

    let database_url =
        std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let pool = db::establish_pool(&database_url);
    let state = AppState { pool };

    let app = Router::new()
        .route("/", get(handlers::index))
        .route("/dashboard", get(handlers::dashboard))
        .route("/queues/browse", get(handlers::queue_browse))
        .route("/jobs/{id}", get(handlers::job_inspect))
        .route("/jobs/{id}/restart", post(handlers::restart_job))
        .route("/jobs/{id}/requeue", post(handlers::requeue_job))
        .route("/api/dashboard/poll", get(handlers::api_dashboard_poll))
        .route("/api/queues/poll", get(handlers::api_queue_poll))
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001")
        .await
        .expect("Failed to bind to 0.0.0.0:3000");

    tracing::info!("Queue Manager running on http://0.0.0.0:3001");
    axum::serve(listener, app).await.unwrap();
}
