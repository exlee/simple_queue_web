use diesel::PgConnection;
use diesel::prelude::*;

pub fn establish_test_connection() -> PgConnection {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/simple_queue".into());
    let mut conn = PgConnection::establish(&database_url)
        .unwrap_or_else(|e| panic!("Failed to connect to {}: {}", database_url, e));
    conn.begin_test_transaction()
        .expect("Failed to begin test transaction");
    crate::queries::set_statement_timeout(&mut conn);
    conn
}

pub fn insert_test_job(
    conn: &mut PgConnection,
    queue: &str,
    status: &str,
    attempt: i32,
    max_attempts: i32,
    reprocess_count: i32,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    diesel::insert_into(crate::schema::job_queue::table)
        .values((
            crate::schema::job_queue::id.eq(id),
            crate::schema::job_queue::queue.eq(queue),
            crate::schema::job_queue::status.eq(status),
            crate::schema::job_queue::attempt.eq(attempt),
            crate::schema::job_queue::max_attempts.eq(max_attempts),
            crate::schema::job_queue::reprocess_count.eq(reprocess_count),
        ))
        .execute(conn)
        .expect("Failed to insert test job");
    id
}

pub fn insert_test_dlq_job(
    conn: &mut PgConnection,
    queue: &str,
    status: &str,
    attempt: i32,
    max_attempts: i32,
    reprocess_count: i32,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    diesel::insert_into(crate::schema::job_queue_dlq::table)
        .values((
            crate::schema::job_queue_dlq::id.eq(id),
            crate::schema::job_queue_dlq::queue.eq(queue),
            crate::schema::job_queue_dlq::status.eq(status),
            crate::schema::job_queue_dlq::attempt.eq(attempt),
            crate::schema::job_queue_dlq::max_attempts.eq(max_attempts),
            crate::schema::job_queue_dlq::reprocess_count.eq(reprocess_count),
        ))
        .execute(conn)
        .expect("Failed to insert test DLQ job");
    id
}

pub fn insert_test_archive_job(
    conn: &mut PgConnection,
    queue: &str,
    status: &str,
    attempt: i32,
    max_attempts: i32,
    reprocess_count: i32,
) -> uuid::Uuid {
    let id = uuid::Uuid::new_v4();
    diesel::insert_into(crate::schema::job_queue_archive::table)
        .values((
            crate::schema::job_queue_archive::id.eq(id),
            crate::schema::job_queue_archive::queue.eq(queue),
            crate::schema::job_queue_archive::status.eq(status),
            crate::schema::job_queue_archive::attempt.eq(attempt),
            crate::schema::job_queue_archive::max_attempts.eq(max_attempts),
            crate::schema::job_queue_archive::reprocess_count.eq(reprocess_count),
        ))
        .execute(conn)
        .expect("Failed to insert test archive job");
    id
}

pub fn existing_job_queue_count(conn: &mut PgConnection) -> i64 {
    crate::schema::job_queue::table
        .count()
        .get_result(conn)
        .unwrap_or(0)
}
