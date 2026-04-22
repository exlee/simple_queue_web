diesel::table! {
    job_queue (id) {
        id -> Uuid,
        fingerprint -> Nullable<Text>,
        unique_key -> Nullable<Text>,
        queue -> Varchar,
        job_data -> Nullable<Jsonb>,
        status -> Varchar,
        created_at -> Timestamptz,
        run_at -> Nullable<Timestamptz>,
        updated_at -> Nullable<Timestamptz>,
        completed_at -> Nullable<Timestamptz>,
        attempt -> Int4,
        max_attempts -> Int4,
        reprocess_count -> Int4,
    }
}

diesel::table! {
    job_queue_dlq (id) {
        id -> Uuid,
        fingerprint -> Nullable<Text>,
        unique_key -> Nullable<Text>,
        queue -> Varchar,
        job_data -> Nullable<Jsonb>,
        status -> Varchar,
        created_at -> Timestamptz,
        run_at -> Nullable<Timestamptz>,
        updated_at -> Nullable<Timestamptz>,
        completed_at -> Nullable<Timestamptz>,
        attempt -> Int4,
        max_attempts -> Int4,
        reprocess_count -> Int4,
    }
}

diesel::table! {
    job_queue_archive (id) {
        id -> Uuid,
        fingerprint -> Nullable<Text>,
        unique_key -> Nullable<Text>,
        queue -> Varchar,
        job_data -> Nullable<Jsonb>,
        status -> Varchar,
        created_at -> Timestamptz,
        run_at -> Nullable<Timestamptz>,
        updated_at -> Nullable<Timestamptz>,
        completed_at -> Nullable<Timestamptz>,
        attempt -> Int4,
        max_attempts -> Int4,
        reprocess_count -> Int4,
    }
}
