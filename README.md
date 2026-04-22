# simple-queue-web

Web dashboard for [simple-queue](https://crates.io/crates/simple-queue) -- a persistent job queue backed by PostgreSQL.

Connects directly to the same PostgreSQL database that `simple-queue` uses and provides a browser-based UI to inspect and manage jobs.

**Mostly vibe-coded. Use at your own risk.**

## Features

- Dashboard with per-queue counters (live, DLQ, archive)
- Browse jobs with filtering by queue, status, and source (queue / DLQ / archive)
- Sortable tables with pagination
- Job detail view with formatted JSON payload
- Actions: restart, cancel, reschedule, requeue (from DLQ/archive back to live queue)
- Auto-refreshing counters via polling

## Usage

```
simple-queue-web <database-url>
```

The first argument is the PostgreSQL connection string pointing to the database used by `simple-queue`.

Example:

```
simple-queue-web "postgres://user:pass@localhost:5432/mydb"
```

Alternatively, set the `DATABASE_URL` environment variable.

The server binds to `0.0.0.0:3001` by default.

## Install

```bash
cargo install simple-queue-web
```

## License

MIT
