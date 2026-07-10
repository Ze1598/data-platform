# Debug Reference: tests/integration

Regression tests against the *live* platform (Trino/Postgres), not unit tests with mocks — same verification philosophy as everywhere else in this project (see the other modules' `Learnings.md`/`DebugReference.md` entries). Requires the cluster up and reachable (`localhost:8080` Trino, `localhost:5432` Postgres via NodePort).

### Run the suite
```bash
cd tests/integration
TRINO_HOST=localhost TRINO_PORT=8080 POSTGRES_HOST=localhost POSTGRES_USER=platform POSTGRES_PASSWORD=platform POSTGRES_PORT=5432 \
  ../../.venv/bin/pytest -v
```

### What's covered so far
- `test_utc_consistency.py` — every `schema_registry` column with `data_type: "timestamp"` must be `with time zone` in both `clean.<feed>` and `staging.<staging_table_name>`, for every currently active feed (metadata-driven, not a hardcoded feed list). Written after finding `clean.customers` was naive while `clean.sales` was correctly tz-aware — same logical bug, two different code paths (see Learnings.md, Phase 6). Also specifically covers the *stale pre-existing table* failure mode: dbt's incremental `MERGE` doesn't retroactively fix an existing table's column types, only a fresh `CREATE TABLE AS SELECT` does — fixing the writer isn't enough by itself if an old table is still sitting there.

### Adding a new regression test
Follow the same shape: prefer metadata-driven checks (query `data_feed`/`schema_registry`, assert against whatever's *actually* configured) over hardcoding feed names, so new feeds get covered automatically rather than requiring someone to remember to add a test for them.
