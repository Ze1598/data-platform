-- Third logical database in the shared Postgres instance, for Dagster's
-- own instance storage (run storage, event log storage, schedule storage).
-- Needed by K8sRunLauncher specifically: the run pod it launches is a
-- separate process (in-cluster, not the host laptop running `dagster dev`)
-- that must read/write the same run/event state to report status back —
-- a local SQLite DAGSTER_HOME won't be reachable from that pod. Tables are
-- created automatically by dagster-postgres on first connection, not here.
create database dagster_db;
