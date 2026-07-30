# Phase 7 — Model layer: Type 1/Type 2 dims + facts

- [x] Unified technical columns (`_scd_id`, `_valid_from`, `_valid_to`, `_updated_at`, `_is_deleted`) defined via `snapshot_meta_column_names`
- [x] At least one `scd_type=2` dimension: dbt snapshot, `check` strategy — `dim_customer_snapshot`
- [x] At least one `scd_type=1` dimension: plain incremental `merge` model, update-in-place — `dim_branch`
- [x] `int_<feed>_with_deletes.sql` intermediate model for `deletions_enabled` feeds — `int_customers_with_deletes.sql`
- [x] At least one incremental fact model joined to a dimension — `fct_sales` joined to `dim_branch`
- [x] **Verify (Type 2)**: simulated attribute change produces a new version with correct `_valid_from`/`_valid_to`/`_scd_id`
- [x] **Verify (Type 2)**: simulated deletion (full-load feed) produces an `is_deleted=true` new version via the synthetic-update path
- [x] **Verify (Type 1)**: unchanged rerun produces zero new rows/snapshots (true idempotency, not just "same end result")

Notes / deviations:
- **`sales` has no FK to `customers`** — checked the actual data first rather than assuming: `customer_type`/`gender` in `stg_sales` are self-reported transaction attributes, not a join key, mirroring the real "supermarket sales" dataset this mirrors. So `dim_customer` (Type 2) stands alone today, not joined to any fact. Built `dim_branch` (Type 1) instead — conformed out of `sales`' own `branch`/`city` columns, a standard Kimball pattern — and joined `fct_sales` to *that*, giving a real fact→dimension join plus both required SCD types, just not customer→sales specifically. Full reasoning in Learnings.md.
- **Fact joins to `dim_branch._key_hash`, not `_scd_id`** — `_scd_id` is Type-2-specific (a new one per version); `dim_branch` is Type 1 and never versions, so `_scd_id` is structurally always null there. The Roadmap's "join to `_scd_id`" applies when the target dimension is actually Type 2.
- **Deletion detection compares `stg_customers` (cumulative) against `clean.customers` (this run's true full snapshot)**, not the dimension's own current rows — the latter would be a circular `ref()` (`int_customers_with_deletes` sits upstream of `dim_customer_snapshot`). Naturally idempotent anyway: a key deleted in a prior run keeps getting resynthesized with `is_deleted=true` every run, but since its `_attr_hash` doesn't change, the snapshot's `check_cols` gate produces zero new versions — confirmed empirically (2 snapshots, 7 rows, unchanged across a third rerun after the deletion).
- **New `model` Iceberg namespace** required its own `generate_schema_name` macro override (dbt's default would've concatenated it onto the target schema, producing `staging_model`) and its own entry in `polaris_client/bootstrap.py`'s `REQUIRED_NAMESPACES` (preventing the exact same `staging`-creation race found in Phase 6 from recurring for `model`).
- **`accepted_values` doesn't coerce against a native boolean column on `dbt-trino`** (`TYPE_MISMATCH: boolean vs varchar` testing `is_deleted` against `[true, false]`) — dropped, redundant anyway since a boolean column's type already rejects anything else; `not_null` is the meaningful check.
- Full story, including the two infra bugs this also surfaced (`bootstrap_kind.sh`'s API-server readiness gap, a stale pre-fix `.venv`), in Learnings.md.
