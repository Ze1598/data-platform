"""Generates the model layer's deletion-synthesis intermediate models
(`int_<table_name>_with_deletes.sql`) from `lakehouse_models` +
`schema_registry` -- one per `deletes_enabled=true` lakehouse_models row, so
nobody hand-authors this model per feed (Roadmap.md "Deletion mechanism").
Lands inside the row's own domain project
(dbt/domains/<model_schema>/models/model/dimensions/generated/), not a
single shared project -- see Roadmap.md "multi-project dbt split". Filename
keyed by table_name (not the dependent feed's name): domains are separate
dbt projects with no cross-project ref(), so two lakehouse_models rows in
different domains that happen to depend on the same feed each need their
own copy -- table_name is unique per row by construction, so this also
removes the old feed-name-keyed dedup collision case entirely (see git
history for the prior "two rows against the same feed would collide"
limitation).

Deliberately a standalone build-time script, not a Dagster op — same
reasoning as generate_serve_views.py (dagster-dbt's `@dbt_assets` reads
target/manifest.json at Python-import time, before any run executes, so
these files have to exist on disk before `dbt parse` builds that manifest).

Column lists come from schema_registry.column_definitions (the current
version for the lakehouse_models row's single dependent data_feed), not live
Trino introspection -- keeps this resolvable from Postgres alone at build
time, consistent with every other codegen step in this project, and avoids
the composite-key Jinja gymnastics a dynamic
`adapter.get_columns_in_relation()` approach would need.

Deletion synthesis inherently compares one feed's cumulative staging against
that same feed's latest clean snapshot -- it requires exactly one entry in
depends_on_feeds. A deletes_enabled row with zero or multiple dependent
feeds has no well-defined "the" clean source to compare against, so it's a
hard error, not a silent skip (no such row exists today; this generator
can't correctly handle one until that concept is designed).

Fully regenerates each domain's `models/model/dimensions/generated/` on
every run (clears first) so stale files never linger after a
lakehouse_models row's deletes_enabled flips or a row is removed.
"""

import os
import shutil
from pathlib import Path

import psycopg

from generate_domain_projects import slugify_domain

CONN_KWARGS = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    user=os.environ.get("POSTGRES_USER", "platform"),
    password=os.environ.get("POSTGRES_PASSWORD", "platform"),
    dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DOMAINS_DIR = REPO_ROOT / "dbt" / "domains"


def fetch_deletion_synthesis_feeds(cur) -> list[dict]:
    cur.execute(
        """
        select lm.friendly_name as model_name, lm.table_name, lm.model_schema,
               lm.depends_on_feeds, lm.business_key_columns
        from lakehouse_models lm
        where lm.is_active = true and lm.deletes_enabled = true
        order by lm.friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    rows = [dict(zip(columns, row)) for row in cur.fetchall()]

    dependency_ids: dict[str, list[str]] = {}
    for row in rows:
        ids = [v for v in (row["depends_on_feeds"] or "").split(",") if v]
        if len(ids) != 1:
            raise ValueError(
                f"lakehouse_models '{row['model_name']}' has deletes_enabled=true but "
                f"depends_on_feeds resolves to {len(ids)} feed(s) ({row['depends_on_feeds']!r}) -- "
                "deletion synthesis requires exactly one."
            )
        dependency_ids[row["model_name"]] = ids

    all_ids = {fid for ids in dependency_ids.values() for fid in ids}
    cur.execute("select id, friendly_name from data_feed where id::text = any(%s)", (list(all_ids),))
    feed_names = {str(fid): name for fid, name in cur.fetchall()}

    feeds = []
    for row in rows:
        (feed_id,) = dependency_ids[row["model_name"]]
        feeds.append(
            {
                "table_name": row["table_name"],
                "domain": slugify_domain(row["model_schema"]),
                "feed_id": feed_id,
                "feed_friendly_name": feed_names[feed_id],
                "business_key_columns": row["business_key_columns"],
            }
        )
    return feeds


def fetch_current_columns(cur, data_feed_id: str) -> list[str]:
    cur.execute(
        "select column_definitions from schema_registry where data_feed_id = %s and is_current",
        (data_feed_id,),
    )
    row = cur.fetchone()
    if row is None:
        raise ValueError(f"No current schema_registry entry for data_feed_id={data_feed_id!r}")
    return [c["name"] for c in sorted(row[0], key=lambda c: c["ordinal"])]


def _render_model(*, feed_friendly_name: str, all_columns: list[str], business_key_columns: list[str]) -> str:
    cols_csv = ", ".join(all_columns)
    keys_csv = ", ".join(business_key_columns)
    return f"""{{{{ config(materialized='view', tags=['{feed_friendly_name}']) }}}}

{{#
    Deletion synthesis for a deletes_enabled lakehouse_models row (see
    Roadmap.md "Deletion mechanism"). Generated by
    scripts/generate_deletion_synthesis_views.py from lakehouse_models +
    schema_registry -- not hand-authored, so a second deletes_enabled
    model doesn't need its own copy-pasted model.

    Compares stg_{feed_friendly_name} (every business key ever seen,
    cumulative -- staging never shrinks) against clean.{feed_friendly_name}
    (this run's true full snapshot -- clean is a fresh per-run load, not
    cumulative, see Roadmap.md "Layer Model"). A key in staging but missing
    from clean's current run is a deletion; its last-known attributes are
    carried forward from staging with is_deleted=true.

    Deliberately does NOT read the downstream Type-2 snapshot's own
    current state to check "is this key already marked deleted" -- that
    would be a circular ref() (this model feeds the snapshot, not the
    other way around). Not needed anyway: a repeatedly-synthesized
    is_deleted=true row for an already-deleted key has the same
    _attr_hash every run (tracked columns are frozen once deleted, and
    is_deleted stays true), so the snapshot's check_cols gate naturally
    produces zero new versions for it. Same idempotency mechanism already
    doing the work, not extra logic.
#}}

with all_known_keys as (

    select {cols_csv}
    from {{{{ ref('stg_{feed_friendly_name}') }}}}

),

current_source_keys as (

    select {keys_csv}
    from {{{{ source('clean', '{feed_friendly_name}') }}}}

),

active as (

    select {cols_csv}, false as is_deleted
    from all_known_keys
    where ({keys_csv}) in (select {keys_csv} from current_source_keys)

),

deleted as (

    select {cols_csv}, true as is_deleted
    from all_known_keys
    where ({keys_csv}) not in (select {keys_csv} from current_source_keys)

)

select * from active
union all
select * from deleted
"""


def generate(feeds: list[dict], columns_by_feed_id: dict[str, list[str]], domains_dir: Path) -> list[Path]:
    by_domain: dict[str, list[dict]] = {}
    for row in feeds:
        by_domain.setdefault(row["domain"], []).append(row)

    written = []
    for domain, rows in by_domain.items():
        output_dir = domains_dir / domain / "models" / "model" / "dimensions" / "generated"
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)

        for row in rows:
            path = output_dir / f"int_{row['table_name']}_with_deletes.sql"
            path.write_text(
                _render_model(
                    feed_friendly_name=row["feed_friendly_name"],
                    all_columns=columns_by_feed_id[row["feed_id"]],
                    business_key_columns=row["business_key_columns"],
                )
            )
            written.append(path)

    return written


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        feeds = fetch_deletion_synthesis_feeds(cur)
        columns_by_feed_id = {row["feed_id"]: fetch_current_columns(cur, row["feed_id"]) for row in feeds}

    written = generate(feeds, columns_by_feed_id, DOMAINS_DIR)
    print(f"Generated {len(written)} deletion-synthesis model(s) for {len(feeds)} deletes_enabled lakehouse_models row(s).")


if __name__ == "__main__":
    main()
