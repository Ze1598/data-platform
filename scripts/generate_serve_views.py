"""Generates the serve layer's `_latest`/`_historical` dbt view models (plus
their schema.yml test declarations) from `lakehouse_models` — one pair per
fact/dimension row, so nobody hand-authors a view -- or its tests -- per
table (Roadmap.md "Serve layer approach"). Views land inside the owning
domain's own dbt project (dbt/domains/<domain>/models/serve/generated/),
not a single shared project -- see Roadmap.md "multi-project dbt split".

Deliberately a standalone build-time script, not a Dagster op: dagster-dbt's
`@dbt_assets` reads `target/manifest.json` at Python-import time, before any
run executes, so these files have to exist on disk *before* `dbt parse`
builds that manifest (Docker image build, or local `dagster dev` startup) --
same category as `dbt parse` itself, not a pipeline step. See Learnings.md
Phase 5, "The manifest must be pre-built into the image".

Fully regenerates each domain's `models/serve/generated/` on every run
(clears first) so stale files never linger after a `lakehouse_models` row
is removed or renamed.
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


def _render_view(*, model_name: str, owning_feed: str, filter_to_current: bool) -> str:
    # materialized='view' comes from dbt_project.yml's `serve:` block, not
    # repeated here. schema='serve' IS repeated here (matching model
    # files' own per-file config(schema='model', ...) pattern, not a
    # project-level default) -- without it, generate_schema_name.sql
    # falls back to the target's default schema (staging), landing every
    # generated view in the wrong namespace even though it builds and
    # tests clean. Fixed literal, independent of which domain this is --
    # physical schema names are pipeline-stage boundaries, not domain
    # boundaries (see metadata/DataModel.md).
    #
    # First tag = exactly the model's owning_feed_id (looked up via
    # lakehouse_models itself, not derived here) -- never every
    # depends_on_feeds member. Tagging with more than one *feed* would make
    # dbt select this view under *multiple* per-feed dbt_assets defs, each
    # independently claiming the same AssetKey, which Dagster rejects at
    # Definitions-construction time (confirmed for real: this broke `just
    # smoketest` for fct_daily_financial_activity_latest/_historical before
    # owning_feed_id existed). depends_on_feeds itself is unaffected and
    # keeps every dependent feed, for gating/updates_enabled-sourcing
    # purposes -- see Learnings.md, "A dbt model tagged with two feed tags
    # gets claimed by two competing @dbt_assets defs".
    #
    # Second tag ('serving_layer') is what _build_serving_assets_for_domain/
    # _build_transformation_assets_for_domain (dbt_assets.py) intersect/exclude
    # on to split transformation from serving into two independently
    # selectable dbt build invocations.
    where_clause = "\nwhere _valid_to is null" if filter_to_current else ""
    return (
        f"{{{{ config(schema='serve', tags=['{owning_feed}', 'serving_layer']) }}}}\n\n"
        f"select * from {{{{ ref('{model_name}') }}}}{where_clause}\n"
    )


def fetch_lakehouse_models(cur) -> list[dict]:
    # A model with 'serving' (pipeline_steps id 3) deselected simply never
    # gets its _latest/_historical views generated -- resolved here, at
    # codegen time, not live per-run (unlike data_feed.pipeline_steps'
    # extraction/validation gates) -- see metadata/DataModel.md's
    # `pipeline_steps` section and the master pipeline design.
    cur.execute(
        """
        select lm.table_name, lm.model_schema, lm.scd_type, df.friendly_name as owning_feed
        from lakehouse_models lm
        join data_feed df on df.id = lm.owning_feed_id
        where lm.is_active = true
          and '3' = ANY(string_to_array(lm.pipeline_steps, ','))
        order by lm.table_name
        """
    )
    rows = []
    for table_name, model_schema, scd_type, owning_feed in cur.fetchall():
        rows.append(
            {
                "model_name": table_name,
                "domain": slugify_domain(model_schema),
                "scd_type": scd_type,
                "owning_feed": owning_feed,
                "has_primary_key": True,
            }
        )
    return rows


def fetch_ods_feeds(cur) -> list[dict]:
    # ODS tables (scripts/generate_ods_models.py) have no lakehouse_models
    # row -- consumers still need standard serve views over them though
    # (consumers never touch `model` directly, see the ODS design,
    # Roadmap.md), so this is a second, parallel candidate source feeding
    # the same generate() loop below. domain comes from batch_ods_name,
    # not model_schema -- each batch group producing ODS output is its own
    # legitimate ODS domain (see data_feed.batch_ods_name,
    # metadata/DataModel.md). scd_type is always 1 (ODS is always Type 1)
    # -- both generated views collapse to identical content, same
    # Type-1-collapse rule any lakehouse_models row already gets.
    # has_primary_key distinguishes a keyed ODS table (has _key_hash) from
    # an insert-only one (doesn't). Same not-exists guard as
    # generate_ods_models.py's own candidate query, so a feed with both
    # ods_enabled=true and a real lakehouse_models row never double-generates.
    cur.execute(
        """
        select df.friendly_name || '_ods' as model_name,
               df.batch_ods_name,
               1 as scd_type,
               df.friendly_name as owning_feed,
               (jsonb_array_length(sr.primary_key_columns) > 0) as has_primary_key
        from data_feed df
        join schema_registry sr on sr.data_feed_id = df.id and sr.is_current
        where df.ods_enabled = true
          and df.is_active = true
          and df.batch_ods_name is not null
          and not exists (select 1 from lakehouse_models lm where lm.owning_feed_id = df.id)
        order by df.friendly_name
        """
    )
    rows = []
    for model_name, batch_ods_name, scd_type, owning_feed, has_primary_key in cur.fetchall():
        rows.append(
            {
                "model_name": model_name,
                "domain": slugify_domain(batch_ods_name),
                "scd_type": scd_type,
                "owning_feed": owning_feed,
                "has_primary_key": has_primary_key,
            }
        )
    return rows


def _render_schema_yml(views: list[tuple[str, bool]]) -> str:
    # Generated alongside the views themselves, not hand-authored --
    # otherwise a new lakehouse_models row would need its test coverage
    # added by hand in a completely different file, exactly the per-table
    # manual step this codegen exists to avoid. _key_hash not_null is the
    # one check that applies uniformly to every generated view regardless
    # of scd_type (the base tables already carry the full test suite; this
    # is just confirming the passthrough didn't silently drop rows/columns)
    # -- except an insert-only ODS view, which has no _key_hash column at
    # all, so emitting that test for one would compile fine but fail for
    # real at `dbt build` time with a genuine "column does not exist"
    # error from Trino. has_key_hash skips the test block in that case.
    lines = ["version: 2", "", "models:"]
    for name, has_key_hash in views:
        lines.append(f"  - name: {name}")
        if has_key_hash:
            lines += [
                "    columns:",
                "      - name: _key_hash",
                "        tests: [not_null]",
            ]
    return "\n".join(lines) + "\n"


def generate(candidates: list[dict], domains_dir: Path) -> list[Path]:
    by_domain: dict[str, list[dict]] = {}
    for row in candidates:
        by_domain.setdefault(row["domain"], []).append(row)

    written = []
    for domain, rows in by_domain.items():
        output_dir = domains_dir / domain / "models" / "serve" / "generated"
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)

        views: list[tuple[str, bool]] = []
        for row in rows:
            name, scd_type, owning_feed = row["model_name"], row["scd_type"], row["owning_feed"]
            has_primary_key = row["has_primary_key"]

            latest_name = f"{name}_latest"
            latest_path = output_dir / f"{latest_name}.sql"
            latest_path.write_text(
                _render_view(model_name=name, owning_feed=owning_feed, filter_to_current=scd_type == 2)
            )
            written.append(latest_path)
            views.append((latest_name, has_primary_key))

            historical_name = f"{name}_historical"
            historical_path = output_dir / f"{historical_name}.sql"
            historical_path.write_text(_render_view(model_name=name, owning_feed=owning_feed, filter_to_current=False))
            written.append(historical_path)
            views.append((historical_name, has_primary_key))

        schema_path = output_dir / "schema.yml"
        schema_path.write_text(_render_schema_yml(views))
        written.append(schema_path)

    return written


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        candidates = fetch_lakehouse_models(cur) + fetch_ods_feeds(cur)

    written = generate(candidates, DOMAINS_DIR)
    print(f"Generated {len(written)} file(s) ({2 * len(candidates)} views + schema.yml per domain) for {len(candidates)} lakehouse_models/ods_enabled row(s).")
    for p in written:
        if p.name == "schema.yml":
            print(f"  {p.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
