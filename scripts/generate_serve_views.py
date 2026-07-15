"""Generates the serve layer's `_latest`/`_historical` dbt view models (plus
their schema.yml test declarations) from `lakehouse_models` — one pair per
fact/dimension row, so nobody hand-authors a view -- or its tests -- per
table (Roadmap.md "Serve layer approach").

Deliberately a standalone build-time script, not a Dagster op: dagster-dbt's
`@dbt_assets` reads `target/manifest.json` at Python-import time, before any
run executes, so these files have to exist on disk *before* `dbt parse`
builds that manifest (Docker image build, or local `dagster dev` startup) --
same category as `dbt parse` itself, not a pipeline step. See Learnings.md
Phase 5, "The manifest must be pre-built into the image".

Fully regenerates `models/serve/generated/` on every run (clears first) so
stale files never linger after a `lakehouse_models` row is removed or
renamed.
"""

import os
import shutil
from pathlib import Path

import psycopg

CONN_KWARGS = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    user=os.environ.get("POSTGRES_USER", "platform"),
    password=os.environ.get("POSTGRES_PASSWORD", "platform"),
    dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
)

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "dbt" / "data_platform" / "models" / "serve" / "generated"


def _render_view(*, model_name: str, owning_feed: str, filter_to_current: bool) -> str:
    # materialized='view' comes from dbt_project.yml's `serve:` block, not
    # repeated here. schema='serve' IS repeated here (matching model/*.sql's
    # own per-file config(schema='model', ...) pattern, not a project-level
    # default) -- without it, generate_schema_name.sql falls back to the
    # target's default schema (staging), landing every generated view in
    # the wrong namespace even though it builds and tests clean.
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
    # Second tag ('serving_layer') is what _build_serving_assets_for_feed/
    # _build_transformation_assets_for_feed (dbt_assets.py) intersect/exclude
    # on to split transformation from serving into two independently
    # selectable dbt build invocations -- a `path:` selector was tried first
    # and confirmed NOT to work here: @dbt_assets resolves select=/exclude=
    # in-process against a synthetic Manifest object with no real project
    # root, so `path:` (which needs to resolve a relative filesystem path)
    # silently matches nothing, while `tag:` (pure attribute matching, no
    # filesystem context needed) works reliably. Confirmed directly: `dbt
    # ls --select` (the real CLI, different code path) matched `path:`
    # correctly, but the actual in-process @dbt_assets resolution did not --
    # don't trust `dbt ls` alone to validate a selector used inside
    # @dbt_assets(select=...).
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
        select lm.friendly_name, lm.scd_type, df.friendly_name as owning_feed
        from lakehouse_models lm
        join data_feed df on df.id = lm.owning_feed_id
        where lm.is_active = true
          and '3' = ANY(string_to_array(lm.pipeline_steps, ','))
        order by lm.friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def fetch_ods_feeds(cur) -> list[dict]:
    # ODS tables (scripts/generate_ods_models.py) have no lakehouse_models
    # row -- consumers still need standard serve views over them though
    # (consumers never touch `model` directly, see the ODS design,
    # Roadmap.md), so this is a second, parallel candidate source feeding
    # the same generate() loop below. scd_type is always 1 (ODS is always
    # Type 1) -- both generated views collapse to identical content, same
    # Type-1-collapse rule any lakehouse_models row already gets.
    # has_primary_key distinguishes a keyed ODS table (has _key_hash) from
    # an insert-only one (doesn't) -- lakehouse_models-sourced rows always
    # have a real _key_hash, so they don't carry this field at all
    # (generate() defaults it to True for them). Same not-exists guard as
    # generate_ods_models.py's own candidate query, so a feed with both
    # ods_enabled=true and a real lakehouse_models row never double-generates.
    cur.execute(
        """
        select df.friendly_name || '_ods' as friendly_name,
               1 as scd_type,
               df.friendly_name as owning_feed,
               (jsonb_array_length(sr.primary_key_columns) > 0) as has_primary_key
        from data_feed df
        join schema_registry sr on sr.data_feed_id = df.id and sr.is_current
        where df.ods_enabled = true
          and df.is_active = true
          and not exists (select 1 from lakehouse_models lm where lm.owning_feed_id = df.id)
        order by df.friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


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


def generate(candidates: list[dict], output_dir: Path) -> list[Path]:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    written = []
    views: list[tuple[str, bool]] = []
    for row in candidates:
        name, scd_type, owning_feed = row["friendly_name"], row["scd_type"], row["owning_feed"]
        has_primary_key = row.get("has_primary_key", True)

        latest_name = f"{name}_latest"
        latest_path = output_dir / f"{latest_name}.sql"
        latest_path.write_text(_render_view(model_name=name, owning_feed=owning_feed, filter_to_current=scd_type == 2))
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

    written = generate(candidates, OUTPUT_DIR)
    print(f"Generated {len(written)} file(s) ({2 * len(candidates)} views + schema.yml) for {len(candidates)} lakehouse_models/ods_enabled row(s) in {OUTPUT_DIR}.")


if __name__ == "__main__":
    main()
