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


def _render_schema_yml(view_names: list[str]) -> str:
    # Generated alongside the views themselves, not hand-authored --
    # otherwise a new lakehouse_models row would need its test coverage
    # added by hand in a completely different file, exactly the per-table
    # manual step this codegen exists to avoid. _key_hash not_null is the
    # one check that applies uniformly to every generated view regardless
    # of scd_type (the base tables already carry the full test suite; this
    # is just confirming the passthrough didn't silently drop rows/columns).
    lines = ["version: 2", "", "models:"]
    for name in view_names:
        lines += [
            f"  - name: {name}",
            "    columns:",
            "      - name: _key_hash",
            "        tests: [not_null]",
        ]
    return "\n".join(lines) + "\n"


def generate(lakehouse_models: list[dict], output_dir: Path) -> list[Path]:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    written = []
    view_names = []
    for row in lakehouse_models:
        name, scd_type, owning_feed = row["friendly_name"], row["scd_type"], row["owning_feed"]

        latest_name = f"{name}_latest"
        latest_path = output_dir / f"{latest_name}.sql"
        latest_path.write_text(_render_view(model_name=name, owning_feed=owning_feed, filter_to_current=scd_type == 2))
        written.append(latest_path)
        view_names.append(latest_name)

        historical_name = f"{name}_historical"
        historical_path = output_dir / f"{historical_name}.sql"
        historical_path.write_text(_render_view(model_name=name, owning_feed=owning_feed, filter_to_current=False))
        written.append(historical_path)
        view_names.append(historical_name)

    schema_path = output_dir / "schema.yml"
    schema_path.write_text(_render_schema_yml(view_names))
    written.append(schema_path)

    return written


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        lakehouse_models = fetch_lakehouse_models(cur)

    written = generate(lakehouse_models, OUTPUT_DIR)
    print(f"Generated {len(written)} file(s) ({2 * len(lakehouse_models)} views + schema.yml) for {len(lakehouse_models)} lakehouse_models row(s) in {OUTPUT_DIR}.")


if __name__ == "__main__":
    main()
