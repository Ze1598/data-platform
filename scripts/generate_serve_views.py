"""Generates the serve layer's `_latest`/`_historical` dbt view models (plus
their schema.yml test declarations) from `model_feed` — one pair per
fact/dimension row, so nobody hand-authors a view -- or its tests -- per
table (Roadmap.md "Serve layer approach").

Deliberately a standalone build-time script, not a Dagster op: dagster-dbt's
`@dbt_assets` reads `target/manifest.json` at Python-import time, before any
run executes, so these files have to exist on disk *before* `dbt parse`
builds that manifest (Docker image build, or local `dagster dev` startup) --
same category as `dbt parse` itself, not a pipeline step. See Learnings.md
Phase 5, "The manifest must be pre-built into the image".

Fully regenerates `models/serve/generated/` on every run (clears first) so
stale files never linger after a `model_feed` row is removed or renamed.
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


def _render_view(*, model_code: str, feed_tag: str, filter_to_current: bool) -> str:
    # materialized='view' comes from dbt_project.yml's `serve:` block, not
    # repeated here. schema='serve' IS repeated here (matching model/*.sql's
    # own per-file config(schema='model', ...) pattern, not a project-level
    # default) -- without it, generate_schema_name.sql falls back to the
    # target's default schema (staging), landing every generated view in
    # the wrong namespace even though it builds and tests clean.
    where_clause = "\nwhere _valid_to is null" if filter_to_current else ""
    return (
        f"{{{{ config(schema='serve', tags=['{feed_tag}']) }}}}\n\n"
        f"select * from {{{{ ref('{model_code}') }}}}{where_clause}\n"
    )


def fetch_model_feeds(cur) -> list[dict]:
    cur.execute(
        """
        select mf.code, mf.scd_type, df.code as feed_tag
        from model_feed mf
        join data_feed df on df.id = mf.staging_source_data_feed_id
        where mf.is_active = true
        order by mf.code
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _render_schema_yml(view_names: list[str]) -> str:
    # Generated alongside the views themselves, not hand-authored --
    # otherwise a new model_feed row would need its test coverage added by
    # hand in a completely different file, exactly the per-table manual
    # step this codegen exists to avoid. _key_hash not_null is the one
    # check that applies uniformly to every generated view regardless of
    # scd_type (the base tables already carry the full test suite; this is
    # just confirming the passthrough didn't silently drop rows/columns).
    lines = ["version: 2", "", "models:"]
    for name in view_names:
        lines += [
            f"  - name: {name}",
            "    columns:",
            "      - name: _key_hash",
            "        tests: [not_null]",
        ]
    return "\n".join(lines) + "\n"


def generate(model_feeds: list[dict], output_dir: Path) -> list[Path]:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    written = []
    view_names = []
    for row in model_feeds:
        code, scd_type, feed_tag = row["code"], row["scd_type"], row["feed_tag"]

        latest_name = f"{code}_latest"
        latest_path = output_dir / f"{latest_name}.sql"
        latest_path.write_text(_render_view(model_code=code, feed_tag=feed_tag, filter_to_current=scd_type == 2))
        written.append(latest_path)
        view_names.append(latest_name)

        historical_name = f"{code}_historical"
        historical_path = output_dir / f"{historical_name}.sql"
        historical_path.write_text(_render_view(model_code=code, feed_tag=feed_tag, filter_to_current=False))
        written.append(historical_path)
        view_names.append(historical_name)

    schema_path = output_dir / "schema.yml"
    schema_path.write_text(_render_schema_yml(view_names))
    written.append(schema_path)

    return written


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        model_feeds = fetch_model_feeds(cur)

    written = generate(model_feeds, OUTPUT_DIR)
    print(f"Generated {len(written)} file(s) ({2 * len(model_feeds)} views + schema.yml) for {len(model_feeds)} model_feed row(s) in {OUTPUT_DIR}.")


if __name__ == "__main__":
    main()
