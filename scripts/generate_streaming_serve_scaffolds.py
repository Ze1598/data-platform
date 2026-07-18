"""Generates a bare serve-view scaffold (plus its source() declaration
companion) for each active `streaming_source` row whose target file
doesn't exist yet on disk -- Roadmap Phase 11 generalization, the direct
streaming analog of generate_model_scaffolds.py's own pattern for batch
dimension/fact models: pre-fill what metadata can derive, leave the real
business logic (which model-layer dimension(s) to join, on what key) as a
TODO placeholder, and never touch an existing target file again, forever,
even after its streaming_source row is later deactivated.

Deliberately a BARE scaffold with no pre-filled join -- same reasoning
generate_model_scaffolds.py already gives for not auto-deriving a fact's
join to a dimension ("no metadata describes which dimension a fact should
join to or on what key"): nothing in streaming_source names a target
dimension either, so the scaffold is just a source() reference and a
`SELECT *` TODO.

Deliberately UNLIKE generate_streaming_ingestion.py (100% generated,
safe to wipe-and-regenerate every run) -- this script's output becomes a
permanent mix of generated skeleton and hand-written join logic in the
SAME file once a user fills in the TODO, so an existing target is left
completely untouched.
"""

import os
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


def fetch_candidate_rows(cur) -> list[dict]:
    cur.execute(
        """
        select friendly_name, table_name, model_schema
        from streaming_source
        where is_active = true
        order by friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def target_dir(row: dict) -> Path:
    return DOMAINS_DIR / slugify_domain(row["model_schema"]) / "models" / "serve" / "streaming"


def _render_scaffold(row: dict) -> str:
    # Three tags, not one -- tags=['streaming'] alone (blanket exclusion
    # from the batch build graph, see dbt_assets.py's _STREAMING_TAG) is
    # necessary but not sufficient: 'streaming_<model_schema>' and
    # 'streaming_<table_name>' let one domain's or one specific stream's
    # serve view be selected/excluded independently, the same granularity
    # generate_model_scaffolds.py already gives per-feed batch models via
    # tag:<feed>. Prefixed, not the bare model_schema/table_name, for the
    # same reason domain_group_name() prefixes with 'domain_' (dbt_assets.py)
    # -- a bare model_schema tag would collide with a same-named feed's own
    # per-feed tag (e.g. the 'sales' domain vs. the 'sales' feed).
    tags = ["streaming", f"streaming_{row['model_schema']}", f"streaming_{row['table_name']}"]
    return f"""{{{{ config(schema='serve', materialized='view', tags={tags}) }}}}

{{#
    TODO: describe this streaming view's real business logic here.
    Generated scaffold (scripts/generate_streaming_serve_scaffolds.py).
    Join to whichever model-layer dimension(s) make this stream useful
    (e.g. ref('sales_dim_branch')) -- no metadata describes which
    dimension or join key to use, same reasoning
    generate_model_scaffolds.py already gives for fact->dimension joins;
    that's real business logic.

    friendly_name (display label): {row['friendly_name']}
    source table:                  streaming.{row['table_name']}
#}}

-- TODO: verify/adjust -- replace with the real business-logic select.
select * from {{{{ source('streaming', '{row['table_name']}') }}}}
"""


def _render_source_yml(table_name: str) -> str:
    # One file per source, not a shared file every source appends to --
    # same YAML-merge collision risk generate_model_scaffolds.py's own
    # docstring already flags for a different case (dbt hard-errors if a
    # model's name appears in schema.yml property blocks in two separate
    # files) -- a shared, growing file every regeneration run would touch
    # doesn't have that specific risk, but keeping one declaration per
    # source file avoids any future temptation to regenerate it wholesale
    # and lets a source be removed by deleting its own file, not editing
    # a shared one.
    return f"""version: 2
sources:
  - name: streaming
    database: iceberg
    schema: streaming
    tables:
      - name: {table_name}
"""


def generate(rows: list[dict]) -> tuple[list[Path], list[Path]]:
    written, skipped = [], []
    for row in rows:
        out_dir = target_dir(row)
        sql_path = out_dir / f"{row['table_name']}.sql"
        yml_path = out_dir / f"_streaming_sources_{row['table_name']}.yml"

        if sql_path.exists():
            skipped.append(sql_path)
            continue

        out_dir.mkdir(parents=True, exist_ok=True)
        sql_path.write_text(_render_scaffold(row))
        written.append(sql_path)

        if not yml_path.exists():
            yml_path.write_text(_render_source_yml(row["table_name"]))
            written.append(yml_path)

    return written, skipped


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        rows = fetch_candidate_rows(cur)

    written, skipped = generate(rows)
    print(
        f"Scaffolded {len(written)} new file(s) (serve view + source .yml); "
        f"left {len(skipped)} existing target(s) untouched, out of {len(rows)} active "
        f"streaming_source row(s)."
    )
    for p in written:
        print(f"  created: {p.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
