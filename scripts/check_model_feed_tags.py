"""Checks that every `lakehouse_models` row's `owning_feed_id` actually
matches the dbt tag on its compiled model/snapshot node -- these two things
are supposed to be a single fact (which feed's job claims this model's
AssetKey), but nothing enforces that a human editing a model's `.sql` file
by hand keeps its `tags=[...]` in sync with the `owning_feed_id` column
(see Backlog.md, "No automated check that a dbt model's tags=[...] actually
matches its owning_feed_id"). A drift here isn't silent data corruption --
it fails loudly as a Definitions-construction crash the moment two models
tag the same feed and collide over one AssetKey (see Learnings.md, "A dbt
model tagged with two feed tags gets claimed by two competing @dbt_assets
defs") -- but that failure only surfaces at `just smoketest` time, long
after the mistake was made. This catches it immediately, against whichever
domains have already been `dbt parse`'d.

Deliberately a standalone script with live Postgres access, run via `just`
after each domain's `dbt test` -- not a dbt-side test (every domain's
profiles.yml targets Trino/Iceberg only; dbt has no connection to
platform_metadata Postgres, so a dbt test structurally cannot cross-
reference `owning_feed_id`), and not inside orchestration/Dockerfile's
`dbt parse` step either (a pure Docker build, no DB connectivity there).

A Type 2 (SCD) dimension compiles to a dbt *snapshot* node, not a *model*
node -- confirmed against a real manifest.json (dim_customer, Type 2,
shows up with resource_type='snapshot'). Both resource types land in the
same `model` schema and carry tags the same way, so both are checked here.
"""

import json
import os
import sys
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

_MODEL_RESOURCE_TYPES = ("model", "snapshot")


def fetch_model_feed_rows(cur) -> list[dict]:
    cur.execute(
        """
        select lm.table_name, lm.model_schema, df.friendly_name as owning_feed_friendly_name
        from lakehouse_models lm
        join data_feed df on df.id = lm.owning_feed_id
        where lm.is_active = true
        """
    )
    columns = [c.name for c in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def check_row(row: dict, mismatches: list[str]) -> None:
    domain = slugify_domain(row["model_schema"])
    manifest_path = DOMAINS_DIR / domain / "target" / "manifest.json"
    if not manifest_path.exists():
        mismatches.append(f"{row['table_name']}: domain {domain!r} has no compiled manifest.json yet -- run dbt parse first")
        return

    manifest = json.loads(manifest_path.read_text())
    node = next(
        (
            n
            for n in manifest["nodes"].values()
            if n.get("resource_type") in _MODEL_RESOURCE_TYPES
            and n.get("schema") == "model"
            and n.get("name") == row["table_name"]
        ),
        None,
    )
    if node is None:
        mismatches.append(f"{row['table_name']}: no model/snapshot node named {row['table_name']!r} found in {domain!r}'s manifest")
        return

    expected = [row["owning_feed_friendly_name"]]
    if node.get("tags") != expected:
        mismatches.append(
            f"{row['table_name']}: tags={node.get('tags')} but owning_feed_id -> {row['owning_feed_friendly_name']!r} "
            f"(expected tags={expected})"
        )


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        rows = fetch_model_feed_rows(cur)

    mismatches: list[str] = []
    for row in rows:
        check_row(row, mismatches)

    print(f"Checked {len(rows)} lakehouse model(s) against their domain manifests.")
    if mismatches:
        for m in mismatches:
            print(f"  MISMATCH: {m}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
