"""Generates model-layer scaffold files (Type 1 dimension/fact `.sql` models,
Type 2 dimension snapshot `.sql` files, plus a matching per-model `.yml` test
companion) from `lakehouse_models` -- one per `is_active=true` row whose
target file doesn't exist yet on disk, so nobody hand-copies the full
`config()`/`row_hash()`/`classify_changes()`/technical-column boilerplate per
model (Roadmap.md "Model Layer: SCD Design"). Only the business-logic select
(the `base` CTE's real column list/joins/casts) stays hand-written -- that's
explicitly out of scope for automation, by design, not an oversight.

Deliberately UNLIKE generate_serve_views.py/generate_deletion_synthesis_views.py:
those output directories are 100% generated and safe to wipe-and-regenerate
every run. This script's output is a permanent MIX of generated boilerplate
and hand-written business logic in the SAME file (the `base` CTE, or a Type 2
snapshot's `select` list) -- so an existing target file is left completely
untouched, forever, even after its lakehouse_models row is later deactivated
(a deactivated row's file is simply not built, since nothing references it --
see DataModel.md/Backlog.md). Only a MISSING file is a scaffold candidate;
is_active=false rows are never candidates for new scaffolding either.

A Type 2 dimension (scd_type=2) is not a regular model file -- it's a dbt
snapshot at snapshots/<friendly_name>.sql. Facts are always Type-1-style
in-place merge regardless of scd_type (Roadmap.md: "facts use the same
in-place merge mechanics as Type 1"), so table_type='fact' always renders via
_render_type1_model; table_type='dimension' branches on scd_type.

Deliberately NOT filtered on pipeline_steps -- unlike generate_serve_views.py
(which only cares about the 'serving' step), pipeline_steps never gates
whether a model/snapshot should exist at all (metadata/DataModel.md).

Schema-test entries go into a per-model companion `.yml` file next to each
scaffolded `.sql` file, not into the existing shared
models/model/schema.yml -- reuses the same write-if-missing/never-touch
mechanism as the .sql file, no YAML-merge risk against that hand-maintained
file, no new dependency (plain string formatting, same as
generate_serve_views.py's own `_render_schema_yml`).

FK-join boilerplate (a fact joining to a dimension's _key_hash for a
dimensional key, e.g. fct_sales -> dim_branch) is NOT auto-derivable: no
metadata describes which dimension a fact should join to or on what key.
Left out of the scaffold entirely -- the TODO placeholder only pre-fills
columns lakehouse_models itself knows about (business keys, tracked columns,
is_deleted); anything beyond that (joins, renames, extra passthrough
columns) is real hand-written business logic, same category as every other
model's base CTE.

WARNING confirmed via `dbt parse`: dbt hard-errors ("dbt found two schema.yml
entries for the same resource") if a model's name appears in property blocks
in two separate YAML files. This script's companion-`.yml`-if-missing
mechanism is safe under normal use, because it only ever creates a companion
for a genuinely new model (one whose `.sql` file doesn't exist yet), which by
definition has no pre-existing entry in the shared models/model/schema.yml to
collide with -- but if a `.sql` file is ever deleted by hand WITHOUT also
removing its corresponding models/model/schema.yml entry, re-running this
script will scaffold a colliding companion `.yml` and break `dbt parse`.
Remove the stale shared-schema.yml entry (or the companion file) if this
happens.
"""

import os
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
DIMENSIONS_DIR = REPO_ROOT / "dbt" / "data_platform" / "models" / "model" / "dimensions"
FACTS_DIR = REPO_ROOT / "dbt" / "data_platform" / "models" / "model" / "facts"
SNAPSHOTS_DIR = REPO_ROOT / "dbt" / "data_platform" / "snapshots"


def fetch_candidate_rows(cur) -> list[dict]:
    cur.execute(
        """
        select
            lm.friendly_name, lm.model_schema, lm.table_type,
            lm.business_key_columns, lm.tracked_columns, lm.scd_type,
            lm.deletes_enabled, lm.depends_on_feeds,
            df.friendly_name as owning_feed
        from lakehouse_models lm
        join data_feed df on df.id = lm.owning_feed_id
        where lm.is_active = true
        order by lm.friendly_name
        """
    )
    columns = [desc.name for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def resolve_deletes_enabled_feed_names(cur, rows: list[dict]) -> dict[str, str]:
    """depends_on_feeds -> exactly-one-friendly_name, same hard-error-on-!=1
    behavior as generate_deletion_synthesis_views.fetch_deletion_synthesis_feeds
    (mirrored, not imported -- these scripts don't share a common module
    today). Only resolves this run's deletes_enabled=true candidates, so a
    scaffold with deletes_enabled=false never pays this cost or hits this
    error path.
    """
    dependency_ids: dict[str, list[str]] = {}
    for row in rows:
        if not row["deletes_enabled"]:
            continue
        ids = [v for v in (row["depends_on_feeds"] or "").split(",") if v]
        if len(ids) != 1:
            raise ValueError(
                f"lakehouse_models '{row['friendly_name']}' has deletes_enabled=true but "
                f"depends_on_feeds resolves to {len(ids)} feed(s) ({row['depends_on_feeds']!r}) -- "
                "deletion synthesis requires exactly one."
            )
        dependency_ids[row["friendly_name"]] = ids

    if not dependency_ids:
        return {}

    all_ids = {fid for ids in dependency_ids.values() for fid in ids}
    cur.execute("select id, friendly_name from data_feed where id::text = any(%s)", (list(all_ids),))
    feed_names = {str(fid): name for fid, name in cur.fetchall()}
    return {model_name: feed_names[ids[0]] for model_name, ids in dependency_ids.items()}


def target_path(row: dict) -> tuple[Path, bool]:
    """Returns (path, is_type2_snapshot). Pure derivation, no I/O -- the
    existence check belongs to the generation loop so it can log
    created-vs-skipped cleanly."""
    is_type2 = row["table_type"] == "dimension" and row["scd_type"] == 2
    if is_type2:
        return SNAPSHOTS_DIR / f"{row['friendly_name']}.sql", True
    subdir = DIMENSIONS_DIR if row["table_type"] == "dimension" else FACTS_DIR
    return subdir / f"{row['friendly_name']}.sql", False


def _py_list_literal(cols: list[str]) -> str:
    return "[" + ", ".join(f"'{c}'" for c in cols) + "]"


def _render_type1_model(
    *, friendly_name: str, model_schema: str, owning_feed: str,
    business_key_columns: list[str], tracked_columns: list[str],
    deletes_enabled: bool, source_ref: str,
) -> str:
    key_hash_args = _py_list_literal(business_key_columns)
    attr_hash_args = _py_list_literal(tracked_columns + ["is_deleted"])
    select_cols = business_key_columns + tracked_columns
    cols_block = ",\n".join(f"        {c}" for c in select_cols)
    is_deleted_line = "        is_deleted" if deletes_enabled else "        false as is_deleted"
    is_deleted_note = (
        f"is_deleted (sourced from ref('{source_ref}') directly -- do not hardcode false)"
        if deletes_enabled
        else "false as is_deleted (deletes_enabled=false)"
    )

    return f"""{{{{
  config(
    schema='{model_schema}',
    unique_key='_key_hash',
    alias='{friendly_name}',
    tags=['{owning_feed}']
  )
}}}}

{{#
    TODO: describe this model's real business logic here.
    Generated scaffold (scripts/generate_model_scaffolds.py) -- `base`
    below is pre-filled from lakehouse_models' business_key_columns/
    tracked_columns only. Verify the column names/source, and add any
    joins this model needs (e.g. a dimensional FK via another model's
    _key_hash -- see fct_sales.sql for the pattern; that join can't be
    auto-derived, no metadata describes it).

    business_key_columns: {business_key_columns}
    tracked_columns:      {tracked_columns}
    is_deleted:            {is_deleted_note}
#}}

{{% set updates_enabled = var('updates_enabled_by_model', {{}}).get(model.name, true) %}}

with base as (

    -- TODO: verify/adjust -- replace with the real business-logic select.
    select
{cols_block},
{is_deleted_line}
    from {{{{ ref('{source_ref}') }}}}

),

hashed as (

    select
        *,
        {{{{ row_hash({key_hash_args}) }}}} as _key_hash,
        {{{{ row_hash({attr_hash_args}) }}}} as _attr_hash
    from base

)

{{% if is_incremental() %}}

, to_merge as (
    {{{{ classify_changes('hashed', updates_enabled) }}}}
)

{{% endif %}}

select
    *,
    cast(null as varchar) as _scd_id,
    cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to,
    {{{{ dbt.current_timestamp() }}}} as _updated_at
from {{{{ 'to_merge' if is_incremental() else 'hashed' }}}}
"""


def _render_type2_snapshot(
    *, friendly_name: str, model_schema: str, owning_feed: str,
    business_key_columns: list[str], tracked_columns: list[str],
    deletes_enabled: bool, source_ref: str,
) -> str:
    key_hash_args = _py_list_literal(business_key_columns)
    attr_hash_args = _py_list_literal(tracked_columns + ["is_deleted"])
    select_cols = business_key_columns + tracked_columns
    cols_block = ",\n".join(f"        {c}" for c in select_cols)
    if deletes_enabled:
        cols_block += ",\n        is_deleted"
    else:
        cols_block += ",\n        false as is_deleted"
    is_deleted_note = (
        f"is_deleted (sourced from ref('{source_ref}') directly -- do not hardcode false)"
        if deletes_enabled
        else "false as is_deleted (deletes_enabled=false)"
    )

    return f"""{{% snapshot {friendly_name} %}}

{{{{
    config(
        target_schema='{model_schema}',
        unique_key='_key_hash',
        strategy='check',
        check_cols=['_attr_hash'],
        snapshot_meta_column_names={{
            "dbt_scd_id": "_scd_id",
            "dbt_valid_from": "_valid_from",
            "dbt_valid_to": "_valid_to",
            "dbt_updated_at": "_updated_at",
        }},
        tags=['{owning_feed}'],
    )
}}}}

{{#
    TODO: describe this Type 2 dimension's real business logic here.
    Generated scaffold (scripts/generate_model_scaffolds.py) -- `hashed`'s
    select list below is pre-filled from lakehouse_models'
    business_key_columns/tracked_columns only. Verify/adjust and add any
    extra passthrough columns you need (e.g. updated_at).

    business_key_columns: {business_key_columns}
    tracked_columns:      {tracked_columns}
    is_deleted:            {is_deleted_note}
#}}

with hashed as (

    -- TODO: verify/adjust -- replace with the real business-logic select.
    select
{cols_block},
        {{{{ row_hash({key_hash_args}) }}}} as _key_hash,
        {{{{ row_hash({attr_hash_args}) }}}} as _attr_hash
    from {{{{ ref('{source_ref}') }}}}

)

select * from hashed

{{% endsnapshot %}}
"""


def _render_schema_yml_companion(friendly_name: str, is_type2: bool) -> str:
    # Fixed shape confirmed against the real, hand-maintained
    # models/model/schema.yml: Type 1 dimension/fact -> _key_hash is
    # unique (one row per business key); Type 2 snapshot -> _key_hash is
    # NOT unique (multiple versions legitimately share it), _scd_id is
    # the unique one instead. Lives under `models:` vs `snapshots:`
    # respectively -- dbt discovers property files by content, not by the
    # literal filename `schema.yml` (already proven by the existing
    # shared schema.yml itself, which describes dim_customer_snapshot --
    # a snapshots/ file -- from a file physically under models/).
    if is_type2:
        return f"""version: 2

snapshots:
  - name: {friendly_name}
    columns:
      - name: _key_hash
        tests: [not_null]
      - name: _scd_id
        tests: [not_null, unique]
      - name: _attr_hash
        tests: [not_null]
      - name: is_deleted
        tests: [not_null]
"""
    return f"""version: 2

models:
  - name: {friendly_name}
    columns:
      - name: _key_hash
        tests: [not_null, unique]
      - name: _attr_hash
        tests: [not_null]
      - name: is_deleted
        tests: [not_null]
"""


def generate(rows: list[dict], feed_ref_by_model: dict[str, str]) -> tuple[list[Path], list[Path]]:
    written, skipped = [], []
    for row in rows:
        path, is_type2 = target_path(row)
        if path.exists():
            skipped.append(path)
            continue

        owning_feed = row["owning_feed"]
        if row["deletes_enabled"]:
            # deletes_enabled's source is the deletion-synthesis
            # intermediate (int_<feed>_with_deletes.sql, already generated
            # separately by generate_deletion_synthesis_views.py -- not
            # touched here), not the raw feed name resolved above.
            source_ref = f"int_{feed_ref_by_model[row['friendly_name']]}_with_deletes"
        else:
            source_ref = f"stg_{owning_feed}"

        render = _render_type2_snapshot if is_type2 else _render_type1_model
        content = render(
            friendly_name=row["friendly_name"],
            model_schema=row["model_schema"],
            owning_feed=owning_feed,
            business_key_columns=row["business_key_columns"],
            tracked_columns=row["tracked_columns"],
            deletes_enabled=row["deletes_enabled"],
            source_ref=source_ref,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        written.append(path)

        yml_path = path.with_suffix(".yml")
        yml_path.write_text(_render_schema_yml_companion(row["friendly_name"], is_type2))
        written.append(yml_path)

    return written, skipped


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        rows = fetch_candidate_rows(cur)
        feed_ref_by_model = resolve_deletes_enabled_feed_names(cur, rows)

    written, skipped = generate(rows, feed_ref_by_model)
    print(
        f"Scaffolded {len(written)} new file(s) (model/snapshot + companion .yml); "
        f"left {len(skipped)} existing target(s) untouched, out of {len(rows)} active "
        f"lakehouse_models row(s)."
    )
    for p in written:
        print(f"  created: {p.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
