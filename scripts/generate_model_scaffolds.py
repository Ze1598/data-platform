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

Files land inside the OWNING DOMAIN's own dbt project
(dbt/domains/<model_schema>/...), not a single shared project -- see
Roadmap.md "multi-project dbt split". table_name (not friendly_name) is the
technical identifier: it drives both the physical `alias=` and the dbt
model's own filename (already a complete, human-entered string following
the "<model_schema>_<fct|dim>_<name>" convention, not composed here).
friendly_name stays a pure display label, referenced only in generated
comments for human context. The physical Trino schema (`schema='model'`)
is a fixed literal, independent of model_schema's value -- model_schema
now means "which domain/dbt project", not "which physical schema"; that
distinction is exactly what this split introduced (see
metadata/DataModel.md, `lakehouse_models.model_schema`).

A Type 2 dimension (scd_type=2) is not a regular model file -- it's a dbt
snapshot at dbt/domains/<model_schema>/snapshots/<table_name>.sql. Facts are
always Type-1-style in-place merge regardless of scd_type (Roadmap.md:
"facts use the same in-place merge mechanics as Type 1"), so
table_type='fact' always renders via _render_type1_model; table_type=
'dimension' branches on scd_type.

Deliberately NOT filtered on pipeline_steps -- unlike generate_serve_views.py
(which only cares about the 'serving' step), pipeline_steps never gates
whether a model/snapshot should exist at all (metadata/DataModel.md).

Schema-test entries go into a per-model companion `.yml` file next to each
scaffolded `.sql` file, not into a shared schema.yml -- reuses the same
write-if-missing/never-touch mechanism as the .sql file, no YAML-merge
risk, no new dependency (plain string formatting, same as
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
for a genuinely new model (one whose `.sql` file doesn't exist yet).
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

# Physical Trino/Iceberg schema for the model layer -- a fixed literal,
# pipeline-stage boundary (see module docstring). NOT lakehouse_models.
# model_schema's value, which now means "which domain/dbt project", a
# completely different axis since the multi-project split.
_MODEL_PHYSICAL_SCHEMA = "model"


def fetch_candidate_rows(cur) -> list[dict]:
    cur.execute(
        """
        select
            lm.id as model_id, lm.friendly_name, lm.table_name, lm.model_schema, lm.table_type,
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


def fetch_model_columns(cur) -> dict:
    """lakehouse_model_columns rows, grouped by model_id and ordered by
    ordinal_position -- the frontend's column-definition editor
    (frontend/pages/6_Model_Table_Columns.py). A model with no rows here
    is absent from the returned dict entirely and keeps the original
    business_key_columns/tracked_columns-scaffolded flow untouched."""
    cur.execute(
        """
        select
            lmc.model_id, lmc.column_name, lmc.data_type, lmc.is_nullable,
            lmc.is_business_key, lmc.is_tracked, df.friendly_name as source_feed
        from lakehouse_model_columns lmc
        join data_feed df on df.id = lmc.source_feed_id
        order by lmc.model_id, lmc.ordinal_position
        """
    )
    columns = [desc.name for desc in cur.description]
    by_model: dict = {}
    for row in cur.fetchall():
        r = dict(zip(columns, row))
        by_model.setdefault(str(r["model_id"]), []).append(r)
    return by_model


# Same 5-type vocabulary lakehouse_model_columns.data_type is constrained
# to (schema_validation.py's TYPE_MAP) -- these are the casts every
# hand-written staging model already uses for the same 5 values
# (stg_customers.sql/stg_sales.sql), reused verbatim rather than invented.
_SQL_TYPE_BY_DATA_TYPE = {
    "string": "varchar",
    "long": "bigint",
    "double": "double",
    "boolean": "boolean",
    "timestamp": "timestamp(6) with time zone",
}


def _cast_expr(column_name: str, data_type: str) -> str:
    """A plain `cast(col as timestamp(6) with time zone)` fails in Trino
    against an ISO8601 string with a literal 'T'/'Z' (`INVALID_CAST_ARGUMENT:
    Value cannot be cast to timestamp: ...T...Z`, confirmed live) -- every
    connector-driven feed's `clean` layer stores a schema_registry
    `timestamp` column as varchar regardless of connector kind (confirmed
    via `describe iceberg.clean.<feed>`), so this is universal, not specific
    to one feed. stg_financial_transactions.sql already established the
    correct pattern for this exact case (`from_iso8601_timestamp(...) at
    time zone 'UTC'`) -- reused here rather than inventing a second one."""
    if data_type == "timestamp":
        return f"cast(from_iso8601_timestamp({column_name}) at time zone 'UTC' as timestamp(6) with time zone) as {column_name}"
    return f"cast({column_name} as {_SQL_TYPE_BY_DATA_TYPE[data_type]}) as {column_name}"


def staging_target_path(row: dict) -> Path:
    """dbt/domains/<domain>/models/staging/stg_<table_name>.sql -- a
    DEDICATED per-model staging file, distinct from the pre-existing
    per-feed stg_<feed>.sql staging models (which stay shared/hand-written
    and are never touched by this script). Only generated for a model with
    lakehouse_model_columns rows -- see generate()."""
    domain = slugify_domain(row["model_schema"])
    return DOMAINS_DIR / domain / "models" / "staging" / f"stg_{row['table_name']}.sql"


def _render_dedicated_staging(*, table_name: str, owning_feed: str, columns: list[dict]) -> str:
    """Renders a dedicated per-model staging file, driven by
    lakehouse_model_columns instead of hand-picked columns -- same
    cast/_key_hash/_attr_hash/incremental/classify_changes pattern every
    hand-written staging model already uses (stg_customers.sql/
    stg_sales.sql), just parameterized. tags/config alias use `owning_feed`
    (not every distinct source feed) -- same reasoning lakehouse_models.
    owning_feed_id already exists for at the model layer: exactly one
    feed's tag, never more, is what avoids the "two feed tags" @dbt_assets
    ownership-conflict bug class (see Learnings.md).

    A model spanning more than one distinct source feed gets one CTE per
    feed (each casting only that feed's own declared columns), but the
    final select combining them is left as a hand-written TODO -- no
    metadata describes how two feeds' rows relate (same reasoning
    generate_model_scaffolds.py's model-layer FK-join boilerplate is
    already left out for, see _render_type1_model's own TODO comment)."""
    feeds = sorted({c["source_feed"] for c in columns})
    key_cols = [c["column_name"] for c in columns if c["is_business_key"]]
    tracked_cols = [c["column_name"] for c in columns if c["is_tracked"]]
    key_hash_args = _py_list_literal(key_cols)
    attr_hash_args = _py_list_literal(tracked_cols)

    def _cte_for_feed(feed: str) -> str:
        feed_cols = [c for c in columns if c["source_feed"] == feed]
        cols_block = ",\n".join(f"        {_cast_expr(c['column_name'], c['data_type'])}" for c in feed_cols)
        return f"""{feed}_raw as (

    select
{cols_block}
    from {{{{ source('clean', '{feed}') }}}}

)"""

    ctes = ",\n\n".join(_cte_for_feed(f) for f in feeds)

    if len(feeds) == 1:
        combine_comment = ""
    else:
        combine_comment = (
            f"\n    -- TODO: combine the CTEs above ({', '.join(f + '_raw' for f in feeds)}) -- no metadata\n"
            "    -- describes how these feeds' rows relate (join key/condition, or a union if\n"
            "    -- they're the same shape from different sources). Replace this placeholder\n"
            "    -- select with the real business logic.\n"
        )

    return f"""{{{{
  config(
    unique_key='_key_hash',
    alias='{table_name}',
    tags=['{owning_feed}']
  )
}}}}

{{#
    Generated scaffold (scripts/generate_model_scaffolds.py), driven by
    lakehouse_model_columns (frontend/pages/6_Model_Table_Columns.py) --
    dedicated staging for {table_name}, distinct from any per-feed
    stg_<feed>.sql. Casts and the key/tracked column split come from that
    table.
#}}

{{% set updates_enabled = var('updates_enabled_by_model', {{}}).get(model.name, true) %}}

with {ctes},
{combine_comment}
source_raw as (

    select
        *,
        {{{{ row_hash({key_hash_args}) }}}} as _key_hash,
        {{{{ row_hash({attr_hash_args}) }}}} as _attr_hash
    from {feeds[0]}_raw

)

{{% if is_incremental() %}}

, source as (
    {{{{ classify_changes('source_raw', updates_enabled) }}}}
)

{{% endif %}}

select
    *,
    {{{{ dbt.current_timestamp() }}}} as _loaded_at
from {{{{ 'source' if is_incremental() else 'source_raw' }}}}
"""


def target_path(row: dict) -> tuple[Path, bool]:
    """Returns (path, is_type2_snapshot). Pure derivation, no I/O -- the
    existence check belongs to the generation loop so it can log
    created-vs-skipped cleanly. Path lands inside the row's own domain
    project (dbt/domains/<model_schema>/...), filename from table_name."""
    domain = slugify_domain(row["model_schema"])
    domain_dir = DOMAINS_DIR / domain
    is_type2 = row["table_type"] == "dimension" and row["scd_type"] == 2
    if is_type2:
        return domain_dir / "snapshots" / f"{row['table_name']}.sql", True
    subdir = domain_dir / "models" / "model" / ("dimensions" if row["table_type"] == "dimension" else "facts")
    return subdir / f"{row['table_name']}.sql", False


def _py_list_literal(cols: list[str]) -> str:
    return "[" + ", ".join(f"'{c}'" for c in cols) + "]"


def _render_type1_model(
    *, friendly_name: str, table_name: str, owning_feed: str,
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
    schema='{_MODEL_PHYSICAL_SCHEMA}',
    unique_key='_key_hash',
    alias='{table_name}',
    tags=['{owning_feed}']
  )
}}}}

{{#
    TODO: describe this model's real business logic here.
    Generated scaffold (scripts/generate_model_scaffolds.py) -- `base`
    below is pre-filled from lakehouse_models' business_key_columns/
    tracked_columns only. Verify the column names/source, and add any
    joins this model needs (e.g. a dimensional FK via another model's
    _key_hash -- see an existing fct_*.sql for the pattern; that join
    can't be auto-derived, no metadata describes it).

    friendly_name (display label): {friendly_name}
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
    *, friendly_name: str, table_name: str, owning_feed: str,
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

    return f"""{{% snapshot {table_name} %}}

{{{{
    config(
        target_schema='{_MODEL_PHYSICAL_SCHEMA}',
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

    friendly_name (display label): {friendly_name}
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


def _render_schema_yml_companion(table_name: str, is_type2: bool, not_null_columns: list[str] | None = None) -> str:
    # Fixed shape confirmed against the real, hand-maintained schema.yml
    # this pattern originated from: Type 1 dimension/fact -> _key_hash is
    # unique (one row per business key); Type 2 snapshot -> _key_hash is
    # NOT unique (multiple versions legitimately share it), _scd_id is
    # the unique one instead. Lives under `models:` vs `snapshots:`
    # respectively -- dbt discovers property files by content, not by the
    # literal filename `schema.yml`.
    #
    # not_null_columns is only populated for a model with
    # lakehouse_model_columns rows (is_nullable=false there) -- the
    # original business_key_columns/tracked_columns text-field flow has no
    # per-column nullability concept to test against, so this list is
    # empty/None for every model that hasn't opted into column definitions.
    extra_column_tests = "".join(
        f"      - name: {c}\n        tests: [not_null]\n" for c in (not_null_columns or [])
    )
    if is_type2:
        return f"""version: 2

snapshots:
  - name: {table_name}
    columns:
      - name: _key_hash
        tests: [not_null]
      - name: _scd_id
        tests: [not_null, unique]
      - name: _attr_hash
        tests: [not_null]
      - name: is_deleted
        tests: [not_null]
{extra_column_tests}"""
    return f"""version: 2

models:
  - name: {table_name}
    columns:
      - name: _key_hash
        tests: [not_null, unique]
      - name: _attr_hash
        tests: [not_null]
      - name: is_deleted
        tests: [not_null]
{extra_column_tests}"""


def generate(rows: list[dict], model_columns: dict) -> tuple[list[Path], list[Path]]:
    written, skipped = [], []
    for row in rows:
        cols = model_columns.get(str(row["model_id"]), [])
        owning_feed = row["owning_feed"]
        not_null_columns: list[str] = []

        # Dedicated per-model staging generation is independent of whether
        # the model/snapshot file itself already exists below -- a model
        # can get column definitions added well after its .sql was hand-
        # written, and this backfills the staging file without touching
        # that already-existing (possibly hand-edited) model file.
        if cols and row["deletes_enabled"]:
            # deletes_enabled's is_deleted column comes from the
            # deletion-synthesis intermediate (int_<table_name>_with_deletes,
            # generate_deletion_synthesis_views.py), which itself still
            # wraps the OLD per-feed stg_<owning_feed> -- not this new
            # dedicated staging. Known gap, not silently worked around:
            # print a warning and fall back to today's behavior entirely
            # rather than generate an unused dedicated staging file.
            print(
                f"  NOTE: {row['table_name']!r} has lakehouse_model_columns rows but "
                f"deletes_enabled=true -- dedicated staging generation is skipped for this "
                f"model (deletion-synthesis still wraps stg_{owning_feed}), falling back to "
                f"the original business_key_columns/tracked_columns flow."
            )
            business_key_columns = row["business_key_columns"]
            tracked_columns = row["tracked_columns"]
            source_ref = f"int_{row['table_name']}_with_deletes"
        elif cols:
            business_key_columns = [c["column_name"] for c in cols if c["is_business_key"]]
            tracked_columns = [c["column_name"] for c in cols if c["is_tracked"]]
            # Restricted to columns that actually appear in the MODEL layer
            # (business_key_columns + tracked_columns, selected into the
            # base CTE below) -- a column that's neither (e.g. a
            # passthrough kept in staging only) has no column of that name
            # in the model table at all, so a not_null test on it there
            # would fail with COLUMN_NOT_FOUND, not a real test failure.
            not_null_columns = [
                c["column_name"] for c in cols
                if not c["is_nullable"] and (c["is_business_key"] or c["is_tracked"])
            ]
            source_ref = f"stg_{row['table_name']}"

            staging_path = staging_target_path(row)
            if staging_path.exists():
                skipped.append(staging_path)
            else:
                staging_path.parent.mkdir(parents=True, exist_ok=True)
                staging_path.write_text(
                    _render_dedicated_staging(table_name=row["table_name"], owning_feed=owning_feed, columns=cols)
                )
                written.append(staging_path)
        else:
            business_key_columns = row["business_key_columns"]
            tracked_columns = row["tracked_columns"]
            if row["deletes_enabled"]:
                # deletes_enabled's source is the deletion-synthesis
                # intermediate (int_<table_name>_with_deletes.sql, generated
                # separately by generate_deletion_synthesis_views.py into this
                # same domain -- not touched here), keyed by THIS row's own
                # table_name -- domains are separate dbt projects with no
                # cross-project ref(), so each lakehouse_models row gets its
                # own copy rather than sharing one per feed.
                source_ref = f"int_{row['table_name']}_with_deletes"
            else:
                source_ref = f"stg_{owning_feed}"

        path, is_type2 = target_path(row)
        if path.exists():
            skipped.append(path)
            continue

        render = _render_type2_snapshot if is_type2 else _render_type1_model
        content = render(
            friendly_name=row["friendly_name"],
            table_name=row["table_name"],
            owning_feed=owning_feed,
            business_key_columns=business_key_columns,
            tracked_columns=tracked_columns,
            deletes_enabled=row["deletes_enabled"],
            source_ref=source_ref,
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        written.append(path)

        yml_path = path.with_suffix(".yml")
        yml_path.write_text(_render_schema_yml_companion(row["table_name"], is_type2, not_null_columns))
        written.append(yml_path)

    return written, skipped


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        rows = fetch_candidate_rows(cur)
        model_columns = fetch_model_columns(cur)

    written, skipped = generate(rows, model_columns)
    print(
        f"Scaffolded {len(written)} new file(s) (staging/model/snapshot + companion .yml); "
        f"left {len(skipped)} existing target(s) untouched, out of {len(rows)} active "
        f"lakehouse_models row(s)."
    )
    for p in written:
        print(f"  created: {p.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
