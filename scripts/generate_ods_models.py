"""Generates the ODS (Operational Data Store) layer -- a fully automatic,
zero-hand-written-SQL clean -> staging -> model passthrough for any
`data_feed` row with `ods_enabled = true` and no `lakehouse_models` row
referencing it (Roadmap.md "ODS layer"). Resolves Phase 13's per-model
transformation-gating gap a different way than a per-model dbt `--exclude`
list would have: rather than let a feed skip transformation and produce
nothing, a feed that never gets a hand-modeled dimension/fact still gets
something real in `model` -- just an as-is, no-cast, standard-columns-only
Type 1 table, driven purely by schema_registry.

Deliberately UNLIKE scripts/generate_model_scaffolds.py: this output is
100% generated, with zero hand-written content ever expected in it (no
casts, no business logic -- "no modeling aside from the standard columns
we use" was the explicit design instruction), so it's safe -- and correct
-- to fully wipe-and-regenerate `models/staging/generated/` and
`models/model/ods/generated/` on every run, same pattern as
generate_serve_views.py/generate_deletion_synthesis_views.py, not the
write-once-then-freeze pattern generate_model_scaffolds.py uses for
hand-filled dimension/fact files.

Two files per candidate feed:
  - models/staging/generated/<feed>.sql  (dbt name stg_<feed>_ods,
    alias='<feed>' -> physical staging.<feed>)
  - models/model/ods/generated/<feed>.sql (dbt name <feed>_ods,
    alias='<feed>' -> physical model.<feed>)
Both tagged tags=['<feed>'] only -- this is what sweeps them into that
feed's EXISTING per-feed transformation dbt build (dbt_assets.py's
tag-based select=/exclude=) with zero new Dagster asset-graph wiring;
every real feed today is already in dbt_assets.py's _CLEAN_SOURCE_TABLES,
so no change is needed there either for any feed that could plausibly
enable ODS.

Primary key precedence is NOT recomputed here -- schema_registry.
primary_key_columns is already the fully-resolved value (see
PostgresMetadataResource.sync_schema_registry()), so this script just
reads it. Empty means no key is known at all: the generated tables become
plain incremental appends with no unique_key -- correct specifically
because clean.<feed> is a full atomic overwrite per run, not cumulative
(processing/raw_to_clean's write_clean_snapshot()), so "everything in this
run's clean snapshot" already *is* "what's new"; no dedup is needed to
avoid re-appending old rows.

No is_deleted/deletion-synthesis concept anywhere in ODS -- deliberately
out of scope (no deletes_enabled-equivalent exists on data_feed, none was
requested).
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
STAGING_OUTPUT_DIR = REPO_ROOT / "dbt" / "data_platform" / "models" / "staging" / "generated"
MODEL_OUTPUT_DIR = REPO_ROOT / "dbt" / "data_platform" / "models" / "model" / "ods" / "generated"


def fetch_ods_candidates(cur) -> list[dict]:
    cur.execute(
        """
        select df.friendly_name, df.extraction_type, sr.column_definitions, sr.primary_key_columns
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


def _py_list_literal(cols: list[str]) -> str:
    return "[" + ", ".join(f"'{c}'" for c in cols) + "]"


def _render_ods_staging(*, feed: str, all_columns: list[str], primary_key_columns: list[str], extraction_type: str) -> str:
    cols_block = ",\n        ".join(all_columns)
    is_keyed = len(primary_key_columns) > 0

    if not is_keyed and extraction_type == "full":
        # No key AND extraction_type='full' -- clean.<feed> is re-delivered
        # as the COMPLETE dataset every run (not just new rows), so a
        # plain incremental append would duplicate every previously-seen
        # row on every run. Confirmed for real: this duplicated a
        # 2-row batch into 6 rows across two runs before this branch
        # existed. materialized='table' fully replaces the physical table
        # every run instead -- mirrors clean's own "complete snapshot per
        # run" semantics, and is honest about what's achievable without a
        # key anyway (no way to detect a real change without one, so a
        # separate _historical view was never meaningfully different from
        # _latest for this case).
        return f"""{{{{ config(materialized='table', alias='{feed}', tags=['{feed}']) }}}}

{{#
    ODS staging passthrough for '{feed}' -- generated by
    scripts/generate_ods_models.py from data_feed.ods_enabled + the
    current schema_registry entry. No primary key resolved (neither
    data_feed.source_pk nor a discovered key), and extraction_type='full'
    -- clean.{feed} is re-delivered as the complete dataset every run, so
    this fully replaces the table every run (materialized='table'), not
    an incremental append (which would duplicate every previously-seen
    row -- confirmed for real before this branch existed). Fully
    regenerated on every run -- never hand-edit this file, edits will be
    silently discarded.
#}}

select
        {cols_block},
    {{{{ dbt.current_timestamp() }}}} as _loaded_at
from {{{{ source('clean', '{feed}') }}}}
"""

    if not is_keyed:
        return f"""{{{{ config(alias='{feed}', tags=['{feed}']) }}}}

{{#
    ODS staging passthrough for '{feed}' -- generated by
    scripts/generate_ods_models.py from data_feed.ods_enabled + the
    current schema_registry entry. No primary key resolved (neither
    data_feed.source_pk nor a discovered key), extraction_type='incremental'
    -- plain incremental append, no unique_key, no dedup. Correct because
    clean.{feed} only ever contains new rows since the last watermark on
    an incremental feed: every row selected here is genuinely new. Fully
    regenerated on every run -- never hand-edit this file, edits will be
    silently discarded.
#}}

select
        {cols_block},
    {{{{ dbt.current_timestamp() }}}} as _loaded_at
from {{{{ source('clean', '{feed}') }}}}
"""

    non_pk_columns = [c for c in all_columns if c not in primary_key_columns]
    key_hash_args = _py_list_literal(primary_key_columns)
    attr_hash_args = _py_list_literal(non_pk_columns)

    return f"""{{{{
  config(
    unique_key='_key_hash',
    alias='{feed}',
    tags=['{feed}']
  )
}}}}

{{#
    ODS staging passthrough for '{feed}' -- generated by
    scripts/generate_ods_models.py from data_feed.ods_enabled + the
    current schema_registry entry. No hand-written business logic here --
    every column below is exactly what schema_registry.column_definitions
    says, with no casts (ODS is an as-is passthrough of clean, not a
    curated contract -- contrast stg_customers.sql). Fully regenerated on
    every run -- never hand-edit this file, edits will be silently
    discarded.

    Primary key: {primary_key_columns} (resolved via
    schema_registry.primary_key_columns -- data_feed.source_pk if set,
    else a live-discovered key, see
    connectors.postgres.PostgresConnector.discover_primary_key()).

    updates_enabled hardcoded true, not sourced from the usual
    var('updates_enabled_by_model', ...) lookup -- there's no
    lakehouse_models row for an ODS feed to compute that map's
    stg_<feed> entry from (see get_updates_enabled_map()). ODS always
    tracks attribute changes.
#}}

{{% set updates_enabled = true %}}

with source_raw as (

    select
        {cols_block},
        {{{{ row_hash({key_hash_args}) }}}} as _key_hash,
        {{{{ row_hash({attr_hash_args}) }}}} as _attr_hash
    from {{{{ source('clean', '{feed}') }}}}

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


def _render_ods_model(*, feed: str, all_columns: list[str], primary_key_columns: list[str], extraction_type: str) -> str:
    cols_block = ",\n        ".join(all_columns)
    is_keyed = len(primary_key_columns) > 0
    staging_ref = f"stg_{feed}_ods"

    if not is_keyed and extraction_type == "full":
        # Mirrors the staging layer's own full/no-key branch exactly --
        # ref('{staging_ref}') is itself now a full replace every run
        # (not an accumulating append), so this has to fully replace too,
        # not append on top of an already-complete staging table.
        return f"""{{{{
  config(
    schema='model',
    materialized='table',
    alias='{feed}',
    tags=['{feed}']
  )
}}}}

{{#
    ODS model-layer table for '{feed}' -- generated by
    scripts/generate_ods_models.py. No primary key resolved, extraction_type
    ='full' -- fully replaces every run (materialized='table'), mirroring
    {{{{ ref('{staging_ref}') }}}}'s own full-replace semantics (an
    incremental append here would duplicate every row on top of an
    already-complete staging table). Fully regenerated on every run --
    never hand-edit this file, edits will be silently discarded.
#}}

select
    {cols_block},
    cast(null as varchar) as _scd_id,
    cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to,
    {{{{ dbt.current_timestamp() }}}} as _updated_at
from {{{{ ref('{staging_ref}') }}}}
"""

    if not is_keyed:
        return f"""{{{{
  config(
    schema='model',
    alias='{feed}',
    tags=['{feed}']
  )
}}}}

{{#
    ODS model-layer table for '{feed}' -- generated by
    scripts/generate_ods_models.py. No primary key resolved,
    extraction_type='incremental' -- plain incremental append, no
    unique_key, no dedup (mirrors the staging layer's own insert-only
    reasoning: {{{{ ref('{staging_ref}') }}}} only ever contains new rows
    since the last watermark). Fully regenerated on every run -- never
    hand-edit this file, edits will be silently discarded.
#}}

select
    {cols_block},
    cast(null as varchar) as _scd_id,
    cast(null as timestamp(6)) as _valid_from,
    cast(null as timestamp(6)) as _valid_to,
    {{{{ dbt.current_timestamp() }}}} as _updated_at
from {{{{ ref('{staging_ref}') }}}}
"""

    key_hash_args = _py_list_literal(primary_key_columns)
    non_pk_columns = [c for c in all_columns if c not in primary_key_columns]
    attr_hash_args = _py_list_literal(non_pk_columns)

    return f"""{{{{
  config(
    schema='model',
    unique_key='_key_hash',
    alias='{feed}',
    tags=['{feed}']
  )
}}}}

{{#
    ODS model-layer table for '{feed}' -- generated by
    scripts/generate_ods_models.py. Type 1 (upsert-in-place), no
    is_deleted/deletion-synthesis concept -- ODS has no
    deletes_enabled-equivalent. Independently recomputes _key_hash/
    _attr_hash from {{{{ ref('{staging_ref}') }}}} rather than reusing
    staging's already-computed values, matching this project's
    established "every layer computes its own hashes" pattern (see
    scripts/generate_model_scaffolds.py's Type 1 template). Fully
    regenerated on every run -- never hand-edit this file, edits will
    be silently discarded.
#}}

{{% set updates_enabled = true %}}

with base as (

    select
        {cols_block}
    from {{{{ ref('{staging_ref}') }}}}

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


def generate(candidates: list[dict], staging_dir: Path, model_dir: Path) -> tuple[list[Path], list[Path]]:
    for output_dir in (staging_dir, model_dir):
        if output_dir.exists():
            shutil.rmtree(output_dir)
        output_dir.mkdir(parents=True)

    staging_written, model_written = [], []
    for row in candidates:
        feed = row["friendly_name"]
        all_columns = [c["name"] for c in sorted(row["column_definitions"], key=lambda c: c["ordinal"])]
        primary_key_columns = row["primary_key_columns"]
        extraction_type = row["extraction_type"]

        # dbt derives a model's NAME from its filename (never from
        # `alias=`, which only sets the physical table name) -- these must
        # be distinct project-wide, so the files themselves are named
        # stg_<feed>_ods.sql / <feed>_ods.sql, not <feed>.sql in both
        # folders (confirmed the hard way: dbt parse rejected two models
        # both literally named "police_crimes" when both files were named
        # police_crimes.sql, one per folder).
        staging_path = staging_dir / f"stg_{feed}_ods.sql"
        staging_path.write_text(
            _render_ods_staging(
                feed=feed, all_columns=all_columns, primary_key_columns=primary_key_columns,
                extraction_type=extraction_type,
            )
        )
        staging_written.append(staging_path)

        model_path = model_dir / f"{feed}_ods.sql"
        model_path.write_text(
            _render_ods_model(
                feed=feed, all_columns=all_columns, primary_key_columns=primary_key_columns,
                extraction_type=extraction_type,
            )
        )
        model_written.append(model_path)

    return staging_written, model_written


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        candidates = fetch_ods_candidates(cur)

    staging_written, model_written = generate(candidates, STAGING_OUTPUT_DIR, MODEL_OUTPUT_DIR)
    print(
        f"Generated {len(staging_written)} ODS staging model(s) and {len(model_written)} ODS model-layer "
        f"table(s) for {len(candidates)} ods_enabled data_feed row(s)."
    )
    for p in staging_written + model_written:
        print(f"  created: {p.relative_to(REPO_ROOT)}")


if __name__ == "__main__":
    main()
