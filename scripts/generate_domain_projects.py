"""Generates a dbt project skeleton per "domain" -- a business/domain
grouping of related lakehouse model tables (lakehouse_models.model_schema)
or of ODS-enabled feeds sharing one data_feed.batch_ods_name -- so each
domain's staging -> model -> serve build can be genuinely compile-isolated
(its own dbt_project.yml, its own manifest) instead of sharing one project
where a broken model in domain B fails domain A's `dbt parse` too. See
Roadmap.md "multi-project dbt split".

model_schema-derived domains and batch_ods_name-derived domains share the
exact same dbt/domains/<domain>/ namespace -- a hand-modeled domain and an
ODS domain are allowed to collide on name (both would just mean that one
domain project hosts both hand-modeled and auto-generated ODS tables
together), not guarded against.

Create-if-missing semantics, same philosophy as
scripts/generate_model_scaffolds.py: a domain's dbt_project.yml may be
hand-edited after creation (materialization overrides, etc.), so an
existing domain directory is never touched, only genuinely new domains get
scaffolded.

dbt/_shared/ is NOT installed as a dbt package dependency (no
dependencies.yml, no `dbt deps`) -- every domain gets a direct, physical
COPY of every macro in dbt/_shared/macros/, synced unconditionally on every
run (unlike the rest of this script's create-if-missing output, since these
files are never meant to be hand-edited per domain). This was a deliberate
correction, not the original design: package-based sharing was tried first
and looked correct (`dbt deps`/`dbt parse` both succeeded cleanly in every
domain), but broke for real, live, the first time any domain's model was
built a SECOND time in the same environment -- 'row_hash is undefined' (and
the same for generate_schema_name before that), reproducibly, even with
`--full-refresh` and a fully wiped target/ dir. Confirmed fixed immediately
by physically copying dbt/_shared/macros/*.sql into the domain's own
macros/ directory instead. Root cause not further isolated; dbt/_shared/
stays as the single canonical source a human edits, this script is what
keeps every domain's copy in sync with it.
"""

import os
import shutil
from pathlib import Path

import psycopg
from domain_naming import slugify_domain

CONN_KWARGS = dict(
    host=os.environ.get("POSTGRES_HOST", "localhost"),
    port=int(os.environ.get("POSTGRES_PORT", "5432")),
    user=os.environ.get("POSTGRES_USER", "platform"),
    password=os.environ.get("POSTGRES_PASSWORD", "platform"),
    dbname=os.environ.get("POSTGRES_DB", "platform_metadata"),
)

REPO_ROOT = Path(__file__).resolve().parents[1]
DOMAINS_DIR = REPO_ROOT / "dbt" / "domains"
SHARED_MACROS_DIR = REPO_ROOT / "dbt" / "_shared" / "macros"

# slugify_domain() re-exported from the domain_naming package (not defined
# here anymore) -- every other generate_*.py script in this directory
# imports it from this module (`from generate_domain_projects import
# slugify_domain`), so the import above is kept re-exportable rather than
# updating all six call sites to import from domain_naming directly.


def fetch_domain_names(cur) -> list[str]:
    cur.execute(
        """
        select distinct model_schema as domain from lakehouse_models where is_active = true
        union
        select distinct batch_ods_name as domain from data_feed
        where ods_enabled = true and batch_ods_name is not null and is_active = true
        """
    )
    return sorted({slugify_domain(row[0]) for row in cur.fetchall()})


def _render_dbt_project_yml(domain: str) -> str:
    return f"""name: "domain_{domain}"
version: "1.0.0"
config-version: 2

profile: "domain_{domain}"

model-paths: ["models"]
macro-paths: ["macros"]
snapshot-paths: ["snapshots"]

clean-targets:
  - "target"
  - "dbt_packages"

models:
  domain_{domain}:
    staging:
      +materialized: incremental
      +incremental_strategy: delete+insert
    model:
      +materialized: incremental
      +incremental_strategy: delete+insert
    serve:
      +materialized: view
"""


def sync_shared_macros(domain_dir: Path) -> None:
    """Physically copies every macro in dbt/_shared/macros/ into this
    domain's own macros/ directory -- see this module's docstring for why
    dbt/_shared/ is a template source copied from, not an installed
    package. Unconditional overwrite on every run (unlike the rest of this
    script's create-if-missing output): these files are never meant to be
    hand-edited per domain, dbt/_shared/ is the one place a human edits
    shared macro logic."""
    macros_dir = domain_dir / "macros"
    macros_dir.mkdir(parents=True, exist_ok=True)
    for src in SHARED_MACROS_DIR.glob("*.sql"):
        shutil.copy2(src, macros_dir / src.name)


def _render_profiles_yml(domain: str) -> str:
    return f"""domain_{domain}:
  target: dev
  outputs:
    dev:
      type: trino
      host: "{{{{ env_var('TRINO_HOST', 'localhost') }}}}"
      port: "{{{{ env_var('TRINO_PORT', '8080') | as_number }}}}"
      user: dbt
      catalog: iceberg
      schema: staging
      threads: 4
"""


def scaffold_domain(domain: str, domains_dir: Path) -> bool:
    """Returns True if a new domain project was created, False if it
    already existed (untouched)."""
    domain_dir = domains_dir / domain
    if (domain_dir / "dbt_project.yml").exists():
        return False

    domain_dir.mkdir(parents=True, exist_ok=True)
    (domain_dir / "dbt_project.yml").write_text(_render_dbt_project_yml(domain))

    profiles_dir = domain_dir / "profiles"
    profiles_dir.mkdir(exist_ok=True)
    (profiles_dir / "profiles.yml").write_text(_render_profiles_yml(domain))

    for sub in (
        "macros",
        "models/staging",
        "models/model/dimensions",
        "models/model/facts",
        "models/serve",
        "snapshots",
    ):
        (domain_dir / sub).mkdir(parents=True, exist_ok=True)

    return True


def main() -> None:
    with psycopg.connect(**CONN_KWARGS) as conn, conn.cursor() as cur:
        domains = fetch_domain_names(cur)

    created = [d for d in domains if scaffold_domain(d, DOMAINS_DIR)]
    skipped = [d for d in domains if d not in created]
    # Synced for EVERY domain, new or existing (unlike scaffold_domain()'s
    # create-if-missing gate) -- see sync_shared_macros()'s own comment for why.
    for d in domains:
        sync_shared_macros(DOMAINS_DIR / d)
    print(f"Scaffolded {len(created)} new domain project(s); left {len(skipped)} existing domain(s) untouched.")
    for d in created:
        print(f"  created: dbt/domains/{d}/")


if __name__ == "__main__":
    main()
