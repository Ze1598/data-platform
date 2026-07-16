"""Single source of truth for turning a free-text `lakehouse_models.model_schema`/
`data_feed.batch_ods_name` value into the slug used as a domain's directory
name, dbt project/profile name, and Dagster `DOMAIN_JOBS`/`DOMAIN_FEEDS` dict
key (see Roadmap.md "multi-project dbt split").

Exists as its own tiny workspace member specifically so `scripts/` (build-time
codegen, e.g. `generate_domain_projects.py`) and `orchestration/dagster_data_platform`
(`postgres_metadata_resource.py`'s live trigger-by-model resolver) can both
depend on the exact same implementation, rather than maintaining two
independent copies that could silently drift apart -- confirmed as a real
gap, not hypothetical: `postgres_metadata_resource.py` used to carry its own
`_slugify_domain()`, commented "mirrors ... exactly -- not imported", because
`scripts` (`[tool.uv] package = false`) isn't an importable package neither
side could depend on directly. A real shared package removes that
constraint outright.
"""

import re

_SLUG_INVALID = re.compile(r"[^a-z0-9_]+")


def slugify_domain(raw: str) -> str:
    """Lowercase, non-alnum -> underscore, never starting with a digit. The
    frontend validates new domain names against this same shape at entry
    time (see `frontend/pages/2_Data_Feeds.py`/`3_Lakehouse_Models.py`) so a
    value reaching this function should already be valid; this is a
    defensive second pass, not the primary guard.
    """
    slug = _SLUG_INVALID.sub("_", raw.strip().lower()).strip("_")
    if not slug:
        raise ValueError(f"model_schema/batch_ods_name {raw!r} has no valid characters for a domain name")
    if slug[0].isdigit():
        slug = f"d_{slug}"
    return slug
