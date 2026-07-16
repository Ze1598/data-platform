"""police_crimes' bespoke RestConnector subclass -- the one piece of
hand-written code a nested-JSON source still needs (see connectors.base.
JsonConnector's docstring: pagination and flattening are genuinely
source-specific, no generic implementation is possible). Moved verbatim
from the extraction/raw/clean logic that used to be hand-written in
police_assets.py, since replaced entirely by
scripts/generate_dagster_pipeline.py's connector-driven codegen (that file
no longer exists).
"""

from typing import Any, Optional

import polars as pl

from connectors import RestConnector


class Connector(RestConnector):
    def __init__(self, *, base_url: str, last_watermark: Optional[str] = None):
        super().__init__(base_url=base_url)
        self._last_watermark = last_watermark

    def _months_to_pull(self) -> list[str]:
        """Every month strictly after last_watermark through the latest
        month the API currently has data for -- a watermark means "synced
        up to and including here", so a run catches all the way up to
        what's currently available, it doesn't artificially process one
        month at a time (that pattern only makes sense for a dedicated,
        deliberate historical-backfill job, not a regular incremental
        run). Empty on first run only if the API itself has no data at
        all; otherwise the first run's list is every available month,
        oldest first, all pulled in this one run.
        """
        available_months = sorted(entry["date"] for entry in self._get("crimes-street-dates"))
        if self._last_watermark is None:
            return available_months
        return [m for m in available_months if m > self._last_watermark]

    def fetch(self) -> pl.DataFrame:
        months = self._months_to_pull()
        rows: list[dict[str, Any]] = []
        # One request per month -- 15 req/s rate limit (burst 30, confirmed
        # against the live API) makes even several dozen months trivial to
        # pull sequentially in one run, no throttling needed.
        for month in months:
            rows.extend(self._get("crimes-street/all-crime", params={"lat": "51.5074", "lng": "-0.1278", "date": month}))
        # infer_schema_length=None scans every row before inferring a
        # column's type/struct shape, rather than just the first N -- see
        # _outcome_field()'s docstring for the failure mode this avoids.
        return pl.DataFrame(rows, infer_schema_length=None) if rows else pl.DataFrame()

    @staticmethod
    def _outcome_field(df: pl.DataFrame, field: str) -> pl.Expr:
        """outcome_status is null for most crimes (no outcome recorded yet)
        -- Polars only infers a Struct dtype for it if at least one row in
        the batch actually has a dict there. If literally every row lacks
        an outcome (plausible early in a month, before any investigations
        have concluded), the whole column infers as Null instead of
        Struct, and .struct.field() has nothing to extract from -- fall
        back to an explicit null column of the right type in that case."""
        if isinstance(df.schema["outcome_status"], pl.Struct):
            return pl.col("outcome_status").struct.field(field)
        return pl.lit(None, dtype=pl.Utf8)

    def flatten(self, raw: pl.DataFrame) -> pl.DataFrame:
        # Flattens the API's nested location/location.street/outcome_status
        # structs into the flat row shape schema_registry expects --
        # vectorized struct field access, no per-row Python loop.
        return raw.select(
            pl.col("id"),
            pl.col("persistent_id").fill_null(""),
            pl.col("category"),
            pl.col("location_type").fill_null(""),
            pl.col("location_subtype").fill_null(""),
            pl.col("location").struct.field("street").struct.field("id").alias("street_id"),
            pl.col("location").struct.field("street").struct.field("name").alias("street_name"),
            pl.col("location").struct.field("latitude").cast(pl.Float64, strict=False).alias("latitude"),
            pl.col("location").struct.field("longitude").cast(pl.Float64, strict=False).alias("longitude"),
            pl.col("context").fill_null(""),
            pl.col("month"),
            self._outcome_field(raw, "category").alias("outcome_category"),
            self._outcome_field(raw, "date").alias("outcome_date"),
        )
