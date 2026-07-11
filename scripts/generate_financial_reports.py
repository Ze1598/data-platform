"""Generates one new batch of synthetic financial (general-ledger-style)
transactions as a CSV file, simulating a periodic export from an upstream
accounting/ERP system landing in `data-lake/landing/financial_transactions/`.

Deliberately standalone, not run by the pipeline itself (Phase 9 -- see
Roadmap.md) -- generation and extraction are decoupled, the same way a real
file-drop source would be. `landing_financial_transactions` (Dagster asset)
discovers and reads whatever's actually sitting in the landing directory; it
never generates anything itself. Each invocation of this script produces one
new batch, with genuinely new transaction IDs/dates -- run it multiple times
to simulate multiple periodic drops.

One vectorized (numpy + Polars) generator, parameterized by row count --
`--count` defaults to 25 (a normal periodic drop); pass a much larger value
(e.g. `--count 3000000`) to stress-test at real scale. A Python
for-loop-per-row approach is fine at 25 rows and falls over completely at
millions, so there's only one code path here, not a small/bulk split.
"""

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
LANDING_DIR = REPO_ROOT / "data-lake" / "landing" / "financial_transactions"

# A small, plausible chart of accounts -- not exhaustive, just enough to
# produce realistic-looking journal entries.
_ACCOUNTS = [
    ("1000", "Cash"),
    ("1200", "Accounts Receivable"),
    ("2000", "Accounts Payable"),
    ("4000", "Revenue"),
    ("5000", "Cost of Goods Sold"),
    ("6100", "Salaries Expense"),
    ("6200", "Rent Expense"),
    ("6300", "Utilities Expense"),
    ("6400", "Marketing Expense"),
    ("6500", "Office Supplies"),
]
_COST_CENTERS = ["Sales", "Operations", "Marketing", "Finance", "Engineering"]
_DESCRIPTIONS = [
    "Invoice payment received",
    "Vendor payment",
    "Monthly accrual",
    "Payroll run",
    "Office lease payment",
    "Utility bill",
    "Ad campaign spend",
    "Supplies purchase",
    "Customer refund",
    "Bank fee",
]


def generate_batch(count: int, min_date: datetime, max_date: datetime) -> pl.DataFrame:
    """Vectorized (numpy + Polars) generation -- every column is built as a
    single array operation rather than a per-row Python call, so this scales
    the same way whether `count` is 25 or several million. Returns a
    DataFrame directly (not list[dict] + a separate CSV writer) so a batch
    never round-trips through Python objects at all before hitting disk.
    """
    rng = np.random.default_rng()
    account_idx = rng.integers(0, len(_ACCOUNTS), size=count)
    account_codes = np.array([a[0] for a in _ACCOUNTS])[account_idx]
    account_names = np.array([a[1] for a in _ACCOUNTS])[account_idx]
    descriptions = np.array(_DESCRIPTIONS)[rng.integers(0, len(_DESCRIPTIONS), size=count)]
    cost_centers = np.array(_COST_CENTERS)[rng.integers(0, len(_COST_CENTERS), size=count)]

    amounts = np.round(rng.uniform(50, 25000, size=count), 2)
    is_debit = rng.random(size=count) < 0.5
    debit_amounts = np.where(is_debit, amounts, 0.0)
    credit_amounts = np.where(is_debit, 0.0, amounts)

    epoch_seconds = rng.uniform(min_date.timestamp(), max_date.timestamp(), size=count)

    batch_label = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    return (
        pl.DataFrame(
            {
                "transaction_id": [f"TXN-{batch_label}-{i:08d}" for i in range(count)],
                "posted_date_epoch": epoch_seconds,
                "account_code": account_codes,
                "account_name": account_names,
                "description": descriptions,
                "debit_amount": debit_amounts,
                "credit_amount": credit_amounts,
                "currency": ["USD"] * count,
                "cost_center": cost_centers,
            }
        )
        .with_columns(
            pl.from_epoch("posted_date_epoch", time_unit="s").dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias("posted_date")
        )
        .drop("posted_date_epoch")
        .select(
            "transaction_id", "posted_date", "account_code", "account_name", "description",
            "debit_amount", "credit_amount", "currency", "cost_center",
        )
    )


def write_batch(df: pl.DataFrame, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"transactions_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}.csv"
    path = output_dir / filename
    df.write_csv(path)
    return path


def _parse_date(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=25, help="Number of transactions to generate (default: 25)")
    parser.add_argument("--min-date", type=_parse_date, default=None, help="YYYY-MM-DD, earliest posted_date (default: 3 days ago)")
    parser.add_argument("--max-date", type=_parse_date, default=None, help="YYYY-MM-DD, latest posted_date (default: now)")
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    min_date = args.min_date or (now - timedelta(days=3))
    max_date = args.max_date or now
    df = generate_batch(args.count, min_date, max_date)
    path = write_batch(df, LANDING_DIR)
    print(f"Wrote {args.count} transactions ({min_date.date()} to {max_date.date()}) to {path}")


if __name__ == "__main__":
    main()
