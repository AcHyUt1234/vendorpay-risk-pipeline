"""
Stage 2: Transformation
Merges the ingested stream events and bulk CSV, deduplicates, and produces
a single clean payments table alongside the clean vendor profiles table.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

INGESTED_DIR = Path(__file__).parent.parent / "data" / "ingested"
TRANSFORMED_DIR = Path(__file__).parent.parent / "data" / "transformed"


# ---------------------------------------------------------------------------
# Payments
# ---------------------------------------------------------------------------

def transform_payments(ingested_dir: Path, out_path: Path) -> pd.DataFrame:
    events = pd.read_parquet(ingested_dir / "payment_events.parquet")
    bulk = pd.read_parquet(ingested_dir / "payment_bulk.parquet")

    combined = pd.concat([events, bulk], ignore_index=True)
    before = len(combined)

    # Deduplicate: stream records are authoritative over bulk records for the
    # same payment_id.  Sort stream first so it survives keep='first'.
    combined = combined.sort_values(
        "source",
        key=lambda s: s.map({"stream": 0, "bulk": 1}),
    )
    combined = combined.drop_duplicates(subset="payment_id", keep="first")
    log.info(
        "payments: %d rows after merge+dedup (removed %d duplicates)",
        len(combined),
        before - len(combined),
    )

    # Normalise statuses to lowercase
    combined["status"] = combined["status"].str.lower().str.strip()
    combined["payment_type"] = combined["payment_type"].str.lower().str.strip()
    combined["payment_method_type"] = combined["payment_method_type"].str.lower().str.strip()

    # Ensure payment_time is UTC-aware
    if combined["payment_time"].dt.tz is None:
        combined["payment_time"] = combined["payment_time"].dt.tz_localize("UTC")

    # Expose a date column for window filters
    combined["payment_date"] = combined["payment_time"].dt.normalize()

    out_path.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(out_path, index=False)
    log.info("Clean payments written to %s (%d rows)", out_path, len(combined))
    return combined


# ---------------------------------------------------------------------------
# Vendor profiles
# ---------------------------------------------------------------------------

def transform_vendors(ingested_dir: Path, out_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(ingested_dir / "vendor_profiles.parquet")

    # Ensure sign_up_time is UTC-aware
    for col in ("sign_up_time", "created_at", "cdc_timestamp"):
        if col in df.columns:
            if df[col].dt.tz is None:
                df[col] = df[col].dt.tz_localize("UTC")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    log.info("Clean vendors written to %s (%d rows)", out_path, len(df))
    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(
    ingested_dir: Path = INGESTED_DIR,
    transformed_dir: Path = TRANSFORMED_DIR,
) -> None:
    transform_payments(ingested_dir, transformed_dir / "payments.parquet")
    transform_vendors(ingested_dir, transformed_dir / "vendors.parquet")
    log.info("Transformation complete. Files written to %s", transformed_dir)


if __name__ == "__main__":
    run()
