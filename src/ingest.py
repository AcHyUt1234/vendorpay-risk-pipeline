"""
Stage 1: Ingestion
Reads raw sources (JSONL stream, CDC vendor profiles, CSV bulk export),
normalises structure, and writes to the ingested layer as Parquet files.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Iterator

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
INGESTED_DIR = Path(__file__).parent.parent / "data" / "ingested"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _iter_jsonl(path: Path) -> Iterator[dict]:
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def _parse_payment_time(ts: int) -> pd.Timestamp | None:
    """
    payment_time is inconsistently encoded: some values are Unix seconds,
    some microseconds, some are corrupted (>1e18).  We detect the unit by
    magnitude and fall back to NaT for unrecoverable values.
    """
    if ts > 1e18:
        return pd.NaT
    elif ts > 1e15:
        return pd.Timestamp(ts / 1_000, unit="ms", tz="UTC")  # microseconds -> ms
    elif ts > 1e12:
        return pd.Timestamp(ts, unit="ms", tz="UTC")
    else:
        return pd.Timestamp(ts, unit="s", tz="UTC")


# ---------------------------------------------------------------------------
# Payment events (streaming JSONL)
# ---------------------------------------------------------------------------

def _flatten_event(record: dict) -> dict:
    p = record["payment"]
    m = record["merchant"]
    pm = record["payment_method"]
    cust = record["customer"]
    dev = record["device"]
    stream = record["stream"]
    risk = record.get("risk", {})

    # payment_method type can be a list or a string
    pm_types = pm.get("type", [])
    pm_type = pm_types[0] if isinstance(pm_types, list) and pm_types else pm_types

    return {
        "payment_id": p.get("payment_id"),
        "vendor_id": m.get("merchant_id"),
        "payment_type": p.get("type"),
        "status": p.get("status"),
        "payment_time": _parse_payment_time(p["payment_time"]),
        "stream_event_timestamp": pd.Timestamp(stream["stream_event_timestamp"]),
        "amount": p.get("amount"),
        "currency": p.get("currency"),
        "channel": p.get("channel"),
        "payment_method_type": pm_type,
        "device_id": dev.get("device_id"),
        "customer_id": cust.get("customer_id"),
        "risk_score": risk.get("score"),
        "source": "stream",
    }


def ingest_payment_events(raw_path: Path, out_path: Path) -> pd.DataFrame:
    rows = [_flatten_event(r) for r in _iter_jsonl(raw_path)]
    df = pd.DataFrame(rows)

    before = len(df)
    df = df.dropna(subset=["payment_id", "vendor_id", "payment_time"])
    log.info(
        "payment_events: %d raw rows, %d after dropping bad timestamps/ids",
        before,
        len(df),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    return df


# ---------------------------------------------------------------------------
# Bulk CSV
# ---------------------------------------------------------------------------

def ingest_payment_bulk(raw_path: Path, out_path: Path) -> pd.DataFrame:
    df = pd.read_csv(raw_path)

    df = df.rename(
        columns={
            "merchant_id": "vendor_id",
            "payment_type": "payment_type",
        }
    )

    # Normalise timestamps to UTC-aware
    df["payment_time"] = pd.to_datetime(df["payment_time"], utc=True, errors="coerce")
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True, errors="coerce")

    # Align column names with the stream schema
    df = df.rename(columns={"payment_type": "payment_type"})
    df["source"] = "bulk"
    df["stream_event_timestamp"] = df["created_at"]
    df["risk_score"] = float("nan")
    df["customer_id"] = float("nan")

    # Keep only the columns that match the stream schema
    keep = [
        "payment_id", "vendor_id", "payment_type", "status",
        "payment_time", "stream_event_timestamp", "amount", "currency",
        "channel", "payment_method_type", "device_id",
        "customer_id", "risk_score", "source",
    ]
    df = df[keep]

    before = len(df)
    df = df.dropna(subset=["payment_id", "vendor_id", "payment_time"])
    log.info(
        "payment_bulk: %d raw rows, %d after dropping bad timestamps/ids",
        before,
        len(df),
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    return df


# ---------------------------------------------------------------------------
# Vendor profiles (CDC JSONL)
# ---------------------------------------------------------------------------

def _flatten_vendor(record: dict) -> dict:
    after = record.get("after") or {}
    biz = after.get("business", {})
    kyc = after.get("kyc", {})
    dev = after.get("device", {})
    stream = record.get("stream", {})

    return {
        "vendor_id": after.get("vendor_id"),
        "op": record.get("op"),          # 'c' = create, 'u' = update
        "cdc_timestamp": pd.Timestamp(stream["cdc_timestamp"]) if stream.get("cdc_timestamp") else pd.NaT,
        "sign_up_time": pd.Timestamp(after["sign_up_time"]) if after.get("sign_up_time") else pd.NaT,
        "created_at": pd.Timestamp(after.get("created_at")) if after.get("created_at") else pd.NaT,
        "country_code": after.get("country", {}).get("code"),
        "mcc": biz.get("mcc"),
        "currency": biz.get("currency"),
        "kyc_status": kyc.get("status"),
        "onboarding_stage": kyc.get("onboarding_stage"),
        "risk_score": kyc.get("risk_score"),
        "device_id": dev.get("device_id"),
    }


def ingest_vendor_profiles(raw_path: Path, out_path: Path) -> pd.DataFrame:
    rows = [_flatten_vendor(r) for r in _iter_jsonl(raw_path)]
    df = pd.DataFrame(rows)

    # Keep the latest CDC record per vendor (highest sequence = last known state)
    df = df.sort_values("cdc_timestamp").drop_duplicates("vendor_id", keep="last")
    log.info("vendor_profiles: %d unique vendors after CDC dedup", len(df))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)
    return df


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(raw_dir: Path = RAW_DIR, ingested_dir: Path = INGESTED_DIR) -> None:
    ingest_payment_events(
        raw_dir / "payment_events.jsonl",
        ingested_dir / "payment_events.parquet",
    )
    ingest_payment_bulk(
        raw_dir / "payment_bulk.csv",
        ingested_dir / "payment_bulk.parquet",
    )
    ingest_vendor_profiles(
        raw_dir / "vendor_profiles.jsonl",
        ingested_dir / "vendor_profiles.parquet",
    )
    log.info("Ingestion complete. Files written to %s", ingested_dir)


if __name__ == "__main__":
    run()
