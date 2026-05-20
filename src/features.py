"""
Stage 3: Feature Engineering
Computes all vendor-level risk features required by the stakeholder spec.

Point-in-time anchor: 2026-02-25 12:00:00 UTC
"""

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger(__name__)

TRANSFORMED_DIR = Path(__file__).parent.parent / "data" / "transformed"
FEATURES_DIR = Path(__file__).parent.parent / "data" / "features"

# Point-in-time anchor (spec: "now() is 12:00 of 25 Feb 2026")
NOW = pd.Timestamp("2026-02-25 12:00:00", tz="UTC")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _window_mask(df: pd.DataFrame, days: int | None) -> pd.Series:
    """Boolean mask for rows within the last *days* days before NOW.
    Pass days=None for all-time (returns all-True mask)."""
    if days is None:
        return pd.Series(True, index=df.index)
    cutoff = NOW - pd.Timedelta(days=days)
    return (df["payment_time"] >= cutoff) & (df["payment_time"] <= NOW)


def _save(df: pd.DataFrame, name: str, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{name}.csv"
    df.to_csv(path, index=False)
    log.info("Saved %s (%d rows x %d cols)", path, len(df), df.shape[1])


# ---------------------------------------------------------------------------
# F1: Total number of transfers per vendor, all time
# ---------------------------------------------------------------------------

def feature_total_transfers(payments: pd.DataFrame) -> pd.DataFrame:
    transfers = payments[payments["payment_type"].isin(["transfer_in", "transfer_out"])]
    result = (
        transfers.groupby("vendor_id")
        .size()
        .reset_index(name="total_transfers_all_time")
    )
    return result


# ---------------------------------------------------------------------------
# F2: Total number of successful transactions per vendor, all time
# ---------------------------------------------------------------------------

def feature_total_successful_transactions(payments: pd.DataFrame) -> pd.DataFrame:
    succ = payments[payments["status"] == "successful"]
    result = (
        succ.groupby("vendor_id")
        .size()
        .reset_index(name="total_successful_transactions_all_time")
    )
    return result


# ---------------------------------------------------------------------------
# F3: Total amount out of successful payments per vendor, all time
# ---------------------------------------------------------------------------

def feature_total_amount_out(payments: pd.DataFrame) -> pd.DataFrame:
    """
    'Amount out' = successful payments where payment_type is transfer_out or
    transaction_out (money leaving the vendor).
    """
    out_types = {"transfer_out", "transaction_out"}
    mask = (payments["status"] == "successful") & (payments["payment_type"].isin(out_types))
    result = (
        payments[mask]
        .groupby("vendor_id")["amount"]
        .sum()
        .reset_index(name="total_amount_out_successful_all_time")
    )
    return result


# ---------------------------------------------------------------------------
# F4: Total amount of payments IN per vendor, for last 3/5/7/15/30 days
# ---------------------------------------------------------------------------

def feature_amount_in_windows(payments: pd.DataFrame) -> pd.DataFrame:
    in_types = {"transfer_in", "transaction_in"}
    payments_in = payments[payments["payment_type"].isin(in_types)]

    windows = [3, 5, 7, 15, 30]
    aggs = []
    for days in windows:
        mask = _window_mask(payments_in, days)
        col = f"amount_in_last_{days}d"
        agg = (
            payments_in[mask]
            .groupby("vendor_id")["amount"]
            .sum()
            .reset_index(name=col)
        )
        aggs.append(agg)

    result = aggs[0]
    for agg in aggs[1:]:
        result = result.merge(agg, on="vendor_id", how="outer")
    result = result.fillna(0.0)
    return result


# ---------------------------------------------------------------------------
# F5: Z-score of successful payments per payment_method_type × MCC,
#     for last 10 and 30 days
# ---------------------------------------------------------------------------

def feature_zscore_by_method_mcc(
    payments: pd.DataFrame, vendors: pd.DataFrame
) -> pd.DataFrame:
    """
    For each (payment_method_type, mcc, time_window) group, compute the
    z-score of each vendor's successful payment count within that group.
    """
    succ = payments[payments["status"] == "successful"].copy()
    succ = succ.merge(vendors[["vendor_id", "mcc"]], on="vendor_id", how="left")

    windows = [10, 30]
    results = []

    for days in windows:
        mask = _window_mask(succ, days)
        windowed = succ[mask]

        counts = (
            windowed.groupby(["vendor_id", "payment_method_type", "mcc"])
            .size()
            .reset_index(name="txn_count")
        )

        # Compute group-level mean and std for (payment_method_type, mcc)
        group_stats = (
            counts.groupby(["payment_method_type", "mcc"])["txn_count"]
            .agg(group_mean="mean", group_std="std")
            .reset_index()
        )

        counts = counts.merge(group_stats, on=["payment_method_type", "mcc"], how="left")
        # std=0 (single vendor in group) => z-score is 0
        counts[f"zscore_last_{days}d"] = np.where(
            counts["group_std"].isna() | (counts["group_std"] == 0),
            0.0,
            (counts["txn_count"] - counts["group_mean"]) / counts["group_std"],
        )
        counts = counts.rename(columns={"txn_count": f"txn_count_last_{days}d"})
        counts = counts.drop(columns=["group_mean", "group_std"])
        results.append(counts)

    # Merge both windows on vendor × method × mcc
    result = results[0].merge(
        results[1],
        on=["vendor_id", "payment_method_type", "mcc"],
        how="outer",
    )
    return result


# ---------------------------------------------------------------------------
# F6: Vendors sharing the same devices per device, all time
# ---------------------------------------------------------------------------

def feature_device_sharing_alltime(payments: pd.DataFrame) -> pd.DataFrame:
    """
    For each device, list the vendors that have used it.
    Only include devices used by more than one vendor.
    """
    device_vendors = (
        payments[payments["device_id"].notna()]
        .groupby("device_id")["vendor_id"]
        .unique()
        .reset_index()
    )
    device_vendors.columns = ["device_id", "vendors"]
    device_vendors["vendor_count"] = device_vendors["vendors"].apply(len)
    shared = device_vendors[device_vendors["vendor_count"] > 1].copy()
    shared["vendors"] = shared["vendors"].apply(lambda v: sorted(v))
    return shared[["device_id", "vendors", "vendor_count"]]


# ---------------------------------------------------------------------------
# F7: Number of days since sign-up per vendor
# ---------------------------------------------------------------------------

def feature_days_since_signup(vendors: pd.DataFrame) -> pd.DataFrame:
    df = vendors[["vendor_id", "sign_up_time"]].copy()
    df["days_since_signup"] = (NOW - df["sign_up_time"]).dt.days
    return df[["vendor_id", "days_since_signup"]]


# ---------------------------------------------------------------------------
# F8a: Per-device, list of vendors and count — across time windows
#       (only devices shared by >1 vendor)
# ---------------------------------------------------------------------------

def feature_device_shared_vendors_windows(payments: pd.DataFrame) -> pd.DataFrame:
    """
    For each device shared by >1 vendor, and for each time window,
    return the list of vendors and their count.
    """
    windows = [3, 5, 10, 40, None]  # None = all time
    results = []

    base = payments[payments["device_id"].notna()][["device_id", "vendor_id", "payment_time"]].copy()

    for days in windows:
        label = f"last_{days}d" if days is not None else "all_time"
        mask = _window_mask(base, days)
        windowed = base[mask]

        agg = (
            windowed.groupby("device_id")["vendor_id"]
            .unique()
            .reset_index()
        )
        agg.columns = ["device_id", f"vendors_{label}"]
        agg[f"vendor_count_{label}"] = agg[f"vendors_{label}"].apply(len)

        # Keep only devices used by >1 vendor in this window
        agg = agg[agg[f"vendor_count_{label}"] > 1].copy()
        agg[f"vendors_{label}"] = agg[f"vendors_{label}"].apply(lambda v: sorted(v))
        results.append(agg)

    result = results[0]
    for agg in results[1:]:
        result = result.merge(agg, on="device_id", how="outer")

    return result


# ---------------------------------------------------------------------------
# F8b: Per vendor, list of other vendors sharing at least one device — windows
# ---------------------------------------------------------------------------

def feature_vendor_shared_devices_windows(payments: pd.DataFrame) -> pd.DataFrame:
    """
    For each vendor that shares at least one device with another vendor,
    return the list of co-vendors, the shared devices, and counts —
    for each time window.
    """
    windows = [3, 5, 10, 40, None]
    base = payments[payments["device_id"].notna()][["device_id", "vendor_id", "payment_time"]].copy()

    all_results = []

    for days in windows:
        label = f"last_{days}d" if days is not None else "all_time"
        mask = _window_mask(base, days)
        windowed = base[mask][["device_id", "vendor_id"]].drop_duplicates()

        # For each device, find all vendor pairs
        device_vendors = (
            windowed.groupby("device_id")["vendor_id"]
            .unique()
            .reset_index()
        )
        device_vendors = device_vendors[device_vendors["vendor_id"].apply(len) > 1]

        # Expand to vendor × co_vendor pairs
        rows = []
        for _, row in device_vendors.iterrows():
            vendors = sorted(row["vendor_id"])
            for i, v1 in enumerate(vendors):
                for v2 in vendors[i + 1:]:
                    rows.append({"vendor_id": v1, "co_vendor": v2, "device_id": row["device_id"]})
                    rows.append({"vendor_id": v2, "co_vendor": v1, "device_id": row["device_id"]})

        if not rows:
            continue

        pairs = pd.DataFrame(rows)

        agg = (
            pairs.groupby("vendor_id")
            .agg(
                co_vendors=("co_vendor", lambda x: sorted(set(x))),
                shared_devices=("device_id", lambda x: sorted(set(x))),
            )
            .reset_index()
        )
        agg[f"co_vendor_count_{label}"] = agg["co_vendors"].apply(len)
        agg[f"shared_device_count_{label}"] = agg["shared_devices"].apply(len)
        agg = agg.rename(columns={
            "co_vendors": f"co_vendors_{label}",
            "shared_devices": f"shared_devices_{label}",
        })

        all_results.append(agg)

    if not all_results:
        return pd.DataFrame(columns=["vendor_id"])

    result = all_results[0]
    for agg in all_results[1:]:
        result = result.merge(agg, on="vendor_id", how="outer")

    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(
    transformed_dir: Path = TRANSFORMED_DIR,
    features_dir: Path = FEATURES_DIR,
) -> None:
    payments = pd.read_parquet(transformed_dir / "payments.parquet")
    vendors = pd.read_parquet(transformed_dir / "vendors.parquet")

    log.info("Computing features with NOW=%s", NOW)

    # Filter to point-in-time: exclude any future records
    payments = payments[payments["payment_time"] <= NOW].copy()

    f1 = feature_total_transfers(payments)
    _save(f1, "f1_total_transfers", features_dir)

    f2 = feature_total_successful_transactions(payments)
    _save(f2, "f2_total_successful_transactions", features_dir)

    f3 = feature_total_amount_out(payments)
    _save(f3, "f3_total_amount_out", features_dir)

    f4 = feature_amount_in_windows(payments)
    _save(f4, "f4_amount_in_windows", features_dir)

    f5 = feature_zscore_by_method_mcc(payments, vendors)
    _save(f5, "f5_zscore_method_mcc", features_dir)

    f6 = feature_device_sharing_alltime(payments)
    _save(f6, "f6_device_sharing_alltime", features_dir)

    f7 = feature_days_since_signup(vendors)
    _save(f7, "f7_days_since_signup", features_dir)

    f8a = feature_device_shared_vendors_windows(payments)
    _save(f8a, "f8a_device_shared_vendors_windows", features_dir)

    f8b = feature_vendor_shared_devices_windows(payments)
    _save(f8b, "f8b_vendor_shared_devices_windows", features_dir)

    log.info("Feature engineering complete. Files written to %s", features_dir)


if __name__ == "__main__":
    run()
