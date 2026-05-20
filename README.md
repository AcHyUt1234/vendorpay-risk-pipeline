# VendorPay Risk & Analytics Pipeline

A three-stage data pipeline that ingests raw payment and vendor data, transforms it into a clean unified dataset, and computes vendor-level risk features for the R&C team.

## Setup

**Requirements:** Python 3.10+, no virtual environment required beyond the two pip dependencies.

```bash
pip install pandas pyarrow
```

Place the raw source files in `data/raw/`:
```
data/raw/payment_events.jsonl   # streaming payment events (JSONL)
data/raw/payment_bulk.csv       # 12-month bulk CSV export
data/raw/vendor_profiles.jsonl  # vendor CDC stream (JSONL)
```

## Running

**Full end-to-end pipeline:**
```bash
python pipeline.py
```

**Individual stages:**
```bash
python pipeline.py ingest       # Stage 1 – raw → ingested/
python pipeline.py transform    # Stage 2 – ingested/ → transformed/
python pipeline.py features     # Stage 3 – transformed/ → features/
```

Each stage can run independently provided its upstream inputs exist.

## Outputs

Feature CSVs are written to `data/features/`:

| File | Description |
|---|---|
| `f1_total_transfers.csv` | Total transfers per vendor, all time |
| `f2_total_successful_transactions.csv` | Total successful transactions per vendor, all time |
| `f3_total_amount_out.csv` | Total successful amount out per vendor, all time |
| `f4_amount_in_windows.csv` | Amount in per vendor for last 3/5/7/15/30 days |
| `f5_zscore_method_mcc.csv` | Z-score of successful payments by method × MCC, last 10 and 30 days |
| `f6_device_sharing_alltime.csv` | Devices shared by >1 vendor with vendor lists, all time |
| `f7_days_since_signup.csv` | Days since sign-up per vendor |
| `f8a_device_shared_vendors_windows.csv` | Per device: vendor lists and counts, across time windows |
| `f8b_vendor_shared_devices_windows.csv` | Per vendor: co-vendors, shared devices, and counts, across time windows |

## Project layout

```
vendorpay/
├── pipeline.py          # End-to-end runner
├── src/
│   ├── ingest.py        # Stage 1 – ingestion
│   ├── transform.py     # Stage 2 – transformation
│   └── features.py      # Stage 3 – feature engineering
├── data/
│   ├── raw/             # Source files (input)
│   ├── ingested/        # Normalised Parquet (stage 1 output)
│   ├── transformed/     # Cleaned, merged Parquet (stage 2 output)
│   └── features/        # Feature CSVs (stage 3 output)
└── docs/
    └── design.md        # Architecture and design decisions
```
