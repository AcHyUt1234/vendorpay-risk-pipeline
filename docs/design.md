# VendorPay Pipeline — Design Documentation

## Architecture Overview

The pipeline follows a classic **medallion** pattern: raw → ingested → transformed → features. Each stage has a single responsibility and writes self-contained Parquet or CSV files, so any stage can be re-run or inspected independently.

```
┌──────────────────────────────────────────────────────────────────┐
│  RAW (data/raw/)                                                  │
│  payment_events.jsonl  payment_bulk.csv  vendor_profiles.jsonl   │
└──────────────────┬───────────────────────────────────────────────┘
                   │  Stage 1: ingest.py
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│  INGESTED (data/ingested/)                                        │
│  payment_events.parquet  payment_bulk.parquet  vendor_profiles.parquet │
└──────────────────┬───────────────────────────────────────────────┘
                   │  Stage 2: transform.py
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│  TRANSFORMED (data/transformed/)                                  │
│  payments.parquet (merged, deduped)    vendors.parquet            │
└──────────────────┬───────────────────────────────────────────────┘
                   │  Stage 3: features.py
                   ▼
┌──────────────────────────────────────────────────────────────────┐
│  FEATURES (data/features/)                                        │
│  f1_ … f8b_  (CSV per feature)                                   │
└──────────────────────────────────────────────────────────────────┘
```

**Storage choice:** Parquet for intermediate layers (columnar, typed, fast to read partially), CSV for feature outputs (human-readable, easy to import into notebooks or BI tools). Both formats work without a running database, keeping the pipeline portable.

---

## Design Decisions & Assumptions

### Stage 1 — Ingestion

**JSONL format:** Both `payment_events` and `vendor_profiles` are JSONL (one JSON object per line), not JSON arrays. This is consistent with the stated production intent of treating the files as a continuous stream, where appending a line is cheaper than rewriting the whole array.

**Vendor CDC deduplication:** `vendor_profiles.jsonl` is a CDC (Change Data Capture) stream containing both `c` (create) and `u` (update) operations for 40 vendors across 113 events. The ingestion stage deduplicates to the **latest CDC record per vendor** (sorted by `cdc_timestamp`), capturing the vendor's most recent known state at ingestion time. This is appropriate for the point-in-time anchor used in Stage 3.

**Stream vs. bulk priority:** The same `payment_id` can appear in both the streaming JSONL and the bulk CSV (the bulk is described as "similar in structure"). In `transform.py`, stream records are treated as authoritative over bulk records for duplicates, since stream events carry richer metadata (risk score, structured customer/device objects, `stream_event_id`).

**Corrupted timestamps:** `payment_time` in `payment_events.jsonl` contains values encoded as Unix seconds (~1.77e9), milliseconds (~1.77e12), and microseconds (~1.77e15), with a subset of values exceeding 1e18 that are unrecoverable. The ingestion stage auto-detects the unit by magnitude and drops unrecoverable records (517/2000) with a logged warning. Using `stream_event_timestamp` as the canonical timestamp was considered, but `payment_time` is the actual transaction time, which is required for point-in-time feature windows.

### Stage 2 — Transformation

**Deduplication strategy:** After union-ing stream and bulk, the combined dataset is sorted with stream records first, then deduplicated on `payment_id` keeping the first occurrence. This is a single-pass dedup that is easy to verify and avoids complex merge logic.

**Status / type normalisation:** Status and payment type values are lowercased and stripped. The stream events introduce a `chargeback` status absent in the bulk CSV; this is preserved as-is for downstream use.

### Stage 3 — Feature Engineering

**Point-in-time discipline:** The NOW constant (`2026-02-25 12:00:00 UTC`) is defined once at the top of `features.py`. All window filters use `payment_time <= NOW` as the upper bound, preventing any future data from leaking into features.

**"Transfers" definition (F1):** The spec says "total number of transfers". The data contains four `payment_type` values: `transfer_in`, `transfer_out`, `transaction_in`, `transaction_out`. I count both `transfer_in` and `transfer_out` as transfers (F1), and `transaction_in`/`transaction_out` as transactions (F2 counts all successful events regardless of type, matching the spec wording "successful transactions").

**"Amount out" definition (F3):** Interpreted as the sum of `amount` where `status = 'successful'` and `payment_type` is `transfer_out` or `transaction_out` (money leaving the vendor's account).

**"Payments in" definition (F4):** Interpreted as `payment_type` in `{'transfer_in', 'transaction_in'}`. All statuses are included (not just successful), matching the spec wording "total amount of payments in" without a success qualifier.

**Z-score (F5):** Computed as `(vendor_count - group_mean) / group_std` where the group is `(payment_method_type, mcc)`. Groups with a single vendor get a z-score of 0.0 (undefined distribution). This is a reasonable default and is transparent to downstream consumers.

**Device sharing (F6, F8a, F8b):** Only rows where `device_id` is non-null are considered. The spec says "only cases where a device has been associated with more than one vendor" — this filter is applied after aggregating by window, so a device that appears with two vendors in the last 3 days but only one vendor all-time would still appear in the all-time output.

**Days since signup (F7):** Uses `sign_up_time` from the vendor profile (the earliest CDC record's field). If a vendor has no sign-up time, the value is NaT.

---

## Data Quality Challenges

| Issue | Location | Handling |
|---|---|---|
| Mixed-unit timestamps (`payment_time`) | payment_events | Auto-detect by magnitude; drop if >1e18 |
| Duplicate payment IDs across stream + bulk | Both | Stream takes priority; bulk deduplicated on `payment_id` |
| CDC fan-out (multiple records per vendor) | vendor_profiles | Keep latest by `cdc_timestamp` |
| Missing `device_id` (~5% of rows) | Both | Excluded from device-sharing features |
| Vendors in payments not in vendor_profiles | payments | Retained in payment features; NaT for sign-up features |
| `mcc` not available in payments directly | payments + vendors | Joined from vendor_profiles via `vendor_id` for F5 |

---

## Dependencies

The pipeline has **two runtime dependencies**:

- `pandas` — DataFrame operations throughout
- `pyarrow` — Parquet read/write backend

No database, message broker, or cloud service is required. The pipeline is entirely file-based and runs offline.

---

## What I Would Do Differently with More Time

### Productionisation

**Incremental ingestion:** The current pipeline is a full recompute. For a production stream, Stage 1 would track the last-processed `stream_event_id` (for the event stream) or `export_batch_id` (for bulk), processing only new records. Stage 3 would maintain a rolling window state table rather than recomputing from scratch.

**Idempotency:** Each stage currently overwrites its output. Adding content-addressed output paths (e.g., partitioned by processing date) would make reruns safe and auditable.

**Orchestration:** The three stages would be wrapped as tasks in a DAG (Airflow, Prefect, or similar) with retry logic, SLA alerts, and dependency tracking.

### Data Quality

**Schema validation:** Add `pandera` or `pydantic` schemas at ingestion to catch upstream schema drift early — e.g., if `payment_time` encoding changes or a new `payment_type` is introduced.

**Timestamp repair:** The corrupted `payment_time` values (>1e18) could be partially recovered by cross-referencing `stream_event_timestamp` from the same record, with a flag column marking the substitution.

**Deduplication audit:** Expose a report of how many records from each source were dropped as duplicates, broken down by `vendor_id`, so the data team can investigate upstream double-publishing.

### Feature Store

**Versioning:** Features used for ML training need to be versioned and tied to the pipeline run that produced them, so models can be retrained against the exact feature set they were trained on.

**Feature serving:** For near-real-time fraud rules, the windowed aggregations (F4, F8) would need to be pre-materialised and served from a low-latency store (Redis, DynamoDB) rather than computed at query time.

### Testing

- Unit tests for each feature function with known small datasets
- Integration test that runs the full pipeline on the provided sample data and asserts row counts and specific values
- Property-based tests for the timestamp parser (any value in range [1e9, 1e18] must parse without error)

---

## Open Questions

1. **Canonical vendor identifier:** The bulk CSV uses `merchant_id` values with both `ldg_` and `vnd_` prefixes, while vendor profiles use `vendor_id` with only `vnd_` prefixes. Are `ldg_` IDs a different entity type (ledger accounts vs. vendors)? If so, features for `ldg_` IDs cannot be joined to vendor profiles, and F7 (days since signup) will be null for them. This needs clarification with the product team.

2. **"Successful transactions" vs. "successful payments":** The spec uses both terms. F2 counts all rows with `status = 'successful'` regardless of `payment_type`. If the intent is to exclude transfers from this count, the feature definition changes.

3. **Currency normalisation:** Transactions are in multiple currencies (USD, EUR, GBP, …). F3 and F4 sum raw amounts without FX conversion. For cross-currency vendor comparisons this would be misleading. Is there a canonical exchange rate source?

4. **Device ID in vendor profiles vs. payments:** Vendor profiles contain a `device.device_id` field (presumably the device used at sign-up). Should this be included in device-sharing features alongside the `device_id` from transactions?
