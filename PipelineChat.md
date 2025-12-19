I want to create a clean, scalable pipeline that:

* **extracts directly from WRDS TAQ** (no CSV),
* writes **lossless raw Parquet** (your archival truth),
* builds **microstructure-ready enriched trades** via **ASOF join to NBBO**,
* and creates **1-minute bars** (trade OHLCV + NBBO features).

Below is a concrete design:

---

# 1) Canonical schemas (lossless raw Parquet)

## 1.1 Common conventions

### Normalized symbol

Create a canonical `symbol`:

* `symbol = sym_root` if `sym_suffix` is blank/null
* else `symbol = sym_root || '.' || sym_suffix`

Store `sym_root` and `sym_suffix` too (lossless).

### Canonical event timestamp

All three datasets have `date`, `time_m`, `time_m_nano`. Make this your primary `ts_event`.

* `ts_event = datetime(date) + time_m + time_m_nano nanoseconds`

Also keep `part_time`, `trf_time`, `finra_adf_time` as provided.

### Types (pragmatic)

* prices as `DECIMAL(p,s)` (or Arrow `decimal128`) to be lossless
* nanos offset as `INT16`
* sizes as `INT32`
* seqnums as `INT64`
* condition flags as fixed strings/chars

---

## 1.2 Raw trades Parquet

**Dataset:** `parquet_raw/trades/`

**Partitioning:**

```
parquet_raw/trades/trade_date=YYYY-MM-DD/symbol=.../part-*.parquet
```

**Columns (store all original + derived keys):**

* Original:

  * `date`, `time_m`, `time_m_nano`
  * `part_time`, `part_time_nano`
  * `trf_time`, `trf_time_nano`
  * `sym_root`, `sym_suffix`
  * `ex`
  * `price`, `size`
  * `tr_corr`, `tr_id`, `tr_rf`, `tr_scond`, `tr_seqnum`, `tr_source`, `tr_stop_ind`, `tte_ind`
* Added:

  * `trade_date` (same as `date`) — for partition/filters
  * `symbol` (string)
  * `ts_event` (timestamp) — derived from `date/time_m/time_m_nano`
  * `extract_run_id` (uuid/string), `ingest_ts` (timestamp) — provenance

---

## 1.3 Raw quotes Parquet

Same partitioning:

```
parquet_raw/quotes/trade_date=YYYY-MM-DD/symbol=.../part-*.parquet
```

Store all quote fields + `symbol`, `ts_event`, provenance.

---

## 1.4 Raw NBBO Parquet (high value, keep long-term)

```
parquet_raw/nbbo/trade_date=YYYY-MM-DD/symbol=.../part-*.parquet
```

Store all NBBO fields + `symbol`, `ts_event`, provenance.

---

# 2) Pipeline stages (direct WRDS → Parquet)

## Stage A — Extract (direct from WRDS TAQ)

**Unit of work:** `(trade_date, symbol)` (best for incremental updates and parallelism)

* Query WRDS for a given date + symbol
* Stream results and write Parquet parts
* Write into a temp directory, then atomic rename to final partition folder

**Idempotency rule:**

* if `parquet_raw/.../trade_date=.../symbol=.../_SUCCESS` exists, skip
* unless `--overwrite-symbol` is passed

This makes “add tickers later” trivial.

---

## Stage B — Enrich trades with NBBO (microstructure core)

Create:

```
parquet_derived/enriched_trades/trade_date=YYYY-MM-DD/symbol=.../part-*.parquet
```

**Definition:** for each trade, attach the most recent NBBO record at or before trade time.

Fields:

* all trade raw fields (or a selected subset if you prefer)
* NBBO snapshot at trade time:

  * `best_bid`, `best_ask`, `best_bidsiz`, `best_asksiz`
  * `best_bidex`, `best_askex`
  * `mid = (best_bid + best_ask)/2`
  * `spread = best_ask - best_bid`
  * NBBO flags at trade time (`nbbo_qu_cond`, `secstat_ind`, `luld_*`)

**Filtering (recommended for derived, not raw):**

* drop or flag NBBO rows where `best_ask < best_bid` or prices <= 0
* optionally ignore NBBO rows with cancel/correction codes (`qu_cancel`) depending on your use

---

## Stage C — 1-minute bars (fast research)

Create:

```
parquet_derived/bars_1m/trade_date=YYYY-MM-DD/part-*.parquet
```

Per `(symbol, minute)`:

* Trade OHLCV
* `vwap`, `num_trades`
* NBBO features sampled at minute close (carry forward):

  * `mid_close`, `spread_close`, `best_bid_close`, `best_ask_close`
  * optional: `avg_spread` (time-weighted), quote update counts if you keep NBBO events

This gives you two research modes:

* **microstructure**: `enriched_trades`
* **coarse**: `bars_1m`

---

## Stage D — QC + manifests (must-have)

Per day, store `manifest.json` and `qc.json`:

**Manifest includes:**

* `trade_date`
* symbol universe requested
* extraction SQL hash / schema version
* per-symbol row counts (trades, nbbo, quotes if present)
* min/max `ts_event` per symbol
* checksums or file list

**QC checks to compute:**

* trades or nbbo with empty time ranges
* percent of NBBO states with invalid market (`best_ask < best_bid`)
* percent of trades with suspect conditions (e.g., corrections `tr_corr`, sale conditions `tr_scond` distribution)
* bar coverage stats


---

# 3) Practical notes for WRDS TAQ extraction

* Use `tr_seqnum` and `qu_seqnum` as stable ordering keys (in addition to `ts_event`)
* Preserve `tr_id` and condition flags — they’re valuable later for filtering
* Make sure trades and NBBO use the **same timestamp basis** (`date + time_m + time_m_nano`) so ASOF joins behave consistently
