# Stage A Alpaca: Historical SIP Data Extraction

This module extracts historical SIP (Securities Information Processor) data from Alpaca API and saves it in the same format as TAQ data, enabling unified analysis across data sources.

## Features

- **Historical SIP Data**: Extract trades and NBBO quotes from Alpaca's historical API
- **Unified Format**: Data is transformed to match TAQ canonical schema for compatibility
- **Same Storage Structure**: Uses the same Parquet partitioning scheme as Stage A TAQ
- **Date Range Support**: Extract data for single dates or date ranges
- **Resume Capability**: Skip already-ingested symbols when resuming extractions

## Setup

### 1. Alpaca API Credentials

You need an Alpaca account (free tier works). Get your API credentials from:
https://app.alpaca.markets/paper/dashboard/overview

**Option 1: Environment Variables (Recommended)**
```bash
export ALPACA_API_KEY="your_api_key"
export ALPACA_SECRET_KEY="your_secret_key"
```

**Option 2: Config File**
Add to `config.yaml`:
```yaml
stage_a_alpaca:
  alpaca_api_key: your_api_key
  alpaca_secret_key: your_secret_key
```

### 2. Configuration

Update `config.yaml` with Alpaca settings:

```yaml
stage_a_alpaca:
  parquet_raw_root: /home/mingyuan/data/taq/parquet_raw
  alpaca_base_url: https://paper-api.alpaca.markets  # Paper trading (free tier)
  feed: sip  # Use SIP feed for consolidated data
  chunk_size: 50
  compression: snappy
  partition_by_symbol: true
  timezone: America/New_York
```

## Usage

### Extract data for a single date

```bash
python -m src.stage_a_alpaca.extract \
  --date 2024-05-01 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml
```

### Extract data for a date range

```bash
python -m src.stage_a_alpaca.extract \
  --start-date 2024-05-01 \
  --end-date 2024-05-05 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml
```

### Extract specific data types

```bash
# Extract only trades
python -m src.stage_a_alpaca.extract \
  --date 2024-05-01 \
  --symbols AAPL \
  --config config.yaml \
  --type trades

# Extract only NBBO
python -m src.stage_a_alpaca.extract \
  --date 2024-05-01 \
  --symbols AAPL \
  --config config.yaml \
  --type nbbo
```

### Resume interrupted extraction

```bash
python -m src.stage_a_alpaca.extract \
  --date 2024-05-01 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml \
  --resume
```

### Extract from symbol file

Create `symbols.txt`:
```
AAPL
MSFT
GOOGL
TSLA
```

Then:
```bash
python -m src.stage_a_alpaca.extract \
  --date 2024-05-01 \
  --symbols symbols.txt \
  --config config.yaml
```

## Data Format

The extracted data follows the same schema as TAQ data:

### Trades Schema
- Original fields: `date`, `time_m`, `time_m_nano`, `sym_root`, `ex`, `price`, `size`, `tr_source`
- Derived fields: `trade_date`, `symbol`, `ts_event`, `extract_run_id`, `ingest_ts`

### NBBO Schema
- Original fields: `date`, `time_m`, `time_m_nano`, `sym_root`, `best_bid`, `best_bidsiz`, `best_ask`, `best_asksiz`, `best_bidex`, `best_askex`
- Derived fields: `trade_date`, `symbol`, `ts_event`, `extract_run_id`, `ingest_ts`

Data is stored in the same directory structure:
```
parquet_raw/
  trades/
    trade_date=2024-05-01/
      symbol=AAPL/
        part_0000.parquet
  nbbo/
    trade_date=2024-05-01/
      symbol=AAPL/
        part_0000.parquet
```

## Limitations

- **Free Tier**: Alpaca free tier has rate limits. The extractor includes basic rate limit handling.
- **Historical Data**: Alpaca provides historical data going back a limited time (varies by plan).
- **Market Hours**: Data is extracted for standard market hours (9:30 AM - 4:00 PM ET).
- **Data Completeness**: Some TAQ fields are not available from Alpaca and are set to `None`.

## Differences from TAQ Data

- **Source Field**: Alpaca data has `tr_source = "ALPACA"` to distinguish it from WRDS TAQ data
- **Missing Fields**: Some TAQ-specific fields (e.g., `tr_corr`, `tr_seqnum`, `part_time`) are not available
- **Exchange Codes**: Exchange codes may differ from TAQ format

## Integration with Analysis

Since Alpaca data uses the same schema and storage format as TAQ data, you can use the same analysis notebooks and pipelines. The `symbol` and `trade_date` fields allow seamless joining and analysis across data sources.

