# Market Data Pipeline

A clean, scalable pipeline for extracting and processing WRDS TAQ data.

## Overview

This pipeline implements Stage A: extraction of raw TAQ data from WRDS, with support for:
- Direct extraction from WRDS TAQ tables (ctm, cqm, complete_nbbo)
- Lossless Parquet storage with canonical schemas
- Memory-efficient streaming processing
- CSV fallback support
- Idempotent extraction (skip if already ingested)

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

1. Copy `config.example.yaml` to `config.yaml`
2. Update paths and settings as needed:

```yaml
stage_a:
  parquet_raw_root: /Volumes/Data/parquet_raw
  csv_root: /Volumes/Data/taq
  wrds_username: your_username
  chunk_size: 50
  streaming_chunk_rows: 1000000
  compression: snappy
  partition_by_symbol: true
  timezone: America/New_York
```

## Usage

### Extract data for S&P 500 + ETFs (default)

If `--symbols` is not provided, the pipeline automatically fetches S&P 500 constituents plus 16 top ETFs:

```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --config config.yaml
```

The default ETFs are: SPY, QQQ, DIA, IWM, JETS, XLE, XLK, XLF, XLU, XLY, XLP, XLI, XLB, XLV, XLRE, XLC

### Extract data for specific symbols

```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml
```

### Extract from symbol file

Create a file `symbols.txt` with one symbol per line:
```
AAPL
MSFT
GOOGL
```

Then:
```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --symbols symbols.txt \
  --config config.yaml
```

### Overwrite existing data

```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --symbols AAPL \
  --config config.yaml \
  --overwrite
```

## Data Structure

Raw Parquet files are stored with the following structure:

```
parquet_raw/
  trades/
    trade_date=YYYY-MM-DD/
      symbol=SYMBOL/
        part-*.parquet
  quotes/
    trade_date=YYYY-MM-DD/
      symbol=SYMBOL/
        part-*.parquet
  nbbo/
    trade_date=YYYY-MM-DD/
      symbol=SYMBOL/
        part-*.parquet
```

Each partition includes a `_SUCCESS` marker file when extraction completes.

## Features

- **Idempotent**: Checks for existing data and skips if already ingested
- **Memory efficient**: Streaming/chunked processing for large datasets
- **CSV fallback**: Uses CSV files if available before querying WRDS
- **NAS optimized**: Atomic writes for network storage efficiency
- **Lossless**: Preserves all original fields plus derived canonical fields

## Stage A Process

1. **Check ingestion status**: Verify if (date, symbol) data already exists
2. **Check CSV files**: Look for CSV files in configured location
3. **Extract from WRDS**: Query WRDS TAQ tables if CSV not available
4. **Write to Parquet**: Stream data directly to Parquet (no intermediate CSV)

## Next Steps

Stage B (enrichment) and Stage C (bars) will be implemented next.

