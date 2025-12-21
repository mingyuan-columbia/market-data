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

### Setting Up WRDS Credentials

To avoid entering your WRDS password every time, you can securely store your credentials using a `.pgpass` file. The WRDS Python library will automatically use this file.

**Option 1: Use the setup script (recommended)**

Run the helper script to set up your credentials:

```bash
python setup_wrds_credentials.py
```

This script will:
- Prompt you for your WRDS username and password
- Create/update `~/.pgpass` with secure permissions (600)
- Store your credentials in the standard PostgreSQL password file format

**Option 2: Manual setup**

Create or edit `~/.pgpass` with the following format:

```
wrds-pgdata.wharton.upenn.edu:9737:wrds:your_username:your_password
```

Then set restrictive permissions:

```bash
chmod 600 ~/.pgpass
```

**Important Security Notes:**
- The `.pgpass` file should have permissions 600 (read/write for owner only)
- Never commit `.pgpass` to version control
- Make sure your `config.yaml` includes your WRDS username: `wrds_username: your_username`

## Usage

### Extract data for S&P 500 + ETFs (default)

If `--symbols` is not provided, the pipeline automatically fetches S&P 500 constituents plus 16 top ETFs:

```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --config config.yaml
```

The default ETFs are: SPY, QQQ, DIA, IWM, JETS, XLE, XLK, XLF, XLU, XLY, XLP, XLI, XLB, XLV, XLRE, XLC

By default, trades and nbbo data types are extracted. Use `--type` to specify which types to extract.

### Extract data for a date range

Extract data for multiple dates at once. The pipeline will:
1. Check if each date is a trading day (weekday)
2. Check if TAQ tables are available for that date
3. Extract data only for dates that meet both conditions

```bash
python -m src.stage_a.extract \
  --start-date 2024-06-10 \
  --end-date 2024-06-14 \
  --config config.yaml
```

This will process all trading days between June 10-14, 2024 (excluding weekends) and skip any dates where TAQ tables are not available.

You can combine date range extraction with other options:

```bash
# Extract date range for specific symbols
python -m src.stage_a.extract \
  --start-date 2024-06-10 \
  --end-date 2024-06-14 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml

# Extract date range with resume mode
python -m src.stage_a.extract \
  --start-date 2024-06-10 \
  --end-date 2024-06-14 \
  --config config.yaml \
  --resume
```

**Note:** When using date range mode, the pipeline checks table availability before attempting extraction, which helps avoid errors for dates when data is not yet available in WRDS.

### Extract specific data types

Extract only trades:
```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --config config.yaml \
  --type trades
```

Extract multiple types:
```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --config config.yaml \
  --type trades quotes
```

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

### Extract specific data type for symbols

Extract only NBBO data for specific symbols:
```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --symbols AAPL,MSFT \
  --config config.yaml \
  --type nbbo
```

### Resume interrupted extraction

If extraction was interrupted, use `--resume` to continue from where it left off. This will skip symbols that are already ingested:

```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --config config.yaml \
  --resume
```

You can combine `--resume` with `--type` to resume only specific data types:
```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --config config.yaml \
  --type quotes \
  --resume
```

### CSV file handling

The pipeline will use local CSV files if `csv_root` is configured in `config.yaml`. If `csv_root` is set to `null` or not provided, the pipeline will extract directly from WRDS without checking for CSV files.

To use CSV files, set `csv_root` in your config:
```yaml
stage_a:
  csv_root: /path/to/csv/files
```

To extract directly from WRDS (default when csv_root is null), simply leave `csv_root` as `null` in the config.

### Force overwrite existing data

The `--overwrite` flag will delete existing partitions and re-extract all data:

```bash
python -m src.stage_a.extract \
  --date 2024-06-10 \
  --symbols AAPL \
  --config config.yaml \
  --overwrite
```

**Warning**: This will permanently delete existing Parquet files for the specified date and symbols before re-extraction.

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

