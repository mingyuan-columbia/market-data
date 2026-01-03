# Market Data Pipeline

A clean, scalable pipeline for extracting and processing market data from multiple sources.

## Overview

This pipeline implements Stage A: extraction of raw market data from multiple sources, with support for:
- **WRDS TAQ**: Direct extraction from WRDS TAQ tables (ctm, cqm, complete_nbbo)
- **Alpaca API**: Historical SIP data from Alpaca's free-tier API
- **Local CSV**: Import from local CSV files
- Lossless Parquet storage with canonical schemas
- Memory-efficient streaming processing
- Idempotent extraction (skip if already ingested)
- Unified data format across all sources

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

1. Copy `config.example.yaml` to `config.yaml`
2. Update paths and settings for each data source you want to use:

### WRDS TAQ Configuration

```yaml
stage_a:
  parquet_raw_root: /home/mingyuan/data/taq/parquet_raw
  wrds_username: your_username
  chunk_size: 50
  streaming_chunk_rows: 1000000
  compression: snappy
  partition_by_symbol: true
  timezone: America/New_York
```

### Alpaca Configuration

```yaml
stage_a_alpaca:
  parquet_raw_root: /home/mingyuan/data/alpaca/parquet_raw
  alpaca_api_key: null  # Set in config.secrets.yaml or environment variables
  alpaca_secret_key: null  # Set in config.secrets.yaml or environment variables
  alpaca_base_url: https://paper-api.alpaca.markets  # Paper trading (free tier)
  feed: sip
  chunk_size: 50
  compression: snappy
  partition_by_symbol: true
  timezone: America/New_York
```

### CSV Configuration

```yaml
stage_a_csv:
  parquet_raw_root: /home/mingyuan/data/csv/parquet_raw
  csv_root: /path/to/your/csv/files  # Point to wherever your CSV files are located
  chunk_size: 1000000
  compression: snappy
  partition_by_symbol: true
  timezone: America/New_York
  csv_prefix_trades: taq_trade
  csv_prefix_quotes: taq_quote
  csv_prefix_nbbo: taq_nbbo
```

### Setting Up Alpaca API Credentials

To avoid committing API keys to git, use one of these secure methods (in order of priority):

**Option 1: Environment Variables (Recommended for CI/CD)**

```bash
export ALPACA_API_KEY="your_api_key"
export ALPACA_SECRET_KEY="your_secret_key"
```

**Option 2: Secrets File (Recommended for local development)**

1. Copy the example secrets file:
```bash
cp config.secrets.example.yaml config.secrets.yaml
```

2. Edit `config.secrets.yaml` and add your credentials:
```yaml
stage_a_alpaca:
  alpaca_api_key: your_api_key_here
  alpaca_secret_key: your_secret_key_here
```

3. The `config.secrets.yaml` file is already in `.gitignore` and will not be committed to git.

**Option 3: Config File (Not Recommended)**

You can add credentials directly to `config.yaml`, but **DO NOT commit them to git**. This is not recommended for security reasons.

**Priority Order:**
1. Environment variables (highest priority)
2. `config.secrets.yaml` file
3. `config.yaml` file (lowest priority)

Get your API credentials from: https://app.alpaca.markets/paper/dashboard/overview

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

### WRDS TAQ Data Extraction

#### Extract data for S&P 500 + ETFs (default)

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

---

## Alpaca Data Extraction

Extract historical SIP data from Alpaca API. Requires a free Alpaca account.

### Setup Alpaca Credentials

**Option 1: Environment Variables (Recommended)**

```bash
export ALPACA_API_KEY="your_api_key"
export ALPACA_SECRET_KEY="your_secret_key"
```

**Option 2: Config File**

Add credentials to `config.yaml`:
```yaml
stage_a_alpaca:
  alpaca_api_key: your_api_key
  alpaca_secret_key: your_secret_key
```

Get your API credentials from: https://app.alpaca.markets/paper/dashboard/overview

### Extract Alpaca data for a single date

```bash
python -m src.stage_a_alpaca.extract \
  --date 2024-05-01 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml
```

### Extract Alpaca data for a date range

```bash
python -m src.stage_a_alpaca.extract \
  --start-date 2024-05-01 \
  --end-date 2024-05-05 \
  --symbols AAPL,MSFT \
  --config config.yaml
```

### Extract specific data types from Alpaca

```bash
# Extract only trades
python -m src.stage_a_alpaca.extract \
  --date 2024-05-01 \
  --symbols AAPL \
  --config config.yaml \
  --type trades

# Extract trades and NBBO
python -m src.stage_a_alpaca.extract \
  --date 2024-05-01 \
  --symbols AAPL \
  --config config.yaml \
  --type trades nbbo
```

### Resume Alpaca extraction

```bash
python -m src.stage_a_alpaca.extract \
  --date 2024-05-01 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml \
  --resume
```

**Note:** Alpaca free tier has rate limits. The extractor includes basic rate limit handling with automatic retries.

---

## CSV Data Extraction

Import data from local CSV files. CSV files don't need to be copied or moved - just point `csv_root` to wherever your files are located.

### CSV File Format

CSV files should follow the TAQ format with columns like:
- `date`, `time_m`, `time_m_nano`
- `sym_root`, `sym_suffix`
- For trades: `price`, `size`, `ex`, etc.
- For quotes/NBBO: `bid`, `ask`, `best_bid`, `best_ask`, etc.

### CSV File Naming

Files should be named: `{prefix}_YYYYMMDD.csv`
- Trades: `taq_trade_20240501.csv`
- Quotes: `taq_quote_20240501.csv`
- NBBO: `taq_nbbo_20240501.csv`

The prefix can be configured in `config.yaml`.

### Extract CSV data for a single date

```bash
python -m src.stage_a_csv.extract \
  --date 2024-05-01 \
  --config config.yaml
```

If `--symbols` is not provided, all symbols found in the CSV will be extracted.

### Extract CSV data for specific symbols

```bash
python -m src.stage_a_csv.extract \
  --date 2024-05-01 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml
```

### Extract CSV data for a date range

```bash
python -m src.stage_a_csv.extract \
  --start-date 2024-05-01 \
  --end-date 2024-05-05 \
  --config config.yaml
```

### Extract specific data types from CSV

```bash
# Extract only trades
python -m src.stage_a_csv.extract \
  --date 2024-05-01 \
  --config config.yaml \
  --type trades

# Extract trades and NBBO
python -m src.stage_a_csv.extract \
  --date 2024-05-01 \
  --config config.yaml \
  --type trades nbbo
```

### Resume CSV extraction

```bash
python -m src.stage_a_csv.extract \
  --date 2024-05-01 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml \
  --resume
```

---

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

Raw Parquet files are stored with the following structure, with separate directories for each data source:

```
/home/mingyuan/data/
├── taq/
│   └── parquet_raw/          # WRDS TAQ data
│       ├── trades/
│       ├── quotes/
│       └── nbbo/
├── alpaca/
│   └── parquet_raw/          # Alpaca API data
│       ├── trades/
│       └── nbbo/
└── csv/
    └── parquet_raw/          # CSV imported data
        ├── trades/
        ├── quotes/
        └── nbbo/
```

Each data type follows the same partitioning structure:

```
{data_type}/
  trade_date=YYYY-MM-DD/
    symbol=SYMBOL/
      part-*.parquet
```

Each partition includes a `_SUCCESS` marker file when extraction completes.

**Important:** All three data sources use the same canonical schema, enabling unified analysis across sources. The `symbol` and `trade_date` fields allow seamless joining and analysis.

## Features

- **Multiple Data Sources**: Extract from WRDS TAQ, Alpaca API, or local CSV files
- **Unified Format**: All sources use the same canonical schema for seamless analysis
- **Idempotent**: Checks for existing data and skips if already ingested
- **Memory efficient**: Streaming/chunked processing for large datasets
- **NAS optimized**: Atomic writes for network storage efficiency
- **Lossless**: Preserves all original fields plus derived canonical fields
- **Resume capability**: Skip already-ingested symbols when resuming extractions
- **Date range support**: Extract data for multiple dates at once

## Stage A Process

### WRDS TAQ Extraction

1. **Check ingestion status**: Verify if (date, symbol) data already exists
2. **Extract from WRDS**: Query WRDS TAQ tables (ctm, cqm, complete_nbbo)
3. **Write to Parquet**: Stream data directly to Parquet

### Alpaca Extraction

1. **Check ingestion status**: Verify if (date, symbol) data already exists
2. **Extract from Alpaca API**: Fetch historical SIP data via REST API
3. **Transform to canonical format**: Map Alpaca fields to TAQ schema
4. **Write to Parquet**: Stream data to Parquet

### CSV Extraction

1. **Check ingestion status**: Verify if (date, symbol) data already exists
2. **Read CSV files**: Load CSV files from configured directory
3. **Enrich with canonical fields**: Add `symbol`, `ts_event`, `extract_run_id`, etc.
4. **Write to Parquet**: Stream data to Parquet in chunks

## Data Source Comparison

| Feature | WRDS TAQ | Alpaca | CSV |
|---------|----------|--------|-----|
| **Source** | WRDS database | Alpaca API | Local files |
| **Cost** | Requires WRDS subscription | Free tier available | Free |
| **Historical Data** | Extensive (decades) | Limited by plan | Depends on files |
| **Data Types** | Trades, Quotes, NBBO | Trades, NBBO | Trades, Quotes, NBBO |
| **Rate Limits** | None (database) | Free tier limits | None |
| **Setup Complexity** | Medium (WRDS access) | Low (API key) | Low (file path) |
| **Best For** | Comprehensive historical analysis | Recent data, testing | Legacy data import |

## Unified Analysis

Since all three data sources use the same canonical schema and storage format, you can:
- Analyze data from multiple sources together
- Use the same analysis notebooks and pipelines
- Join data across sources using `symbol` and `trade_date`
- Compare data quality and consistency across sources

The `tr_source` field in trades data indicates the source: `"WRDS"`, `"ALPACA"`, or `"CSV"`.

## Streamlit Visualization App

An interactive web application for visualizing and analyzing market data.

### Running Locally

```bash
./run_streamlit.sh
```

The app will be available at `http://localhost:8500`

### Accessing from External Devices (Cloudflare Tunnel)

To access the Streamlit app from your iPhone, iPad, or any device outside your local network:

1. **Install Cloudflare Tunnel:**
   ```bash
   ./setup_cloudflare_tunnel.sh
   ```

2. **Run with tunnel:**
   ```bash
   ./run_streamlit_with_tunnel.sh
   ```

3. **Access via the provided URL:**
   - The script will display a Cloudflare Tunnel URL (e.g., `https://random-name.trycloudflare.com`)
   - Open this URL in any browser on any device
   - The URL is valid until you stop the tunnel

**Security Note:** The app is password-protected. Make sure you have set a password in `.streamlit/secrets.toml`.

For detailed setup instructions and security considerations, see [CLOUDFLARE_TUNNEL.md](CLOUDFLARE_TUNNEL.md).

## Next Steps

Stage B (enrichment) and Stage C (bars) will be implemented next.

