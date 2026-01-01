# Stage A CSV: Local CSV File Ingestion

This module extracts raw data from local CSV files and saves it in the same format as TAQ data, enabling unified analysis across data sources.

## Features

- **CSV File Reading**: Read trades, quotes, and NBBO data from local CSV files
- **Unified Format**: Data is transformed to match TAQ canonical schema for compatibility
- **Same Storage Structure**: Uses the same Parquet partitioning scheme as Stage A TAQ
- **Date Range Support**: Extract data for single dates or date ranges
- **Resume Capability**: Skip already-ingested symbols when resuming extractions
- **Memory Efficient**: Streams large CSV files in chunks

## Setup

### 1. CSV File Format

CSV files should follow the TAQ format with columns like:
- `date`, `time_m`, `time_m_nano`
- `sym_root`, `sym_suffix`
- For trades: `price`, `size`, `ex`, etc.
- For quotes/NBBO: `bid`, `ask`, `best_bid`, `best_ask`, etc.

### 2. CSV File Naming

Files should be named according to the pattern:
- Trades: `{prefix}_YYYYMMDD.csv` (e.g., `taq_trade_20240501.csv`)
- Quotes: `{prefix}_YYYYMMDD.csv` (e.g., `taq_quote_20240501.csv`)
- NBBO: `{prefix}_YYYYMMDD.csv` (e.g., `taq_nbbo_20240501.csv`)

The prefix can be configured in `config.yaml` (default: `taq_trade`, `taq_quote`, `taq_nbbo`).

### 3. Configuration

Update `config.yaml` with CSV settings. The `csv_root` can point to any directory where your CSV files are located:

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

**Note:** CSV files don't need to be copied or moved. Just point `csv_root` to wherever your CSV files currently exist.

## Usage

### Extract data for a single date

```bash
python -m src.stage_a_csv.extract \
  --date 2024-05-01 \
  --config config.yaml
```

### Extract data for specific symbols

```bash
python -m src.stage_a_csv.extract \
  --date 2024-05-01 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml
```

### Extract data for a date range

```bash
python -m src.stage_a_csv.extract \
  --start-date 2024-05-01 \
  --end-date 2024-05-05 \
  --config config.yaml
```

### Extract specific data types

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

### Resume interrupted extraction

```bash
python -m src.stage_a_csv.extract \
  --date 2024-05-01 \
  --symbols AAPL,MSFT,GOOGL \
  --config config.yaml \
  --resume
```

## Data Format

The extracted data follows the same schema as TAQ data:

### Trades Schema
- Original fields: `date`, `time_m`, `time_m_nano`, `sym_root`, `sym_suffix`, `ex`, `price`, `size`, etc.
- Derived fields: `trade_date`, `symbol`, `ts_event`, `extract_run_id`, `ingest_ts`

### NBBO Schema
- Original fields: `date`, `time_m`, `time_m_nano`, `sym_root`, `sym_suffix`, `best_bid`, `best_ask`, etc.
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

## Integration with Analysis

Since CSV data uses the same schema and storage format as TAQ data, you can use the same analysis notebooks and pipelines. The `symbol` and `trade_date` fields allow seamless joining and analysis across data sources.

## Notes

- If `--symbols` is not provided, all symbols found in the CSV will be extracted
- Large CSV files are processed in chunks for memory efficiency
- The module automatically enriches CSV data with canonical fields (`symbol`, `ts_event`, etc.)

