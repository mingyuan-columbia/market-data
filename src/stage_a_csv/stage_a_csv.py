"""Stage A CSV: Extract raw data from local CSV files."""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from typing import Literal

import polars as pl

from ..stage_a.ingestion_checker import (
    check_ingestion_status,
    delete_partitions_for_symbols,
    get_missing_data,
)
from ..stage_a.schemas import build_canonical_symbol, build_ts_event
from .config import StageACsvConfig
from .csv_reader import check_csv_exists
from .csv_writer import write_chunked_from_csv

logger = logging.getLogger(__name__)

DataType = Literal["trades", "quotes", "nbbo"]


def extract_stage_a_csv(
    config: StageACsvConfig,
    trade_date: date,
    symbols: list[str] | None = None,
    overwrite: bool = False,
    data_types: list[str] | None = None,
    resume: bool = False,
) -> dict[str, int]:
    """
    Execute Stage A CSV extraction for the given date.
    
    Process:
    1. Check if data already ingested (skip if fully ingested unless overwrite=True or resume=True)
    2. Read from CSV files and write to Parquet
    
    Args:
        config: Stage A CSV configuration
        trade_date: Trade date
        symbols: Optional list of symbols to extract (if None, extracts all symbols from CSV)
        overwrite: If True, overwrite existing data
        data_types: List of data types to extract (default: ["trades", "nbbo"])
        resume: If True, skip symbols that are already ingested
        
    Returns:
        Dictionary with row counts: {"trades": 1000, "quotes": 2000, "nbbo": 1500}
    """
    # Set default data types if not provided
    if data_types is None:
        data_types = ["trades", "nbbo"]
    
    # Validate data types
    valid_types = {"trades", "quotes", "nbbo"}
    invalid_types = set(data_types) - valid_types
    if invalid_types:
        raise ValueError(f"Invalid data types: {invalid_types}. Valid types: {valid_types}")
    
    logger.info("=" * 80)
    logger.info(f"Stage A CSV: Extract raw data for {trade_date}")
    logger.info(f"Data types: {', '.join(data_types)}")
    logger.info(f"CSV root: {config.csv_root}")
    logger.info("=" * 80)
    
    extract_run_id = str(uuid.uuid4())
    logger.info(f"Extract run ID: {extract_run_id}")
    
    # Step 0: Delete existing partitions if overwrite is enabled
    if overwrite and symbols:
        logger.info("Overwrite mode: deleting existing partitions...")
        deleted = delete_partitions_for_symbols(
            config.parquet_raw_root,
            trade_date,
            symbols,
            data_types=data_types,
            partition_by_symbol=config.partition_by_symbol,
        )
        logger.info(f"Deleted {deleted} existing partition(s)")
    
    # Step 1: Check ingestion status
    if overwrite:
        logger.info("Overwrite mode: extracting all data")
        symbols_to_extract = {dt: symbols for dt in data_types}
    elif resume and symbols:
        # Resume mode: skip symbols that are already ingested
        logger.info("Resume mode: checking which symbols are already ingested...")
        missing = get_missing_data(config.parquet_raw_root, trade_date, symbols, config.partition_by_symbol)
        
        logger.info("Ingestion status:")
        for dt in data_types:
            missing_dt = missing.get(dt, [])
            logger.info(f"  {dt}: {len(symbols) - len(missing_dt)}/{len(symbols)} symbols ingested")
            if missing_dt:
                logger.info(f"    Missing: {missing_dt[:10]}{'...' if len(missing_dt) > 10 else ''}")
        
        symbols_to_extract = {dt: missing.get(dt, symbols) for dt in data_types}
    else:
        # Extract all symbols from CSV
        symbols_to_extract = {dt: symbols for dt in data_types}
    
    results = {}
    
    # Step 2: Process each data type
    for data_type in data_types:
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Processing {data_type.upper()}")
        logger.info(f"{'=' * 80}")
        
        # Map data type to CSV prefix
        prefix_map = {
            "trades": config.csv_prefix_trades,
            "quotes": config.csv_prefix_quotes,
            "nbbo": config.csv_prefix_nbbo,
        }
        csv_prefix = prefix_map[data_type]
        
        # Check if CSV file exists
        csv_path = check_csv_exists(config.csv_root, trade_date, data_type, csv_prefix)
        
        if not csv_path:
            logger.warning(f"No CSV file found for {data_type} on {trade_date}")
            logger.warning(f"Expected: {config.csv_root}/{csv_prefix}_{trade_date.strftime('%Y%m%d')}.csv")
            results[data_type] = 0
            continue
        
        logger.info(f"Found CSV file: {csv_path}")
        
        # Determine which symbols to extract
        missing_symbols = symbols_to_extract.get(data_type, symbols)
        
        if resume and missing_symbols and len(missing_symbols) == 0:
            logger.info(f"No missing symbols for {data_type}, skipping CSV processing")
            results[data_type] = 0
            continue
        
        if missing_symbols:
            logger.info(f"Filtering CSV to only extract {len(missing_symbols)} symbols: {missing_symbols[:10]}{'...' if len(missing_symbols) > 10 else ''}")
        
        # Define enrichment function
        def enrich_chunk(chunk_df):
            if data_type == "trades":
                return chunk_df.with_columns([
                    pl.lit(trade_date).alias("trade_date"),
                    build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
                    build_ts_event(
                        pl.col("date"),
                        pl.col("time_m"),
                        pl.col("time_m_nano"),
                        config.timezone,
                    ).alias("ts_event"),
                    pl.lit(extract_run_id).alias("extract_run_id"),
                    pl.lit(datetime.now()).alias("ingest_ts"),
                ])
            elif data_type == "quotes":
                return chunk_df.with_columns([
                    pl.lit(trade_date).alias("trade_date"),
                    build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
                    build_ts_event(
                        pl.col("date"),
                        pl.col("time_m"),
                        pl.col("time_m_nano"),
                        config.timezone,
                    ).alias("ts_event"),
                    pl.lit(extract_run_id).alias("extract_run_id"),
                    pl.lit(datetime.now()).alias("ingest_ts"),
                ])
            else:  # nbbo
                return chunk_df.with_columns([
                    pl.lit(trade_date).alias("trade_date"),
                    build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
                    build_ts_event(
                        pl.col("date"),
                        pl.col("time_m"),
                        pl.col("time_m_nano"),
                        config.timezone,
                    ).alias("ts_event"),
                    pl.lit(extract_run_id).alias("extract_run_id"),
                    pl.lit(datetime.now()).alias("ingest_ts"),
                ])
        
        # Read from CSV and write to Parquet (streaming mode)
        rows_written = write_chunked_from_csv(
            csv_path,
            config.parquet_raw_root,
            data_type,
            trade_date,
            compression=config.compression,
            partition_by_symbol=config.partition_by_symbol,
            chunk_size=config.chunk_size,
            enrich_fn=enrich_chunk,
            symbols_to_extract=missing_symbols,
        )
        
        results[data_type] = rows_written
        logger.info(f"{data_type.upper()} Summary: {rows_written:,} rows written")
    
    logger.info("\n" + "=" * 80)
    logger.info("Extraction Complete")
    logger.info("=" * 80)
    for data_type, count in results.items():
        logger.info(f"  {data_type}: {count:,} rows")
    
    return results

