"""Stage A: Extract raw data from WRDS TAQ."""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import polars as pl

from .config import StageAConfig
from .csv_reader import check_csv_exists
from .ingestion_checker import (
    check_ingestion_status,
    check_partition_exists,
    delete_partitions_for_symbols,
    get_missing_data,
    is_fully_ingested,
)
from .parquet_writer import (
    write_chunked_from_csv,
    write_chunks_incrementally,
    write_partitioned_streaming,
)
from .schemas import build_canonical_symbol, build_ts_event
from .wrds_extractor import WRDSExtractor

logger = logging.getLogger(__name__)

DataType = Literal["trades", "quotes", "nbbo"]


def extract_stage_a(
    config: StageAConfig,
    trade_date: date,
    symbols: list[str],
    overwrite: bool = False,
    data_types: list[str] | None = None,
    resume: bool = False,
    skip_csv: bool = False,
) -> dict[str, int]:
    """
    Execute Stage A extraction for the given date and symbols.
    
    Process:
    1. Check if data already ingested (skip if fully ingested unless overwrite=True or resume=True)
    2. Check for CSV files and use them if available (unless skip_csv=True)
    3. Extract from WRDS for missing data
    
    Args:
        config: Stage A configuration
        trade_date: Trade date
        symbols: List of symbols to extract
        overwrite: If True, overwrite existing data
        data_types: List of data types to extract (default: ["trades", "quotes", "nbbo"])
        resume: If True, skip symbols that are already ingested (useful for resuming interrupted extractions)
        skip_csv: If True, skip local CSV files and extract directly from WRDS
        
    Returns:
        Dictionary with row counts: {"trades": 1000, "quotes": 2000, "nbbo": 1500}
    """
    # Set default data types if not provided
    if data_types is None:
        data_types = ["trades", "quotes", "nbbo"]
    
    # Validate data types
    valid_types = {"trades", "quotes", "nbbo"}
    invalid_types = set(data_types) - valid_types
    if invalid_types:
        raise ValueError(f"Invalid data types: {invalid_types}. Valid types: {valid_types}")
    
    logger.info("=" * 80)
    logger.info(f"Stage A: Extract raw data for {trade_date} ({len(symbols)} symbols)")
    logger.info(f"Data types: {', '.join(data_types)}")
    logger.info("=" * 80)
    
    extract_run_id = str(uuid.uuid4())
    logger.info(f"Extract run ID: {extract_run_id}")
    
    # Step 0: Delete existing partitions if overwrite is enabled
    if overwrite:
        logger.info("Overwrite mode: deleting existing partitions...")
        deleted = delete_partitions_for_symbols(
            config.parquet_raw_root,
            trade_date,
            symbols,
            data_types=data_types,  # Only delete specified data types
            partition_by_symbol=config.partition_by_symbol,
        )
        logger.info(f"Deleted {deleted} existing partition(s)")
    
    # Step 1: Check ingestion status
    if overwrite:
        logger.info("Overwrite mode: extracting all data")
        symbols_to_extract = {dt: symbols for dt in data_types}
    elif resume:
        # Resume mode: skip symbols that are already ingested
        logger.info("Resume mode: checking which symbols are already ingested...")
        logger.info(f"Checking parquet_root: {config.parquet_raw_root}")
        logger.info(f"partition_by_symbol: {config.partition_by_symbol}")
        
        # Verify parquet root exists
        if not config.parquet_raw_root.exists():
            logger.warning(f"Parquet root directory does not exist: {config.parquet_raw_root}")
            logger.warning("This might indicate the NAS is not mounted or path is incorrect.")
        else:
            logger.debug(f"Parquet root exists: {config.parquet_raw_root}")
            # Check if dataset directories exist
            for dt in data_types:
                dataset_dir = config.parquet_raw_root / dt
                if dataset_dir.exists():
                    logger.debug(f"  Dataset directory exists: {dataset_dir}")
                else:
                    logger.debug(f"  Dataset directory does not exist: {dataset_dir}")
        
        missing = get_missing_data(config.parquet_raw_root, trade_date, symbols, config.partition_by_symbol)
        
        logger.info("Ingestion status:")
        for dt in data_types:
            ingested_count = len(symbols) - len(missing[dt])
            logger.info(f"  {dt}: {ingested_count}/{len(symbols)} symbols already ingested, {len(missing[dt])} remaining")
            
            # Debug: show a few examples of what we're checking
            if ingested_count == 0 and len(symbols) > 0:
                # If nothing found, check a sample symbol to see what's happening
                sample_symbol = symbols[0]
                sample_exists = check_partition_exists(
                    config.parquet_raw_root, dt, trade_date, sample_symbol, config.partition_by_symbol
                )
                logger.debug(f"  Sample check for {dt} symbol '{sample_symbol}': exists={sample_exists}")
                if not sample_exists:
                    # Show what directory we're looking for
                    date_str = trade_date.isoformat()
                    if config.partition_by_symbol:
                        expected_dir = config.parquet_raw_root / dt / f"trade_date={date_str}" / f"symbol={sample_symbol}"
                    else:
                        expected_dir = config.parquet_raw_root / dt / f"trade_date={date_str}"
                    logger.debug(f"  Expected directory: {expected_dir}")
                    logger.debug(f"  Directory exists: {expected_dir.exists()}")
        
        # Filter symbols to only those missing data for specified types
        symbols_to_extract = {}
        for dt in data_types:
            symbols_to_extract[dt] = missing[dt]
        
        # Check if anything needs to be extracted
        all_ingested = True
        for dt in data_types:
            if len(symbols_to_extract[dt]) > 0:
                all_ingested = False
                break
        
        if all_ingested:
            logger.info(f"✓ All specified data types already ingested. Nothing to resume.")
            results = {"trades": 0, "quotes": 0, "nbbo": 0}
            return results
    else:
        # Normal mode: check ingestion status and skip if fully ingested
        missing = get_missing_data(config.parquet_raw_root, trade_date, symbols, config.partition_by_symbol)
        
        # Filter to only check specified data types
        all_ingested = True
        for dt in data_types:
            if len(missing[dt]) > 0:
                all_ingested = False
                break
        
        if all_ingested:
            logger.info(f"✓ All specified data types already ingested. Use --overwrite to re-extract or --resume to continue.")
            results = {"trades": 0, "quotes": 0, "nbbo": 0}
            return results
        
        logger.info("Ingestion status:")
        for dt in data_types:
            logger.info(f"  Missing {dt}: {len(missing[dt])} symbols")
        
        # Filter symbols to only those missing data for specified types
        symbols_to_extract = {}
        for dt in data_types:
            symbols_to_extract[dt] = missing[dt]
    
    results = {"trades": 0, "quotes": 0, "nbbo": 0}
    
    # Step 2: Process each specified data type
    for data_type in data_types:
        if not symbols_to_extract[data_type]:
            logger.info(f"\nSkipping {data_type}: all symbols already ingested")
            continue
        
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Processing {data_type.upper()}")
        logger.info(f"{'=' * 80}")
        
        # Check for CSV file (unless skip_csv is enabled)
        csv_path = None
        if not skip_csv:
            csv_type = "trade" if data_type == "trades" else ("quote" if data_type == "quotes" else "nbbo")
            csv_path = check_csv_exists(config.csv_root, trade_date, csv_type)
        else:
            logger.info("Skip CSV mode: ignoring local CSV files, extracting directly from WRDS")
        
        if csv_path:
            logger.info(f"Found CSV file: {csv_path}")
            logger.info("Using CSV file (streaming mode)...")
            
            # Create enrichment function with captured variables
            def make_enrich_fn(td: date, run_id: str, tz: str):
                def enrich_chunk(chunk: pl.DataFrame) -> pl.DataFrame:
                    return chunk.with_columns([
                        pl.lit(td).alias("trade_date"),
                        build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
                        build_ts_event(
                            pl.col("date"),
                            pl.col("time_m"),
                            pl.col("time_m_nano"),
                            tz,
                        ).alias("ts_event"),
                        pl.lit(run_id).alias("extract_run_id"),
                        pl.lit(datetime.utcnow()).alias("ingest_ts"),
                    ])
                return enrich_chunk
            
            enrich_fn = make_enrich_fn(trade_date, extract_run_id, config.timezone)
            
            # Read from CSV and write to Parquet (streaming mode)
            # Only extract missing symbols
            missing_symbols = symbols_to_extract[data_type]
            
            if not missing_symbols:
                logger.info(f"No missing symbols for {data_type}, skipping CSV processing")
                results[data_type] = 0
                continue
            
            logger.info(f"Filtering CSV to only extract {len(missing_symbols)} missing symbols: {missing_symbols[:10]}{'...' if len(missing_symbols) > 10 else ''}")
            
            results[data_type] = write_chunked_from_csv(
                csv_path,
                config.parquet_raw_root,
                data_type,
                trade_date,
                compression=config.compression,
                partition_by_symbol=config.partition_by_symbol,
                chunk_size=config.streaming_chunk_rows,
                enrich_fn=enrich_fn,
                symbols_to_extract=missing_symbols if missing_symbols else None,
            )
        else:
            # Extract from WRDS using streaming (memory-efficient)
            logger.info("No CSV file found. Extracting from WRDS (streaming mode)...")
            
            with WRDSExtractor(config) as extractor:
                # Create iterator based on data type
                if data_type == "trades":
                    chunk_iterator = extractor.extract_trades_streaming(
                        trade_date, symbols_to_extract[data_type], extract_run_id
                    )
                elif data_type == "quotes":
                    chunk_iterator = extractor.extract_quotes_streaming(
                        trade_date, symbols_to_extract[data_type], extract_run_id
                    )
                else:  # nbbo
                    chunk_iterator = extractor.extract_nbbo_streaming(
                        trade_date, symbols_to_extract[data_type], extract_run_id
                    )
                
                # Write chunks incrementally (avoids accumulating in memory)
                results[data_type] = write_chunks_incrementally(
                    chunk_iterator,
                    config.parquet_raw_root,
                    data_type,
                    trade_date,
                    compression=config.compression,
                    partition_by_symbol=config.partition_by_symbol,
                )
                
                if results[data_type] == 0:
                    logger.warning(f"No data extracted for {data_type}")
    
    logger.info("\n" + "=" * 80)
    logger.info("Stage A Complete!")
    logger.info("=" * 80)
    for dt in data_types:
        logger.info(f"{dt.capitalize()}: {results[dt]:,} rows")
    
    return results

