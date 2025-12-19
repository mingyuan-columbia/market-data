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
from .ingestion_checker import get_missing_data, is_fully_ingested
from .parquet_writer import write_chunked_from_csv, write_partitioned_streaming
from .schemas import build_canonical_symbol, build_ts_event
from .wrds_extractor import WRDSExtractor

logger = logging.getLogger(__name__)

DataType = Literal["trades", "quotes", "nbbo"]


def extract_stage_a(
    config: StageAConfig,
    trade_date: date,
    symbols: list[str],
    overwrite: bool = False,
) -> dict[str, int]:
    """
    Execute Stage A extraction for the given date and symbols.
    
    Process:
    1. Check if data already ingested (skip if fully ingested unless overwrite=True)
    2. Check for CSV files and use them if available
    3. Extract from WRDS for missing data
    
    Args:
        config: Stage A configuration
        trade_date: Trade date
        symbols: List of symbols to extract
        overwrite: If True, overwrite existing data
        
    Returns:
        Dictionary with row counts: {"trades": 1000, "quotes": 2000, "nbbo": 1500}
    """
    logger.info("=" * 80)
    logger.info(f"Stage A: Extract raw data for {trade_date} ({len(symbols)} symbols)")
    logger.info("=" * 80)
    
    extract_run_id = str(uuid.uuid4())
    logger.info(f"Extract run ID: {extract_run_id}")
    
    # Step 1: Check ingestion status
    if not overwrite:
        if is_fully_ingested(config.parquet_raw_root, trade_date, symbols, config.partition_by_symbol):
            logger.info("✓ All data already ingested. Use --overwrite to re-extract.")
            return {"trades": 0, "quotes": 0, "nbbo": 0}
        
        missing = get_missing_data(config.parquet_raw_root, trade_date, symbols, config.partition_by_symbol)
        logger.info("Ingestion status:")
        logger.info(f"  Missing trades: {len(missing['trades'])} symbols")
        logger.info(f"  Missing quotes: {len(missing['quotes'])} symbols")
        logger.info(f"  Missing NBBO: {len(missing['nbbo'])} symbols")
        
        # Filter symbols to only those missing data
        # If missing list is empty, all symbols are already ingested for that data type
        symbols_to_extract = {
            "trades": missing["trades"],
            "quotes": missing["quotes"],
            "nbbo": missing["nbbo"],
        }
    else:
        logger.info("Overwrite mode: extracting all data")
        symbols_to_extract = {
            "trades": symbols,
            "quotes": symbols,
            "nbbo": symbols,
        }
    
    results = {"trades": 0, "quotes": 0, "nbbo": 0}
    
    # Step 2: Process each data type
    for data_type in ["trades", "quotes", "nbbo"]:
        if not symbols_to_extract[data_type]:
            logger.info(f"\nSkipping {data_type}: all symbols already ingested")
            continue
        
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Processing {data_type.upper()}")
        logger.info(f"{'=' * 80}")
        
        # Check for CSV file
        csv_type = "trade" if data_type == "trades" else ("quote" if data_type == "quotes" else "nbbo")
        csv_path = check_csv_exists(config.csv_root, trade_date, csv_type)
        
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
            results[data_type] = write_chunked_from_csv(
                csv_path,
                config.parquet_raw_root,
                data_type,
                trade_date,
                compression=config.compression,
                partition_by_symbol=config.partition_by_symbol,
                chunk_size=config.streaming_chunk_rows,
                enrich_fn=enrich_fn,
            )
        else:
            # Extract from WRDS
            logger.info("No CSV file found. Extracting from WRDS...")
            
            with WRDSExtractor(config) as extractor:
                chunks = []
                
                if data_type == "trades":
                    for chunk in extractor.extract_trades_streaming(
                        trade_date, symbols_to_extract[data_type], extract_run_id
                    ):
                        chunks.append(chunk)
                elif data_type == "quotes":
                    for chunk in extractor.extract_quotes_streaming(
                        trade_date, symbols_to_extract[data_type], extract_run_id
                    ):
                        chunks.append(chunk)
                else:  # nbbo
                    for chunk in extractor.extract_nbbo_streaming(
                        trade_date, symbols_to_extract[data_type], extract_run_id
                    ):
                        chunks.append(chunk)
                
                # Write chunks to Parquet
                if chunks:
                    results[data_type] = write_partitioned_streaming(
                        chunks,
                        config.parquet_raw_root,
                        data_type,
                        trade_date,
                        compression=config.compression,
                        partition_by_symbol=config.partition_by_symbol,
                    )
                else:
                    logger.warning(f"No data extracted for {data_type}")
    
    logger.info("\n" + "=" * 80)
    logger.info("Stage A Complete!")
    logger.info("=" * 80)
    logger.info(f"Trades: {results['trades']:,} rows")
    logger.info(f"Quotes: {results['quotes']:,} rows")
    logger.info(f"NBBO: {results['nbbo']:,} rows")
    
    return results

