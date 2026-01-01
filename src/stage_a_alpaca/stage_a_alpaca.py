"""Stage A Alpaca: Extract raw data from Alpaca API."""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import polars as pl

from .alpaca_extractor import AlpacaExtractor
from .config import StageAAlpacaConfig
from ..stage_a.parquet_writer import write_partitioned_streaming

logger = logging.getLogger(__name__)

DataType = Literal["trades", "nbbo"]


def extract_stage_a_alpaca(
    config: StageAAlpacaConfig,
    trade_date: date,
    symbols: list[str],
    overwrite: bool = False,
    data_types: list[str] | None = None,
    resume: bool = False,
) -> dict[str, int]:
    """
    Execute Stage A Alpaca extraction for the given date and symbols.
    
    Args:
        config: Stage A Alpaca configuration
        trade_date: Trade date
        symbols: List of symbols to extract
        overwrite: If True, overwrite existing data
        data_types: List of data types to extract (default: ["trades", "nbbo"])
        resume: If True, skip symbols that are already ingested
        
    Returns:
        Dictionary with row counts: {"trades": 1000, "nbbo": 1500}
    """
    # Set default data types if not provided
    if data_types is None:
        data_types = ["trades", "nbbo"]
    
    # Validate data types
    valid_types = {"trades", "nbbo"}
    invalid_types = set(data_types) - valid_types
    if invalid_types:
        raise ValueError(f"Invalid data types: {invalid_types}. Valid types: {valid_types}")
    
    logger.info("=" * 80)
    logger.info(f"Stage A Alpaca: Extract raw data for {trade_date} ({len(symbols)} symbols)")
    logger.info(f"Data types: {', '.join(data_types)}")
    logger.info("=" * 80)
    
    extract_run_id = str(uuid.uuid4())
    logger.info(f"Extract run ID: {extract_run_id}")
    
    # Initialize Alpaca extractor
    if not config.alpaca_api_key or not config.alpaca_secret_key:
        raise ValueError("Alpaca API key and secret key must be configured")
    
    extractor = AlpacaExtractor(
        api_key=config.alpaca_api_key,
        secret_key=config.alpaca_secret_key,
        base_url=config.alpaca_base_url,
        feed=config.feed,
    )
    
    results = {}
    from zoneinfo import ZoneInfo
    ingest_ts = datetime.now(tz=ZoneInfo("UTC"))
    
    # Process each data type
    for data_type in data_types:
        logger.info(f"\n{'=' * 80}")
        logger.info(f"Processing {data_type.upper()}")
        logger.info(f"{'=' * 80}")
        
        total_rows = 0
        
        # Process symbols in chunks
        for i in range(0, len(symbols), config.chunk_size):
            chunk_symbols = symbols[i:i + config.chunk_size]
            logger.info(f"\nProcessing chunk {i // config.chunk_size + 1} ({len(chunk_symbols)} symbols)")
            
            for symbol in chunk_symbols:
                try:
                    # Check if already ingested (if resume mode)
                    if resume and not overwrite:
                        partition_path = (
                            config.parquet_raw_root / data_type / 
                            f"trade_date={trade_date.isoformat()}" / 
                            f"symbol={symbol}"
                        )
                        if partition_path.exists() and any(partition_path.glob("*.parquet")):
                            logger.info(f"  {symbol}: Already ingested, skipping")
                            continue
                    
                    logger.info(f"  Extracting {symbol}...")
                    
                    # Extract data
                    chunks = []
                    if data_type == "trades":
                        for df_chunk in extractor.extract_trades(symbol, trade_date, config.timezone):
                            # Add metadata columns
                            df_chunk = df_chunk.with_columns([
                                pl.lit(extract_run_id).alias("extract_run_id"),
                                pl.lit(ingest_ts).alias("ingest_ts"),
                            ])
                            chunks.append(df_chunk)
                    elif data_type == "nbbo":
                        for df_chunk in extractor.extract_quotes(symbol, trade_date, config.timezone):
                            # Add metadata columns
                            df_chunk = df_chunk.with_columns([
                                pl.lit(extract_run_id).alias("extract_run_id"),
                                pl.lit(ingest_ts).alias("ingest_ts"),
                            ])
                            chunks.append(df_chunk)
                    
                    if chunks:
                        # Write to Parquet
                        rows_written = write_partitioned_streaming(
                            data_chunks=chunks,
                            parquet_root=config.parquet_raw_root,
                            dataset=data_type,
                            trade_date=trade_date,
                            compression=config.compression,
                            partition_by_symbol=config.partition_by_symbol,
                        )
                        total_rows += rows_written
                        logger.info(f"    ✓ Wrote {rows_written:,} rows")
                    else:
                        logger.warning(f"    ⚠ No data found for {symbol}")
                        
                except Exception as e:
                    logger.error(f"    ✗ Error processing {symbol}: {e}", exc_info=True)
                    continue
        
        results[data_type] = total_rows
        logger.info(f"\n{data_type.upper()} Summary: {total_rows:,} total rows")
    
    logger.info("\n" + "=" * 80)
    logger.info("Extraction Complete")
    logger.info("=" * 80)
    for data_type, count in results.items():
        logger.info(f"  {data_type}: {count:,} rows")
    
    return results

