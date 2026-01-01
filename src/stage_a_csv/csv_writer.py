"""Write CSV data to Parquet format."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)


def write_chunked_from_csv(
    csv_path: Path,
    parquet_root: Path,
    dataset: str,
    trade_date: date,
    compression: str = "snappy",
    partition_by_symbol: bool = True,
    chunk_size: int = 1_000_000,
    enrich_fn=None,
    symbols_to_extract: list[str] | None = None,
) -> int:
    """
    Read CSV in chunks and write to Parquet incrementally.
    
    Args:
        csv_path: Path to CSV file
        parquet_root: Root directory for Parquet files
        dataset: Dataset name
        trade_date: Trade date
        compression: Compression algorithm
        partition_by_symbol: Whether to partition by symbol
        chunk_size: Rows per chunk
        enrich_fn: Optional function to enrich each chunk (takes DataFrame, returns DataFrame)
        symbols_to_extract: Optional list of symbols to extract. If provided, only these symbols will be written.
                           If None, all symbols will be written.
        
    Returns:
        Total number of rows written
    """
    logger.info(f"Reading {csv_path} in chunks of {chunk_size:,} rows...")
    
    # Use lazy scan for memory efficiency
    lf = pl.scan_csv(
        csv_path,
        infer_schema_length=10000,
        ignore_errors=True,
    )
    
    # Get total count
    total_count = lf.select(pl.len()).collect().item()
    logger.info(f"Total rows in CSV: {total_count:,}")
    
    date_str = trade_date.isoformat()
    final_dir = parquet_root / dataset / f"trade_date={date_str}"
    final_dir.mkdir(parents=True, exist_ok=True)
    
    # Process in chunks
    offset = 0
    chunk_num = 0
    total_written = 0
    
    while offset < total_count:
        logger.info(f"Processing chunk {chunk_num + 1}: rows {offset:,} to {min(offset + chunk_size, total_count):,}")
        
        chunk = lf.slice(offset, chunk_size).collect()
        
        if chunk.is_empty():
            break
        
        # Apply enrichment if provided (this adds the 'symbol' column)
        if enrich_fn:
            chunk = enrich_fn(chunk)
        
        # Filter to only missing symbols if specified (after enrichment so we have 'symbol' column)
        if symbols_to_extract is not None and len(symbols_to_extract) > 0 and "symbol" in chunk.columns:
            chunk = chunk.filter(pl.col("symbol").is_in(symbols_to_extract))
            if chunk.is_empty():
                logger.debug(f"  Chunk {chunk_num + 1}: No rows for missing symbols, skipping")
                offset += chunk_size
                chunk_num += 1
                continue
        
        if partition_by_symbol and "symbol" in chunk.columns:
            # Write per symbol
            for symbol_key, symbol_df in chunk.partition_by("symbol", as_dict=True).items():
                # Clean symbol key - remove tuple notation if present
                if isinstance(symbol_key, tuple):
                    symbol = symbol_key[0] if len(symbol_key) > 0 else str(symbol_key)
                elif isinstance(symbol_key, str):
                    if symbol_key.startswith("('") and symbol_key.endswith("',)"):
                        # Handle string representation of tuple: "('AAPL',)"
                        symbol = symbol_key[2:-3]
                    elif symbol_key.startswith('("') and symbol_key.endswith('",)'):
                        # Handle string representation with double quotes: '("AAPL",)'
                        symbol = symbol_key[2:-3]
                    else:
                        symbol = symbol_key
                else:
                    symbol = str(symbol_key)
                
                symbol_dir = final_dir / f"symbol={symbol}"
                symbol_dir.mkdir(parents=True, exist_ok=True)
                chunk_file = symbol_dir / f"part_{chunk_num:04d}.parquet"
                symbol_df.write_parquet(chunk_file, compression=compression)
                total_written += len(symbol_df)
        else:
            # Write single chunk
            chunk_file = final_dir / f"part_{chunk_num:04d}.parquet"
            chunk.write_parquet(chunk_file, compression=compression)
            total_written += len(chunk)
        
        chunk_num += 1
        offset += chunk_size
        
        if offset % (chunk_size * 10) == 0:
            logger.info(f"Progress: {offset:,} / {total_count:,} rows processed, {total_written:,} written")
    
    # Create _SUCCESS marker
    success_marker = final_dir / "_SUCCESS"
    success_marker.touch()
    
    logger.info(f"✓ Wrote {total_written:,} rows from CSV to {final_dir}")
    return total_written

