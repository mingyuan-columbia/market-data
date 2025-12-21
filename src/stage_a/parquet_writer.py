"""Efficient Parquet writing with NAS-aware optimizations."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator

import polars as pl

logger = logging.getLogger(__name__)


def write_partitioned_streaming(
    data_chunks: list[pl.DataFrame],
    parquet_root: Path,
    dataset: str,
    trade_date: date,
    compression: str = "snappy",
    partition_by_symbol: bool = True,
) -> int:
    """
    Write data chunks to Parquet with partitioning.
    
    Uses atomic writes (temp dir + rename) for NAS efficiency.
    
    Args:
        data_chunks: List of DataFrames to write
        parquet_root: Root directory for Parquet files
        dataset: Dataset name (trades, quotes, nbbo)
        trade_date: Trade date for partitioning
        compression: Compression algorithm
        partition_by_symbol: Whether to partition by symbol
        
    Returns:
        Total number of rows written
    """
    if not data_chunks:
        logger.warning(f"No data chunks to write for {dataset}")
        return 0
    
    # Combine chunks
    logger.info(f"Combining {len(data_chunks)} chunks for {dataset}...")
    df = pl.concat(data_chunks)
    total_rows = len(df)
    
    if total_rows == 0:
        logger.warning(f"Empty DataFrame for {dataset}")
        return 0
    
    date_str = trade_date.isoformat()
    
    # Use temp directory for atomic writes (NAS-friendly)
    with TemporaryDirectory(prefix=f"{dataset}_{date_str}_") as temp_dir:
        temp_path = Path(temp_dir)
        
        if partition_by_symbol and "symbol" in df.columns:
            # Partition by symbol
            # Note: partition_by returns keys that might have tuple notation, so we need to clean them
            for symbol_key, symbol_df in df.partition_by("symbol", as_dict=True).items():
                # Clean symbol key - remove tuple notation if present
                if isinstance(symbol_key, tuple):
                    symbol = symbol_key[0] if len(symbol_key) > 0 else str(symbol_key)
                elif symbol_key.startswith("('") and symbol_key.endswith("',)"):
                    # Handle string representation of tuple: "('AAPL',)"
                    symbol = symbol_key[2:-3]
                elif symbol_key.startswith('("') and symbol_key.endswith('",)'):
                    # Handle string representation with double quotes: '("AAPL",)'
                    symbol = symbol_key[2:-3]
                else:
                    symbol = symbol_key
                
                symbol_dir = temp_path / f"symbol={symbol}"
                symbol_dir.mkdir(parents=True, exist_ok=True)
                out_path = symbol_dir / "part.parquet"
                symbol_df.write_parquet(out_path, compression=compression)
                logger.debug(f"  Wrote {len(symbol_df):,} rows for symbol={symbol}")
        else:
            # Single partition
            out_path = temp_path / "part.parquet"
            df.write_parquet(out_path, compression=compression)
            logger.debug(f"  Wrote {total_rows:,} rows")
        
        # Atomic move to final location
        final_dir = parquet_root / dataset / f"trade_date={date_str}"
        if partition_by_symbol and "symbol" in df.columns:
            # Move symbol subdirectories
            for symbol_dir in temp_path.iterdir():
                if symbol_dir.is_dir():
                    final_symbol_dir = final_dir / symbol_dir.name
                    final_symbol_dir.mkdir(parents=True, exist_ok=True)
                    # Move parquet files
                    for parquet_file in symbol_dir.glob("*.parquet"):
                        final_file = final_symbol_dir / parquet_file.name
                        parquet_file.rename(final_file)
        else:
            # Move single partition
            final_dir.mkdir(parents=True, exist_ok=True)
            for parquet_file in temp_path.glob("*.parquet"):
                final_file = final_dir / parquet_file.name
                parquet_file.rename(final_file)
        
        # Create _SUCCESS marker
        success_marker = final_dir / "_SUCCESS"
        success_marker.touch()
        
        logger.info(f"✓ Wrote {total_rows:,} rows to {final_dir}")
    
    return total_rows


def write_chunks_incrementally(
    chunk_iterator: Iterator[pl.DataFrame],
    parquet_root: Path,
    dataset: str,
    trade_date: date,
    compression: str = "snappy",
    partition_by_symbol: bool = True,
) -> int:
    """
    Write chunks incrementally as they arrive (memory-efficient streaming).
    
    Each chunk is written immediately to avoid accumulating data in memory.
    Uses incremental file naming (part_0000.parquet, part_0001.parquet, etc.)
    for each symbol partition.
    
    Args:
        chunk_iterator: Iterator yielding DataFrames (one chunk at a time)
        parquet_root: Root directory for Parquet files
        dataset: Dataset name (trades, quotes, nbbo)
        trade_date: Trade date for partitioning
        compression: Compression algorithm
        partition_by_symbol: Whether to partition by symbol
        
    Returns:
        Total number of rows written
    """
    date_str = trade_date.isoformat()
    final_dir = parquet_root / dataset / f"trade_date={date_str}"
    final_dir.mkdir(parents=True, exist_ok=True)
    
    # Track chunk numbers per symbol for incremental naming
    symbol_chunk_counters: dict[str, int] = {}
    total_rows = 0
    chunk_num = 0
    
    logger.info(f"Writing chunks incrementally to {final_dir}...")
    
    for chunk in chunk_iterator:
        chunk_num += 1
        chunk_rows = len(chunk)
        
        if chunk.is_empty():
            logger.debug(f"  Chunk {chunk_num}: Empty, skipping")
            continue
        
        logger.info(f"  Chunk {chunk_num}: {chunk_rows:,} rows")
        
        if partition_by_symbol and "symbol" in chunk.columns:
            # Partition by symbol and write each symbol's data incrementally
            for symbol_key, symbol_df in chunk.partition_by("symbol", as_dict=True).items():
                # Clean symbol key - remove tuple notation if present
                if isinstance(symbol_key, tuple):
                    symbol = symbol_key[0] if len(symbol_key) > 0 else str(symbol_key)
                elif isinstance(symbol_key, str):
                    if symbol_key.startswith("('") and symbol_key.endswith("',)"):
                        symbol = symbol_key[2:-3]
                    elif symbol_key.startswith('("') and symbol_key.endswith('",)'):
                        symbol = symbol_key[2:-3]
                    else:
                        symbol = symbol_key
                else:
                    symbol = str(symbol_key)
                
                # Get or initialize chunk counter for this symbol
                if symbol not in symbol_chunk_counters:
                    symbol_chunk_counters[symbol] = 0
                
                symbol_dir = final_dir / f"symbol={symbol}"
                symbol_dir.mkdir(parents=True, exist_ok=True)
                
                # Write incremental chunk file
                chunk_file = symbol_dir / f"part_{symbol_chunk_counters[symbol]:04d}.parquet"
                symbol_df.write_parquet(chunk_file, compression=compression)
                symbol_chunk_counters[symbol] += 1
                total_rows += len(symbol_df)
                
                logger.debug(f"    Wrote {len(symbol_df):,} rows for symbol={symbol} (chunk {symbol_chunk_counters[symbol]})")
        else:
            # Single partition - write incremental chunk
            chunk_file = final_dir / f"part_{chunk_num:04d}.parquet"
            chunk.write_parquet(chunk_file, compression=compression)
            total_rows += chunk_rows
            logger.debug(f"    Wrote chunk {chunk_num} ({chunk_rows:,} rows)")
        
        # Log progress periodically
        if chunk_num % 10 == 0:
            logger.info(f"  Progress: {chunk_num} chunks processed, {total_rows:,} total rows written")
    
    # Create _SUCCESS marker when done
    success_marker = final_dir / "_SUCCESS"
    success_marker.touch()
    
    logger.info(f"✓ Wrote {total_rows:,} rows in {chunk_num} chunks to {final_dir}")
    
    return total_rows


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
