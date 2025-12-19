"""Efficient Parquet writing with NAS-aware optimizations."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

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
            for symbol, symbol_df in df.partition_by("symbol", as_dict=True).items():
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


def write_chunked_from_csv(
    csv_path: Path,
    parquet_root: Path,
    dataset: str,
    trade_date: date,
    compression: str = "snappy",
    partition_by_symbol: bool = True,
    chunk_size: int = 1_000_000,
    enrich_fn=None,
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
        
        # Apply enrichment if provided
        if enrich_fn:
            chunk = enrich_fn(chunk)
        
        if partition_by_symbol and "symbol" in chunk.columns:
            # Write per symbol
            for symbol, symbol_df in chunk.partition_by("symbol", as_dict=True).items():
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

