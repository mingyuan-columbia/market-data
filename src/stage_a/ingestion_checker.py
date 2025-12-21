"""Check if data has already been ingested for a given (date, symbol) combination."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Literal

logger = logging.getLogger(__name__)

DataType = Literal["trades", "quotes", "nbbo"]


def check_partition_exists(
    parquet_root: Path,
    data_type: DataType,
    trade_date: date,
    symbol: str | None = None,
    partition_by_symbol: bool = True,
) -> bool:
    """
    Check if a partition exists for the given date and optional symbol.
    
    Returns True if _SUCCESS marker exists or if parquet files exist.
    """
    if data_type == "trades":
        dataset = "trades"
    elif data_type == "quotes":
        dataset = "quotes"
    elif data_type == "nbbo":
        dataset = "nbbo"
    else:
        raise ValueError(f"Unknown data type: {data_type}")
    
    date_str = trade_date.isoformat()
    date_partition_dir = parquet_root / dataset / f"trade_date={date_str}"
    
    if symbol and partition_by_symbol:
        # Polars partition_by creates directories with tuple notation: symbol=('AAPL',)
        # We need to check all existing symbol directories and match by symbol value
        if not date_partition_dir.exists():
            logger.debug(f"  Date partition directory does not exist: {date_partition_dir}")
            return False
        
        # List all symbol directories and find the one matching our symbol
        symbol_dirs = [d for d in date_partition_dir.iterdir() 
                      if d.is_dir() and d.name.startswith("symbol=")]
        
        # Try to find matching directory
        partition_dir = None
        for sym_dir in symbol_dirs:
            # Extract symbol from directory name (handle both formats)
            sym_name = sym_dir.name.replace("symbol=", "")
            # Handle tuple notation: ('AAPL',) or ("AAPL",)
            if sym_name.startswith("('") and sym_name.endswith("',)"):
                extracted_symbol = sym_name[2:-3]
            elif sym_name.startswith('("') and sym_name.endswith('",)'):
                extracted_symbol = sym_name[2:-3]
            else:
                extracted_symbol = sym_name
            
            if extracted_symbol == symbol:
                partition_dir = sym_dir
                break
        
        if partition_dir is None:
            logger.debug(f"  No partition directory found for symbol={symbol}")
            if symbol_dirs:
                logger.debug(f"  Available symbol dirs (sample): {[d.name for d in symbol_dirs[:5]]}")
            return False
    else:
        partition_dir = date_partition_dir
    
    # Debug: log what we're checking
    logger.debug(f"Checking partition: {partition_dir}")
    
    # Check if base directory exists
    if not partition_dir.exists():
        logger.debug(f"  Partition directory does not exist: {partition_dir}")
        return False
    
    # Check for _SUCCESS marker
    success_marker = partition_dir / "_SUCCESS"
    if success_marker.exists():
        logger.debug(f"  Found _SUCCESS marker: {success_marker}")
        return True
    
    # Check for parquet files (including subdirectories if partitioned by symbol)
    parquet_files = []
    if partition_by_symbol and symbol:
        # Check specific symbol directory
        # Look for any .parquet files (including part_*.parquet from chunked writes)
        parquet_files = list(partition_dir.glob("*.parquet"))
        if not parquet_files:
            # Also check subdirectories in case structure is different
            parquet_files = list(partition_dir.glob("**/*.parquet"))
    else:
        # Check date directory and all symbol subdirectories recursively
        parquet_files = list(partition_dir.glob("**/*.parquet"))
    
    found = len(parquet_files) > 0
    if found:
        logger.debug(f"  Found {len(parquet_files)} parquet file(s) in {partition_dir}")
        logger.debug(f"  Sample files: {[str(f.name) for f in parquet_files[:3]]}")
    else:
        logger.debug(f"  No parquet files found in {partition_dir}")
        # Check if parent directories exist for debugging
        if partition_dir.parent.exists():
            logger.debug(f"  Parent directory exists: {partition_dir.parent}")
            parent_contents = list(partition_dir.parent.iterdir())
            logger.debug(f"  Parent contents: {[p.name for p in parent_contents[:10]]}")
    
    return found


def check_ingestion_status(
    parquet_root: Path,
    trade_date: date,
    symbols: list[str],
    partition_by_symbol: bool = True,
) -> dict[str, dict[DataType, bool]]:
    """
    Check ingestion status for all symbols and data types.
    
    Returns:
        {
            "AAPL": {"trades": True, "quotes": True, "nbbo": False},
            ...
        }
    """
    status: dict[str, dict[DataType, bool]] = {}
    
    for symbol in symbols:
        status[symbol] = {
            "trades": check_partition_exists(parquet_root, "trades", trade_date, symbol, partition_by_symbol),
            "quotes": check_partition_exists(parquet_root, "quotes", trade_date, symbol, partition_by_symbol),
            "nbbo": check_partition_exists(parquet_root, "nbbo", trade_date, symbol, partition_by_symbol),
        }
    
    return status


def get_missing_data(
    parquet_root: Path,
    trade_date: date,
    symbols: list[str],
    partition_by_symbol: bool = True,
) -> dict[DataType, list[str]]:
    """
    Get list of symbols missing for each data type.
    
    Returns:
        {
            "trades": ["AAPL", "MSFT"],
            "quotes": [],
            "nbbo": ["AAPL", "MSFT", "GOOGL"]
        }
    """
    status = check_ingestion_status(parquet_root, trade_date, symbols, partition_by_symbol)
    
    missing: dict[DataType, list[str]] = {
        "trades": [],
        "quotes": [],
        "nbbo": [],
    }
    
    for symbol, data_status in status.items():
        for data_type in ["trades", "quotes", "nbbo"]:
            if not data_status[data_type]:
                missing[data_type].append(symbol)
    
    return missing


def is_fully_ingested(
    parquet_root: Path,
    trade_date: date,
    symbols: list[str],
    partition_by_symbol: bool = True,
) -> bool:
    """Check if all symbols have all data types ingested."""
    missing = get_missing_data(parquet_root, trade_date, symbols, partition_by_symbol)
    return (
        len(missing["trades"]) == 0
        and len(missing["quotes"]) == 0
        and len(missing["nbbo"]) == 0
    )


def delete_partition(
    parquet_root: Path,
    data_type: DataType,
    trade_date: date,
    symbol: str | None = None,
    partition_by_symbol: bool = True,
) -> bool:
    """
    Delete a partition for the given date and optional symbol.
    
    Returns True if partition was deleted, False if it didn't exist.
    """
    if data_type == "trades":
        dataset = "trades"
    elif data_type == "quotes":
        dataset = "quotes"
    elif data_type == "nbbo":
        dataset = "nbbo"
    else:
        raise ValueError(f"Unknown data type: {data_type}")
    
    date_str = trade_date.isoformat()
    
    if symbol and partition_by_symbol:
        partition_dir = parquet_root / dataset / f"trade_date={date_str}" / f"symbol={symbol}"
    else:
        partition_dir = parquet_root / dataset / f"trade_date={date_str}"
    
    if not partition_dir.exists():
        return False
    
    # Delete the entire partition directory
    import shutil
    shutil.rmtree(partition_dir)
    return True


def delete_partitions_for_symbols(
    parquet_root: Path,
    trade_date: date,
    symbols: list[str],
    data_types: list[DataType] | None = None,
    partition_by_symbol: bool = True,
) -> int:
    """
    Delete partitions for given symbols and data types.
    
    Args:
        parquet_root: Root directory for Parquet files
        trade_date: Trade date
        symbols: List of symbols to delete partitions for
        data_types: List of data types to delete (default: all)
        partition_by_symbol: Whether partitions are by symbol
        
    Returns:
        Number of partitions deleted
    """
    if data_types is None:
        data_types = ["trades", "quotes", "nbbo"]
    
    deleted_count = 0
    
    for data_type in data_types:
        if partition_by_symbol:
            # Delete per-symbol partitions
            for symbol in symbols:
                if delete_partition(parquet_root, data_type, trade_date, symbol, partition_by_symbol):
                    deleted_count += 1
        else:
            # Delete entire date partition (affects all symbols)
            if delete_partition(parquet_root, data_type, trade_date, None, partition_by_symbol):
                deleted_count += 1
    
    return deleted_count

