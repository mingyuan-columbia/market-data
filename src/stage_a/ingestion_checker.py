"""Check if data has already been ingested for a given (date, symbol) combination."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Literal

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
    
    if symbol and partition_by_symbol:
        partition_dir = parquet_root / dataset / f"trade_date={date_str}" / f"symbol={symbol}"
    else:
        partition_dir = parquet_root / dataset / f"trade_date={date_str}"
    
    # Check for _SUCCESS marker
    success_marker = partition_dir / "_SUCCESS"
    if success_marker.exists():
        return True
    
    # Check for parquet files (including subdirectories if partitioned by symbol)
    if partition_by_symbol and symbol:
        # Check specific symbol directory
        parquet_files = list(partition_dir.glob("*.parquet"))
    else:
        # Check date directory and all symbol subdirectories
        parquet_files = list(partition_dir.glob("**/*.parquet"))
    
    return len(parquet_files) > 0


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

