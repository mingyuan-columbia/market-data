"""Discover symbols from existing TAQ data directories."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)


def discover_symbols_from_taq(
    taq_parquet_root: Path,
    trade_date: date,
    data_type: str = "trades",
) -> list[str]:
    """
    Discover symbols from TAQ parquet directory structure.
    
    Looks for symbol subdirectories under:
    {taq_parquet_root}/{data_type}/trade_date={date}/symbol=XXX/
    
    Args:
        taq_parquet_root: Root directory for TAQ parquet files
        trade_date: Trade date to look for
        data_type: Data type to check (default: "trades")
        
    Returns:
        List of discovered symbols
    """
    date_str = trade_date.isoformat()
    date_dir = taq_parquet_root / data_type / f"trade_date={date_str}"
    
    if not date_dir.exists():
        logger.warning(f"TAQ directory does not exist: {date_dir}")
        return []
    
    symbols = []
    symbol_dirs = [d for d in date_dir.iterdir() if d.is_dir() and d.name.startswith("symbol=")]
    
    for sym_dir in symbol_dirs:
        # Extract symbol from directory name
        sym_name = sym_dir.name.replace("symbol=", "")
        
        # Handle tuple notation: ('AAPL',) or ("AAPL",)
        if sym_name.startswith("('") and sym_name.endswith("',)"):
            symbol = sym_name[2:-3]
        elif sym_name.startswith('("') and sym_name.endswith('",)'):
            symbol = sym_name[2:-3]
        else:
            symbol = sym_name
        
        # Check if directory has parquet files
        parquet_files = list(sym_dir.glob("*.parquet"))
        if parquet_files:
            symbols.append(symbol)
    
    symbols.sort()  # Return sorted list
    logger.info(f"Discovered {len(symbols)} symbols from {date_dir}")
    
    return symbols

