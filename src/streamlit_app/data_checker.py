"""Check data availability and suggest alternatives."""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path
from typing import Literal, Optional

logger = logging.getLogger(__name__)

DataType = Literal["trades", "quotes", "nbbo"]


def check_data_available(
    data_root: Path,
    data_source: str,
    trade_date: date,
    symbol: Optional[str] = None,
    data_type: DataType = "trades",
) -> bool:
    """
    Check if data is available for the given parameters.
    
    Args:
        data_root: Root data directory (e.g., /home/mingyuan/data)
        data_source: Data source name (taq, alpaca, alpaca_iex, csv)
        trade_date: Trade date to check
        symbol: Optional symbol to check
        data_type: Type of data (trades, quotes, nbbo)
        
    Returns:
        True if data exists, False otherwise
    """
    # Construct path: {data_root}/{data_source}/parquet_raw/{data_type}/trade_date={date}/symbol={symbol}/
    parquet_root = data_root / data_source / "parquet_raw"
    date_str = trade_date.isoformat()
    date_dir = parquet_root / data_type / f"trade_date={date_str}"
    
    if not date_dir.exists():
        return False
    
    if symbol:
        # Check if symbol directory exists
        symbol_dirs = list(date_dir.glob("symbol=*"))
        for sym_dir in symbol_dirs:
            # Extract symbol from directory name (handle tuple notation)
            sym_name = sym_dir.name.replace("symbol=", "")
            if sym_name.startswith("('") and sym_name.endswith("',)"):
                extracted_symbol = sym_name[2:-3]
            elif sym_name.startswith('("') and sym_name.endswith('",)'):
                extracted_symbol = sym_name[2:-3]
            else:
                extracted_symbol = sym_name
            
            if extracted_symbol == symbol:
                # Check if parquet files exist
                parquet_files = list(sym_dir.glob("*.parquet")) or list(sym_dir.glob("**/*.parquet"))
                return len(parquet_files) > 0
        
        return False
    else:
        # Check if any parquet files exist in date directory
        parquet_files = list(date_dir.glob("**/*.parquet"))
        return len(parquet_files) > 0


def find_available_dates(
    data_root: Path,
    data_source: str,
    data_type: DataType = "trades",
    symbol: Optional[str] = None,
    max_days: int = 30,
) -> list[date]:
    """
    Find available dates for a given data source and symbol.
    
    Args:
        data_root: Root data directory
        data_source: Data source name
        data_type: Type of data
        symbol: Optional symbol to filter by
        max_days: Maximum number of days to return
        
    Returns:
        List of available dates (sorted, most recent first)
    """
    parquet_root = data_root / data_source / "parquet_raw"
    data_type_dir = parquet_root / data_type
    
    if not data_type_dir.exists():
        return []
    
    available_dates = []
    
    # Find all trade_date= directories
    for date_dir in data_type_dir.iterdir():
        if date_dir.is_dir() and date_dir.name.startswith("trade_date="):
            try:
                date_str = date_dir.name.replace("trade_date=", "")
                trade_date = date.fromisoformat(date_str)
                
                if symbol:
                    # Check if this symbol exists for this date
                    if check_data_available(data_root, data_source, trade_date, symbol, data_type):
                        available_dates.append(trade_date)
                else:
                    # Check if any data exists for this date
                    parquet_files = list(date_dir.glob("**/*.parquet"))
                    if parquet_files:
                        available_dates.append(trade_date)
            except ValueError:
                continue
    
    # Sort descending (most recent first) and limit
    available_dates.sort(reverse=True)
    return available_dates[:max_days]


def find_closest_date(
    data_root: Path,
    data_source: str,
    target_date: date,
    symbol: Optional[str] = None,
    data_type: DataType = "trades",
    max_days_away: int = 30,
) -> Optional[date]:
    """
    Find the closest available date to the target date.
    
    Args:
        data_root: Root data directory
        data_source: Data source name
        target_date: Target date to find closest match for
        symbol: Optional symbol to filter by
        data_type: Type of data
        max_days_away: Maximum days away to search
        
    Returns:
        Closest available date, or None if none found
    """
    available_dates = find_available_dates(
        data_root, data_source, data_type, symbol, max_days=max_days_away * 2
    )
    
    if not available_dates:
        return None
    
    # Find closest date
    closest_date = None
    min_diff = float('inf')
    
    for avail_date in available_dates:
        diff = abs((avail_date - target_date).days)
        if diff < min_diff and diff <= max_days_away:
            min_diff = diff
            closest_date = avail_date
    
    return closest_date


def find_available_symbols(
    data_root: Path,
    data_source: str,
    trade_date: date,
    data_type: DataType = "trades",
) -> list[str]:
    """
    Find all available symbols for a given date and data source.
    
    Args:
        data_root: Root data directory
        data_source: Data source name
        trade_date: Trade date
        data_type: Type of data
        
    Returns:
        List of available symbols (sorted)
    """
    parquet_root = data_root / data_source / "parquet_raw"
    date_str = trade_date.isoformat()
    date_dir = parquet_root / data_type / f"trade_date={date_str}"
    
    if not date_dir.exists():
        return []
    
    symbols = set()
    symbol_dirs = list(date_dir.glob("symbol=*"))
    
    for sym_dir in symbol_dirs:
        # Extract symbol from directory name
        sym_name = sym_dir.name.replace("symbol=", "")
        if sym_name.startswith("('") and sym_name.endswith("',)"):
            extracted_symbol = sym_name[2:-3]
        elif sym_name.startswith('("') and sym_name.endswith('",)'):
            extracted_symbol = sym_name[2:-3]
        else:
            extracted_symbol = sym_name
        
        # Verify parquet files exist
        parquet_files = list(sym_dir.glob("*.parquet")) or list(sym_dir.glob("**/*.parquet"))
        if parquet_files:
            symbols.add(extracted_symbol)
    
    return sorted(list(symbols))


def suggest_alternatives(
    data_root: Path,
    data_sources: dict[str, Path],
    target_date: date,
    target_source: str,
    symbol: Optional[str] = None,
    data_type: DataType = "trades",
) -> dict:
    """
    Suggest alternative dates and data sources if target is not available.
    
    Returns:
        Dictionary with suggestions:
        {
            "closest_date": date or None,
            "alternative_sources": [{"source": str, "date": date}, ...],
            "available_dates": [date, ...],
        }
    """
    suggestions = {
        "closest_date": None,
        "alternative_sources": [],
        "available_dates": [],
    }
    
    # Check current source for closest date
    if target_source in data_sources:
        closest_date = find_closest_date(
            data_root, target_source, target_date, symbol, data_type
        )
        suggestions["closest_date"] = closest_date
        
        # Get some available dates
        available_dates = find_available_dates(
            data_root, target_source, data_type, symbol, max_days=10
        )
        suggestions["available_dates"] = available_dates
    
    # Check alternative sources
    for source_name, source_path in data_sources.items():
        if source_name == target_source:
            continue
        
        if check_data_available(data_root, source_name, target_date, symbol, data_type):
            suggestions["alternative_sources"].append({
                "source": source_name,
                "date": target_date,
            })
        else:
            # Find closest date in alternative source
            closest = find_closest_date(
                data_root, source_name, target_date, symbol, data_type
            )
            if closest:
                suggestions["alternative_sources"].append({
                    "source": source_name,
                    "date": closest,
                })
    
    return suggestions


def check_symbol_availability_across_sources(
    data_root: Path,
    data_sources: dict[str, Path],
    trade_date: date,
    symbols: list[str],
    data_type: DataType = "trades",
) -> dict[str, dict[str, bool]]:
    """
    Check symbol availability across all data sources.
    
    Args:
        data_root: Root data directory
        data_sources: Dictionary of data source names to paths
        trade_date: Trade date to check
        symbols: List of symbols to check
        data_type: Type of data (trades, quotes, nbbo)
        
    Returns:
        Dictionary mapping symbol to dict of source availability:
        {
            "AAPL": {"taq": True, "alpaca": False},
            "MSFT": {"taq": True, "alpaca": True},
        }
    """
    availability = {}
    
    for symbol in symbols:
        availability[symbol] = {}
        for source_name in data_sources.keys():
            available = check_data_available(
                data_root, source_name, trade_date, symbol, data_type
            )
            availability[symbol][source_name] = available
    
    return availability


def find_common_sources_for_symbols(
    data_root: Path,
    data_sources: dict[str, Path],
    trade_date: date,
    symbols: list[str],
    data_type: DataType = "trades",
) -> dict[str, list[str]]:
    """
    Find which data sources have all requested symbols available.
    
    Args:
        data_root: Root data directory
        data_sources: Dictionary of data source names to paths
        trade_date: Trade date to check
        symbols: List of symbols to check
        data_type: Type of data
        
    Returns:
        Dictionary with:
        {
            "sources_with_all": ["taq", "alpaca"],  # Sources that have all symbols
            "sources_with_some": ["taq"],  # Sources that have some symbols
            "missing_symbols": {"taq": ["XYZ"], "alpaca": []},  # Missing per source
        }
    """
    availability = check_symbol_availability_across_sources(
        data_root, data_sources, trade_date, symbols, data_type
    )
    
    sources_with_all = []
    sources_with_some = []
    missing_symbols = {source: [] for source in data_sources.keys()}
    
    for source_name in data_sources.keys():
        missing = [
            symbol for symbol in symbols
            if not availability[symbol].get(source_name, False)
        ]
        
        if not missing:
            sources_with_all.append(source_name)
        elif len(missing) < len(symbols):
            sources_with_some.append(source_name)
        
        missing_symbols[source_name] = missing
    
    return {
        "sources_with_all": sources_with_all,
        "sources_with_some": sources_with_some,
        "missing_symbols": missing_symbols,
    }


def find_symbols_across_dates(
    data_root: Path,
    data_sources: list[str],
    dates: list[date],
    data_type: DataType = "trades",
) -> list[str]:
    """
    Find symbols that are available across all given dates in any of the data sources.
    
    Args:
        data_root: Root data directory
        data_sources: List of data source names to check
        dates: List of dates to check
        data_type: Type of data (trades, quotes, nbbo)
        
    Returns:
        List of symbols available across all dates (in at least one source)
    """
    if not dates:
        return []
    
    # For each date, find symbols available in any source
    symbols_by_date = []
    for check_date in dates:
        date_symbols = set()
        for source in data_sources:
            # Check trades first
            symbols = find_available_symbols(data_root, source, check_date, data_type)
            if not symbols:
                # Fallback to NBBO if no trades
                symbols = find_available_symbols(data_root, source, check_date, "nbbo")
            date_symbols.update(symbols)
        symbols_by_date.append(date_symbols)
    
    # Find intersection: symbols available in all dates
    if not symbols_by_date:
        return []
    
    common_symbols = symbols_by_date[0]
    for date_symbols in symbols_by_date[1:]:
        common_symbols = common_symbols.intersection(date_symbols)
    
    return sorted(list(common_symbols))

