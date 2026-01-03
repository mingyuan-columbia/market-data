"""Data loading utilities for Streamlit app."""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import polars as pl

logger = logging.getLogger(__name__)


def load_trades(
    data_root: Path,
    data_source: str,
    trade_date: date,
    symbol: Optional[str | list[str]] = None,
    timezone: str = "America/New_York",
) -> Optional[pl.DataFrame]:
    """
    Load trades data for a given date and optional symbol(s).
    
    Args:
        data_root: Root data directory
        data_source: Data source name
        trade_date: Trade date
        symbol: Optional symbol (str) or list of symbols (list[str]) to filter by
        timezone: Timezone to convert timestamps to
        
    Returns:
        Polars DataFrame with trades, or None if not found
    """
    parquet_root = data_root / data_source / "parquet_raw"
    date_str = trade_date.isoformat()
    date_dir = parquet_root / "trades" / f"trade_date={date_str}"
    
    if not date_dir.exists():
        logger.warning(f"Trades directory not found: {date_dir}")
        return None
    
    parquet_files = []
    
    if symbol:
        # Handle both single symbol (str) and multiple symbols (list)
        symbols_to_load = symbol if isinstance(symbol, list) else [symbol]
        
        # Find symbol directories
        symbol_dirs = list(date_dir.glob("symbol=*"))
        for sym_dir in symbol_dirs:
            sym_name = sym_dir.name.replace("symbol=", "")
            if sym_name.startswith("('") and sym_name.endswith("',)"):
                extracted_symbol = sym_name[2:-3]
            elif sym_name.startswith('("') and sym_name.endswith('",)'):
                extracted_symbol = sym_name[2:-3]
            else:
                extracted_symbol = sym_name
            
            if extracted_symbol in symbols_to_load:
                files = list(sym_dir.glob("*.parquet")) or list(sym_dir.glob("**/*.parquet"))
                parquet_files.extend(files)
    else:
        # Load all symbols
        parquet_files = list(date_dir.glob("**/*.parquet"))
    
    if not parquet_files:
        logger.warning(f"No parquet files found for {trade_date}, symbol={symbol}")
        return None
    
    logger.info(f"Loading {len(parquet_files)} parquet files...")
    
    # Use lazy evaluation for better performance with many files
    # This allows Polars to optimize the query plan and can utilize multiple cores
    if len(parquet_files) > 10:
        # For many files, use lazy scan_parquet for better performance
        try:
            lazy_frames = []
            for pf in parquet_files:
                lf = pl.scan_parquet(str(pf))
                # Normalize trade_date if needed (check schema first)
                schema = lf.schema
                if "trade_date" in schema and schema["trade_date"] == pl.String:
                    lf = lf.with_columns(
                        pl.col("trade_date").str.strptime(pl.Date, "%Y-%m-%d")
                    )
                lazy_frames.append(lf)
            
            # Concatenate lazy frames and collect
            trades = pl.concat(lazy_frames).collect()
        except Exception as e:
            logger.warning(f"Error using lazy loading, falling back to eager: {e}")
            # Fallback to eager loading
            dfs = []
            for pf in parquet_files:
                try:
                    df = pl.read_parquet(pf)
                    if "trade_date" in df.columns:
                        if df["trade_date"].dtype == pl.String:
                            df = df.with_columns(
                                pl.col("trade_date").str.strptime(pl.Date, "%Y-%m-%d")
                            )
                    dfs.append(df)
                except Exception as e2:
                    logger.warning(f"Error loading {pf}: {e2}")
                    continue
            
            if not dfs:
                return None
            trades = pl.concat(dfs)
    else:
        # For few files, use eager loading
        dfs = []
        for pf in parquet_files:
            try:
                df = pl.read_parquet(pf)
                # Normalize trade_date if needed
                if "trade_date" in df.columns:
                    if df["trade_date"].dtype == pl.String:
                        df = df.with_columns(
                            pl.col("trade_date").str.strptime(pl.Date, "%Y-%m-%d")
                        )
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error loading {pf}: {e}")
                continue
        
        if not dfs:
            return None
        
        trades = pl.concat(dfs)
    
    # Convert timestamps to specified timezone if ts_event exists
    if "ts_event" in trades.columns:
        # Check if already in timezone or needs conversion
        if trades["ts_event"].dtype.time_zone != timezone:
            trades = trades.with_columns([
                pl.col("ts_event").dt.convert_time_zone(timezone)
            ])
    
    logger.info(f"Loaded {len(trades):,} trades")
    return trades


def load_nbbo(
    data_root: Path,
    data_source: str,
    trade_date: date,
    symbol: Optional[str | list[str]] = None,
    timezone: str = "America/New_York",
) -> Optional[pl.DataFrame]:
    """
    Load NBBO data for a given date and optional symbol(s).
    
    Args:
        data_root: Root data directory
        data_source: Data source name
        trade_date: Trade date
        symbol: Optional symbol (str) or list of symbols (list[str]) to filter by
        timezone: Timezone to convert timestamps to
        
    Returns:
        Polars DataFrame with NBBO, or None if not found
    """
    parquet_root = data_root / data_source / "parquet_raw"
    date_str = trade_date.isoformat()
    date_dir = parquet_root / "nbbo" / f"trade_date={date_str}"
    
    if not date_dir.exists():
        logger.warning(f"NBBO directory not found: {date_dir}")
        return None
    
    parquet_files = []
    
    if symbol:
        # Handle both single symbol (str) and multiple symbols (list)
        symbols_to_load = symbol if isinstance(symbol, list) else [symbol]
        
        # Find symbol directories
        symbol_dirs = list(date_dir.glob("symbol=*"))
        for sym_dir in symbol_dirs:
            sym_name = sym_dir.name.replace("symbol=", "")
            if sym_name.startswith("('") and sym_name.endswith("',)"):
                extracted_symbol = sym_name[2:-3]
            elif sym_name.startswith('("') and sym_name.endswith('",)'):
                extracted_symbol = sym_name[2:-3]
            else:
                extracted_symbol = sym_name
            
            if extracted_symbol in symbols_to_load:
                files = list(sym_dir.glob("*.parquet")) or list(sym_dir.glob("**/*.parquet"))
                parquet_files.extend(files)
    else:
        # Load all symbols
        parquet_files = list(date_dir.glob("**/*.parquet"))
    
    if not parquet_files:
        logger.warning(f"No parquet files found for {trade_date}, symbol={symbol}")
        return None
    
    logger.info(f"Loading {len(parquet_files)} parquet files...")
    
    # Use lazy evaluation for better performance with many files
    if len(parquet_files) > 10:
        # For many files, use lazy scan_parquet for better performance
        try:
            lazy_frames = []
            for pf in parquet_files:
                lf = pl.scan_parquet(str(pf))
                # Normalize trade_date if needed (check schema first)
                schema = lf.schema
                if "trade_date" in schema and schema["trade_date"] == pl.String:
                    lf = lf.with_columns(
                        pl.col("trade_date").str.strptime(pl.Date, "%Y-%m-%d")
                    )
                lazy_frames.append(lf)
            
            # Concatenate lazy frames and collect
            nbbo = pl.concat(lazy_frames).collect()
        except Exception as e:
            logger.warning(f"Error using lazy loading, falling back to eager: {e}")
            # Fallback to eager loading
            dfs = []
            for pf in parquet_files:
                try:
                    df = pl.read_parquet(pf)
                    if "trade_date" in df.columns:
                        if df["trade_date"].dtype == pl.String:
                            df = df.with_columns(
                                pl.col("trade_date").str.strptime(pl.Date, "%Y-%m-%d")
                            )
                    dfs.append(df)
                except Exception as e2:
                    logger.warning(f"Error loading {pf}: {e2}")
                    continue
            
            if not dfs:
                return None
            nbbo = pl.concat(dfs)
    else:
        # For few files, use eager loading
        dfs = []
        for pf in parquet_files:
            try:
                df = pl.read_parquet(pf)
                # Normalize trade_date if needed
                if "trade_date" in df.columns:
                    if df["trade_date"].dtype == pl.String:
                        df = df.with_columns(
                            pl.col("trade_date").str.strptime(pl.Date, "%Y-%m-%d")
                        )
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error loading {pf}: {e}")
                continue
        
        if not dfs:
            return None
        
        nbbo = pl.concat(dfs)
    
    # Convert timestamps to specified timezone if ts_event exists
    if "ts_event" in nbbo.columns:
        if nbbo["ts_event"].dtype.time_zone != timezone:
            nbbo = nbbo.with_columns([
                pl.col("ts_event").dt.convert_time_zone(timezone)
            ])
    
    # Calculate derived fields if not present
    if "best_bid" in nbbo.columns and "best_ask" in nbbo.columns:
        nbbo = nbbo.with_columns([
            ((pl.col("best_bid") + pl.col("best_ask")) / 2).alias("mid_price"),
            (pl.col("best_ask") - pl.col("best_bid")).alias("spread"),
        ])
    
    logger.info(f"Loaded {len(nbbo):,} NBBO records")
    return nbbo

