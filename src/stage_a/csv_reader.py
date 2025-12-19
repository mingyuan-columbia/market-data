"""Read CSV files and convert to canonical format."""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import polars as pl

from .schemas import build_canonical_symbol, build_ts_event

logger = logging.getLogger(__name__)


def read_trades_csv(
    csv_path: Path,
    trade_date: date,
    extract_run_id: str,
    timezone: str,
) -> pl.DataFrame:
    """Read trades CSV and enrich with canonical fields."""
    logger.info(f"Reading trades CSV: {csv_path}")
    
    df = pl.read_csv(
        csv_path,
        infer_schema_length=10000,
        ignore_errors=True,
    )
    
    ingest_ts = datetime.utcnow()
    
    return df.with_columns([
        pl.lit(trade_date).alias("trade_date"),
        build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
        build_ts_event(
            pl.col("date"),
            pl.col("time_m"),
            pl.col("time_m_nano"),
            timezone,
        ).alias("ts_event"),
        pl.lit(extract_run_id).alias("extract_run_id"),
        pl.lit(ingest_ts).alias("ingest_ts"),
    ])


def read_quotes_csv(
    csv_path: Path,
    trade_date: date,
    extract_run_id: str,
    timezone: str,
) -> pl.DataFrame:
    """Read quotes CSV and enrich with canonical fields."""
    logger.info(f"Reading quotes CSV: {csv_path}")
    
    df = pl.read_csv(
        csv_path,
        infer_schema_length=10000,
        ignore_errors=True,
    )
    
    ingest_ts = datetime.utcnow()
    
    return df.with_columns([
        pl.lit(trade_date).alias("trade_date"),
        build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
        build_ts_event(
            pl.col("date"),
            pl.col("time_m"),
            pl.col("time_m_nano"),
            timezone,
        ).alias("ts_event"),
        pl.lit(extract_run_id).alias("extract_run_id"),
        pl.lit(ingest_ts).alias("ingest_ts"),
    ])


def read_nbbo_csv(
    csv_path: Path,
    trade_date: date,
    extract_run_id: str,
    timezone: str,
) -> pl.DataFrame:
    """Read NBBO CSV and enrich with canonical fields."""
    logger.info(f"Reading NBBO CSV: {csv_path}")
    
    df = pl.read_csv(
        csv_path,
        infer_schema_length=10000,
        ignore_errors=True,
    )
    
    ingest_ts = datetime.utcnow()
    
    return df.with_columns([
        pl.lit(trade_date).alias("trade_date"),
        build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
        build_ts_event(
            pl.col("date"),
            pl.col("time_m"),
            pl.col("time_m_nano"),
            timezone,
        ).alias("ts_event"),
        pl.lit(extract_run_id).alias("extract_run_id"),
        pl.lit(ingest_ts).alias("ingest_ts"),
    ])


def check_csv_exists(csv_root: Path, trade_date: date, data_type: str) -> Path | None:
    """
    Check if CSV file exists for the given date and data type.
    
    Args:
        csv_root: Root directory for CSV files
        trade_date: Trade date
        data_type: One of "trade", "quote", "nbbo"
        
    Returns:
        Path to CSV file if exists, None otherwise
    """
    date_str = trade_date.strftime("%Y%m%d")
    
    if data_type == "trade":
        csv_path = csv_root / f"taq_trade_{date_str}.csv"
    elif data_type == "quote":
        csv_path = csv_root / f"taq_quote_{date_str}.csv"
    elif data_type == "nbbo":
        csv_path = csv_root / f"taq_nbbo_{date_str}.csv"
    else:
        raise ValueError(f"Unknown data type: {data_type}")
    
    if csv_path.exists():
        return csv_path
    return None

