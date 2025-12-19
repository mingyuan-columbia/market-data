"""Canonical schemas for raw TAQ data."""

from __future__ import annotations

import polars as pl


# Raw trade schema (lossless - all original fields + derived)
RAW_TRADE_SCHEMA = {
    # Original fields
    "date": pl.Date,
    "time_m": pl.Time,
    "time_m_nano": pl.Int16,
    "part_time": pl.Time,
    "part_time_nano": pl.Int16,
    "trf_time": pl.Time,
    "trf_time_nano": pl.Int16,
    "sym_root": pl.Utf8,
    "sym_suffix": pl.Utf8,
    "ex": pl.Utf8,
    "price": pl.Decimal(precision=10, scale=4),  # Lossless price
    "size": pl.Int32,
    "tr_corr": pl.Utf8,
    "tr_id": pl.Int64,
    "tr_rf": pl.Utf8,
    "tr_scond": pl.Utf8,
    "tr_seqnum": pl.Int64,
    "tr_source": pl.Utf8,
    "tr_stop_ind": pl.Utf8,
    "tte_ind": pl.Utf8,
    # Derived fields
    "trade_date": pl.Date,  # Same as date, for partitioning
    "symbol": pl.Utf8,  # Canonical symbol
    "ts_event": pl.Datetime(time_zone="UTC"),  # Canonical timestamp
    "extract_run_id": pl.Utf8,  # UUID string
    "ingest_ts": pl.Datetime(time_zone="UTC"),  # When ingested
}

# Raw quote schema
RAW_QUOTE_SCHEMA = {
    # Original fields (add all quote fields from WRDS)
    "date": pl.Date,
    "time_m": pl.Time,
    "time_m_nano": pl.Int16,
    "sym_root": pl.Utf8,
    "sym_suffix": pl.Utf8,
    "ex": pl.Utf8,
    "bid": pl.Decimal(precision=10, scale=4),
    "bidsiz": pl.Int32,
    "ask": pl.Decimal(precision=10, scale=4),
    "asksiz": pl.Int32,
    "qu_seqnum": pl.Int64,
    "qu_cancel": pl.Utf8,
    "qu_source": pl.Utf8,
    # Derived fields
    "trade_date": pl.Date,
    "symbol": pl.Utf8,
    "ts_event": pl.Datetime(time_zone="UTC"),
    "extract_run_id": pl.Utf8,
    "ingest_ts": pl.Datetime(time_zone="UTC"),
}

# Raw NBBO schema
RAW_NBBO_SCHEMA = {
    # Original fields (add all NBBO fields from WRDS)
    "date": pl.Date,
    "time_m": pl.Time,
    "time_m_nano": pl.Int16,
    "sym_root": pl.Utf8,
    "sym_suffix": pl.Utf8,
    "best_bid": pl.Decimal(precision=10, scale=4),
    "best_bidsiz": pl.Int32,
    "best_ask": pl.Decimal(precision=10, scale=4),
    "best_asksiz": pl.Int32,
    "best_bidex": pl.Utf8,
    "best_askex": pl.Utf8,
    "nbbo_qu_cond": pl.Utf8,
    "secstat_ind": pl.Utf8,
    "luld_indicator": pl.Utf8,
    "luld_bbo_cqs": pl.Decimal(precision=10, scale=4),
    "luld_bbo_uts": pl.Decimal(precision=10, scale=4),
    # Derived fields
    "trade_date": pl.Date,
    "symbol": pl.Utf8,
    "ts_event": pl.Datetime(time_zone="UTC"),
    "extract_run_id": pl.Utf8,
    "ingest_ts": pl.Datetime(time_zone="UTC"),
}


def build_canonical_symbol(sym_root: pl.Expr, sym_suffix: pl.Expr) -> pl.Expr:
    """Build canonical symbol: sym_root if suffix is blank/null, else sym_root.suffix."""
    return pl.when(sym_suffix.is_null() | (sym_suffix.str.strip_chars() == ""))\
        .then(sym_root.str.strip_chars())\
        .otherwise(sym_root.str.strip_chars() + pl.lit(".") + sym_suffix.str.strip_chars())


def build_ts_event(
    date_col: pl.Expr,
    time_m_col: pl.Expr,
    time_m_nano_col: pl.Expr,
    timezone: str,
) -> pl.Expr:
    """Build canonical timestamp from date, time_m, and time_m_nano."""
    # Combine date and time_m
    datetime_str = (
        date_col.cast(pl.Utf8) + pl.lit(" ") + time_m_col.cast(pl.Utf8)
    )
    # Parse as datetime with timezone
    dt = datetime_str.str.to_datetime(
        format="%Y-%m-%d %H:%M:%S%.f",
        time_zone=timezone,
    )
    # Convert to UTC and add nanoseconds
    return dt.dt.convert_time_zone("UTC") + pl.duration(
        nanoseconds=time_m_nano_col.fill_null(0).cast(pl.Int64)
    )

