"""Extract data from WRDS TAQ tables with streaming support."""

from __future__ import annotations

import logging
import uuid
from datetime import date, datetime
from typing import Iterator

import polars as pl
import wrds

from .config import StageAConfig
from .schemas import build_canonical_symbol, build_ts_event

logger = logging.getLogger(__name__)

# Top ETFs to include (same as sparsesignal)
TOP_ETFS = [
    "SPY", "QQQ", "DIA", "IWM", "JETS", "XLE", "XLK", "XLF", "XLU",
    "XLY", "XLP", "XLI", "XLB", "XLV", "XLRE", "XLC"
]


class WRDSExtractor:
    """Extract data from WRDS TAQ tables."""
    
    def __init__(self, config: StageAConfig):
        self.config = config
        self.db: wrds.Connection | None = None
    
    def connect(self):
        """Connect to WRDS."""
        if self.db is None:
            logger.info("Connecting to WRDS...")
            # Only pass username if explicitly configured, otherwise let library use .pgpass
            if self.config.wrds_username:
                self.db = wrds.Connection(wrds_username=self.config.wrds_username)
            else:
                # Let library use .pgpass file automatically
                self.db = wrds.Connection()
            logger.info("✓ Connected to WRDS")
    
    def close(self):
        """Close WRDS connection."""
        if self.db is not None:
            self.db.close()
            self.db = None
            logger.info("✓ WRDS connection closed")
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def get_sp500_tickers(self, as_of_date: date) -> list[str]:
        """
        Get S&P 500 tickers from WRDS for a specific date.
        
        Args:
            as_of_date: Date to get S&P 500 constituents for
            
        Returns:
            List of ticker symbols
        """
        if self.db is None:
            self.connect()
        
        date_str = as_of_date.isoformat()
        logger.info(f"Fetching S&P 500 constituents as of {date_str}...")
        
        query = f"""
        SELECT DISTINCT ticker
        FROM crsp.msenames AS t1
        INNER JOIN crsp.msp500list AS t2
        ON t1.permno = t2.permno
        WHERE t2.start <= '{date_str}'
          AND (t2.ending >= '{date_str}' OR t2.ending IS NULL)
          AND t1.namedt <= '{date_str}'
          AND t1.nameendt >= '{date_str}'
          AND t1.ticker IS NOT NULL
        ORDER BY ticker
        """
        
        try:
            df = self.db.raw_sql(query)
            tickers = df['ticker'].tolist()
            logger.info(f"✓ Found {len(tickers)} S&P 500 stocks")
            return tickers
        except Exception as e:
            logger.error(f"✗ Error fetching S&P 500 tickers: {e}")
            raise
    
    def get_default_symbols(self, as_of_date: date) -> list[str]:
        """
        Get default symbol list: S&P 500 constituents + top ETFs.
        
        Args:
            as_of_date: Date to get S&P 500 constituents for
            
        Returns:
            Sorted list of unique symbols
        """
        sp500_tickers = self.get_sp500_tickers(as_of_date)
        logger.info(f"Adding {len(TOP_ETFS)} top ETFs...")
        all_symbols = sorted(list(set(sp500_tickers + TOP_ETFS)))
        logger.info(f"✓ Total symbols: {len(all_symbols)} ({len(sp500_tickers)} stocks + {len(TOP_ETFS)} ETFs)")
        return all_symbols
    
    def _find_schema(self, table_name: str, year: str) -> str:
        """Find the correct schema for a table."""
        schemas_to_try = [f"taqm_{year}", "taqmsec"]
        
        for schema in schemas_to_try:
            try:
                test_query = f"SELECT 1 FROM {schema}.{table_name} LIMIT 1"
                self.db.raw_sql(test_query)
                logger.debug(f"Found table {schema}.{table_name}")
                return schema
            except Exception as e:
                error_msg = str(e).lower()
                if 'does not exist' in error_msg or 'undefined' in error_msg:
                    continue
                else:
                    # Might still be valid, try it
                    return schema
        
        raise ValueError(f"Could not find table {table_name} in any schema (tried: {schemas_to_try})")
    
    def check_tables_available(self, trade_date: date, data_types: list[str]) -> bool:
        """
        Check if TAQ tables are available for the given date and data types.
        
        Args:
            trade_date: Trade date to check
            data_types: List of data types to check (e.g., ["trades", "nbbo"])
            
        Returns:
            True if all requested tables exist, False otherwise
        """
        if self.db is None:
            self.connect()
        
        date_str = trade_date.strftime("%Y%m%d")
        year = trade_date.strftime("%Y")
        
        # Map data types to table name patterns
        table_patterns = {
            "trades": f"ctm_{date_str}",
            "quotes": f"cqm_{date_str}",
            "nbbo": f"complete_nbbo_{date_str}",
        }
        
        schemas_to_try = [f"taqm_{year}", "taqmsec"]
        
        for data_type in data_types:
            table_name = table_patterns.get(data_type)
            if not table_name:
                logger.warning(f"Unknown data type: {data_type}, skipping table check")
                continue
            
            # Try to find the table in any schema
            found = False
            for schema in schemas_to_try:
                try:
                    test_query = f"SELECT 1 FROM {schema}.{table_name} LIMIT 1"
                    self.db.raw_sql(test_query)
                    found = True
                    logger.debug(f"Found {data_type} table: {schema}.{table_name}")
                    break
                except Exception as e:
                    error_msg = str(e).lower()
                    if 'does not exist' in error_msg or 'undefined' in error_msg:
                        continue
                    else:
                        # Other error might mean table exists but query failed
                        found = True
                        break
            
            if not found:
                logger.debug(f"Table not found for {data_type} on {trade_date}")
                return False
        
        return True
    
    def extract_trades_streaming(
        self,
        trade_date: date,
        symbols: list[str],
        extract_run_id: str,
    ) -> Iterator[pl.DataFrame]:
        """
        Extract trades from WRDS in streaming chunks.
        
        Yields DataFrames with raw trade data (chunked by symbol groups).
        """
        date_str = trade_date.strftime("%Y%m%d")
        year = trade_date.strftime("%Y")
        table_name = f"ctm_{date_str}"
        
        schema = self._find_schema(table_name, year)
        full_table = f"{schema}.{table_name}"
        
        logger.info(f"Extracting trades from {full_table} for {len(symbols)} symbols")
        
        # Process symbols in chunks
        chunk_size = self.config.chunk_size
        for i in range(0, len(symbols), chunk_size):
            chunk_symbols = symbols[i:i + chunk_size]
            ticker_str = "','".join(chunk_symbols)
            
            query = f"""
            SELECT *
            FROM {full_table}
            WHERE tr_corr = '00'
              AND time_m >= '09:30:00'
              AND time_m <= '16:00:00'
              AND sym_root IN ('{ticker_str}')
            ORDER BY sym_root, tr_seqnum
            """
            
            try:
                logger.debug(f"Querying chunk {i//chunk_size + 1} ({len(chunk_symbols)} symbols)...")
                df = self.db.raw_sql(query)
                
                if len(df) > 0:
                    # Convert to Polars and add derived fields
                    df_pl = pl.from_pandas(df)
                    df_pl = self._enrich_trades(df_pl, trade_date, extract_run_id)
                    logger.info(f"  Chunk {i//chunk_size + 1}: {len(df_pl):,} trades")
                    yield df_pl
                else:
                    logger.debug(f"  Chunk {i//chunk_size + 1}: No trades found")
            except Exception as e:
                logger.error(f"  ✗ Error processing chunk {i//chunk_size + 1}: {e}")
                continue
    
    def extract_quotes_streaming(
        self,
        trade_date: date,
        symbols: list[str],
        extract_run_id: str,
    ) -> Iterator[pl.DataFrame]:
        """Extract quotes from WRDS in streaming chunks."""
        date_str = trade_date.strftime("%Y%m%d")
        year = trade_date.strftime("%Y")
        table_name = f"cqm_{date_str}"
        
        schema = self._find_schema(table_name, year)
        full_table = f"{schema}.{table_name}"
        
        logger.info(f"Extracting quotes from {full_table} for {len(symbols)} symbols")
        
        chunk_size = self.config.chunk_size
        for i in range(0, len(symbols), chunk_size):
            chunk_symbols = symbols[i:i + chunk_size]
            ticker_str = "','".join(chunk_symbols)
            
            query = f"""
            SELECT *
            FROM {full_table}
            WHERE time_m >= '09:30:00'
              AND time_m <= '16:00:00'
              AND bid > 0
              AND ask > 0
              AND bid < ask
              AND sym_root IN ('{ticker_str}')
            ORDER BY sym_root, qu_seqnum
            """
            
            try:
                logger.debug(f"Querying chunk {i//chunk_size + 1} ({len(chunk_symbols)} symbols)...")
                df = self.db.raw_sql(query)
                
                if len(df) > 0:
                    df_pl = pl.from_pandas(df)
                    df_pl = self._enrich_quotes(df_pl, trade_date, extract_run_id)
                    logger.info(f"  Chunk {i//chunk_size + 1}: {len(df_pl):,} quotes")
                    yield df_pl
                else:
                    logger.debug(f"  Chunk {i//chunk_size + 1}: No quotes found")
            except Exception as e:
                logger.error(f"  ✗ Error processing chunk {i//chunk_size + 1}: {e}")
                continue
    
    def extract_nbbo_streaming(
        self,
        trade_date: date,
        symbols: list[str],
        extract_run_id: str,
    ) -> Iterator[pl.DataFrame]:
        """Extract NBBO from WRDS in streaming chunks."""
        date_str = trade_date.strftime("%Y%m%d")
        year = trade_date.strftime("%Y")
        table_name = f"complete_nbbo_{date_str}"
        
        schema = self._find_schema(table_name, year)
        full_table = f"{schema}.{table_name}"
        
        logger.info(f"Extracting NBBO from {full_table} for {len(symbols)} symbols")
        
        chunk_size = self.config.chunk_size
        for i in range(0, len(symbols), chunk_size):
            chunk_symbols = symbols[i:i + chunk_size]
            ticker_str = "','".join(chunk_symbols)
            
            query = f"""
            SELECT *
            FROM {full_table}
            WHERE time_m >= '09:30:00'
              AND time_m <= '16:00:00'
              AND best_bid > 0
              AND best_ask > 0
              AND best_ask >= best_bid
              AND sym_root IN ('{ticker_str}')
            ORDER BY sym_root, time_m, time_m_nano
            """
            
            try:
                logger.debug(f"Querying chunk {i//chunk_size + 1} ({len(chunk_symbols)} symbols)...")
                df = self.db.raw_sql(query)
                
                if len(df) > 0:
                    df_pl = pl.from_pandas(df)
                    df_pl = self._enrich_nbbo(df_pl, trade_date, extract_run_id)
                    logger.info(f"  Chunk {i//chunk_size + 1}: {len(df_pl):,} NBBO records")
                    yield df_pl
                else:
                    logger.debug(f"  Chunk {i//chunk_size + 1}: No NBBO records found")
            except Exception as e:
                logger.error(f"  ✗ Error processing chunk {i//chunk_size + 1}: {e}")
                continue
    
    def _enrich_trades(
        self,
        df: pl.DataFrame,
        trade_date: date,
        extract_run_id: str,
    ) -> pl.DataFrame:
        """Add derived fields to trades DataFrame."""
        ingest_ts = datetime.utcnow()
        
        return df.with_columns([
            pl.lit(trade_date).alias("trade_date"),
            build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
            build_ts_event(
                pl.col("date"),
                pl.col("time_m"),
                pl.col("time_m_nano"),
                self.config.timezone,
            ).alias("ts_event"),
            pl.lit(extract_run_id).alias("extract_run_id"),
            pl.lit(ingest_ts).alias("ingest_ts"),
        ])
    
    def _enrich_quotes(
        self,
        df: pl.DataFrame,
        trade_date: date,
        extract_run_id: str,
    ) -> pl.DataFrame:
        """Add derived fields to quotes DataFrame."""
        ingest_ts = datetime.utcnow()
        
        return df.with_columns([
            pl.lit(trade_date).alias("trade_date"),
            build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
            build_ts_event(
                pl.col("date"),
                pl.col("time_m"),
                pl.col("time_m_nano"),
                self.config.timezone,
            ).alias("ts_event"),
            pl.lit(extract_run_id).alias("extract_run_id"),
            pl.lit(ingest_ts).alias("ingest_ts"),
        ])
    
    def _enrich_nbbo(
        self,
        df: pl.DataFrame,
        trade_date: date,
        extract_run_id: str,
    ) -> pl.DataFrame:
        """Add derived fields to NBBO DataFrame."""
        ingest_ts = datetime.utcnow()
        
        return df.with_columns([
            pl.lit(trade_date).alias("trade_date"),
            build_canonical_symbol(pl.col("sym_root"), pl.col("sym_suffix")).alias("symbol"),
            build_ts_event(
                pl.col("date"),
                pl.col("time_m"),
                pl.col("time_m_nano"),
                self.config.timezone,
            ).alias("ts_event"),
            pl.lit(extract_run_id).alias("extract_run_id"),
            pl.lit(ingest_ts).alias("ingest_ts"),
        ])

