"""Alpaca API client for extracting historical SIP data."""

from __future__ import annotations

import logging
from datetime import date, datetime
from typing import Iterator, Optional
from zoneinfo import ZoneInfo

import polars as pl
import requests
from dateutil import parser

logger = logging.getLogger(__name__)


class AlpacaExtractor:
    """Extract historical trades and quotes from Alpaca API."""
    
    def __init__(
        self,
        api_key: str,
        secret_key: str,
        base_url: str = "https://paper-api.alpaca.markets",
        feed: str = "sip",
    ):
        """
        Initialize Alpaca API client.
        
        Args:
            api_key: Alpaca API key
            secret_key: Alpaca secret key
            base_url: Base URL for Alpaca API (default: paper trading)
            feed: Data feed to use ("sip" for consolidated SIP data)
        """
        self.api_key = api_key
        self.secret_key = secret_key
        self.base_url = base_url.rstrip("/")
        self.feed = feed
        self.session = requests.Session()
        self.session.headers.update({
            "APCA-API-KEY-ID": api_key,
            "APCA-API-SECRET-KEY": secret_key,
        })
    
    def _get_trades(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int = 10000,
        page_token: Optional[str] = None,
    ) -> dict:
        """
        Fetch trades from Alpaca API.
        
        Args:
            symbol: Stock symbol
            start: Start datetime (inclusive)
            end: End datetime (exclusive)
            limit: Maximum number of records per page
            page_token: Token for pagination
            
        Returns:
            API response dictionary
            
        Raises:
            requests.exceptions.HTTPError: If API request fails
        """
        url = f"{self.base_url}/v2/stocks/{symbol}/trades"
        params = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": limit,
        }
        # Try with feed parameter first
        if self.feed:
            params["feed"] = self.feed
        if page_token:
            params["page_token"] = page_token
        
        response = self.session.get(url, params=params)
        
        # Handle 404 gracefully - try without feed parameter if SIP feed fails
        if response.status_code == 404 and self.feed and self.feed != "iex":
            logger.warning(f"SIP feed not available, trying without feed parameter...")
            params.pop("feed", None)
            response = self.session.get(url, params=params)
        
        # Handle 404 gracefully - data might not be available
        if response.status_code == 404:
            logger.warning(f"No trades data available for {symbol} on {start.date()} (404)")
            logger.debug(f"URL: {url}, Params: {params}")
            return {"trades": []}
        
        response.raise_for_status()
        return response.json()
    
    def _get_quotes(
        self,
        symbol: str,
        start: datetime,
        end: datetime,
        limit: int = 10000,
        page_token: Optional[str] = None,
    ) -> dict:
        """
        Fetch quotes (NBBO) from Alpaca API.
        
        Args:
            symbol: Stock symbol
            start: Start datetime (inclusive)
            end: End datetime (exclusive)
            limit: Maximum number of records per page
            page_token: Token for pagination
            
        Returns:
            API response dictionary
            
        Raises:
            requests.exceptions.HTTPError: If API request fails (except 404)
        """
        url = f"{self.base_url}/v2/stocks/{symbol}/quotes"
        params = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "limit": limit,
        }
        # Try with feed parameter first
        if self.feed:
            params["feed"] = self.feed
        if page_token:
            params["page_token"] = page_token
        
        response = self.session.get(url, params=params)
        
        # Handle 404 gracefully - try without feed parameter if SIP feed fails
        if response.status_code == 404 and self.feed and self.feed != "iex":
            logger.warning(f"SIP feed not available, trying without feed parameter...")
            params.pop("feed", None)
            response = self.session.get(url, params=params)
        
        # Handle 404 gracefully - data might not be available
        if response.status_code == 404:
            logger.warning(f"No quotes data available for {symbol} on {start.date()} (404)")
            logger.debug(f"URL: {url}, Params: {params}")
            return {"quotes": []}
        
        response.raise_for_status()
        return response.json()
    
    def extract_trades(
        self,
        symbol: str,
        trade_date: date,
        timezone: str = "America/New_York",
    ) -> Iterator[pl.DataFrame]:
        """
        Extract all trades for a symbol on a given date.
        
        Args:
            symbol: Stock symbol
            trade_date: Trade date
            timezone: Timezone for market hours
            
        Yields:
            DataFrames with trade data
        """
        tz = ZoneInfo(timezone)
        
        # Market hours: 9:30 AM - 4:00 PM ET
        start_dt = datetime.combine(trade_date, datetime.min.time().replace(hour=9, minute=30), tzinfo=tz)
        end_dt = datetime.combine(trade_date, datetime.min.time().replace(hour=16, minute=0), tzinfo=tz)
        
        # Convert to UTC for API
        start_utc = start_dt.astimezone(ZoneInfo("UTC"))
        end_utc = end_dt.astimezone(ZoneInfo("UTC"))
        
        logger.info(f"Fetching trades for {symbol} on {trade_date} ({start_dt} to {end_dt} ET)")
        
        page_token = None
        total_records = 0
        
        while True:
            try:
                response = self._get_trades(symbol, start_utc, end_utc, page_token=page_token)
                trades = response.get("trades", [])
                
                if not trades:
                    break
                
                # Convert to DataFrame
                df = self._trades_to_dataframe(trades, symbol, trade_date, timezone)
                total_records += len(df)
                
                logger.debug(f"  Fetched {len(df):,} trades (total: {total_records:,})")
                yield df
                
                # Check for next page
                page_token = response.get("next_page_token")
                if not page_token:
                    break
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    logger.warning("Rate limit hit, waiting...")
                    import time
                    time.sleep(1)
                    continue
                elif e.response.status_code == 404:
                    logger.warning(f"No trades data available for {symbol} on {trade_date}")
                    break
                else:
                    logger.error(f"HTTP error {e.response.status_code}: {e.response.text[:200]}")
                    raise
        
        logger.info(f"Total trades extracted for {symbol}: {total_records:,}")
    
    def extract_quotes(
        self,
        symbol: str,
        trade_date: date,
        timezone: str = "America/New_York",
    ) -> Iterator[pl.DataFrame]:
        """
        Extract all quotes (NBBO) for a symbol on a given date.
        
        Args:
            symbol: Stock symbol
            trade_date: Trade date
            timezone: Timezone for market hours
            
        Yields:
            DataFrames with quote/NBBO data
        """
        tz = ZoneInfo(timezone)
        
        # Market hours: 9:30 AM - 4:00 PM ET
        start_dt = datetime.combine(trade_date, datetime.min.time().replace(hour=9, minute=30), tzinfo=tz)
        end_dt = datetime.combine(trade_date, datetime.min.time().replace(hour=16, minute=0), tzinfo=tz)
        
        # Convert to UTC for API
        start_utc = start_dt.astimezone(ZoneInfo("UTC"))
        end_utc = end_dt.astimezone(ZoneInfo("UTC"))
        
        logger.info(f"Fetching quotes for {symbol} on {trade_date} ({start_dt} to {end_dt} ET)")
        
        page_token = None
        total_records = 0
        
        while True:
            try:
                response = self._get_quotes(symbol, start_utc, end_utc, page_token=page_token)
                quotes = response.get("quotes", [])
                
                if not quotes:
                    break
                
                # Convert to DataFrame
                df = self._quotes_to_dataframe(quotes, symbol, trade_date, timezone)
                total_records += len(df)
                
                logger.debug(f"  Fetched {len(df):,} quotes (total: {total_records:,})")
                yield df
                
                # Check for next page
                page_token = response.get("next_page_token")
                if not page_token:
                    break
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    logger.warning("Rate limit hit, waiting...")
                    import time
                    time.sleep(1)
                    continue
                elif e.response.status_code == 404:
                    logger.warning(f"No trades data available for {symbol} on {trade_date}")
                    break
                else:
                    logger.error(f"HTTP error {e.response.status_code}: {e.response.text[:200]}")
                    raise
        
        logger.info(f"Total quotes extracted for {symbol}: {total_records:,}")
    
    def _trades_to_dataframe(
        self,
        trades: list[dict],
        symbol: str,
        trade_date: date,
        timezone: str,
    ) -> pl.DataFrame:
        """
        Convert Alpaca trades API response to DataFrame matching TAQ schema.
        
        Args:
            trades: List of trade records from API
            symbol: Stock symbol
            trade_date: Trade date
            timezone: Timezone string
            
        Returns:
            DataFrame with TAQ-compatible schema
        """
        if not trades:
            return pl.DataFrame()
        
        tz = ZoneInfo(timezone)
        utc_tz = ZoneInfo("UTC")
        
        records = []
        for trade in trades:
            # Parse timestamp (Alpaca returns ISO format in UTC)
            ts_utc = parser.parse(trade["t"]).replace(tzinfo=utc_tz)
            ts_local = ts_utc.astimezone(tz)
            
            # Extract date and time components
            date_val = ts_local.date()
            time_m = ts_local.time().replace(microsecond=0)
            time_m_nano = ts_local.microsecond * 1000  # Convert microseconds to nanoseconds
            
            records.append({
                # Original fields (mapped from Alpaca)
                "date": date_val,
                "time_m": time_m,
                "time_m_nano": time_m_nano,
                "part_time": None,  # Not available from Alpaca
                "part_time_nano": None,
                "trf_time": None,
                "trf_time_nano": None,
                "sym_root": symbol,
                "sym_suffix": None,
                "ex": trade.get("x", ""),  # Exchange code
                "price": float(trade.get("p", 0.0)),  # Price
                "size": int(trade.get("s", 0)),  # Size
                "tr_corr": None,  # Not available
                "tr_id": None,
                "tr_rf": None,
                "tr_scond": None,
                "tr_seqnum": None,
                "tr_source": "ALPACA",  # Mark as from Alpaca
                "tr_stop_ind": None,
                "tte_ind": None,
                # Derived fields
                "trade_date": date_val,
                "symbol": symbol,
                "ts_event": ts_utc,  # UTC timestamp
            })
        
        df = pl.DataFrame(records)
        return df
    
    def _quotes_to_dataframe(
        self,
        quotes: list[dict],
        symbol: str,
        trade_date: date,
        timezone: str,
    ) -> pl.DataFrame:
        """
        Convert Alpaca quotes API response to DataFrame matching TAQ NBBO schema.
        
        Args:
            quotes: List of quote records from API
            symbol: Stock symbol
            trade_date: Trade date
            timezone: Timezone string
            
        Returns:
            DataFrame with TAQ-compatible NBBO schema
        """
        if not quotes:
            return pl.DataFrame()
        
        tz = ZoneInfo(timezone)
        utc_tz = ZoneInfo("UTC")
        
        records = []
        for quote in quotes:
            # Parse timestamp (Alpaca returns ISO format in UTC)
            ts_utc = parser.parse(quote["t"]).replace(tzinfo=utc_tz)
            ts_local = ts_utc.astimezone(tz)
            
            # Extract date and time components
            date_val = ts_local.date()
            time_m = ts_local.time().replace(microsecond=0)
            time_m_nano = ts_local.microsecond * 1000
            
            records.append({
                # Original fields (mapped from Alpaca)
                "date": date_val,
                "time_m": time_m,
                "time_m_nano": time_m_nano,
                "sym_root": symbol,
                "sym_suffix": None,
                "best_bid": float(quote.get("bp", 0.0)),  # Best bid price
                "best_bidsiz": int(quote.get("bs", 0)),  # Best bid size
                "best_ask": float(quote.get("ap", 0.0)),  # Best ask price
                "best_asksiz": int(quote.get("as", 0)),  # Best ask size
                "best_bidex": quote.get("bx", ""),  # Bid exchange
                "best_askex": quote.get("ax", ""),  # Ask exchange
                "nbbo_qu_cond": None,  # Not available from Alpaca
                "qu_cond": None,
                "natbbo_ind": None,
                "qu_source": "ALPACA",
                "best_bidsizeshares": int(quote.get("bs", 0)),  # Alias for compatibility
                "best_asksizeshares": int(quote.get("as", 0)),
                # Derived fields
                "trade_date": date_val,
                "symbol": symbol,
                "ts_event": ts_utc,  # UTC timestamp
            })
        
        df = pl.DataFrame(records)
        return df

