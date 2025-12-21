"""Utilities for date handling and trading day checks."""

from __future__ import annotations

import logging
from datetime import date, timedelta

logger = logging.getLogger(__name__)


def is_weekday(check_date: date) -> bool:
    """
    Check if a date is a weekday (Monday-Friday).
    
    Args:
        check_date: Date to check
        
    Returns:
        True if weekday, False if weekend
    """
    return check_date.weekday() < 5  # 0-4 are Monday-Friday


def get_date_range(start_date: date, end_date: date) -> list[date]:
    """
    Get all dates in the range [start_date, end_date] inclusive.
    
    Both start_date and end_date are included in the result.
    For example, get_date_range(2024-06-10, 2024-06-12) returns:
    [2024-06-10, 2024-06-11, 2024-06-12]
    
    Args:
        start_date: Start date (inclusive - will be included in result)
        end_date: End date (inclusive - will be included in result)
        
    Returns:
        List of dates in the range, including both start_date and end_date
        
    Raises:
        ValueError: If start_date > end_date
    """
    if start_date > end_date:
        raise ValueError(f"Start date {start_date} must be <= end date {end_date}")
    
    dates = []
    current = start_date
    # Loop includes end_date because we use <= (inclusive)
    while current <= end_date:
        dates.append(current)
        current += timedelta(days=1)
    
    return dates


def filter_trading_days(dates: list[date]) -> list[date]:
    """
    Filter dates to only include weekdays (trading days).
    
    Note: This does not check for market holidays. For more accurate
    trading day detection, you may want to use a calendar library
    like pandas_market_calendars or check against actual data availability.
    
    Args:
        dates: List of dates to filter
        
    Returns:
        List of dates that are weekdays
    """
    return [d for d in dates if is_weekday(d)]

