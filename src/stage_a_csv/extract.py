"""CLI entry point for Stage A CSV extraction."""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from .config import load_config
from .stage_a_csv import extract_stage_a_csv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)

logger = logging.getLogger(__name__)


def parse_symbols(symbols_str: str) -> list[str]:
    """
    Parse symbols from comma-separated string or file.
    
    Args:
        symbols_str: Comma-separated symbols or path to file
        
    Returns:
        List of symbols
    """
    if Path(symbols_str).exists():
        # Read from file
        with open(symbols_str, "r") as f:
            return [s.strip().upper() for s in f if s.strip()]
    else:
        # Parse comma-separated string
        return [s.strip().upper() for s in symbols_str.split(",") if s.strip()]


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Extract raw data from local CSV files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    # Date arguments (mutually exclusive: single date OR date range)
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument(
        "--date",
        type=str,
        help="Single trade date (YYYY-MM-DD)",
    )
    date_group.add_argument(
        "--start-date",
        type=str,
        help="Start date for range extraction (YYYY-MM-DD)",
    )
    date_group.add_argument(
        "--end-date",
        type=str,
        help="End date for range extraction (YYYY-MM-DD)",
    )
    
    # Symbols (optional - if not provided, extracts all symbols from CSV)
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated symbols or path to file with one symbol per line (optional - extracts all if not provided)",
        default=None,
    )
    
    # Config
    parser.add_argument(
        "--config",
        type=str,
        required=True,
        help="Path to config YAML file",
    )
    
    # Options
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing data",
    )
    
    parser.add_argument(
        "--type",
        nargs="+",
        choices=["trades", "quotes", "nbbo"],
        default=["trades", "nbbo"],
        help="Data types to extract (default: trades nbbo)",
    )
    
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume extraction (skip already ingested symbols)",
    )
    
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load config
    config = load_config(args.config)
    
    # Parse symbols (optional)
    symbols = parse_symbols(args.symbols) if args.symbols else None
    
    # Parse dates
    if args.date:
        # Single date
        trade_date = date.fromisoformat(args.date)
        results = extract_stage_a_csv(
            config=config,
            trade_date=trade_date,
            symbols=symbols,
            overwrite=args.overwrite,
            data_types=args.type,
            resume=args.resume,
        )
        logger.info("\nExtraction Results:")
        for data_type, count in results.items():
            logger.info(f"  {data_type}: {count:,} rows")
    else:
        # Date range
        if not args.start_date or not args.end_date:
            parser.error("--start-date and --end-date must be provided together")
        
        start_date = date.fromisoformat(args.start_date)
        end_date = date.fromisoformat(args.end_date)
        
        if start_date > end_date:
            parser.error("--start-date must be <= --end-date")
        
        # Import date utilities
        from ..stage_a.date_utils import filter_trading_days, get_date_range
        
        dates = filter_trading_days(get_date_range(start_date, end_date))
        logger.info(f"Processing {len(dates)} trading days from {start_date} to {end_date}")
        
        all_results = {}
        for trade_date in dates:
            logger.info(f"\n{'=' * 80}")
            logger.info(f"Processing {trade_date}")
            logger.info(f"{'=' * 80}")
            
            try:
                results = extract_stage_a_csv(
                    config=config,
                    trade_date=trade_date,
                    symbols=symbols,
                    overwrite=args.overwrite,
                    data_types=args.type,
                    resume=args.resume,
                )
                
                # Accumulate results
                for data_type, count in results.items():
                    all_results[data_type] = all_results.get(data_type, 0) + count
                    
            except Exception as e:
                logger.error(f"Error processing {trade_date}: {e}", exc_info=True)
                continue
        
        logger.info("\n" + "=" * 80)
        logger.info("Date Range Extraction Complete")
        logger.info("=" * 80)
        for data_type, count in all_results.items():
            logger.info(f"  {data_type}: {count:,} total rows")


if __name__ == "__main__":
    main()

