"""CLI entry point for Stage A extraction."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from .config import load_config
from .stage_a import extract_stage_a, extract_stage_a_range
from .wrds_extractor import WRDSExtractor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def parse_symbols(symbols_str: str) -> list[str]:
    """Parse comma-separated symbols or read from file."""
    if Path(symbols_str).exists():
        # Read from file (one symbol per line)
        with open(symbols_str, "r") as f:
            return [line.strip().upper() for line in f if line.strip()]
    else:
        # Comma-separated list
        return [s.strip().upper() for s in symbols_str.split(",") if s.strip()]


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Stage A: Extract raw TAQ data from WRDS",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract for a single date (S&P 500 + ETFs, default: trades and nbbo)
  python -m src.stage_a.extract --date 2024-06-10 --config config.yaml
  
  # Extract for a date range (checks trading days and table availability)
  python -m src.stage_a.extract --start-date 2024-06-10 --end-date 2024-06-14 --config config.yaml
  
  # Extract only trades data
  python -m src.stage_a.extract --date 2024-06-10 --config config.yaml --type trades
  
  # Extract trades and quotes (multiple types)
  python -m src.stage_a.extract --date 2024-06-10 --config config.yaml --type trades quotes
  
  # Extract for specific symbols
  python -m src.stage_a.extract --date 2024-06-10 --symbols AAPL,MSFT,GOOGL --config config.yaml
  
  # Extract date range for specific symbols
  python -m src.stage_a.extract --start-date 2024-06-10 --end-date 2024-06-14 --symbols AAPL,MSFT --config config.yaml
  
  # Resume interrupted extraction (skip already ingested symbols)
  python -m src.stage_a.extract --date 2024-06-10 --config config.yaml --resume
  
  # Overwrite existing data
  python -m src.stage_a.extract --date 2024-06-10 --symbols AAPL --config config.yaml --overwrite
        """,
    )
    
    parser.add_argument(
        "--date",
        help="Single trade date in YYYY-MM-DD format (mutually exclusive with --start-date/--end-date)",
    )
    parser.add_argument(
        "--start-date",
        help="Start date for date range extraction (YYYY-MM-DD, inclusive). Requires --end-date",
    )
    parser.add_argument(
        "--end-date",
        help="End date for date range extraction (YYYY-MM-DD, inclusive). Requires --start-date",
    )
    parser.add_argument(
        "--symbols",
        required=False,
        default=None,
        help="Comma-separated symbols or path to file with one symbol per line. "
             "If not provided, will fetch S&P 500 constituents + top ETFs",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="Path to config YAML file",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Force overwrite: delete existing partitions and re-extract all data",
    )
    parser.add_argument(
        "--type",
        choices=["trades", "quotes", "nbbo"],
        nargs="+",
        default=None,
        help="Data type(s) to extract: trades, quotes, nbbo. "
             "Can specify multiple types (e.g., --type trades quotes). "
             "Default: extract trades and nbbo",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume extraction: skip symbols that are already ingested. "
             "Useful when extraction was interrupted and you want to continue from where it left off.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    
    args = parser.parse_args()
    
    # Validate date arguments: must have either --date OR (--start-date AND --end-date)
    has_single_date = args.date is not None
    has_start_date = args.start_date is not None
    has_end_date = args.end_date is not None
    
    if not has_single_date and not (has_start_date and has_end_date):
        parser.error("Must provide either --date OR both --start-date and --end-date")
    
    if has_single_date and (has_start_date or has_end_date):
        parser.error("--date cannot be used with --start-date or --end-date")
    
    if has_start_date and not has_end_date:
        parser.error("--start-date requires --end-date")
    
    if has_end_date and not has_start_date:
        parser.error("--end-date requires --start-date")
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load config first (needed for WRDS connection if fetching symbols)
    try:
        config = load_config(args.config)
        logger.info(f"Loaded config from {args.config}")
        logger.info(f"  Parquet root: {config.parquet_raw_root}")
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        sys.exit(1)
    
    # Parse date(s)
    if args.date:
        # Single date mode
        try:
            trade_date = datetime.fromisoformat(args.date).date()
            start_date = None
            end_date = None
        except ValueError:
            logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    else:
        # Date range mode
        try:
            start_date = datetime.fromisoformat(args.start_date).date()
            end_date = datetime.fromisoformat(args.end_date).date()
            trade_date = None
        except ValueError as e:
            logger.error(f"Invalid date format. Use YYYY-MM-DD: {e}")
            sys.exit(1)
    
    # Determine reference date for fetching symbols (use start_date if in range mode, else trade_date)
    reference_date = start_date if start_date else trade_date
    
    # Parse symbols or fetch default (S&P 500 + ETFs)
    if args.symbols:
        try:
            symbols = parse_symbols(args.symbols)
            if not symbols:
                logger.error("No symbols provided")
                sys.exit(1)
            logger.info(f"Processing {len(symbols)} symbols from --symbols argument")
        except Exception as e:
            logger.error(f"Error parsing symbols: {e}")
            sys.exit(1)
    else:
        # Fetch S&P 500 + ETFs from WRDS
        logger.info("No --symbols provided. Fetching S&P 500 constituents + top ETFs...")
        try:
            with WRDSExtractor(config) as extractor:
                symbols = extractor.get_default_symbols(reference_date)
            logger.info(f"Will process {len(symbols)} symbols (S&P 500 + ETFs)")
        except Exception as e:
            logger.error(f"Error fetching default symbols: {e}")
            logger.error("Please provide --symbols or ensure WRDS connection is configured")
            sys.exit(1)
    
    # Parse data types
    if args.type:
        data_types = args.type
        logger.info(f"Extracting data types: {', '.join(data_types)}")
    else:
        data_types = ["trades", "nbbo"]  # Default: trades and nbbo
        logger.info("Extracting default data types: trades, nbbo")
    
    # Execute Stage A (single date or date range)
    try:
        if trade_date:
            # Single date mode
            results = extract_stage_a(
                config=config,
                trade_date=trade_date,
                symbols=symbols,
                overwrite=args.overwrite,
                data_types=data_types,
                resume=args.resume,
            )
            
            logger.info("\n" + "=" * 80)
            logger.info("Extraction Summary")
            logger.info("=" * 80)
            for dt in data_types:
                logger.info(f"{dt.capitalize()}: {results[dt]:,} rows")
        else:
            # Date range mode
            all_results = extract_stage_a_range(
                config=config,
                start_date=start_date,
                end_date=end_date,
                symbols=symbols,
                overwrite=args.overwrite,
                data_types=data_types,
                resume=args.resume,
            )
            
            # Summary already printed by extract_stage_a_range
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

