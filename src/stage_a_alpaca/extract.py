"""CLI entry point for Stage A Alpaca extraction."""

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from .config import load_config
from .stage_a_alpaca import extract_stage_a_alpaca

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
        description="Extract historical SIP data from Alpaca API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    # Date arguments
    # --date is mutually exclusive with date range (--start-date + --end-date)
    parser.add_argument(
        "--date",
        type=str,
        help="Single trade date (YYYY-MM-DD). Mutually exclusive with --start-date/--end-date",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date for range extraction (YYYY-MM-DD). Must be used with --end-date",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date for range extraction (YYYY-MM-DD). Must be used with --start-date",
    )
    
    # Symbols (optional - if not provided, will discover from TAQ data)
    parser.add_argument(
        "--symbols",
        type=str,
        help="Comma-separated symbols or path to file with one symbol per line. If not provided, symbols will be discovered from TAQ data directory.",
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
        choices=["trades", "nbbo"],
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
    
    # Validate date arguments
    if args.date and (args.start_date or args.end_date):
        parser.error("--date cannot be used with --start-date or --end-date")
    
    if not args.date and not (args.start_date and args.end_date):
        if args.start_date or args.end_date:
            parser.error("--start-date and --end-date must be provided together")
        else:
            parser.error("Either --date or both --start-date and --end-date must be provided")
    
    # Load config
    config = load_config(args.config)
    
    # Parse or discover symbols (needs date for discovery)
    # For date ranges, we'll discover symbols per date in the loop
    if args.symbols:
        symbols = parse_symbols(args.symbols)
        logger.info(f"Using provided symbols: {len(symbols)}")
    elif args.date:
        # Single date - discover symbols now
        if config.taq_parquet_root is None:
            logger.error("No symbols provided and taq_parquet_root not configured.")
            logger.error("Either provide --symbols or set taq_parquet_root in config.yaml")
            sys.exit(1)
        
        discovery_date = date.fromisoformat(args.date)
        logger.info(f"Discovering symbols from TAQ data for {discovery_date}...")
        from .symbol_discovery import discover_symbols_from_taq
        
        # Discover from trades directory (most common)
        symbols = discover_symbols_from_taq(
            config.taq_parquet_root,
            discovery_date,
            data_type="trades",
        )
        
        if not symbols:
            logger.warning(f"No symbols found in TAQ directory: {config.taq_parquet_root}/trades/trade_date={discovery_date}")
            logger.warning("Skipping this date. Provide symbols explicitly with --symbols if you want to extract for this date.")
            return  # Skip this date instead of exiting
        
        logger.info(f"Discovered {len(symbols)} symbols from TAQ data: {symbols[:10]}{'...' if len(symbols) > 10 else ''}")
    else:
        # Date range - symbols will be discovered per date in the loop
        if config.taq_parquet_root is None:
            logger.error("No symbols provided and taq_parquet_root not configured.")
            logger.error("Either provide --symbols or set taq_parquet_root in config.yaml")
            sys.exit(1)
        symbols = None  # Will be discovered per date
    
    # Parse dates and execute extraction
    if args.date:
        # Single date
        trade_date = date.fromisoformat(args.date)
        
        # Check if we have symbols (might have been skipped during discovery)
        if not symbols:
            logger.warning(f"No symbols available for {trade_date}. Skipping extraction.")
            return
        
        results = extract_stage_a_alpaca(
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
        skipped_dates = []
        for trade_date in dates:
            logger.info(f"\n{'=' * 80}")
            logger.info(f"Processing {trade_date}")
            logger.info(f"{'=' * 80}")
            
            # For date ranges, discover symbols for each date if not provided
            current_symbols = symbols
            if not args.symbols:
                # Re-discover symbols for this date
                from .symbol_discovery import discover_symbols_from_taq
                current_symbols = discover_symbols_from_taq(
                    config.taq_parquet_root,
                    trade_date,
                    data_type="trades",
                )
                
                if not current_symbols:
                    logger.warning(f"No symbols found for {trade_date}. Skipping.")
                    skipped_dates.append(trade_date)
                    continue
            
            try:
                results = extract_stage_a_alpaca(
                    config=config,
                    trade_date=trade_date,
                    symbols=current_symbols,
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
        
        if skipped_dates:
            logger.info(f"\nSkipped {len(skipped_dates)} dates with no symbols: {skipped_dates[:10]}{'...' if len(skipped_dates) > 10 else ''}")
        
        logger.info("\n" + "=" * 80)
        logger.info("Date Range Extraction Complete")
        logger.info("=" * 80)
        for data_type, count in all_results.items():
            logger.info(f"  {data_type}: {count:,} total rows")


if __name__ == "__main__":
    main()

