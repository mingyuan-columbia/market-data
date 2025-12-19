"""CLI entry point for Stage A extraction."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from .config import load_config
from .stage_a import extract_stage_a
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
  # Extract for S&P 500 + ETFs (default)
  python -m src.stage_a.extract --date 2024-06-10 --config config.yaml
  
  # Extract for specific symbols
  python -m src.stage_a.extract --date 2024-06-10 --symbols AAPL,MSFT,GOOGL --config config.yaml
  
  # Extract from symbol file
  python -m src.stage_a.extract --date 2024-06-10 --symbols symbols.txt --config config.yaml
  
  # Overwrite existing data
  python -m src.stage_a.extract --date 2024-06-10 --symbols AAPL --config config.yaml --overwrite
        """,
    )
    
    parser.add_argument(
        "--date",
        required=True,
        help="Trade date in YYYY-MM-DD format",
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
        help="Overwrite existing data (default: skip if already ingested)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Load config first (needed for WRDS connection if fetching symbols)
    try:
        config = load_config(args.config)
        logger.info(f"Loaded config from {args.config}")
        logger.info(f"  Parquet root: {config.parquet_raw_root}")
        logger.info(f"  CSV root: {config.csv_root}")
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        sys.exit(1)
    
    # Parse date
    try:
        trade_date = datetime.fromisoformat(args.date).date()
    except ValueError:
        logger.error(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
        sys.exit(1)
    
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
                symbols = extractor.get_default_symbols(trade_date)
            logger.info(f"Will process {len(symbols)} symbols (S&P 500 + ETFs)")
        except Exception as e:
            logger.error(f"Error fetching default symbols: {e}")
            logger.error("Please provide --symbols or ensure WRDS connection is configured")
            sys.exit(1)
    
    # Execute Stage A
    try:
        results = extract_stage_a(
            config=config,
            trade_date=trade_date,
            symbols=symbols,
            overwrite=args.overwrite,
        )
        
        logger.info("\n" + "=" * 80)
        logger.info("Extraction Summary")
        logger.info("=" * 80)
        logger.info(f"Trades: {results['trades']:,} rows")
        logger.info(f"Quotes: {results['quotes']:,} rows")
        logger.info(f"NBBO: {results['nbbo']:,} rows")
        
    except Exception as e:
        logger.error(f"Extraction failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

