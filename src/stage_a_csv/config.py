"""Configuration management for Stage A CSV extraction."""

from __future__ import annotations

import dataclasses
from pathlib import Path

import yaml


@dataclasses.dataclass
class StageACsvConfig:
    """Configuration for Stage A CSV extraction."""
    
    # Paths
    parquet_raw_root: Path
    csv_root: Path  # Root directory containing CSV files
    
    # Extraction settings
    chunk_size: int = 1_000_000  # Rows per chunk when reading CSV files
    
    # Parquet settings
    compression: str = "snappy"
    partition_by_symbol: bool = True
    
    # Timezone
    timezone: str = "America/New_York"
    
    # CSV file naming pattern
    # Files should be named: {prefix}_{date}.csv where date is YYYYMMDD
    csv_prefix_trades: str = "taq_trade"
    csv_prefix_quotes: str = "taq_quote"
    csv_prefix_nbbo: str = "taq_nbbo"


def load_config(config_path: str) -> StageACsvConfig:
    """Load configuration from YAML file."""
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    
    stage_a_csv = raw.get("stage_a_csv", {})
    
    return StageACsvConfig(
        parquet_raw_root=Path(stage_a_csv.get("parquet_raw_root", "/home/mingyuan/data/csv/parquet_raw")),
        csv_root=Path(stage_a_csv.get("csv_root", "/home/mingyuan/data/csv")),
        chunk_size=stage_a_csv.get("chunk_size", 1_000_000),
        compression=stage_a_csv.get("compression", "snappy"),
        partition_by_symbol=stage_a_csv.get("partition_by_symbol", True),
        timezone=stage_a_csv.get("timezone", "America/New_York"),
        csv_prefix_trades=stage_a_csv.get("csv_prefix_trades", "taq_trade"),
        csv_prefix_quotes=stage_a_csv.get("csv_prefix_quotes", "taq_quote"),
        csv_prefix_nbbo=stage_a_csv.get("csv_prefix_nbbo", "taq_nbbo"),
    )

