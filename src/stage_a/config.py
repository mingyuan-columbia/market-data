"""Configuration management for Stage A extraction."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Optional

import yaml


@dataclasses.dataclass
class StageAConfig:
    """Configuration for Stage A extraction."""
    
    # Paths
    parquet_raw_root: Path
    
    # WRDS connection
    wrds_username: Optional[str] = None
    
    # Extraction settings
    chunk_size: int = 50  # Number of symbols to process per chunk
    streaming_chunk_rows: int = 1_000_000  # Rows per streaming chunk
    
    # Parquet settings
    compression: str = "snappy"
    partition_by_symbol: bool = True
    
    # Timezone
    timezone: str = "America/New_York"


def load_config(config_path: str) -> StageAConfig:
    """Load configuration from YAML file."""
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    
    stage_a = raw.get("stage_a", {})
    
    return StageAConfig(
        parquet_raw_root=Path(stage_a.get("parquet_raw_root", "/Volumes/Data/parquet_raw")),
        wrds_username=stage_a.get("wrds_username"),
        chunk_size=stage_a.get("chunk_size", 50),
        streaming_chunk_rows=stage_a.get("streaming_chunk_rows", 1_000_000),
        compression=stage_a.get("compression", "snappy"),
        partition_by_symbol=stage_a.get("partition_by_symbol", True),
        timezone=stage_a.get("timezone", "America/New_York"),
    )

