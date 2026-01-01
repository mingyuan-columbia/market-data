"""Configuration management for Stage A Alpaca extraction."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Optional

import yaml


@dataclasses.dataclass
class StageAAlpacaConfig:
    """Configuration for Stage A Alpaca extraction."""
    
    # Paths
    parquet_raw_root: Path
    taq_parquet_root: Optional[Path] = None  # TAQ data root for symbol discovery
    
    # Alpaca API credentials
    alpaca_api_key: Optional[str] = None
    alpaca_secret_key: Optional[str] = None
    alpaca_base_url: str = "https://data.alpaca.markets"  # Data API for historical data
    
    # Extraction settings
    chunk_size: int = 50  # Number of symbols to process per chunk
    streaming_chunk_rows: int = 1_000_000  # Rows per streaming chunk
    
    # Parquet settings
    compression: str = "snappy"
    partition_by_symbol: bool = True
    
    # Timezone
    timezone: str = "America/New_York"
    
    # Alpaca API settings
    feed: str = "sip"  # Use SIP feed for consolidated data
    page_limit: int = 10000  # Max records per API call


def load_config(config_path: str) -> StageAAlpacaConfig:
    """
    Load configuration from YAML file.
    
    Loads from config.yaml first, then overrides with values from config.secrets.yaml if it exists.
    Credentials are checked in this order (highest priority first):
    1. Environment variables (ALPACA_API_KEY, ALPACA_SECRET_KEY)
    2. config.secrets.yaml file
    3. config.yaml file
    """
    import os
    from pathlib import Path
    
    # Load main config
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    
    stage_a_alpaca = raw.get("stage_a_alpaca", {})
    
    # Load secrets file if it exists (same directory as config file)
    config_dir = Path(config_path).parent
    secrets_path = config_dir / "config.secrets.yaml"
    
    if secrets_path.exists():
        with open(secrets_path, "r", encoding="utf-8") as f:
            secrets_raw = yaml.safe_load(f)
        secrets_alpaca = secrets_raw.get("stage_a_alpaca", {})
        # Override with secrets file values
        if secrets_alpaca.get("alpaca_api_key"):
            stage_a_alpaca["alpaca_api_key"] = secrets_alpaca["alpaca_api_key"]
        if secrets_alpaca.get("alpaca_secret_key"):
            stage_a_alpaca["alpaca_secret_key"] = secrets_alpaca["alpaca_secret_key"]
    
    # Check environment variables for credentials (highest priority)
    api_key = os.getenv("ALPACA_API_KEY") or stage_a_alpaca.get("alpaca_api_key")
    secret_key = os.getenv("ALPACA_SECRET_KEY") or stage_a_alpaca.get("alpaca_secret_key")
    
    taq_parquet_root_value = stage_a_alpaca.get("taq_parquet_root")
    taq_parquet_root = Path(taq_parquet_root_value) if taq_parquet_root_value is not None else None
    
    return StageAAlpacaConfig(
        parquet_raw_root=Path(stage_a_alpaca.get("parquet_raw_root", "/home/mingyuan/data/alpaca/parquet_raw")),
        taq_parquet_root=taq_parquet_root,
        alpaca_api_key=api_key,
        alpaca_secret_key=secret_key,
        alpaca_base_url=stage_a_alpaca.get("alpaca_base_url", "https://data.alpaca.markets"),
        chunk_size=stage_a_alpaca.get("chunk_size", 50),
        streaming_chunk_rows=stage_a_alpaca.get("streaming_chunk_rows", 1_000_000),
        compression=stage_a_alpaca.get("compression", "snappy"),
        partition_by_symbol=stage_a_alpaca.get("partition_by_symbol", True),
        timezone=stage_a_alpaca.get("timezone", "America/New_York"),
        feed=stage_a_alpaca.get("feed", "sip"),
        page_limit=stage_a_alpaca.get("page_limit", 10000),
    )

