"""Configuration management for Streamlit app."""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Optional

import yaml


@dataclasses.dataclass
class StreamlitAppConfig:
    """Configuration for Streamlit app."""
    
    # Root data directory (e.g., /home/mingyuan/data)
    data_root: Path
    
    # Data source configurations
    data_sources: dict[str, Path]  # e.g., {"taq": Path(...), "alpaca": Path(...)}
    
    # Timezone
    timezone: str = "America/New_York"


def load_config(config_path: str = "config.yaml") -> StreamlitAppConfig:
    """Load configuration from YAML file."""
    config_file = Path(config_path)
    if not config_file.exists():
        # Try relative to project root
        config_file = Path(__file__).parent.parent.parent / config_path
        if not config_file.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_file, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    
    # Extract data root from first data source (assuming they're all under same root)
    # Default to /home/mingyuan/data
    data_root = Path("/home/mingyuan/data")
    
    # Build data sources dictionary from config
    data_sources = {}
    
    # Check stage_a (TAQ)
    if "stage_a" in raw:
        taq_root_str = raw["stage_a"].get("parquet_raw_root", "")
        if taq_root_str:
            taq_root = Path(taq_root_str)
            # Extract data root (e.g., /home/mingyuan/data/taq/parquet_raw -> /home/mingyuan/data)
            data_root = taq_root.parent.parent
            data_sources["taq"] = taq_root
    
    # Check stage_a_alpaca (Alpaca SIP)
    if "stage_a_alpaca" in raw:
        alpaca_root_str = raw["stage_a_alpaca"].get("parquet_raw_root", "")
        if alpaca_root_str:
            alpaca_root = Path(alpaca_root_str)
            if not data_sources:  # If no previous source found, use this for data_root
                data_root = alpaca_root.parent.parent
            data_sources["alpaca"] = alpaca_root
    
    # Check stage_a_alpaca_iex (Alpaca IEX)
    if "stage_a_alpaca_iex" in raw:
        alpaca_iex_root_str = raw["stage_a_alpaca_iex"].get("parquet_raw_root", "")
        if alpaca_iex_root_str:
            alpaca_iex_root = Path(alpaca_iex_root_str)
            if not data_sources:  # If no previous source found, use this for data_root
                data_root = alpaca_iex_root.parent.parent
            data_sources["alpaca_iex"] = alpaca_iex_root
    
    # Check stage_a_csv
    if "stage_a_csv" in raw:
        csv_root_str = raw["stage_a_csv"].get("parquet_raw_root", "")
        if csv_root_str:
            csv_root = Path(csv_root_str)
            if not data_sources:  # If no previous source found, use this for data_root
                data_root = csv_root.parent.parent
            data_sources["csv"] = csv_root
    
    # Get timezone (default from stage_a if available)
    timezone = "America/New_York"
    if "stage_a" in raw:
        timezone = raw["stage_a"].get("timezone", timezone)
    
    return StreamlitAppConfig(
        data_root=data_root,
        data_sources=data_sources,
        timezone=timezone,
    )

