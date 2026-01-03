"""Main Streamlit app for market data visualization."""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

import streamlit as st
import polars as pl
import yaml
from pathlib import Path

import sys
from pathlib import Path

# Add src directory to path for imports
src_path = Path(__file__).parent.parent
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from streamlit_app.config import load_config, StreamlitAppConfig
from streamlit_app.data_checker import (
    check_data_available,
    find_available_dates,
    find_available_symbols,
    suggest_alternatives,
    check_symbol_availability_across_sources,
    find_common_sources_for_symbols,
)
from streamlit_app.data_loader import load_trades, load_nbbo
from streamlit_app.visualizations import (
    plot_price_panel,
    plot_spread_bps_timeline,
    plot_churn_bar_chart,
    plot_spread_histogram,
    get_highest_churn_minutes,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Suppress harmless WebSocket cleanup errors
logging.getLogger("asyncio").setLevel(logging.ERROR)
import warnings
warnings.filterwarnings("ignore", message=".*WebSocketClosedError.*")
warnings.filterwarnings("ignore", message=".*Stream is closed.*")

# Page config
st.set_page_config(
    page_title="Market Data Analysis",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Password protection
def check_password():
    """Returns `True` if the user had the correct password."""
    import os
    
    # Check if Cloudflare Tunnel is active via environment variable
    # If not set, allow password-free access (local network)
    cloudflare_tunnel_active = os.getenv("CLOUDFLARE_TUNNEL", "").lower() in ("true", "1", "yes")
    
    # If Cloudflare Tunnel is not active, allow access without password
    if not cloudflare_tunnel_active:
        if "password_correct" not in st.session_state:
            st.session_state["password_correct"] = True
        return True
    
    # Cloudflare Tunnel is active - require password
    # Get password from secrets or use default if not set
    try:
        expected_password = st.secrets.get("password", "")
    except Exception:
        # If secrets file doesn't exist, use empty string (no password protection)
        expected_password = ""
    
    # If no password is configured but Cloudflare Tunnel is active, warn but allow access
    if expected_password == "":
        st.warning("⚠️ Cloudflare Tunnel is active but no password is configured. Consider setting a password in `.streamlit/secrets.toml` for security.")
        if "password_correct" not in st.session_state:
            st.session_state["password_correct"] = True
        return True
    
    # If already authenticated, return True immediately (don't show password input)
    if st.session_state.get("password_correct", False):
        return True
    
    # Show password input only if not authenticated
    def password_entered():
        """Checks whether a password entered by the user is correct."""
        # Only process if this callback was actually triggered by password input
        password_key = "auth_password"
        if password_key not in st.session_state:
            return
        
        # Get expected password
        try:
            expected = st.secrets.get("password", "")
        except Exception:
            expected = ""
        
        if expected == "":
            st.session_state["password_correct"] = True
            if password_key in st.session_state:
                del st.session_state[password_key]
            return
        
        entered_password = st.session_state[password_key]
        
        if entered_password == expected:
            st.session_state["password_correct"] = True
            del st.session_state[password_key]
        else:
            st.session_state["password_correct"] = False
    
    # Show password input only if not authenticated
    # Use a unique key to prevent interference with other widgets
    password_key = "auth_password"
    st.text_input(
        "Password", 
        type="password", 
        on_change=password_entered, 
        key=password_key,
        help="Enter password to access the application"
    )
    
    # Show error if password was incorrect (but only if password was actually entered)
    if (
        st.session_state.get("password_correct") == False 
        and password_key in st.session_state 
        and len(st.session_state[password_key]) > 0
    ):
        st.error("Password incorrect")
    
    return False  # Not authenticated yet


@st.cache_data
def load_app_config(config_path: str = "config.yaml") -> StreamlitAppConfig:
    """Load and cache app configuration."""
    return load_config(config_path)




def main():
    """Main app function."""
    # Password check
    if not check_password():
        st.stop()
    
    # Load configuration
    try:
        config = load_app_config()
    except Exception as e:
        st.error(f"Error loading configuration: {e}")
        st.stop()
    
    # Sidebar - Filters
    st.sidebar.header("📊 Data Selection")
    
    # Get data root from config
    data_root = config.data_root
    available_sources = list(config.data_sources.keys())
    if not available_sources:
        st.error("No data sources configured. Please check config.yaml")
        st.stop()
    
    # ===== STEP 1: DATE SELECTION FIRST =====
    st.sidebar.header("📅 Date Selection")
    
    # Find available dates across all sources
    all_available_dates = set()
    for source in available_sources:
        trades_dates = find_available_dates(
            data_root, source, data_type="trades", max_days=365
        )
        nbbo_dates = find_available_dates(
            data_root, source, data_type="nbbo", max_days=365
        )
        all_available_dates.update(trades_dates + nbbo_dates)
    
    available_dates = sorted(list(all_available_dates), reverse=True)  # Most recent first
    
    # Set default date to 2024-01-02
    default_date = date(2024, 1, 2)
    if available_dates:
        if default_date in available_dates:
            default_value = default_date
        else:
            default_value = available_dates[0]  # Most recent
        min_date = min(available_dates)
        max_date = max(available_dates)
    else:
        default_value = default_date
        min_date = date(2020, 1, 1)
        max_date = date.today()
    
    # Date selection
    if available_dates:
        selected_date = st.sidebar.selectbox(
            "Trade Date",
            options=available_dates,
            index=available_dates.index(default_value) if default_value in available_dates else 0,
            format_func=lambda d: d.strftime("%Y-%m-%d"),
        )
    else:
        selected_date = st.sidebar.date_input(
            "Trade Date",
            value=default_value,
            min_value=min_date,
            max_value=max_date,
        )
    
    # ===== STEP 2: SYMBOL MODE SELECTION =====
    st.sidebar.header("📊 Symbol Selection")
    
    # Mode selection: Single or Multiple
    symbol_mode = st.sidebar.radio(
        "Mode",
        options=["Single Symbol", "Multiple Symbols"],
        index=0,
    )
    
    # Find available symbols across all sources for this date
    all_available_symbols = set()
    for source in available_sources:
        symbols = find_available_symbols(data_root, source, selected_date, data_type="trades")
        if not symbols:
            symbols = find_available_symbols(data_root, source, selected_date, data_type="nbbo")
        all_available_symbols.update(symbols)
    
    all_available_symbols = sorted(list(all_available_symbols))
    
    if not all_available_symbols:
        st.sidebar.warning("No symbols found for this date")
        st.info("Please select a different date or check your data.")
        st.stop()
    
    # Symbol selection based on mode
    if symbol_mode == "Single Symbol":
        # Single symbol selection
        default_symbol = "AAPL" if "AAPL" in all_available_symbols else all_available_symbols[0]
        default_idx = all_available_symbols.index(default_symbol) if default_symbol in all_available_symbols else 0
        
        selected_symbols = [st.sidebar.selectbox(
            "Symbol",
            options=all_available_symbols,
            index=default_idx,
        )]
    else:
        # Multiple symbols selection
        selected_symbols = st.sidebar.multiselect(
            "Symbols",
            options=all_available_symbols,
            default=["AAPL"] if "AAPL" in all_available_symbols else all_available_symbols[:1],
        )
        
        if not selected_symbols:
            st.sidebar.warning("Please select at least one symbol")
            st.info("Please select one or more symbols to visualize.")
            st.stop()
    
    # ===== STEP 3: DETERMINE DATA SOURCES =====
    # Check availability for selected symbols across sources
    symbol_availability = check_symbol_availability_across_sources(
        data_root, config.data_sources, selected_date, selected_symbols, data_type="trades"
    )
    
    # Also check NBBO availability
    nbbo_symbol_availability = check_symbol_availability_across_sources(
        data_root, config.data_sources, selected_date, selected_symbols, data_type="nbbo"
    )
    
    # Determine which sources have all symbols
    if symbol_mode == "Single Symbol":
        # For single symbol: check if available in both sources
        symbol = selected_symbols[0]
        available_in_sources = [
            source for source in available_sources
            if symbol_availability.get(symbol, {}).get(source, False) or
               nbbo_symbol_availability.get(symbol, {}).get(source, False)
        ]
        
        if len(available_in_sources) == 0:
            st.error(f"No data available for {symbol} on {selected_date}")
            st.stop()
        elif len(available_in_sources) == 1:
            # Only one source - use it
            selected_sources = available_in_sources
            show_dual_source = False
        else:
            # Multiple sources - show both
            selected_sources = available_in_sources
            show_dual_source = True
    else:
        # For multiple symbols: allow user to choose data source first
        # Find sources that have all symbols
        common_sources = find_common_sources_for_symbols(
            data_root, config.data_sources, selected_date, selected_symbols, data_type="trades"
        )
        
        sources_with_all = common_sources["sources_with_all"]
        
        if len(sources_with_all) == 0:
            # Check if we can use NBBO availability
            common_sources_nbbo = find_common_sources_for_symbols(
                data_root, config.data_sources, selected_date, selected_symbols, data_type="nbbo"
            )
            sources_with_all = common_sources_nbbo["sources_with_all"]
        
        if len(sources_with_all) == 0:
            # No source has all symbols - filter missing ones and let user choose
            st.warning("Not all symbols available in any single source. Filtering missing symbols...")
            # Find sources with at least some symbols
            sources_with_some = []
            for source in available_sources:
                count = sum(
                    1 for sym in selected_symbols
                    if symbol_availability.get(sym, {}).get(source, False) or
                       nbbo_symbol_availability.get(sym, {}).get(source, False)
                )
                if count > 0:
                    sources_with_some.append((source, count))
            
            if sources_with_some:
                # Let user choose from available sources
                source_options = [f"{src} ({count} symbols)" for src, count in sorted(sources_with_some, key=lambda x: x[1], reverse=True)]
                selected_source_str = st.sidebar.selectbox(
                    "Data Source",
                    options=source_options,
                    index=0,
                )
                selected_source = selected_source_str.split(" ")[0]  # Extract source name
                
                # Filter symbols to only those available in selected source
                selected_symbols = [
                    sym for sym in selected_symbols
                    if symbol_availability.get(sym, {}).get(selected_source, False) or
                       nbbo_symbol_availability.get(sym, {}).get(selected_source, False)
                ]
                selected_sources = [selected_source]
                st.info(f"Using {selected_source} with symbols: {', '.join(selected_symbols)}")
            else:
                st.error("No data available for selected symbols")
                st.stop()
        elif len(sources_with_all) == 1:
            selected_sources = sources_with_all
            show_dual_source = False
        else:
            # Multiple sources have all symbols - let user choose
            selected_source = st.sidebar.selectbox(
                "Data Source",
                options=sources_with_all,
                index=0,
            )
            selected_sources = [selected_source]
            show_dual_source = False
    
    # Store in session state for visualization
    st.session_state.selected_symbols = selected_symbols
    st.session_state.selected_sources = selected_sources
    st.session_state.show_dual_source = show_dual_source if symbol_mode == "Single Symbol" else False
    st.session_state.symbol_mode = symbol_mode
    
    # ===== STEP 4: LOAD DATA FROM SELECTED SOURCES =====
    # Load data for each source and symbol combination
    data_by_source = {}  # {source: {"trades": df, "nbbo": df}}
    
    with st.spinner("Loading data..."):
        for source in selected_sources:
            data_by_source[source] = {"trades": None, "nbbo": None}
            
            # Load trades
            trades_available = any(
                symbol_availability.get(sym, {}).get(source, False)
                for sym in selected_symbols
            )
            if trades_available:
                # Pass single symbol as string, multiple as list
                symbol_param = selected_symbols[0] if len(selected_symbols) == 1 else selected_symbols
                trades = load_trades(
                    data_root,
                    source,
                    selected_date,
                    symbol=symbol_param,
                    timezone=config.timezone,
                )
                if trades is not None and len(trades) > 0:
                    data_by_source[source]["trades"] = trades
            
            # Load NBBO
            nbbo_available = any(
                nbbo_symbol_availability.get(sym, {}).get(source, False)
                for sym in selected_symbols
            )
            if nbbo_available:
                # Pass single symbol as string, multiple as list
                symbol_param = selected_symbols[0] if len(selected_symbols) == 1 else selected_symbols
                nbbo = load_nbbo(
                    data_root,
                    source,
                    selected_date,
                    symbol=symbol_param,
                    timezone=config.timezone,
                )
                if nbbo is not None and len(nbbo) > 0:
                    data_by_source[source]["nbbo"] = nbbo
    
    # For backward compatibility, use first source's data as primary
    if selected_sources:
        primary_source = selected_sources[0]
        trades = data_by_source[primary_source]["trades"]
        nbbo = data_by_source[primary_source]["nbbo"]
    else:
        trades = None
        nbbo = None
    
    # Store data_by_source in session state for visualization
    st.session_state.data_by_source = data_by_source
    
    # Time range slider (only show if we have data)
    st.sidebar.header("⏰ Time Range")
    
    if trades is not None and len(trades) > 0:
        min_time = trades["ts_event"].min()
        max_time = trades["ts_event"].max()
    elif nbbo is not None and len(nbbo) > 0:
        min_time = nbbo["ts_event"].min()
        max_time = nbbo["ts_event"].max()
    else:
        min_time = None
        max_time = None
    
    if min_time is not None and max_time is not None:
        # Convert Polars datetime to Python datetime for slider
        try:
            # Polars datetime scalars can be converted using item() if Series, or directly if scalar
            if isinstance(min_time, pl.Series):
                min_time = min_time.item()
            if isinstance(max_time, pl.Series):
                max_time = max_time.item()
            
            # Convert Polars datetime to Python datetime
            # Polars datetime scalars are Python datetime objects when extracted
            if not isinstance(min_time, datetime):
                # Try parsing as string
                min_dt = datetime.fromisoformat(str(min_time).replace('+00:00', ''))
            else:
                min_dt = min_time
            
            if not isinstance(max_time, datetime):
                max_dt = datetime.fromisoformat(str(max_time).replace('+00:00', ''))
            else:
                max_dt = max_time
            
            # Ensure timezone awareness matches the data
            if trades is not None and len(trades) > 0:
                ts_dtype = trades["ts_event"].dtype
                if hasattr(ts_dtype, 'time_zone') and ts_dtype.time_zone:
                    from zoneinfo import ZoneInfo
                    tz = ZoneInfo(ts_dtype.time_zone)
                    if min_dt.tzinfo is None:
                        min_dt = min_dt.replace(tzinfo=tz)
                    if max_dt.tzinfo is None:
                        max_dt = max_dt.replace(tzinfo=tz)
            
            # Set default time range to 9:30 with 390 minute duration (full trading day)
            from zoneinfo import ZoneInfo
            tz = min_dt.tzinfo if min_dt.tzinfo else ZoneInfo("America/New_York")
            default_start = min_dt.replace(hour=9, minute=30, second=0, microsecond=0)
            default_duration_minutes = 390  # 6.5 hours = full trading day (9:30 AM to 4:00 PM)
            
            # Ensure default start is within the data range
            if default_start < min_dt:
                default_start = min_dt
            
            # Calculate default end time
            default_end = default_start + timedelta(minutes=default_duration_minutes)
            if default_end > max_dt:
                default_end = max_dt
                # Adjust duration if needed
                default_duration_minutes = int((default_end - default_start).total_seconds() / 60)
            
            # Use session state to persist time inputs
            if "time_start" not in st.session_state:
                st.session_state.time_start = default_start
            if "duration_minutes" not in st.session_state:
                st.session_state.duration_minutes = default_duration_minutes
            
            # Direct time input fields: Start time + Duration
            start_input = st.sidebar.time_input(
                "Start Time",
                value=st.session_state.time_start.time() if isinstance(st.session_state.time_start, datetime) else default_start.time(),
                key="start_time_input"
            )
            
            duration_minutes = st.sidebar.number_input(
                "Duration (minutes)",
                min_value=1,
                max_value=1440,  # Max 24 hours
                value=st.session_state.duration_minutes,
                step=1,
                key="duration_input"
            )
            
            # Combine date from min_dt with time input
            input_start = datetime.combine(min_dt.date(), start_input)
            
            # Add timezone if needed
            if min_dt.tzinfo:
                input_start = input_start.replace(tzinfo=min_dt.tzinfo)
            
            # Calculate end time from start + duration
            input_end = input_start + timedelta(minutes=duration_minutes)
            
            # Ensure input times are within data range
            input_start = max(input_start, min_dt)
            input_end = min(input_end, max_dt)
            
            # Adjust duration if end time was clipped
            if input_end <= input_start:
                input_end = min(input_start + timedelta(minutes=1), max_dt)
                duration_minutes = max(1, int((input_end - input_start).total_seconds() / 60))
            
            # Update session state
            st.session_state.time_start = input_start
            st.session_state.duration_minutes = duration_minutes
            
            # Also show slider for visual reference
            time_range = st.sidebar.slider(
                "Time Range (Slider)",
                min_value=min_dt,
                max_value=max_dt,
                value=(input_start, input_end),
                format="HH:mm:ss",
            )
            
            # Use slider value if it changed, otherwise use input values
            slider_start, slider_end = time_range
            if slider_start != input_start or slider_end != input_end:
                start_time, end_time = slider_start, slider_end
                # Update inputs based on slider
                st.session_state.time_start = slider_start
                slider_duration = int((slider_end - slider_start).total_seconds() / 60)
                st.session_state.duration_minutes = max(1, slider_duration)
            else:
                start_time, end_time = input_start, input_end
            
        except Exception as e:
            logger.warning(f"Error setting up time range slider: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            st.error(f"Error setting up time range: {e}")
            start_time = None
            end_time = None
    else:
        start_time = None
        end_time = None
    
    # Use form to batch inputs and prevent rerun storms
    with st.sidebar.form("visualization_controls"):
        # Toggle overlays
        st.header("🎛️ Overlays")
        show_trades = st.checkbox("Show Trades", value=True)
        show_nbbo = st.checkbox("Show NBBO", value=True)
        show_mid_price = st.checkbox("Show Mid Price", value=False)
        show_vwap = st.checkbox("Show VWAP", value=False)
        
        # Trade size filter
        st.header("🔍 Filters")
        min_trade_size = st.number_input(
            "Min Trade Size",
            min_value=0,
            value=0,
            step=1,
            help="Filter trades by minimum size (0 = show all)",
        )
        
        # Apply button
        apply_button = st.form_submit_button("Apply Changes", type="primary")
        
        # Store form state in session state
        if apply_button or "viz_settings" not in st.session_state:
            st.session_state.viz_settings = {
                "show_trades": show_trades,
                "show_nbbo": show_nbbo,
                "show_mid_price": show_mid_price,
                "show_vwap": show_vwap,
                "min_trade_size": min_trade_size,
            }
    
    # Use session state values for visualization
    default_viz_settings = {
        "show_trades": True,
        "show_nbbo": True,
        "show_mid_price": False,
        "show_vwap": False,
        "min_trade_size": 0,
    }
    viz_settings = st.session_state.get("viz_settings", default_viz_settings)
    
    # Ensure all keys exist (for backward compatibility with existing session state)
    for key, default_value in default_viz_settings.items():
        if key not in viz_settings:
            viz_settings[key] = default_value
    
    show_trades = viz_settings["show_trades"]
    show_nbbo = viz_settings["show_nbbo"]
    show_mid_price = viz_settings.get("show_mid_price", False)  # Default to False if missing
    show_vwap = viz_settings["show_vwap"]
    min_trade_size = viz_settings["min_trade_size"] if viz_settings["min_trade_size"] > 0 else None
    
    # Filter data by time range
    if trades is not None and len(trades) > 0 and start_time is not None and end_time is not None:
        try:
            trades_before = len(trades)
            # Ensure ts_event is datetime type
            if trades["ts_event"].dtype == pl.Object:
                # Try to parse as datetime if it's Object type
                trades = trades.with_columns(
                    pl.col("ts_event").str.strptime(pl.Datetime(time_unit="us"), "%Y-%m-%d %H:%M:%S%.f")
                )
            
            # Convert to timestamp (microseconds since epoch) for comparison
            start_ts = int(start_time.timestamp() * 1_000_000)
            end_ts = int(end_time.timestamp() * 1_000_000)
            
            trades = trades.filter(
                (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                (pl.col("ts_event").dt.timestamp("us") <= end_ts)
            )
            trades_after = len(trades)
            if trades_after == 0 and trades_before > 0:
                logger.warning(f"Time range filter removed all trades. Before: {trades_before}, After: {trades_after}, Range: {start_time} to {end_time}")
        except Exception as e:
            logger.warning(f"Error filtering trades by time range: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            # Continue without filtering if there's an error
    
    if nbbo is not None and len(nbbo) > 0 and start_time is not None and end_time is not None:
        try:
            nbbo_before = len(nbbo)
            # Ensure ts_event is datetime type
            if nbbo["ts_event"].dtype == pl.Object:
                # Try to parse as datetime if it's Object type
                nbbo = nbbo.with_columns(
                    pl.col("ts_event").str.strptime(pl.Datetime(time_unit="us"), "%Y-%m-%d %H:%M:%S%.f")
                )
            
            # Convert to timestamp (microseconds since epoch) for comparison
            start_ts = int(start_time.timestamp() * 1_000_000)
            end_ts = int(end_time.timestamp() * 1_000_000)
            
            nbbo = nbbo.filter(
                (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                (pl.col("ts_event").dt.timestamp("us") <= end_ts)
            )
            nbbo_after = len(nbbo)
            if nbbo_after == 0 and nbbo_before > 0:
                logger.warning(f"Time range filter removed all NBBO. Before: {nbbo_before}, After: {nbbo_after}, Range: {start_time} to {end_time}")
        except Exception as e:
            logger.warning(f"Error filtering NBBO by time range: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            # Continue without filtering if there's an error
    
    # Use all data for visualization (no limits)
    trades_for_viz = trades
    nbbo_for_viz = nbbo
    
    # Main content
    st.title("📈 Market Data Interactive Analysis")
    
    # Check if we have any data
    has_any_data = any(
        data_by_source.get(source, {}).get("trades") is not None or
        data_by_source.get(source, {}).get("nbbo") is not None
        for source in selected_sources
    )
    
    if not has_any_data:
        st.warning("No data available for the selected date and source.")
        return
    
    # Show summary stats in rows
    show_dual_source = st.session_state.get("show_dual_source", False)
    selected_sources = st.session_state.get("selected_sources", [])
    
    if show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
        # Single symbol with dual source - create a table
        table_data = []
        for source in selected_sources:
            source_trades = data_by_source.get(source, {}).get("trades")
            source_nbbo = data_by_source.get(source, {}).get("nbbo")
            
            row = {"Source": source.upper()}
            
            if source_trades is not None and len(source_trades) > 0:
                row["Total Trades"] = f"{len(source_trades):,}"
                if "size" in source_trades.columns:
                    total_volume = source_trades["size"].sum()
                    row["Total Volume"] = f"{total_volume:,}"
                else:
                    row["Total Volume"] = "N/A"
            else:
                row["Total Trades"] = "N/A"
                row["Total Volume"] = "N/A"
            
            if source_nbbo is not None and len(source_nbbo) > 0:
                row["NBBO Records"] = f"{len(source_nbbo):,}"
            else:
                row["NBBO Records"] = "N/A"
            
            table_data.append(row)
        
        if table_data:
            import pandas as pd
            df = pd.DataFrame(table_data)
            st.dataframe(df, width='stretch', hide_index=True, use_container_width=False)
    elif len(selected_symbols) > 1:
        # Multiple symbols - create a table
        table_data = []
        for symbol in selected_symbols:
            # Filter data for this symbol
            symbol_trades = None
            symbol_nbbo = None
            
            if trades is not None and len(trades) > 0 and "symbol" in trades.columns:
                symbol_trades = trades.filter(pl.col("symbol") == symbol)
            
            if nbbo is not None and len(nbbo) > 0 and "symbol" in nbbo.columns:
                symbol_nbbo = nbbo.filter(pl.col("symbol") == symbol)
            
            row = {"Symbol": symbol}
            
            if symbol_trades is not None and len(symbol_trades) > 0:
                row["Total Trades"] = f"{len(symbol_trades):,}"
                if "size" in symbol_trades.columns:
                    total_volume = symbol_trades["size"].sum()
                    row["Total Volume"] = f"{total_volume:,}"
                else:
                    row["Total Volume"] = "N/A"
            else:
                row["Total Trades"] = "N/A"
                row["Total Volume"] = "N/A"
            
            if symbol_nbbo is not None and len(symbol_nbbo) > 0:
                row["NBBO Records"] = f"{len(symbol_nbbo):,}"
            else:
                row["NBBO Records"] = "N/A"
            
            table_data.append(row)
        
        if table_data:
            import pandas as pd
            df = pd.DataFrame(table_data)
            st.dataframe(df, width='stretch', hide_index=True, use_container_width=False)
    else:
        # Single symbol, single source - one row
        col1, col2, col3, col4 = st.columns(4)
        
        if trades is not None:
            col1.metric("Total Trades", f"{len(trades):,}")
            if "size" in trades.columns:
                total_volume = trades["size"].sum()
                col2.metric("Total Volume", f"{total_volume:,}")
            else:
                col2.metric("Total Volume", "N/A")
        else:
            col1.metric("Total Trades", "N/A")
            col2.metric("Total Volume", "N/A")
        
        if nbbo is not None:
            col3.metric("NBBO Records", f"{len(nbbo):,}")
        else:
            col3.metric("NBBO Records", "N/A")
        
        if selected_symbols:
            col4.metric("Symbol", selected_symbols[0])
        else:
            col4.metric("Symbol", "N/A")
    
    # Main panels
    st.header("Price Panel")
    
    # Check if we should show dual-source visualization (single symbol with data in both sources)
    show_dual_source = st.session_state.get("show_dual_source", False)
    selected_sources = st.session_state.get("selected_sources", [])
    data_by_source = st.session_state.get("data_by_source", {})
    
    # Filter data by time range for dual-source mode (needed for all panels)
    filtered_sources_data = {}
    if show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
        all_prices = []  # Collect all price values for y-axis sync
        
        for source in selected_sources:
            source_trades = data_by_source.get(source, {}).get("trades")
            source_nbbo = data_by_source.get(source, {}).get("nbbo")
            
            # Filter by time range
            if source_trades is not None and len(source_trades) > 0 and start_time is not None and end_time is not None:
                try:
                    start_ts = int(start_time.timestamp() * 1_000_000)
                    end_ts = int(end_time.timestamp() * 1_000_000)
                    source_trades = source_trades.filter(
                        (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                        (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                    )
                    # Collect trade prices
                    if "price" in source_trades.columns:
                        all_prices.extend(source_trades["price"].to_list())
                except Exception:
                    pass
            
            if source_nbbo is not None and len(source_nbbo) > 0 and start_time is not None and end_time is not None:
                try:
                    start_ts = int(start_time.timestamp() * 1_000_000)
                    end_ts = int(end_time.timestamp() * 1_000_000)
                    source_nbbo = source_nbbo.filter(
                        (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                        (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                    )
                    # Collect NBBO prices
                    if "best_bid" in source_nbbo.columns:
                        all_prices.extend(source_nbbo["best_bid"].to_list())
                    if "best_ask" in source_nbbo.columns:
                        all_prices.extend(source_nbbo["best_ask"].to_list())
                except Exception:
                    pass
            
            filtered_sources_data[source] = {
                "trades": source_trades,
                "nbbo": source_nbbo,
            }
        
        # Calculate shared y-axis range with some padding
        yaxis_range = None
        if all_prices:
            min_price = min(all_prices)
            max_price = max(all_prices)
            price_range = max_price - min_price
            padding = price_range * 0.05  # 5% padding on each side
            yaxis_range = (min_price - padding, max_price + padding)
    
    if trades is not None or nbbo is not None:
        # Check if we have any data to visualize
        has_trades = trades_for_viz is not None and len(trades_for_viz) > 0
        has_nbbo = nbbo_for_viz is not None and len(nbbo_for_viz) > 0
        
        if not has_trades and not has_nbbo:
            st.warning("No data available after filtering. Try adjusting the time range or symbol selection.")
        else:
            with st.spinner("Generating visualization..."):
                try:
                    # If dual source mode and single symbol, show side-by-side plots
                    if show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
                        # Create plots with synchronized y-axis
                        source_cols = st.columns(len(selected_sources))
                        for idx, source in enumerate(selected_sources):
                            with source_cols[idx]:
                                st.subheader(f"Price Panel - {source.upper()}")
                                
                                source_trades = filtered_sources_data[source]["trades"]
                                source_nbbo = filtered_sources_data[source]["nbbo"]
                                
                                # Create plot for this source with shared y-axis range
                                fig_price = plot_price_panel(
                                    source_trades,
                                    source_nbbo,
                                    show_trades=show_trades,
                                    show_nbbo=show_nbbo,
                                    show_mid_price=show_mid_price,
                                    show_vwap=show_vwap,
                                    symbol=selected_symbols[0],
                                    start_time=start_time,
                                    end_time=end_time,
                                    min_trade_size=min_trade_size,
                                    yaxis_range=yaxis_range,
                                )
                                
                                if len(fig_price.data) > 0:
                                    st.plotly_chart(fig_price, width='stretch')
                                else:
                                    st.info(f"No data for {source}")
                    elif len(selected_symbols) > 1:
                        # Multiple symbols mode - show one plot per symbol with synchronized x-axis
                        # Filter data by time range first
                        filtered_trades = trades_for_viz
                        filtered_nbbo = nbbo_for_viz
                        
                        if filtered_trades is not None and len(filtered_trades) > 0 and start_time is not None and end_time is not None:
                            try:
                                start_ts = int(start_time.timestamp() * 1_000_000)
                                end_ts = int(end_time.timestamp() * 1_000_000)
                                filtered_trades = filtered_trades.filter(
                                    (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                    (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                )
                            except Exception:
                                pass
                        
                        if filtered_nbbo is not None and len(filtered_nbbo) > 0 and start_time is not None and end_time is not None:
                            try:
                                start_ts = int(start_time.timestamp() * 1_000_000)
                                end_ts = int(end_time.timestamp() * 1_000_000)
                                filtered_nbbo = filtered_nbbo.filter(
                                    (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                    (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                )
                            except Exception:
                                pass
                        
                        # Create plots for each symbol
                        symbol_figs = []
                        
                        for symbol in selected_symbols:
                            # Filter data for this symbol
                            symbol_trades = None
                            symbol_nbbo = None
                            
                            if filtered_trades is not None and len(filtered_trades) > 0 and "symbol" in filtered_trades.columns:
                                symbol_trades = filtered_trades.filter(pl.col("symbol") == symbol)
                            
                            if filtered_nbbo is not None and len(filtered_nbbo) > 0 and "symbol" in filtered_nbbo.columns:
                                symbol_nbbo = filtered_nbbo.filter(pl.col("symbol") == symbol)
                            
                            # Check if we have data for this symbol
                            has_symbol_trades = symbol_trades is not None and len(symbol_trades) > 0
                            has_symbol_nbbo = symbol_nbbo is not None and len(symbol_nbbo) > 0
                            
                            if not has_symbol_trades and not has_symbol_nbbo:
                                st.warning(f"No data available for {symbol} in the selected time range.")
                                continue
                            
                            # Create plot for this symbol
                            fig = plot_price_panel(
                                symbol_trades if has_symbol_trades else None,
                                symbol_nbbo if has_symbol_nbbo else None,
                                show_trades=show_trades,
                                show_nbbo=show_nbbo,
                                show_mid_price=show_mid_price,
                                show_vwap=show_vwap,
                                symbol=symbol,
                                start_time=start_time,
                                end_time=end_time,
                                min_trade_size=min_trade_size,
                            )
                            
                            # Check if figure has any traces
                            if len(fig.data) == 0:
                                st.warning(f"Visualization for {symbol} contains no data traces. Check your overlay settings (Show Trades: {show_trades}, Show NBBO: {show_nbbo}).")
                                st.info(f"  - Has trades: {has_symbol_trades}, Has NBBO: {has_symbol_nbbo}")
                                continue
                            
                            # Set uirevision for x-axis synchronization
                            fig.update_layout(
                                uirevision="sync_time",  # Same uirevision for all plots to sync x-axis
                            )
                            
                            symbol_figs.append((symbol, fig))
                        
                        # Display plots in rows (one per symbol) with synchronized x-axis
                        if symbol_figs:
                            # Store plot keys for synchronization
                            plot_keys = []
                            
                            for symbol, fig in symbol_figs:
                                if len(fig.data) > 0:
                                    plot_key = f"price_plot_{symbol}"
                                    plot_keys.append(plot_key)
                                    st.plotly_chart(
                                        fig,
                                        width='stretch',
                                        key=plot_key,
                                    )
                                else:
                                    st.info(f"No data for {symbol}")
                            
                            # Add JavaScript to synchronize plots
                            if len(plot_keys) > 1:
                                import streamlit.components.v1 as components
                                
                                sync_script = """
                                <script>
                                (function() {
                                    let syncing = false;
                                    const plots = [];
                                    
                                    function findPlotlyPlots() {
                                        // Find all Plotly plot containers
                                        const containers = document.querySelectorAll('[data-testid="stPlotlyChart"]');
                                        plots.length = 0;
                                        
                                        containers.forEach(function(container) {
                                            // Find the actual Plotly div
                                            const plotlyDiv = container.querySelector('.plotly');
                                            if (plotlyDiv && typeof Plotly !== 'undefined') {
                                                // Check if plot is already initialized
                                                if (plotlyDiv.data && plotlyDiv.layout) {
                                                    plots.push(plotlyDiv);
                                                }
                                            }
                                        });
                                        
                                        return plots.length >= 2;
                                    }
                                    
                                    function setupSync() {
                                        if (!findPlotlyPlots()) {
                                            setTimeout(setupSync, 300);
                                            return;
                                        }
                                        
                                        // Set up event listeners for each plot
                                        plots.forEach(function(plotDiv) {
                                            // Remove existing listeners if any
                                            plotDiv.removeAllListeners('plotly_relayout');
                                            
                                            plotDiv.on('plotly_relayout', function(eventData) {
                                                if (syncing) return;
                                                
                                                // Check if x-axis range changed
                                                let xRange = null;
                                                if (eventData['xaxis.range[0]'] !== undefined && 
                                                    eventData['xaxis.range[1]'] !== undefined) {
                                                    xRange = [
                                                        eventData['xaxis.range[0]'],
                                                        eventData['xaxis.range[1]']
                                                    ];
                                                } else if (eventData['xaxis.range'] !== undefined) {
                                                    xRange = eventData['xaxis.range'];
                                                }
                                                
                                                if (xRange) {
                                                    syncing = true;
                                                    
                                                    // Update all other plots
                                                    plots.forEach(function(otherPlot) {
                                                        if (otherPlot !== plotDiv) {
                                                            try {
                                                                Plotly.relayout(otherPlot, {
                                                                    'xaxis.range': xRange
                                                                });
                                                            } catch(e) {
                                                                console.log('Sync error:', e);
                                                            }
                                                        }
                                                    });
                                                    
                                                    setTimeout(function() { syncing = false; }, 150);
                                                }
                                            });
                                        });
                                    }
                                    
                                    // Wait for Plotly to be available
                                    function waitForPlotly() {
                                        if (typeof Plotly === 'undefined') {
                                            setTimeout(waitForPlotly, 100);
                                            return;
                                        }
                                        
                                        // Start setup after plots are rendered
                                        setTimeout(setupSync, 800);
                                        
                                        // Also try to setup when new plots are added
                                        const observer = new MutationObserver(function() {
                                            if (plots.length < 2) {
                                                setTimeout(setupSync, 200);
                                            }
                                        });
                                        
                                        observer.observe(document.body, {
                                            childList: true,
                                            subtree: true
                                        });
                                    }
                                    
                                    waitForPlotly();
                                })();
                                </script>
                                """
                                
                                components.html(sync_script, height=0)
                        else:
                            st.info("No data available for selected symbols")
                    else:
                        # Single symbol, single source - show single plot
                        # Check if at least one overlay is enabled
                        if not show_trades and not show_nbbo:
                            st.warning("⚠️ Please enable at least one overlay: 'Show Trades' or 'Show NBBO' to display the price panel.")
                        else:
                            fig_price = plot_price_panel(
                                trades_for_viz,
                                nbbo_for_viz,
                                show_trades=show_trades,
                                show_nbbo=show_nbbo,
                                show_mid_price=show_mid_price,
                                show_vwap=show_vwap,
                                symbol=selected_symbols[0] if len(selected_symbols) == 1 else None,
                                start_time=start_time,
                                end_time=end_time,
                                min_trade_size=min_trade_size,
                            )
                            
                            # Check if figure has any traces
                            if len(fig_price.data) == 0:
                                st.warning("Visualization created but contains no data traces.")
                                st.info(f"**Settings:** Show Trades: {show_trades}, Show NBBO: {show_nbbo}")
                                st.info(f"**Data:** Has Trades: {has_trades}, Has NBBO: {has_nbbo}")
                                if has_trades and not show_trades:
                                    st.info("💡 Tip: Enable 'Show Trades' to see trade data.")
                                if has_nbbo and not show_nbbo:
                                    st.info("💡 Tip: Enable 'Show NBBO' to see bid/ask data.")
                            else:
                                st.plotly_chart(fig_price, width='stretch')
                                st.caption(f"Figure contains {len(fig_price.data)} trace(s)")
                except Exception as e:
                    st.error(f"Error creating price panel: {e}")
                    import traceback
                    st.code(traceback.format_exc())
    else:
        st.info("No data available for price panel")
    
    # Spread & Churn panel
    st.header("Spread & Churn")
    
    if show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
        # Dual source mode - show side-by-side for each source
        # Spread (bps) Over Time
        st.subheader("Spread (bps) Over Time")
        spread_cols = st.columns(len(selected_sources))
        for idx, source in enumerate(selected_sources):
            with spread_cols[idx]:
                source_nbbo = filtered_sources_data.get(source, {}).get("nbbo")
                if source_nbbo is not None and len(source_nbbo) > 0:
                    fig_spread_bps = plot_spread_bps_timeline(
                        source_nbbo,
                        show_churn=False,
                        symbol=f"{selected_symbols[0]} ({source.upper()})",
                    )
                    st.plotly_chart(fig_spread_bps, width='stretch')
                else:
                    st.info(f"No NBBO data for {source}")
        
        # Quote Churn
        st.subheader("Quote Churn")
        churn_cols = st.columns(len(selected_sources))
        for idx, source in enumerate(selected_sources):
            with churn_cols[idx]:
                source_nbbo = filtered_sources_data.get(source, {}).get("nbbo")
                if source_nbbo is not None and len(source_nbbo) > 0:
                    fig_churn = plot_churn_bar_chart(
                        source_nbbo,
                        symbol=f"{selected_symbols[0]} ({source.upper()})",
                    )
                    st.plotly_chart(fig_churn, width='stretch')
                else:
                    st.info(f"No NBBO data for {source}")
    else:
        # Single source mode
        if len(selected_symbols) > 1:
            # Multiple symbols - show one plot per symbol
            # Filter data by time range first
            filtered_nbbo = nbbo_for_viz
            if filtered_nbbo is not None and len(filtered_nbbo) > 0 and start_time is not None and end_time is not None:
                try:
                    start_ts = int(start_time.timestamp() * 1_000_000)
                    end_ts = int(end_time.timestamp() * 1_000_000)
                    filtered_nbbo = filtered_nbbo.filter(
                        (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                        (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                    )
                except Exception:
                    pass
            
            # Spread (bps) Over Time - one plot per symbol
            st.subheader("Spread (bps) Over Time")
            for symbol in selected_symbols:
                if filtered_nbbo is not None and len(filtered_nbbo) > 0 and "symbol" in filtered_nbbo.columns:
                    symbol_nbbo = filtered_nbbo.filter(pl.col("symbol") == symbol)
                    if len(symbol_nbbo) > 0:
                        fig_spread_bps = plot_spread_bps_timeline(
                            symbol_nbbo,
                            show_churn=False,
                            symbol=symbol,
                        )
                        st.plotly_chart(fig_spread_bps, width='stretch')
                    else:
                        st.info(f"No NBBO data for {symbol}")
            
            # Quote Churn - one plot per symbol
            st.subheader("Quote Churn")
            for symbol in selected_symbols:
                if filtered_nbbo is not None and len(filtered_nbbo) > 0 and "symbol" in filtered_nbbo.columns:
                    symbol_nbbo = filtered_nbbo.filter(pl.col("symbol") == symbol)
                    if len(symbol_nbbo) > 0:
                        fig_churn = plot_churn_bar_chart(
                            symbol_nbbo,
                            symbol=symbol,
                        )
                        st.plotly_chart(fig_churn, width='stretch')
                    else:
                        st.info(f"No NBBO data for {symbol}")
        else:
            # Single symbol - show side-by-side
            col1, col2 = st.columns(2)
            
            with col1:
                if nbbo is not None and len(nbbo) > 0:
                    st.subheader("Spread (bps) Over Time")
                    fig_spread_bps = plot_spread_bps_timeline(
                        nbbo,
                        show_churn=False,
                        symbol=selected_symbols[0] if len(selected_symbols) == 1 else None,
                    )
                    st.plotly_chart(fig_spread_bps, width='stretch')
                else:
                    st.info("NBBO data not available")
            
            with col2:
                if nbbo is not None and len(nbbo) > 0:
                    st.subheader("Quote Churn")
                    fig_churn = plot_churn_bar_chart(
                        nbbo,
                        symbol=selected_symbols[0] if len(selected_symbols) == 1 else None,
                    )
                    st.plotly_chart(fig_churn, width='stretch')
                else:
                    st.info("NBBO data not available")
    
    # Distribution views
    st.header("Distribution Views")
    
    if show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
        # Dual source mode - show side-by-side for each source
        # Spread Distribution
        st.subheader("Spread Distribution")
        spread_dist_cols = st.columns(len(selected_sources))
        for idx, source in enumerate(selected_sources):
            with spread_dist_cols[idx]:
                source_nbbo = filtered_sources_data.get(source, {}).get("nbbo")
                if source_nbbo is not None and len(source_nbbo) > 0:
                    fig_spread_dist = plot_spread_histogram(
                        source_nbbo,
                        symbol=f"{selected_symbols[0]} ({source.upper()})",
                    )
                    st.plotly_chart(fig_spread_dist, width='stretch')
                else:
                    st.info(f"No NBBO data for {source}")
        
        # Trade Size Distribution
        st.subheader("Trade Size Distribution")
        size_dist_cols = st.columns(len(selected_sources))
        for idx, source in enumerate(selected_sources):
            with size_dist_cols[idx]:
                source_trades = filtered_sources_data.get(source, {}).get("trades")
                if source_trades is not None and len(source_trades) > 0 and "size" in source_trades.columns:
                    try:
                        import plotly.express as px
                        trades_pd = source_trades.select(["size"]).to_pandas()
                        fig_size_dist = px.histogram(
                            trades_pd,
                            x="size",
                            nbins=50,
                            title=f"{selected_symbols[0]} ({source.upper()}) - Trade Size Distribution",
                            labels={"size": "Trade Size", "count": "Frequency"},
                        )
                        fig_size_dist.update_layout(height=400)
                        st.plotly_chart(fig_size_dist, width='stretch')
                    except Exception as e:
                        st.error(f"Error creating trade size distribution for {source}: {e}")
                else:
                    st.info(f"No trade size data for {source}")
    else:
        # Single source mode
        if len(selected_symbols) > 1:
            # Multiple symbols - show one plot per symbol
            # Filter data by time range first
            filtered_nbbo = nbbo_for_viz
            filtered_trades = trades_for_viz
            
            if filtered_nbbo is not None and len(filtered_nbbo) > 0 and start_time is not None and end_time is not None:
                try:
                    start_ts = int(start_time.timestamp() * 1_000_000)
                    end_ts = int(end_time.timestamp() * 1_000_000)
                    filtered_nbbo = filtered_nbbo.filter(
                        (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                        (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                    )
                except Exception:
                    pass
            
            if filtered_trades is not None and len(filtered_trades) > 0 and start_time is not None and end_time is not None:
                try:
                    start_ts = int(start_time.timestamp() * 1_000_000)
                    end_ts = int(end_time.timestamp() * 1_000_000)
                    filtered_trades = filtered_trades.filter(
                        (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                        (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                    )
                except Exception:
                    pass
            
            # Spread Distribution - one plot per symbol
            st.subheader("Spread Distribution")
            for symbol in selected_symbols:
                if filtered_nbbo is not None and len(filtered_nbbo) > 0 and "symbol" in filtered_nbbo.columns:
                    symbol_nbbo = filtered_nbbo.filter(pl.col("symbol") == symbol)
                    if len(symbol_nbbo) > 0:
                        fig_spread_dist = plot_spread_histogram(
                            symbol_nbbo,
                            symbol=symbol,
                        )
                        st.plotly_chart(fig_spread_dist, width='stretch')
                    else:
                        st.info(f"No NBBO data for {symbol}")
            
            # Trade Size Distribution - one plot per symbol
            st.subheader("Trade Size Distribution")
            for symbol in selected_symbols:
                if filtered_trades is not None and len(filtered_trades) > 0 and "size" in filtered_trades.columns and "symbol" in filtered_trades.columns:
                    symbol_trades = filtered_trades.filter(pl.col("symbol") == symbol)
                    if len(symbol_trades) > 0:
                        try:
                            import plotly.express as px
                            trades_pd = symbol_trades.select(["size"]).to_pandas()
                            fig_size_dist = px.histogram(
                                trades_pd,
                                x="size",
                                nbins=50,
                                title=f"{symbol} - Trade Size Distribution",
                                labels={"size": "Trade Size", "count": "Frequency"},
                            )
                            fig_size_dist.update_layout(height=400)
                            st.plotly_chart(fig_size_dist, width='stretch')
                        except Exception as e:
                            st.error(f"Error creating trade size distribution for {symbol}: {e}")
                    else:
                        st.info(f"No trade data for {symbol}")
                elif filtered_trades is not None and len(filtered_trades) > 0:
                    st.info(f"Trade size data not available for {symbol}")
        else:
            # Single symbol - show side-by-side
            col1, col2 = st.columns(2)
            
            with col1:
                if nbbo is not None and len(nbbo) > 0:
                    st.subheader("Spread Distribution")
                    fig_spread_dist = plot_spread_histogram(
                        nbbo,
                        symbol=selected_symbols[0] if len(selected_symbols) == 1 else None,
                    )
                    st.plotly_chart(fig_spread_dist, width='stretch')
                else:
                    st.info("NBBO data not available")
            
            with col2:
                if trades is not None and len(trades) > 0 and "size" in trades.columns:
                    st.subheader("Trade Size Distribution")
                    try:
                        # Create trade size distribution histogram
                        import plotly.express as px
                        trades_pd = trades.select(["size"]).to_pandas()
                        fig_size_dist = px.histogram(
                            trades_pd,
                            x="size",
                            nbins=50,
                            title=f"{selected_symbols[0] if selected_symbols else ''} - Trade Size Distribution".strip(),
                            labels={"size": "Trade Size", "count": "Frequency"},
                        )
                        fig_size_dist.update_layout(height=400)
                        st.plotly_chart(fig_size_dist, width='stretch')
                    except Exception as e:
                        st.error(f"Error creating trade size distribution: {e}")
                elif trades is not None and len(trades) > 0:
                    st.info("Trade size data not available")
                else:
                    st.info("Trades data required for size distribution")
    
    # Tables
    st.header("Tables")
    
    if show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
        # Dual source mode - show tables for each source
        # Trade Summary - Largest trades
        st.subheader("Trade Summary - Largest Trades")
        trade_summary_cols = st.columns(len(selected_sources))
        for idx, source in enumerate(selected_sources):
            with trade_summary_cols[idx]:
                st.caption(f"{source.upper()}")
                source_trades = filtered_sources_data.get(source, {}).get("trades")
                if source_trades is not None and len(source_trades) > 0 and "size" in source_trades.columns:
                    try:
                        # Show 100 largest size trades
                        largest_trades = source_trades.sort("size", descending=True).head(100).select([
                            "ts_event",
                            "symbol",
                            "price",
                            "size",
                        ]).to_pandas()
                        
                        if len(largest_trades) > 0:
                            largest_trades["ts_event"] = largest_trades["ts_event"].dt.strftime("%H:%M:%S.%f").str[:-3]
                            largest_trades["price"] = largest_trades["price"].round(4)
                            
                            st.dataframe(
                                largest_trades,
                                width='stretch',
                                hide_index=True,
                            )
                            st.caption(f"Top 100 largest trades. Total: {len(source_trades):,}")
                        else:
                            st.info("No trades to display")
                    except Exception as e:
                        st.error(f"Error displaying trades for {source}: {e}")
                else:
                    st.info(f"No trade data for {source}")
        
        # Highest Churn Minutes
        st.subheader("Highest Churn Minutes")
        churn_table_cols = st.columns(len(selected_sources))
        for idx, source in enumerate(selected_sources):
            with churn_table_cols[idx]:
                st.caption(f"{source.upper()}")
                source_nbbo = filtered_sources_data.get(source, {}).get("nbbo")
                if source_nbbo is not None and len(source_nbbo) > 0:
                    try:
                        highest_churn = get_highest_churn_minutes(source_nbbo, top_n=20)
                        highest_churn_pd = highest_churn.to_pandas()
                        
                        # Format for display
                        if len(highest_churn_pd) > 0:
                            highest_churn_pd["time_bucket"] = highest_churn_pd["time_bucket"].dt.strftime("%H:%M:%S")
                            highest_churn_pd = highest_churn_pd.rename(columns={"time_bucket": "Time", "churn": "Updates"})
                            
                            st.dataframe(
                                highest_churn_pd,
                                width='stretch',
                                hide_index=True,
                            )
                        else:
                            st.info("No churn data available")
                    except Exception as e:
                        st.error(f"Error calculating churn for {source}: {e}")
                else:
                    st.info(f"No NBBO data for {source}")
    else:
        # Single source mode
        if len(selected_symbols) > 1:
            # Multiple symbols - show tables for each symbol
            # Filter data by time range first
            filtered_trades = trades_for_viz
            filtered_nbbo = nbbo_for_viz
            
            if filtered_trades is not None and len(filtered_trades) > 0 and start_time is not None and end_time is not None:
                try:
                    start_ts = int(start_time.timestamp() * 1_000_000)
                    end_ts = int(end_time.timestamp() * 1_000_000)
                    filtered_trades = filtered_trades.filter(
                        (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                        (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                    )
                except Exception:
                    pass
            
            if filtered_nbbo is not None and len(filtered_nbbo) > 0 and start_time is not None and end_time is not None:
                try:
                    start_ts = int(start_time.timestamp() * 1_000_000)
                    end_ts = int(end_time.timestamp() * 1_000_000)
                    filtered_nbbo = filtered_nbbo.filter(
                        (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                        (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                    )
                except Exception:
                    pass
            
            # Trade Summary - Largest trades per symbol
            st.subheader("Trade Summary - Largest Trades")
            for symbol in selected_symbols:
                if filtered_trades is not None and len(filtered_trades) > 0 and "symbol" in filtered_trades.columns:
                    symbol_trades = filtered_trades.filter(pl.col("symbol") == symbol)
                    if len(symbol_trades) > 0:
                        try:
                            # Show 100 largest size trades for this symbol
                            if "size" in symbol_trades.columns:
                                largest_trades = symbol_trades.sort("size", descending=True).head(100).select([
                                    "ts_event",
                                    "symbol",
                                    "price",
                                    "size",
                                ]).to_pandas()
                            else:
                                # Fallback to first 100 if no size column
                                largest_trades = symbol_trades.select([
                                    "ts_event",
                                    "symbol",
                                    "price",
                                ]).head(100).to_pandas()
                            
                            if len(largest_trades) > 0:
                                largest_trades["ts_event"] = largest_trades["ts_event"].dt.strftime("%H:%M:%S.%f").str[:-3]
                                if "price" in largest_trades.columns:
                                    largest_trades["price"] = largest_trades["price"].round(4)
                                
                                st.dataframe(
                                    largest_trades,
                                    width='stretch',
                                    hide_index=True,
                                )
                                st.caption(f"Top 100 largest trades. Total: {len(symbol_trades):,}")
                            else:
                                st.info(f"No trades to display for {symbol}")
                        except Exception as e:
                            st.error(f"Error displaying trades for {symbol}: {e}")
                    else:
                        st.info(f"No trade data for {symbol}")
            
            # Highest Churn Minutes per symbol
            st.subheader("Highest Churn Minutes")
            for symbol in selected_symbols:
                if filtered_nbbo is not None and len(filtered_nbbo) > 0 and "symbol" in filtered_nbbo.columns:
                    symbol_nbbo = filtered_nbbo.filter(pl.col("symbol") == symbol)
                    if len(symbol_nbbo) > 0:
                        try:
                            highest_churn = get_highest_churn_minutes(symbol_nbbo, top_n=20)
                            highest_churn_pd = highest_churn.to_pandas()
                            
                            # Format for display
                            if len(highest_churn_pd) > 0:
                                highest_churn_pd["time_bucket"] = highest_churn_pd["time_bucket"].dt.strftime("%H:%M:%S")
                                highest_churn_pd = highest_churn_pd.rename(columns={"time_bucket": "Time", "churn": "Updates"})
                                
                                st.dataframe(
                                    highest_churn_pd,
                                    width='stretch',
                                    hide_index=True,
                                )
                                st.caption(f"Top 20 minutes with highest quote churn. Total NBBO updates: {len(symbol_nbbo):,}")
                            else:
                                st.info(f"No churn data to display for {symbol}")
                        except Exception as e:
                            st.error(f"Error displaying churn data for {symbol}: {e}")
                    else:
                        st.info(f"No NBBO data for {symbol}")
        else:
            # Single symbol - show side-by-side tables
            col1, col2 = st.columns(2)
            
            with col1:
                if trades is not None and len(trades) > 0:
                    st.subheader("Trade Summary - Largest Trades")
                    try:
                        # Show 100 largest size trades
                        if "size" in trades.columns:
                            largest_trades = trades.sort("size", descending=True).head(100).select([
                                "ts_event",
                                "symbol",
                                "price",
                                "size",
                            ]).to_pandas()
                        else:
                            # Fallback to first 100 if no size column
                            largest_trades = trades.select([
                                "ts_event",
                                "symbol",
                                "price",
                            ]).head(100).to_pandas()
                        
                        if len(largest_trades) > 0:
                            largest_trades["ts_event"] = largest_trades["ts_event"].dt.strftime("%H:%M:%S.%f").str[:-3]
                            if "price" in largest_trades.columns:
                                largest_trades["price"] = largest_trades["price"].round(4)
                            
                            st.dataframe(
                                largest_trades,
                                width='stretch',
                                hide_index=True,
                            )
                            st.caption(f"Top 100 largest trades. Total: {len(trades):,}")
                        else:
                            st.info("No trades to display")
                    except Exception as e:
                        st.error(f"Error displaying trades: {e}")
                else:
                    st.info("Trades data required")
            
            with col2:
                if nbbo is not None and len(nbbo) > 0:
                    st.subheader("Highest Churn Minutes")
                    try:
                        highest_churn = get_highest_churn_minutes(nbbo, top_n=20)
                        highest_churn_pd = highest_churn.to_pandas()
                        
                        # Format for display
                        if len(highest_churn_pd) > 0:
                            highest_churn_pd["time_bucket"] = highest_churn_pd["time_bucket"].dt.strftime("%H:%M:%S")
                            highest_churn_pd = highest_churn_pd.rename(columns={"time_bucket": "Time", "churn": "Updates"})
                            
                            st.dataframe(
                                highest_churn_pd,
                                width='stretch',
                                hide_index=True,
                            )
                            st.caption(f"Top 20 minutes with highest quote churn. Total NBBO updates: {len(nbbo):,}")
                        else:
                            st.info("No churn data available")
                    except Exception as e:
                        st.error(f"Error calculating churn: {e}")
                else:
                    st.info("NBBO data required")


if __name__ == "__main__":
    main()
