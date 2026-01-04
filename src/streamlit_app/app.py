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
    find_symbols_across_dates,
)
from streamlit_app.data_loader import load_trades, load_nbbo
from streamlit_app.visualizations import (
    plot_price_panel,
    plot_spread_bps_timeline,
    plot_churn_bar_chart,
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
    
    # Get data root from config
    data_root = config.data_root
    available_sources = list(config.data_sources.keys())
    if not available_sources:
        st.error("No data sources configured. Please check config.yaml")
        st.stop()
    
    # ===== STEP 1: MODE SELECTION =====
    st.sidebar.header("📊 Mode Selection")
    
    # Mode selection: Single, Multiple, or Cross Comparison
    symbol_mode = st.sidebar.radio(
        "Mode",
        options=["Single Symbol", "Multiple Symbols", "Cross Comparison"],
        index=0,
    )
    
    # Reset symbol selection flag if mode changed
    if "prev_symbol_mode" in st.session_state:
        if st.session_state.prev_symbol_mode != symbol_mode:
            st.session_state.data_selection_applied = False
            # Clear selected dates for Single Symbol mode
            if "single_symbol_dates" in st.session_state:
                del st.session_state["single_symbol_dates"]
    st.session_state.prev_symbol_mode = symbol_mode
    
    # Symbol selection based on mode
    if symbol_mode == "Single Symbol":
            # ===== SINGLE SYMBOL MODE: Multiple dates, then symbol, then sources =====
            
            # Step 1: Date range selection (default 2024-01-02 to 2024-01-05)
            st.sidebar.subheader("📅 Date Selection")
            default_start_date = date(2024, 1, 2)
            default_end_date = date(2024, 1, 5)
            
            # Find available dates to set min/max
            all_available_dates = set()
            for source in available_sources:
                trades_dates = find_available_dates(
                    data_root, source, data_type="trades", max_days=365
                )
                nbbo_dates = find_available_dates(
                    data_root, source, data_type="nbbo", max_days=365
                )
                all_available_dates.update(trades_dates + nbbo_dates)
            
            available_dates = sorted(list(all_available_dates), reverse=True)
            min_date = min(available_dates) if available_dates else date(2020, 1, 1)
            max_date = max(available_dates) if available_dates else date.today()
            
            date_range = st.sidebar.date_input(
                "Date Range",
                value=(default_start_date, default_end_date),
                min_value=min_date,
                max_value=max_date,
                help="Select a date range. Dates with no data will be filtered out.",
            )
            
            # Handle date_range - it can be a single date or tuple
            if isinstance(date_range, tuple) and len(date_range) == 2:
                start_date, end_date = date_range
                # Generate list of dates in range
                selected_dates_list = []
                current_date = start_date
                while current_date <= end_date:
                    selected_dates_list.append(current_date)
                    current_date += timedelta(days=1)
            elif isinstance(date_range, date):
                selected_dates_list = [date_range]
            else:
                selected_dates_list = [default_start_date, default_end_date]
            
            # Step 2: Filter dates with no data in any source
            dates_with_data = []
            for check_date in selected_dates_list:
                has_data = False
                for source in available_sources:
                    # Check if any data exists for this date in this source
                    if check_data_available(data_root, source, check_date, data_type="trades") or \
                       check_data_available(data_root, source, check_date, data_type="nbbo"):
                        has_data = True
                        break
                if has_data:
                    dates_with_data.append(check_date)
            
            if not dates_with_data:
                st.sidebar.error("No data available for any of the selected dates")
                st.info("Please select a different date range or check your data.")
                st.stop()
            
            # Show filtered dates info if some were removed
            if len(dates_with_data) < len(selected_dates_list):
                removed_dates = set(selected_dates_list) - set(dates_with_data)
                st.sidebar.info(f"Filtered out {len(removed_dates)} date(s) with no data: {', '.join([d.strftime('%Y-%m-%d') for d in sorted(removed_dates)])}")
            
            selected_dates_list = dates_with_data
            
            # Step 3: Find symbols available across all selected dates
            all_available_symbols = find_symbols_across_dates(
                data_root, available_sources, selected_dates_list, data_type="trades"
            )
            
            if not all_available_symbols:
                # Fallback to NBBO
                all_available_symbols = find_symbols_across_dates(
                    data_root, available_sources, selected_dates_list, data_type="nbbo"
                )
            
            if not all_available_symbols:
                st.sidebar.warning("No symbols found across all selected dates")
                st.info("Please select a different date range or check your data.")
                st.stop()
            
            # Step 4: Symbol selection (default SPY)
            st.sidebar.subheader("📈 Symbol Selection")
            default_symbol = "SPY" if "SPY" in all_available_symbols else all_available_symbols[0]
            default_idx = all_available_symbols.index(default_symbol) if default_symbol in all_available_symbols else 0
            
            selected_symbols = [st.sidebar.selectbox(
                "Symbol",
                options=all_available_symbols,
                index=default_idx,
            )]
            
            # Step 5: Source selection as checkboxes
            st.sidebar.subheader("🔌 Data Source")
            symbol = selected_symbols[0]
            
            # Check which sources have data for this symbol across all selected dates
            sources_available = {}
            for source in available_sources:
                has_data = False
                for check_date in selected_dates_list:
                    if check_data_available(data_root, source, check_date, symbol, data_type="trades") or \
                       check_data_available(data_root, source, check_date, symbol, data_type="nbbo"):
                        has_data = True
                        break
                sources_available[source] = has_data
            
            # Default to taq if available
            default_source = "taq" if sources_available.get("taq", False) else None
            if default_source is None:
                # Find first available source
                for source in available_sources:
                    if sources_available.get(source, False):
                        default_source = source
                        break
            
            # Initialize session state for source selection
            source_state_key = "single_symbol_sources_state"
            if source_state_key not in st.session_state:
                st.session_state[source_state_key] = {source: (source == default_source and sources_available.get(source, False)) for source in available_sources}
            
            source_state = st.session_state[source_state_key]
            
            # Collect selected sources from checkboxes
            selected_sources = []
            for source in available_sources:
                is_disabled = not sources_available[source]
                label = f"{source.upper()}"
                if is_disabled:
                    label += " (Not Available)"
                
                current_value = source_state.get(source, False) and sources_available.get(source, False)
                
                checkbox_value = st.sidebar.checkbox(
                    label,
                    value=current_value,
                    disabled=is_disabled,
                    key=f"source_check_{source}"
                )
                
                source_state[source] = checkbox_value
                
                if checkbox_value and sources_available.get(source, False):
                    selected_sources.append(source)
            
            st.session_state[source_state_key] = source_state
            
            # Ensure at least one source is selected
            if not selected_sources:
                if default_source and sources_available.get(default_source, False):
                    selected_sources = [default_source]
                    source_state[default_source] = True
                    st.session_state[source_state_key] = source_state
                    st.sidebar.info(f"At least one source required. Defaulting to {default_source.upper()}.")
                else:
                    st.sidebar.warning("Please select at least one data source")
                    st.info("At least one data source must be selected.")
                    st.stop()
            
            show_dual_source = len(selected_sources) > 1
            # Use first date for time range calculation (will be updated later for multi-date support)
            selected_date = selected_dates_list[0]
            
    elif symbol_mode == "Cross Comparison":
        # Cross Comparison mode - will be implemented later
        st.sidebar.info("Cross Comparison mode - coming soon")
        st.info("Cross Comparison mode is not yet implemented.")
        st.stop()
    else:
        # Multiple Symbols mode - keep existing logic but need date selection first
        # For now, use a single date (will be updated later)
        st.sidebar.subheader("📅 Date Selection")
        
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
        
        available_dates = sorted(list(all_available_dates), reverse=True)
        
        # Set default date to 2024-01-02
        default_date = date(2024, 1, 2)
        if available_dates:
            if default_date in available_dates:
                default_value = default_date
            else:
                default_value = available_dates[0]
            min_date = min(available_dates)
            max_date = max(available_dates)
        else:
            default_value = default_date
            min_date = date(2020, 1, 1)
            max_date = date.today()
        
        # Date selection
        # Initialize from session state if exists
        if "selected_date" in st.session_state and st.session_state.selected_date in available_dates:
            default_selected = st.session_state.selected_date
        else:
            default_selected = default_value
        
        if available_dates:
            selected_date = st.sidebar.selectbox(
                "Trade Date",
                options=available_dates,
                index=available_dates.index(default_selected) if default_selected in available_dates else 0,
                format_func=lambda d: d.strftime("%Y-%m-%d"),
            )
        else:
            selected_date = st.sidebar.date_input(
                "Trade Date",
                value=default_selected,
                min_value=min_date,
                max_value=max_date,
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
        
        # Multiple symbols selection
        st.sidebar.subheader("📈 Symbol Selection")
        # Initialize session state if needed
        if "selected_symbols" not in st.session_state:
            default_symbols = ["AAPL"] if "AAPL" in all_available_symbols else all_available_symbols[:1]
            st.session_state.selected_symbols = default_symbols
        
        # Get current selection from session state
        current_selection = st.session_state.get("selected_symbols", [])
        # Filter to only include currently available symbols
        current_selection = [s for s in current_selection if s in all_available_symbols]
        if not current_selection:
            current_selection = all_available_symbols[:1]
        
        selected_symbols = st.multiselect(
            "Symbols",
            options=all_available_symbols,
            default=current_selection,
            help="Select multiple symbols.",
        )
        
        if not selected_symbols:
            st.sidebar.warning("Please select at least one symbol")
    
    # ===== STEP 2: DETERMINE DATA SOURCES =====
    # For Single Symbol mode, sources are already determined above
    # For other modes, determine sources here
    if symbol_mode != "Single Symbol":
        # Check availability for selected symbols across sources
        symbol_availability = check_symbol_availability_across_sources(
            data_root, config.data_sources, selected_date, selected_symbols, data_type="trades"
        )
        
        # Also check NBBO availability
        nbbo_symbol_availability = check_symbol_availability_across_sources(
            data_root, config.data_sources, selected_date, selected_symbols, data_type="nbbo"
        )
        
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
    
    # For Single Symbol mode, limit to maximum 2 sources
    if symbol_mode == "Single Symbol":
        if len(selected_sources) > 2:
            selected_sources = selected_sources[:2]
            st.sidebar.info(f"Maximum 2 data sources allowed. Using: {', '.join([s.upper() for s in selected_sources])}")
    
    # Store selections in session state immediately (no form submission needed)
    st.session_state.selected_symbols = selected_symbols
    st.session_state.selected_sources = selected_sources
    st.session_state.show_dual_source = show_dual_source if symbol_mode == "Single Symbol" else False
    st.session_state.symbol_mode = symbol_mode
    if symbol_mode == "Single Symbol":
        st.session_state.single_symbol_dates = selected_dates_list
    else:
        # Store selected_date for Multiple Symbols mode
        st.session_state.selected_date = selected_date
    
    # ===== STEP 4: LOAD DATA FROM SELECTED SOURCES =====
    # Load data for each source and symbol combination
    # For Single Symbol: {source: {date: {"trades": df, "nbbo": df}}}
    # For others: {source: {"trades": df, "nbbo": df}}
    data_by_source = {}
    
    with st.spinner("Loading data..."):
        if symbol_mode == "Single Symbol":
            # Single Symbol mode: load data for each date and source separately
            for source in selected_sources:
                data_by_source[source] = {}
                for load_date in selected_dates_list:
                    data_by_source[source][load_date] = {"trades": None, "nbbo": None}
                    symbol_param = selected_symbols[0]
                    
                    # Load trades for this date/source
                    trades_date = load_trades(
                        data_root, source, load_date, symbol=symbol_param, timezone=config.timezone
                    )
                    if trades_date is not None and len(trades_date) > 0:
                        data_by_source[source][load_date]["trades"] = trades_date
                    
                    # Load NBBO for this date/source
                    nbbo_date = load_nbbo(
                        data_root, source, load_date, symbol=symbol_param, timezone=config.timezone
                    )
                    if nbbo_date is not None and len(nbbo_date) > 0:
                        data_by_source[source][load_date]["nbbo"] = nbbo_date
            
            # For backward compatibility and time range calculation, combine all data into primary trades/nbbo
            # Combine all dates and sources into single DataFrames for global min/max time
            all_trades = []
            all_nbbo = []
            for source in selected_sources:
                for load_date in selected_dates_list:
                    t = data_by_source[source][load_date]["trades"]
                    n = data_by_source[source][load_date]["nbbo"]
                    if t is not None: all_trades.append(t)
                    if n is not None: all_nbbo.append(n)
            
            # Combine trades
            if all_trades:
                if len(all_trades) == 1:
                    trades = all_trades[0]
                else:
                    try:
                        # Find common columns across all DataFrames
                        common_cols = set(all_trades[0].columns)
                        for df in all_trades[1:]:
                            common_cols = common_cols.intersection(set(df.columns))
                        common_cols = sorted(list(common_cols))
                        
                        # Select only common columns and use vertical_relaxed to handle type differences
                        aligned_trades = [df.select(common_cols) for df in all_trades]
                        trades = pl.concat(aligned_trades, how="vertical_relaxed")
                    except Exception as e:
                        logger.warning(f"Error concatenating trades: {e}, using first DataFrame")
                        trades = all_trades[0]
            else:
                trades = None
            
            # Combine NBBO
            if all_nbbo:
                if len(all_nbbo) == 1:
                    nbbo = all_nbbo[0]
                else:
                    try:
                        # Find common columns across all DataFrames
                        common_cols = set(all_nbbo[0].columns)
                        for df in all_nbbo[1:]:
                            common_cols = common_cols.intersection(set(df.columns))
                        common_cols = sorted(list(common_cols))
                        
                        # Select only common columns and use vertical_relaxed to handle type differences
                        aligned_nbbo = [df.select(common_cols) for df in all_nbbo]
                        nbbo = pl.concat(aligned_nbbo, how="vertical_relaxed")
                    except Exception as e:
                        logger.warning(f"Error concatenating NBBO: {e}, using first DataFrame")
                        nbbo = all_nbbo[0]
            else:
                nbbo = None
        else:
            # Multiple Symbols or Cross Comparison mode: load data normally
            for source in selected_sources:
                data_by_source[source] = {"trades": None, "nbbo": None}
                
                # Check if data is available (different logic for Single Symbol vs Multiple Symbols)
                # For Multiple Symbols mode, use the availability check
                trades_available = any(
                    symbol_availability.get(sym, {}).get(source, False)
                    for sym in selected_symbols
                )
                nbbo_available = any(
                    nbbo_symbol_availability.get(sym, {}).get(source, False)
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
    # For Single Symbol mode, convert date keys to strings to avoid serialization issues
    # Also ensure we're storing the data correctly
    if symbol_mode == "Single Symbol":
        data_by_source_for_storage = {}
        for source in data_by_source:
            data_by_source_for_storage[source] = {}
            source_dict = data_by_source.get(source, {})
            # Handle both date objects and strings as keys
            for date_key, day_data in source_dict.items():
                # Convert date to string for storage
                if isinstance(date_key, date):
                    date_str = date_key.isoformat()
                elif isinstance(date_key, str):
                    date_str = date_key
                else:
                    date_str = str(date_key)
                data_by_source_for_storage[source][date_str] = day_data
        st.session_state.data_by_source = data_by_source_for_storage
    else:
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
            from datetime import time as dt_time
            tz = min_dt.tzinfo if min_dt.tzinfo else ZoneInfo("America/New_York")
            
            # For slider, set min_dt to 9:30 and max_dt to 16:00 on the same date to show full trading day
            # Use the date from the data but set times to trading day boundaries
            slider_date = min_dt.date()  # Use the date from min_dt
            slider_min_dt = datetime.combine(slider_date, dt_time(9, 30, 0))
            slider_max_dt = datetime.combine(slider_date, dt_time(16, 0, 0))
            
            # Add timezone if needed
            if min_dt.tzinfo:
                slider_min_dt = slider_min_dt.replace(tzinfo=min_dt.tzinfo)
                slider_max_dt = slider_max_dt.replace(tzinfo=min_dt.tzinfo)
            
            # Use the full trading day range for the slider
            min_dt = slider_min_dt
            max_dt = slider_max_dt
            
            default_start = min_dt
            default_duration_minutes = 390  # 6.5 hours = full trading day (9:30 AM to 4:00 PM)
            
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
            # Ensure values are within bounds
            slider_start = max(min_dt, min(input_start, max_dt))
            slider_end = max(min_dt, min(input_end, max_dt))
            if slider_end <= slider_start:
                slider_end = min(slider_start + timedelta(minutes=1), max_dt)
            
            time_range = st.sidebar.slider(
                "Time Range (Slider)",
                min_value=min_dt,
                max_value=max_dt,
                value=(slider_start, slider_end),
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
    
    # Filters section
    st.sidebar.header("🔍 Filters")
    
    # Initialize filter value in session state if not exists
    if "filter_min_trade_size" not in st.session_state:
        st.session_state.filter_min_trade_size = 0
    
    min_trade_size = st.sidebar.number_input(
        "Min Trade Size",
        min_value=0,
        value=st.session_state.filter_min_trade_size,
        step=1,
        key="filter_min_trade_size_input",
        help="Filter trades by minimum size (0 = show all)",
    )
    
    # Update session state with current input value
    st.session_state.filter_min_trade_size = min_trade_size
    
    # Apply button outside the filters panel
    apply_button = st.sidebar.button("Apply Changes", type="primary")
    
    # Store form state in session state only when button is clicked
    if apply_button or "viz_settings" not in st.session_state:
        st.session_state.viz_settings = {
            "show_trades": True,  # Always show trades
            "show_nbbo": True,    # Always show NBBO
            "show_mid_price": False,  # Default to False
            "show_vwap": False,  # Default to False
            "min_trade_size": st.session_state.filter_min_trade_size,
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
    
    # Helper function to get data by date, handling both date objects and date strings
    # (defined here so it's available for all visualization sections)
    def get_data_by_date(source_data: dict, target_date: date) -> dict:
        """Get data for a specific date, handling both date object and string keys."""
        if not source_data:
            return {}
        
        # Normalize target_date to ensure it's a date object
        if isinstance(target_date, str):
            try:
                target_date = date.fromisoformat(target_date)
            except (ValueError, AttributeError):
                return {}
        
        # Try direct date object key first
        if target_date in source_data:
            return source_data[target_date]
        
        # Try date string key
        date_str = target_date.isoformat()
        if date_str in source_data:
            return source_data[date_str]
        
        # Try to find matching date by converting all keys
        for key, value in source_data.items():
            if isinstance(key, date) and key == target_date:
                return value
            elif isinstance(key, str):
                try:
                    key_date = date.fromisoformat(key)
                    if key_date == target_date:
                        return value
                except (ValueError, AttributeError):
                    pass
        
        return {}
    
    # Helper function to get day-specific time range (defined at top level for use in all sections)
    def get_day_time_range(plot_date, start_time, end_time):
        """Extract time portion from start_time/end_time and apply to plot_date."""
        from datetime import datetime
        start_time_only = start_time.time()
        end_time_only = end_time.time()
        
        day_start_time = datetime.combine(plot_date, start_time_only)
        day_end_time = datetime.combine(plot_date, end_time_only)
        
        # Handle timezone if start_time has one
        if start_time.tzinfo is not None:
            day_start_time = day_start_time.replace(tzinfo=start_time.tzinfo)
            day_end_time = day_end_time.replace(tzinfo=end_time.tzinfo)
        
        return day_start_time, day_end_time
    
    # Check if we have any data
    symbol_mode = st.session_state.get("symbol_mode", "Single Symbol")
    if symbol_mode == "Single Symbol":
        # For Single Symbol mode, check nested structure: data_by_source[source][date]
        has_any_data = False
        selected_dates_list = st.session_state.get("single_symbol_dates", [])
        for source in selected_sources:
            source_data = data_by_source.get(source, {})
            if isinstance(source_data, dict):
                for date_key in selected_dates_list:
                    day_data = get_data_by_date(source_data, date_key)
                    if day_data.get("trades") is not None or day_data.get("nbbo") is not None:
                        has_any_data = True
                        break
                if has_any_data:
                    break
    else:
        # For other modes, check flat structure: data_by_source[source]
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
    
    if symbol_mode == "Single Symbol":
        # Single Symbol mode: Summary by date and source
        selected_dates_list_raw = st.session_state.get("single_symbol_dates", [])
        # Normalize dates - convert strings to date objects if needed
        selected_dates_list = []
        for d in selected_dates_list_raw:
            if isinstance(d, str):
                try:
                    selected_dates_list.append(date.fromisoformat(d))
                except (ValueError, AttributeError):
                    continue
            elif isinstance(d, date):
                selected_dates_list.append(d)
        
        table_data = []
        for plot_date in selected_dates_list:
            for source in selected_sources:
                source_data = data_by_source.get(source, {})
                day_data = get_data_by_date(source_data, plot_date)
                source_trades = day_data.get("trades")
                source_nbbo = day_data.get("nbbo")
                
                row = {
                    "Date": plot_date.strftime("%Y-%m-%d"),
                    "Symbol": selected_symbols[0],
                    "Source": source.upper(),
                }
                
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
            st.dataframe(df, width='stretch', hide_index=True)
    elif show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
        # Single symbol with dual source (legacy) - create a table
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
            st.dataframe(df, width='stretch', hide_index=True)
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
            st.dataframe(df, width='stretch', hide_index=True)
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
    
    # Get current mode and data
    symbol_mode = st.session_state.get("symbol_mode", "Single Symbol")
    show_dual_source = st.session_state.get("show_dual_source", False)
    selected_sources = st.session_state.get("selected_sources", [])
    data_by_source = st.session_state.get("data_by_source", {})
    
    # Filter data by time range for dual-source mode (needed for all panels)
    filtered_sources_data = {}
    if show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
        all_prices = []  # Collect all price values for shared y-axis range
        
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
    
    # For Single Symbol mode, check individual day data instead of combined data
    if symbol_mode == "Single Symbol":
        # Check if we have any data for any day
        selected_dates_list_raw = st.session_state.get("single_symbol_dates", [])
        has_any_data = False
        for source in selected_sources:
            source_data = data_by_source.get(source, {})
            if source_data:
                # Check if any day has data
                for date_key in source_data.keys():
                    day_data = source_data.get(date_key, {})
                    if day_data.get("trades") is not None or day_data.get("nbbo") is not None:
                        has_any_data = True
                        break
                if has_any_data:
                    break
        
        if not has_any_data:
            st.warning("No data available for the selected dates and sources.")
        else:
            with st.spinner("Generating visualization..."):
                try:
                    # Single Symbol mode: plots by day
                    selected_dates_list_raw = st.session_state.get("single_symbol_dates", [])
                    # Normalize dates - convert strings to date objects if needed
                    selected_dates_list = []
                    for d in selected_dates_list_raw:
                        if isinstance(d, str):
                            try:
                                selected_dates_list.append(date.fromisoformat(d))
                            except (ValueError, AttributeError):
                                continue
                        elif isinstance(d, date):
                            selected_dates_list.append(d)
                    
                    # Remove duplicates while preserving order
                    seen = set()
                    unique_dates = []
                    for d in selected_dates_list:
                        date_key = d.isoformat() if isinstance(d, date) else str(d)
                        if date_key not in seen:
                            seen.add(date_key)
                            unique_dates.append(d)
                    selected_dates_list = unique_dates
                    
                    for plot_date in selected_dates_list:
                        date_str = plot_date.isoformat()  # Define date_str for both paths
                        date_formatted = plot_date.strftime('%Y/%m/%d')  # Format for plot caption
                        
                        if len(selected_sources) == 2:
                            # Two sources: side-by-side plots for each day
                            cols = st.columns(2)
                            for idx, source in enumerate(selected_sources[:2]):  # Only use first 2 sources
                                with cols[idx]:
                                    source_data = data_by_source.get(source, {})
                                    # Since we store dates as strings in session state, use direct string access
                                    day_data = source_data.get(date_str, {})
                                    
                                    # Fallback: try get_data_by_date if direct access didn't work
                                    if not day_data or (day_data.get("trades") is None and day_data.get("nbbo") is None):
                                        day_data = get_data_by_date(source_data, plot_date)
                                    
                                    source_trades = day_data.get("trades") if day_data else None
                                    source_nbbo = day_data.get("nbbo") if day_data else None
                                    
                                    # Filter by time range - apply time portion to the current plot_date
                                    if source_trades is not None and len(source_trades) > 0 and start_time is not None and end_time is not None:
                                        try:
                                            day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                            start_ts = int(day_start_time.timestamp() * 1_000_000)
                                            end_ts = int(day_end_time.timestamp() * 1_000_000)
                                            source_trades = source_trades.filter(
                                                (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                                (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                            )
                                        except Exception as e:
                                            pass
                                    
                                    if source_nbbo is not None and len(source_nbbo) > 0 and start_time is not None and end_time is not None:
                                        try:
                                            day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                            start_ts = int(day_start_time.timestamp() * 1_000_000)
                                            end_ts = int(day_end_time.timestamp() * 1_000_000)
                                            source_nbbo = source_nbbo.filter(
                                                (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                                (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                            )
                                        except Exception as e:
                                            pass
                                    
                                    if (source_trades is not None and len(source_trades) > 0) or (source_nbbo is not None and len(source_nbbo) > 0):
                                        # Use day-specific time range for x-axis
                                        day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                        # For dual source, use unique uirevision per plot to avoid conflicts
                                        unique_plot_id = f"price_{selected_symbols[0]}_{source}_{plot_date}_col{idx}"
                                        fig_price = plot_price_panel(
                                            source_trades,
                                            source_nbbo,
                                            show_trades=show_trades,
                                            show_nbbo=show_nbbo,
                                            show_mid_price=show_mid_price,
                                            show_vwap=show_vwap,
                                            symbol=f"{selected_symbols[0]}-{date_formatted} ({source.upper()})",
                                            start_time=day_start_time,
                                            end_time=day_end_time,
                                            min_trade_size=min_trade_size,
                                            uirevision=unique_plot_id,  # Use unique uirevision per plot
                                        )
                                        st.plotly_chart(fig_price, width='stretch', key=unique_plot_id)
                                    else:
                                        st.info(f"No data for {source.upper()} on {plot_date}")
                        else:
                            # Single source: one plot per day in rows
                            source = selected_sources[0] if selected_sources else None
                            if source:
                                source_data = data_by_source.get(source, {})
                                # Since we store dates as strings in session state, use direct string access
                                day_data = source_data.get(date_str, {})
                                
                                # Fallback: try get_data_by_date if direct access didn't work
                                if not day_data or (day_data.get("trades") is None and day_data.get("nbbo") is None):
                                    day_data = get_data_by_date(source_data, plot_date)
                                
                                source_trades = day_data.get("trades") if day_data else None
                                source_nbbo = day_data.get("nbbo") if day_data else None
                                
                                # Filter by time range - apply time portion to the current plot_date
                                if source_trades is not None and len(source_trades) > 0 and start_time is not None and end_time is not None:
                                    try:
                                        day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                        start_ts = int(day_start_time.timestamp() * 1_000_000)
                                        end_ts = int(day_end_time.timestamp() * 1_000_000)
                                        source_trades = source_trades.filter(
                                            (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                            (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                        )
                                    except Exception as e:
                                        pass
                                
                                if source_nbbo is not None and len(source_nbbo) > 0 and start_time is not None and end_time is not None:
                                    try:
                                        day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                        start_ts = int(day_start_time.timestamp() * 1_000_000)
                                        end_ts = int(day_end_time.timestamp() * 1_000_000)
                                        source_nbbo = source_nbbo.filter(
                                            (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                            (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                        )
                                    except Exception as e:
                                        pass
                                
                                # Debug: Check if we have data after filtering
                                trades_after = len(source_trades) if source_trades is not None else 0
                                nbbo_after = len(source_nbbo) if source_nbbo is not None else 0
                                
                                if (source_trades is not None and len(source_trades) > 0) or (source_nbbo is not None and len(source_nbbo) > 0):
                                    # Use day-specific time range for x-axis
                                    day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                    # Use unique uirevision for each plot
                                    unique_plot_id = f"{selected_symbols[0]}_{source}_{plot_date}"
                                    fig_price = plot_price_panel(
                                        source_trades,
                                        source_nbbo,
                                        show_trades=show_trades,
                                        show_nbbo=show_nbbo,
                                        show_mid_price=show_mid_price,
                                        show_vwap=show_vwap,
                                        symbol=f"{selected_symbols[0]}-{date_formatted}",
                                        start_time=day_start_time,
                                        end_time=day_end_time,
                                        min_trade_size=min_trade_size,
                                        uirevision=f"price_{unique_plot_id}",  # Unique uirevision per plot with prefix
                                    )
                                    st.plotly_chart(fig_price, width='stretch', key=f"price_{unique_plot_id}")
                                else:
                                    st.info(f"No data for {source.upper()} on {plot_date}")
                except Exception as e:
                    st.error(f"Error creating price panel: {e}")
                    import traceback
                    st.code(traceback.format_exc())
    elif trades is not None or nbbo is not None:
        # For other modes (Multiple Symbols, etc.), use the old logic
        # Check if we have any data to visualize
        has_trades = trades_for_viz is not None and len(trades_for_viz) > 0
        has_nbbo = nbbo_for_viz is not None and len(nbbo_for_viz) > 0
        
        if not has_trades and not has_nbbo:
            st.warning("No data available after filtering. Try adjusting the time range or symbol selection.")
        else:
            with st.spinner("Generating visualization..."):
                try:
                    # If dual source mode and single symbol (legacy - for non-Single Symbol mode)
                    if show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
                        # Dual source mode - show one plot per source in separate rows
                        for source in selected_sources:
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
                                symbol=f"{selected_symbols[0]} ({source.upper()})",
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
                        # Multiple symbols mode - show one plot per symbol
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
                            
                            symbol_figs.append((symbol, fig))
                        
                        # Display plots in rows (one per symbol)
                        if symbol_figs:
                            for symbol, fig in symbol_figs:
                                if len(fig.data) > 0:
                                    st.plotly_chart(fig, width='stretch')
                                else:
                                    st.info(f"No data for {symbol}")
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
    
    # Spread panel
    st.header("Spread")
    
    if symbol_mode == "Single Symbol":
        # Single Symbol mode: plots by day
        selected_dates_list_raw = st.session_state.get("single_symbol_dates", [])
        # Normalize dates - convert strings to date objects if needed
        selected_dates_list = []
        for d in selected_dates_list_raw:
            if isinstance(d, str):
                try:
                    selected_dates_list.append(date.fromisoformat(d))
                except (ValueError, AttributeError):
                    continue
            elif isinstance(d, date):
                selected_dates_list.append(d)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_dates = []
        for d in selected_dates_list:
            date_key = d.isoformat() if isinstance(d, date) else str(d)
            if date_key not in seen:
                seen.add(date_key)
                unique_dates.append(d)
        selected_dates_list = unique_dates
        
        for plot_date in selected_dates_list:
            date_str = plot_date.isoformat()
            date_formatted = plot_date.strftime('%Y/%m/%d')  # Format for plot caption
            
            if len(selected_sources) == 2:
                # Two sources: side-by-side plots for each day
                cols = st.columns(2)
                for idx, source in enumerate(selected_sources[:2]):  # Only use first 2 sources
                    with cols[idx]:
                        source_data = data_by_source.get(source, {})
                        # Since we store dates as strings in session state, use direct string access
                        day_data = source_data.get(date_str, {})
                        
                        # Fallback: try get_data_by_date if direct access didn't work
                        if not day_data or day_data.get("nbbo") is None:
                            day_data = get_data_by_date(source_data, plot_date)
                        
                        source_nbbo = day_data.get("nbbo") if day_data else None
                        
                        # Filter by time range - apply time portion to the current plot_date
                        if source_nbbo is not None and len(source_nbbo) > 0 and start_time is not None and end_time is not None:
                            try:
                                day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                start_ts = int(day_start_time.timestamp() * 1_000_000)
                                end_ts = int(day_end_time.timestamp() * 1_000_000)
                                source_nbbo = source_nbbo.filter(
                                    (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                    (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                )
                            except Exception: pass
                        
                        if source_nbbo is not None and len(source_nbbo) > 0:
                            # For dual source, use unique uirevision per plot to avoid conflicts
                            unique_plot_id = f"spread_{selected_symbols[0]}_{source}_{plot_date}_col{idx}"
                            fig_spread = plot_spread_bps_timeline(
                                source_nbbo,
                                show_churn=False,
                                symbol=f"{selected_symbols[0]}-{date_formatted} ({source.upper()})",
                                uirevision=unique_plot_id,  # Use unique uirevision per plot
                            )
                            st.plotly_chart(fig_spread, width='stretch', key=unique_plot_id)
                        else:
                            st.info(f"No NBBO data for {source.upper()} on {plot_date}")
            else:
                # Single source: one plot per day in rows
                source = selected_sources[0] if selected_sources else None
                if source:
                    source_data = data_by_source.get(source, {})
                    day_data = source_data.get(date_str, {})
                    
                    # Fallback: try get_data_by_date if direct access didn't work
                    if not day_data or day_data.get("nbbo") is None:
                        day_data = get_data_by_date(source_data, plot_date)
                    
                    source_nbbo = day_data.get("nbbo") if day_data else None
                    
                    # Filter by time range - apply time portion to the current plot_date
                    if source_nbbo is not None and len(source_nbbo) > 0 and start_time is not None and end_time is not None:
                        try:
                            day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                            start_ts = int(day_start_time.timestamp() * 1_000_000)
                            end_ts = int(day_end_time.timestamp() * 1_000_000)
                            source_nbbo = source_nbbo.filter(
                                (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                            )
                        except Exception: pass
                    
                    if source_nbbo is not None and len(source_nbbo) > 0:
                        # Use unique uirevision for each plot
                        # Include source in the ID to make it more unique
                        unique_plot_id = f"spread_{selected_symbols[0]}_{source}_{plot_date}"
                        fig_spread = plot_spread_bps_timeline(
                            source_nbbo,
                            show_churn=False,
                            symbol=f"{selected_symbols[0]}-{date_formatted}" if selected_symbols else None,
                            uirevision=unique_plot_id,  # Unique uirevision per plot
                        )
                        st.plotly_chart(fig_spread, width='stretch', key=unique_plot_id)
                    else:
                        st.info(f"No NBBO data for {source.upper()} on {plot_date}")
    elif show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
        # Dual source mode (legacy - for non-Single Symbol mode) - show side-by-side for each source
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
        else:
            # Single symbol
            if nbbo is not None and len(nbbo) > 0:
                fig_spread_bps = plot_spread_bps_timeline(
                    nbbo,
                    show_churn=False,
                    symbol=selected_symbols[0] if len(selected_symbols) == 1 else None,
                )
                st.plotly_chart(fig_spread_bps, width='stretch')
            else:
                st.info("NBBO data not available")
    
    # Churn panel
    st.header("Churn")
    
    if symbol_mode == "Single Symbol":
        # Single Symbol mode: plots by day
        selected_dates_list_raw = st.session_state.get("single_symbol_dates", [])
        # Normalize dates - convert strings to date objects if needed
        selected_dates_list = []
        for d in selected_dates_list_raw:
            if isinstance(d, str):
                try:
                    selected_dates_list.append(date.fromisoformat(d))
                except (ValueError, AttributeError):
                    continue
            elif isinstance(d, date):
                selected_dates_list.append(d)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_dates = []
        for d in selected_dates_list:
            date_key = d.isoformat() if isinstance(d, date) else str(d)
            if date_key not in seen:
                seen.add(date_key)
                unique_dates.append(d)
        selected_dates_list = unique_dates
        
        for plot_date in selected_dates_list:
            date_str = plot_date.isoformat()
            date_formatted = plot_date.strftime('%Y/%m/%d')  # Format for plot caption
            
            if len(selected_sources) == 2:
                # Two sources: side-by-side plots for each day
                cols = st.columns(2)
                for idx, source in enumerate(selected_sources[:2]):  # Only use first 2 sources
                    with cols[idx]:
                        source_data = data_by_source.get(source, {})
                        # Since we store dates as strings in session state, use direct string access
                        day_data = source_data.get(date_str, {})
                        
                        # Fallback: try get_data_by_date if direct access didn't work
                        if not day_data or day_data.get("nbbo") is None:
                            day_data = get_data_by_date(source_data, plot_date)
                        
                        source_nbbo = day_data.get("nbbo") if day_data else None
                        
                        # Filter by time range - apply time portion to the current plot_date
                        if source_nbbo is not None and len(source_nbbo) > 0 and start_time is not None and end_time is not None:
                            try:
                                day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                start_ts = int(day_start_time.timestamp() * 1_000_000)
                                end_ts = int(day_end_time.timestamp() * 1_000_000)
                                source_nbbo = source_nbbo.filter(
                                    (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                    (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                )
                            except Exception: pass
                        
                        if source_nbbo is not None and len(source_nbbo) > 0:
                            fig_churn = plot_churn_bar_chart(
                                source_nbbo,
                                symbol=f"{selected_symbols[0]}-{date_formatted} ({source.upper()})",
                            )
                            st.plotly_chart(fig_churn, width='stretch', key=f"churn_{selected_symbols[0]}_{source}_{plot_date}")
                        else:
                            st.info(f"No NBBO data for {source.upper()} on {plot_date}")
            else:
                # Single source: one plot per day in rows
                source = selected_sources[0] if selected_sources else None
                if source:
                    source_data = data_by_source.get(source, {})
                    day_data = source_data.get(date_str, {})
                    
                    # Fallback: try get_data_by_date if direct access didn't work
                    if not day_data or day_data.get("nbbo") is None:
                        day_data = get_data_by_date(source_data, plot_date)
                    
                    source_nbbo = day_data.get("nbbo") if day_data else None
                    
                    # Filter by time range - apply time portion to the current plot_date
                    if source_nbbo is not None and len(source_nbbo) > 0 and start_time is not None and end_time is not None:
                        try:
                            day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                            start_ts = int(day_start_time.timestamp() * 1_000_000)
                            end_ts = int(day_end_time.timestamp() * 1_000_000)
                            source_nbbo = source_nbbo.filter(
                                (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                            )
                        except Exception: pass
                    
                    if source_nbbo is not None and len(source_nbbo) > 0:
                        fig_churn = plot_churn_bar_chart(
                            source_nbbo,
                            symbol=f"{selected_symbols[0]}-{date_formatted}" if selected_symbols else None,
                        )
                        st.plotly_chart(fig_churn, width='stretch', key=f"churn_{selected_symbols[0]}_{source}_{plot_date}")
                    else:
                        st.info(f"No NBBO data for {source.upper()} on {plot_date}")
    elif show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
        # Dual source mode (legacy - for non-Single Symbol mode) - show side-by-side for each source
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
            # Single symbol
            if nbbo is not None and len(nbbo) > 0:
                fig_churn = plot_churn_bar_chart(
                    nbbo,
                    symbol=selected_symbols[0] if len(selected_symbols) == 1 else None,
                )
                st.plotly_chart(fig_churn, width='stretch')
            else:
                st.info("NBBO data not available")
    
    # Tables
    st.header("Tables")
    
    if symbol_mode == "Single Symbol":
        # Single Symbol mode: Tables by date
        selected_dates_list_raw = st.session_state.get("single_symbol_dates", [])
        # Normalize dates - convert strings to date objects if needed
        selected_dates_list = []
        for d in selected_dates_list_raw:
            if isinstance(d, str):
                try:
                    selected_dates_list.append(date.fromisoformat(d))
                except (ValueError, AttributeError):
                    continue
            elif isinstance(d, date):
                selected_dates_list.append(d)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_dates = []
        for d in selected_dates_list:
            date_key = d.isoformat() if isinstance(d, date) else str(d)
            if date_key not in seen:
                seen.add(date_key)
                unique_dates.append(d)
        selected_dates_list = unique_dates
        
        for plot_date in selected_dates_list:
            date_str = plot_date.isoformat()
            date_formatted = plot_date.strftime('%Y/%m/%d')
            
            st.caption(f"{plot_date.strftime('%Y-%m-%d')}")
            
            if len(selected_sources) == 2:
                # Two sources: side-by-side tables for each day
                # Trade Summary - Largest trades
                st.subheader("Trade Summary - Largest Trades")
                trade_summary_cols = st.columns(2)
                for idx, source in enumerate(selected_sources[:2]):
                    with trade_summary_cols[idx]:
                        st.caption(f"{source.upper()}")
                        source_data = data_by_source.get(source, {})
                        day_data = get_data_by_date(source_data, plot_date)
                        source_trades = day_data.get("trades")
                        
                        # Filter by time range
                        if source_trades is not None and len(source_trades) > 0 and start_time is not None and end_time is not None:
                            try:
                                day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                start_ts = int(day_start_time.timestamp() * 1_000_000)
                                end_ts = int(day_end_time.timestamp() * 1_000_000)
                                source_trades = source_trades.filter(
                                    (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                    (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                )
                            except Exception:
                                pass
                        
                        if source_trades is not None and len(source_trades) > 0 and "size" in source_trades.columns:
                            try:
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
                churn_table_cols = st.columns(2)
                for idx, source in enumerate(selected_sources[:2]):
                    with churn_table_cols[idx]:
                        st.caption(f"{source.upper()}")
                        source_data = data_by_source.get(source, {})
                        day_data = get_data_by_date(source_data, plot_date)
                        source_nbbo = day_data.get("nbbo")
                        
                        # Filter by time range
                        if source_nbbo is not None and len(source_nbbo) > 0 and start_time is not None and end_time is not None:
                            try:
                                day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                                start_ts = int(day_start_time.timestamp() * 1_000_000)
                                end_ts = int(day_end_time.timestamp() * 1_000_000)
                                source_nbbo = source_nbbo.filter(
                                    (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                    (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                                )
                            except Exception:
                                pass
                        
                        if source_nbbo is not None and len(source_nbbo) > 0:
                            try:
                                highest_churn = get_highest_churn_minutes(source_nbbo, top_n=20)
                                highest_churn_pd = highest_churn.to_pandas()
                                
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
                # Single source: one table per day
                source = selected_sources[0] if selected_sources else None
                if source:
                    source_data = data_by_source.get(source, {})
                    day_data = get_data_by_date(source_data, plot_date)
                    source_trades = day_data.get("trades")
                    source_nbbo = day_data.get("nbbo")
                    
                    # Filter by time range
                    if source_trades is not None and len(source_trades) > 0 and start_time is not None and end_time is not None:
                        try:
                            day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                            start_ts = int(day_start_time.timestamp() * 1_000_000)
                            end_ts = int(day_end_time.timestamp() * 1_000_000)
                            source_trades = source_trades.filter(
                                (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                            )
                        except Exception:
                            pass
                    
                    if source_nbbo is not None and len(source_nbbo) > 0 and start_time is not None and end_time is not None:
                        try:
                            day_start_time, day_end_time = get_day_time_range(plot_date, start_time, end_time)
                            start_ts = int(day_start_time.timestamp() * 1_000_000)
                            end_ts = int(day_end_time.timestamp() * 1_000_000)
                            source_nbbo = source_nbbo.filter(
                                (pl.col("ts_event").dt.timestamp("us") >= start_ts) &
                                (pl.col("ts_event").dt.timestamp("us") <= end_ts)
                            )
                        except Exception:
                            pass
                    
                    # Trade Summary - Largest trades
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        if source_trades is not None and len(source_trades) > 0:
                            st.subheader("Trade Summary - Largest Trades")
                            try:
                                if "size" in source_trades.columns:
                                    largest_trades = source_trades.sort("size", descending=True).head(100).select([
                                        "ts_event",
                                        "symbol",
                                        "price",
                                        "size",
                                    ]).to_pandas()
                                else:
                                    largest_trades = source_trades.select([
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
                                    st.caption(f"Top 100 largest trades. Total: {len(source_trades):,}")
                                else:
                                    st.info("No trades to display")
                            except Exception as e:
                                st.error(f"Error displaying trades: {e}")
                        else:
                            st.info("Trades data required")
                    
                    with col2:
                        if source_nbbo is not None and len(source_nbbo) > 0:
                            st.subheader("Highest Churn Minutes")
                            try:
                                highest_churn = get_highest_churn_minutes(source_nbbo, top_n=20)
                                highest_churn_pd = highest_churn.to_pandas()
                                
                                if len(highest_churn_pd) > 0:
                                    highest_churn_pd["time_bucket"] = highest_churn_pd["time_bucket"].dt.strftime("%H:%M:%S")
                                    highest_churn_pd = highest_churn_pd.rename(columns={"time_bucket": "Time", "churn": "Updates"})
                                    
                                    st.dataframe(
                                        highest_churn_pd,
                                        width='stretch',
                                        hide_index=True,
                                    )
                                    st.caption(f"Top 20 minutes with highest quote churn. Total NBBO updates: {len(source_nbbo):,}")
                                else:
                                    st.info("No churn data available")
                            except Exception as e:
                                st.error(f"Error calculating churn: {e}")
                        else:
                            st.info("NBBO data required")
    
    elif show_dual_source and len(selected_symbols) == 1 and len(selected_sources) >= 2:
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
