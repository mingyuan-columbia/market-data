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
)
from streamlit_app.data_loader import load_trades, load_nbbo
from streamlit_app.visualizations import (
    plot_price_panel,
    plot_spread_bps_timeline,
    plot_churn_bar_chart,
    plot_spread_histogram,
    plot_slippage_histogram,
    get_worst_slippage_trades,
    get_highest_churn_minutes,
    enrich_trades_with_nbbo,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
    # Get password from secrets or use default if not set
    try:
        expected_password = st.secrets.get("password", "")
    except Exception:
        # If secrets file doesn't exist, use empty string (no password protection)
        expected_password = ""
    
    # If no password is configured, allow access immediately
    if expected_password == "":
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
    
    # Data source selection
    available_sources = list(config.data_sources.keys())
    if not available_sources:
        st.error("No data sources configured. Please check config.yaml")
        st.stop()
    
    selected_source = st.sidebar.selectbox(
        "Data Source",
        options=available_sources,
        index=0,
    )
    
    # Get data root from config
    data_root = config.data_root
    
    # Date selection - find available dates first
    available_dates = []
    if selected_source in config.data_sources:
        # Find available dates for the selected source
        available_dates = find_available_dates(
            data_root,
            selected_source,
            data_type="trades",
            max_days=365,  # Look back up to 1 year
        )
        # Also check NBBO dates
        nbbo_dates = find_available_dates(
            data_root,
            selected_source,
            data_type="nbbo",
            max_days=365,
        )
        # Combine and deduplicate
        all_dates = set(available_dates + nbbo_dates)
        available_dates = sorted(list(all_dates), reverse=True)  # Most recent first
    
    # Set default date
    default_date = date(2024, 1, 2)
    if available_dates:
        # Use most recent available date as default, or 2024-01-02 if it exists
        if default_date in available_dates:
            default_value = default_date
        else:
            default_value = available_dates[0]  # Most recent
        min_date = min(available_dates)
        max_date = max(available_dates)
    else:
        default_value = default_date
        min_date = date(2020, 1, 1)  # Fallback range
        max_date = date.today()
    
    # Date selection with available dates
    if available_dates:
        selected_date = st.sidebar.selectbox(
            "Trade Date",
            options=available_dates,
            index=available_dates.index(default_value) if default_value in available_dates else 0,
            format_func=lambda d: d.strftime("%Y-%m-%d"),
        )
    else:
        # Fallback to date_input if no dates found
        selected_date = st.sidebar.date_input(
            "Trade Date",
            value=default_value,
            min_value=min_date,
            max_value=max_date,
        )
    
    # Check data availability
    trades_available = check_data_available(
        data_root, selected_source, selected_date, data_type="trades"
    )
    nbbo_available = check_data_available(
        data_root, selected_source, selected_date, data_type="nbbo"
    )
    
    # Show availability status
    if trades_available or nbbo_available:
        st.sidebar.success("✓ Data available")
    else:
        st.sidebar.warning("⚠ Data not available for this date/source")
        
        # Suggest alternatives
        suggestions = suggest_alternatives(
            data_root,
            config.data_sources,
            selected_date,
            selected_source,
            data_type="trades",
        )
        
        if suggestions["closest_date"]:
            st.sidebar.info(
                f"💡 Closest available date: {suggestions['closest_date']}"
            )
        
        if suggestions["alternative_sources"]:
            st.sidebar.info("💡 Alternative sources:")
            for alt in suggestions["alternative_sources"]:
                st.sidebar.write(f"  - {alt['source']}: {alt['date']}")
        
        # Don't proceed if no data
        if not trades_available and not nbbo_available:
            st.info("Please select a date/source with available data to continue.")
            st.stop()
    
    # Symbol selection
    if trades_available or nbbo_available:
        available_symbols = find_available_symbols(
            data_root, selected_source, selected_date, data_type="trades"
        )
        
        if not available_symbols:
            # Try NBBO if trades not available
            available_symbols = find_available_symbols(
                data_root, selected_source, selected_date, data_type="nbbo"
            )
        
        if available_symbols:
            # Default to "AAPL" if available, otherwise "All"
            default_index = 0
            if "AAPL" in available_symbols:
                default_index = available_symbols.index("AAPL") + 1  # +1 because "All" is first
            
            selected_symbol = st.sidebar.selectbox(
                "Symbol",
                options=["All"] + available_symbols,
                index=default_index,
            )
        else:
            selected_symbol = None
            st.sidebar.warning("No symbols found for this date")
    else:
        selected_symbol = None
    
    # Load data first to get time range
    symbol_filter = None if selected_symbol == "All" else selected_symbol
    
    with st.spinner("Loading data..."):
        trades = None
        nbbo = None
        
        if trades_available:
            trades = load_trades(
                data_root,
                selected_source,
                selected_date,
                symbol=symbol_filter,
                timezone=config.timezone,
            )
            if trades is not None and len(trades) > 0:
                st.success(f"✓ Loaded {len(trades):,} trades")
        
        if nbbo_available:
            nbbo = load_nbbo(
                data_root,
                selected_source,
                selected_date,
                symbol=symbol_filter,
                timezone=config.timezone,
            )
            if nbbo is not None and len(nbbo) > 0:
                st.success(f"✓ Loaded {len(nbbo):,} NBBO records")
    
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
            
            time_range = st.sidebar.slider(
                "Select time range",
                min_value=min_dt,
                max_value=max_dt,
                value=(min_dt, max_dt),
                format="HH:mm:ss",
            )
            start_time, end_time = time_range
            
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
    
    # Toggle overlays
    st.sidebar.header("🎛️ Overlays")
    show_trades = st.sidebar.checkbox("Show Trades", value=True)
    show_nbbo = st.sidebar.checkbox("Show NBBO", value=True)
    show_vwap = st.sidebar.checkbox("Show VWAP", value=False)
    show_churn = st.sidebar.checkbox("Show Churn", value=False)
    
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
    
    # Sample data for visualization if too large (to prevent freezing)
    MAX_TRADES_FOR_VIS = 100_000  # Limit to 100k trades for visualization
    MAX_NBBO_FOR_VIS = 500_000    # Limit to 500k NBBO records for visualization
    
    if trades is not None and len(trades) > MAX_TRADES_FOR_VIS:
        st.info(f"⚠️ Sampling {MAX_TRADES_FOR_VIS:,} trades from {len(trades):,} for visualization (too many points)")
        trades = trades.sample(n=MAX_TRADES_FOR_VIS, seed=42)
    
    if nbbo is not None and len(nbbo) > MAX_NBBO_FOR_VIS:
        st.info(f"⚠️ Sampling {MAX_NBBO_FOR_VIS:,} NBBO records from {len(nbbo):,} for visualization (too many points)")
        # For NBBO, we want to keep time series continuity, so sample evenly
        step = len(nbbo) // MAX_NBBO_FOR_VIS
        nbbo = nbbo[::step].head(MAX_NBBO_FOR_VIS)
    
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
    
    if not trades_available and not nbbo_available:
        st.warning("No data available for the selected date and source.")
        return
    
    # Show summary stats
    col1, col2, col3, col4 = st.columns(4)
    
    if trades is not None:
        col1.metric("Total Trades", f"{len(trades):,}")
        if "size" in trades.columns:
            total_volume = trades["size"].sum()
            col2.metric("Total Volume", f"{total_volume:,}")
        if selected_symbol and selected_symbol != "All":
            unique_symbols = trades["symbol"].n_unique()
            col3.metric("Symbols", unique_symbols)
    else:
        col1.metric("Total Trades", "N/A")
    
    if nbbo is not None:
        col4.metric("NBBO Records", f"{len(nbbo):,}")
    else:
        col4.metric("NBBO Records", "N/A")
    
    # Main panels
    st.header("Price Panel")
    
    # Debug info
    if trades is not None:
        st.caption(f"Trades after filtering: {len(trades):,} rows")
        if len(trades) > 0:
            st.caption(f"  - Time range: {trades['ts_event'].min()} to {trades['ts_event'].max()}")
    if nbbo is not None:
        st.caption(f"NBBO after filtering: {len(nbbo):,} rows")
        if len(nbbo) > 0:
            st.caption(f"  - Time range: {nbbo['ts_event'].min()} to {nbbo['ts_event'].max()}")
    
    if trades is not None or nbbo is not None:
        # Check if we have any data to visualize
        has_trades = trades_for_viz is not None and len(trades_for_viz) > 0
        has_nbbo = nbbo_for_viz is not None and len(nbbo_for_viz) > 0
        
        if not has_trades and not has_nbbo:
            st.warning("No data available after filtering. Try adjusting the time range or symbol selection.")
        else:
            with st.spinner("Generating visualization..."):
                try:
                    fig_price = plot_price_panel(
                        trades_for_viz,
                        nbbo_for_viz,
                        show_trades=show_trades,
                        show_nbbo=show_nbbo,
                        show_vwap=show_vwap,
                        symbol=selected_symbol if selected_symbol != "All" else None,
                    )
                    
                    # Check if figure has any traces
                    if len(fig_price.data) == 0:
                        st.warning("Visualization created but contains no data traces. Check your overlay settings (Show Trades, Show NBBO).")
                        st.info(f"Show Trades: {show_trades}, Show NBBO: {show_nbbo}, Has Trades: {has_trades}, Has NBBO: {has_nbbo}")
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
    
    col1, col2 = st.columns(2)
    
    with col1:
        if nbbo is not None and len(nbbo) > 0:
            st.subheader("Spread (bps) Over Time")
            fig_spread_bps = plot_spread_bps_timeline(
                nbbo,
                show_churn=show_churn,
                symbol=selected_symbol if selected_symbol != "All" else None,
            )
            st.plotly_chart(fig_spread_bps, width='stretch')
        else:
            st.info("NBBO data not available")
    
    with col2:
        if nbbo is not None and len(nbbo) > 0:
            st.subheader("Quote Churn")
            fig_churn = plot_churn_bar_chart(
                nbbo,
                symbol=selected_symbol if selected_symbol != "All" else None,
            )
            st.plotly_chart(fig_churn, width='stretch')
        else:
            st.info("NBBO data not available")
    
    # Distribution views
    st.header("Distribution Views")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if nbbo is not None and len(nbbo) > 0:
            st.subheader("Spread Distribution")
            fig_spread_dist = plot_spread_histogram(
                nbbo,
                symbol=selected_symbol if selected_symbol != "All" else None,
            )
            st.plotly_chart(fig_spread_dist, width='stretch')
        else:
            st.info("NBBO data not available")
    
    with col2:
        if trades is not None and nbbo is not None and len(trades) > 0 and len(nbbo) > 0:
            st.subheader("Slippage Distribution")
            try:
                enriched_trades = enrich_trades_with_nbbo(trades, nbbo)
                fig_slippage = plot_slippage_histogram(
                    enriched_trades,
                    symbol=selected_symbol if selected_symbol != "All" else None,
                )
                st.plotly_chart(fig_slippage, width='stretch')
            except Exception as e:
                st.error(f"Error calculating slippage: {e}")
        else:
            st.info("Both trades and NBBO data required for slippage analysis")
    
    # Tables
    st.header("Tables")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if trades is not None and nbbo is not None and len(trades) > 0 and len(nbbo) > 0:
            st.subheader("Worst Slippage Trades")
            try:
                enriched_trades = enrich_trades_with_nbbo(trades, nbbo)
                worst_trades = get_worst_slippage_trades(enriched_trades, top_n=20)
                worst_trades_pd = worst_trades.to_pandas()
                
                # Format columns for display
                if len(worst_trades_pd) > 0:
                    worst_trades_pd["ts_event"] = worst_trades_pd["ts_event"].dt.strftime("%H:%M:%S.%f").str[:-3]
                    worst_trades_pd["price"] = worst_trades_pd["price"].round(4)
                    worst_trades_pd["slippage"] = worst_trades_pd["slippage"].round(6)
                    worst_trades_pd["slippage_bps"] = worst_trades_pd["slippage_bps"].round(2)
                    
                    st.dataframe(
                        worst_trades_pd,
                        width='stretch',
                        hide_index=True,
                    )
                else:
                    st.info("No trades with slippage data")
            except Exception as e:
                st.error(f"Error calculating slippage: {e}")
        else:
            st.info("Both trades and NBBO data required")
    
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
                else:
                    st.info("No churn data available")
            except Exception as e:
                st.error(f"Error calculating churn: {e}")
        else:
            st.info("NBBO data required")


if __name__ == "__main__":
    main()
