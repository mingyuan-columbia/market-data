"""Visualization functions for Streamlit app."""

from __future__ import annotations

import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import timedelta, datetime


def calculate_vwap(trades: pl.DataFrame, window: str = "1m") -> pl.DataFrame:
    """
    Calculate Volume-Weighted Average Price (VWAP).
    
    Args:
        trades: Trades DataFrame
        window: Time window for aggregation (e.g., "1m", "5m", "1h") - Polars format
        
    Returns:
        DataFrame with VWAP over time
    """
    # Convert common formats to Polars format
    window_map = {"1min": "1m", "5min": "5m", "1h": "1h", "1m": "1m", "5m": "5m"}
    polars_window = window_map.get(window, window)
    
    trades_agg = trades.with_columns([
        pl.col("ts_event").dt.truncate(polars_window).alias("time_bucket"),
        (pl.col("price") * pl.col("size")).alias("price_volume"),
    ]).group_by("time_bucket").agg([
        pl.col("price_volume").sum().alias("total_price_volume"),
        pl.col("size").sum().alias("total_volume"),
    ]).with_columns([
        (pl.col("total_price_volume") / pl.col("total_volume")).alias("vwap"),
    ]).sort("time_bucket")
    
    return trades_agg


def calculate_churn(nbbo: pl.DataFrame, window: str = "1m") -> pl.DataFrame:
    """
    Calculate quote churn (number of quote updates per time window).
    
    Args:
        nbbo: NBBO DataFrame
        window: Time window for aggregation (e.g., "1m", "5m", "1h") - Polars format
        
    Returns:
        DataFrame with churn over time
    """
    # Convert common formats to Polars format
    window_map = {"1min": "1m", "5min": "5m", "1h": "1h", "1m": "1m", "5m": "5m"}
    polars_window = window_map.get(window, window)
    
    churn = nbbo.with_columns([
        pl.col("ts_event").dt.truncate(polars_window).alias("time_bucket"),
    ]).group_by("time_bucket").agg([
        pl.len().alias("churn"),
    ]).sort("time_bucket")
    
    return churn


def calculate_adaptive_max_points(
    total_seconds: float,
    data_type: str = "nbbo",
) -> int:
    """
    Calculate adaptive max_points based on time window duration.
    Allows more points for shorter time windows to enable better zooming.
    
    Args:
        total_seconds: Duration of time window in seconds
        data_type: "nbbo" or "trades" - trades can handle more points
        
    Returns:
        Maximum number of points to keep
    """
    # Base multiplier: trades can handle more points than NBBO
    base_multiplier = 2.0 if data_type == "trades" else 1.0
    
    # Adaptive limits based on time window
    if total_seconds < 30:  # Less than 30 seconds
        return int(5000 * base_multiplier)  # Allow up to 5k-10k points
    elif total_seconds < 120:  # Less than 2 minutes
        return int(10000 * base_multiplier)  # Allow up to 10k-20k points
    elif total_seconds < 600:  # Less than 10 minutes
        return int(15000 * base_multiplier)  # Allow up to 15k-30k points
    elif total_seconds < 1800:  # Less than 30 minutes
        return int(10000 * base_multiplier)  # Allow up to 10k-20k points
    else:  # 30+ minutes
        return int(10000 * base_multiplier)  # Default limit


def downsample_data(
    df: pl.DataFrame,
    time_col: str,
    max_points: int | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    data_type: str = "nbbo",
) -> pl.DataFrame:
    """
    Downsample data to max_points by time-binning with 'last' aggregation.
    If max_points is None, automatically calculates adaptive limit based on time window.
    
    Args:
        df: DataFrame to downsample
        time_col: Name of timestamp column
        max_points: Maximum number of points to keep (None = auto-calculate)
        start_time: Optional start time for window calculation
        end_time: Optional end time for window calculation
        data_type: "nbbo" or "trades" - used for adaptive max_points calculation
        
    Returns:
        Downsampled DataFrame
    """
    # Calculate time window if provided
    if start_time is not None and end_time is not None:
        window_duration = end_time - start_time
        total_seconds = window_duration.total_seconds()
    else:
        # Use data range
        min_ts = df[time_col].min()
        max_ts = df[time_col].max()
        if isinstance(min_ts, pl.Series):
            min_ts = min_ts.item()
        if isinstance(max_ts, pl.Series):
            max_ts = max_ts.item()
        
        # Convert Polars datetime to Python datetime if needed
        if hasattr(min_ts, 'timestamp'):
            # Already a datetime
            window_duration = max_ts - min_ts
            total_seconds = window_duration.total_seconds()
        else:
            # Try to calculate from timestamps
            try:
                if isinstance(min_ts, (int, float)):
                    total_seconds = (max_ts - min_ts) / 1_000_000  # Assume microseconds
                else:
                    # Fallback: use a default
                    total_seconds = 3600  # 1 hour default
            except:
                total_seconds = 3600
    
    # Auto-calculate max_points if not provided
    if max_points is None:
        max_points = calculate_adaptive_max_points(total_seconds, data_type)
    
    if len(df) <= max_points:
        return df
    
    # Calculate bucket size to get approximately max_points
    bucket_seconds = max(0.001, total_seconds / max_points)  # At least 1ms
    
    # Determine appropriate time unit (Polars format)
    if bucket_seconds >= 1:
        bucket_str = f"{int(bucket_seconds)}s"
    elif bucket_seconds >= 0.1:
        bucket_str = "100ms"
    elif bucket_seconds >= 0.01:
        bucket_str = "10ms"
    else:
        bucket_str = "1ms"
    
    # Group by time bucket and take last value (or aggregate)
    # For NBBO: take last bid/ask per bucket
    # For trades: count/sum per bucket
    df_with_bucket = df.with_columns([
        pl.col(time_col).dt.truncate(bucket_str).alias("_time_bucket"),
    ])
    
    # Get column names (excluding time_col and bucket)
    agg_cols = [c for c in df.columns if c != time_col]
    
    # Aggregate: for numeric columns, take last; for others, take first
    agg_exprs = []
    for col in agg_cols:
        if df[col].dtype in [pl.Int64, pl.Int32, pl.Float64, pl.Float32]:
            agg_exprs.append(pl.col(col).last().alias(col))
        else:
            agg_exprs.append(pl.col(col).first().alias(col))
    
    # Also keep the bucket time
    agg_exprs.append(pl.col("_time_bucket").first().alias(time_col))
    
    downsampled = df_with_bucket.group_by("_time_bucket").agg(agg_exprs).sort(time_col)
    
    # Drop the bucket column
    downsampled = downsampled.drop("_time_bucket")
    
    return downsampled


def plot_price_panel(
    trades: pl.DataFrame | None,
    nbbo: pl.DataFrame | None,
    show_trades: bool = True,
    show_nbbo: bool = True,
    show_mid_price: bool = True,
    show_vwap: bool = False,
    symbol: str | None = None,
    start_time: datetime | None = None,
    end_time: datetime | None = None,
    min_trade_size: int | None = None,
    yaxis_range: tuple[float, float] | None = None,
    source: str | None = None,
    uirevision: str | None = None,
) -> go.Figure:
    """
    Plot price panel with bid/ask/mid and trade prints.
    
    Args:
        trades: Trades DataFrame (optional)
        nbbo: NBBO DataFrame (optional)
        show_trades: Whether to show trades
        show_nbbo: Whether to show NBBO bid/ask/mid
        show_mid_price: Whether to show mid price line
        show_vwap: Whether to show VWAP
        symbol: Optional symbol name for title
        start_time: Start time for downsampling calculation
        end_time: End time for downsampling calculation
        min_trade_size: Minimum trade size to display (None = all)
        yaxis_range: Optional tuple (min, max) to set fixed y-axis range for synchronization
        source: Optional data source name (e.g., "taq", "alpaca") to include in title
        
    Returns:
        Plotly figure
    """
    fig = go.Figure()
    
    # Debug: log what we're working with
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"plot_price_panel called: trades={trades is not None and len(trades) > 0 if trades is not None else False}, "
                f"nbbo={nbbo is not None and len(nbbo) > 0 if nbbo is not None else False}, "
                f"show_trades={show_trades}, show_nbbo={show_nbbo}")
    
    # Filter trades by size if requested
    if trades is not None and len(trades) > 0 and min_trade_size is not None and "size" in trades.columns:
        trades = trades.filter(pl.col("size") >= min_trade_size)
        logger.info(f"Filtered trades by size >= {min_trade_size}: {len(trades):,} remaining")
    
    # Add NBBO lines
    if nbbo is not None and len(nbbo) > 0 and show_nbbo:
        try:
            # Downsample NBBO adaptively based on time window
            nbbo_viz = downsample_data(nbbo, "ts_event", max_points=None, start_time=start_time, end_time=end_time, data_type="nbbo")
            logger.info(f"Downsampled NBBO: {len(nbbo):,} -> {len(nbbo_viz):,} points")
            
            # Convert to pandas for plotting
            nbbo_pd = nbbo_viz.to_pandas()
            
            # Check required columns
            if "best_bid" not in nbbo_pd.columns or "best_ask" not in nbbo_pd.columns:
                logger.warning(f"NBBO missing required columns. Available: {nbbo_pd.columns.tolist()}")
            else:
                # Best Bid - use Scattergl for WebGL rendering
                fig.add_trace(go.Scattergl(
                    x=nbbo_pd["ts_event"],
                    y=nbbo_pd["best_bid"],
                    mode="lines",
                    name="Best Bid",
                    line=dict(color="green", width=1.5),
                    hovertemplate="Bid: $%{y:.4f}<extra></extra>",
                ))
                
                # Best Ask - use Scattergl for WebGL rendering
                fig.add_trace(go.Scattergl(
                    x=nbbo_pd["ts_event"],
                    y=nbbo_pd["best_ask"],
                    mode="lines",
                    name="Best Ask",
                    line=dict(color="red", width=1.5),
                    hovertemplate="Ask: $%{y:.4f}<extra></extra>",
                ))
                
                # Mid Price - use Scattergl for WebGL rendering (only if enabled)
                if show_mid_price:
                    if "mid_price" in nbbo_pd.columns:
                        mid_price = nbbo_pd["mid_price"]
                    else:
                        mid_price = (nbbo_pd["best_bid"] + nbbo_pd["best_ask"]) / 2
                    
                    fig.add_trace(go.Scattergl(
                        x=nbbo_pd["ts_event"],
                        y=mid_price,
                        mode="lines",
                        name="Mid Price",
                        line=dict(color="blue", width=1, dash="dash"),
                        hovertemplate="Mid: $%{y:.4f}<extra></extra>",
                    ))
        except Exception as e:
            logger.error(f"Error adding NBBO traces: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    # Add VWAP if requested
    if trades is not None and len(trades) > 0 and show_vwap:
        vwap_df = calculate_vwap(trades)
        vwap_pd = vwap_df.to_pandas()
        
        fig.add_trace(go.Scattergl(
            x=vwap_pd["time_bucket"],
            y=vwap_pd["vwap"],
            mode="lines",
            name="VWAP",
            line=dict(color="purple", width=2),
            hovertemplate="VWAP: $%{y:.4f}<extra></extra>",
        ))
    
    # Add trades
    if trades is not None and len(trades) > 0 and show_trades:
        try:
            # Downsample trades adaptively based on time window
            trades_viz = downsample_data(trades, "ts_event", max_points=None, start_time=start_time, end_time=end_time, data_type="trades")
            logger.info(f"Downsampled trades: {len(trades):,} -> {len(trades_viz):,} points")
            
            # Plot trades as simple markers
            trades_pd = trades_viz.to_pandas()
            if len(trades_pd) > 0:
                # Prepare hover data - include size if available
                if "size" in trades_pd.columns:
                    hover_data = trades_pd["size"]
                    hovertemplate = "Trade: $%{y:.4f}<br>Size: %{customdata:,}<extra></extra>"
                else:
                    hover_data = None
                    hovertemplate = "Trade: $%{y:.4f}<extra></extra>"
                
                fig.add_trace(go.Scattergl(
                    x=trades_pd["ts_event"],
                    y=trades_pd["price"],
                    mode="markers",
                    name="Trades",
                    marker=dict(size=3, color="black", opacity=0.5),
                    customdata=hover_data,
                    hovertemplate=hovertemplate,
                ))
        except Exception as e:
            logger.error(f"Error adding trade traces: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    title = "Price Panel"
    if symbol:
        title = f"{symbol} - {title}"
    
    # Configure axes to allow zooming on both axes
    # - autorange=True: enables automatic y-axis adjustment
    # - fixedrange=False: allows zooming/panning on both axes
    # - uirevision="keep": preserves zoom state across Streamlit reruns
    xaxis_config = dict(
        rangeslider=dict(visible=False),  # Hide range slider for cleaner look
        fixedrange=False,  # Allow x-axis zooming/panning
    )
    
    yaxis_config = dict(
        autorange=True,  # Enable autorange for automatic y-axis adjustment
        fixedrange=False,  # Allow y-axis zooming/panning
    )
    
    # Set initial x-axis range if start_time and end_time provided
    if start_time is not None and end_time is not None:
        # Convert to milliseconds for Plotly
        xaxis_config["range"] = [
            start_time.timestamp() * 1000,
            end_time.timestamp() * 1000
        ]
    
    layout_dict = dict(
        title=title,
        xaxis_title="Time (NY)",
        yaxis_title="Price ($)",
        hovermode="x unified",
        height=600,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis=xaxis_config,
        yaxis=yaxis_config,
    )
    
    # Only set uirevision if explicitly provided (use unique values for dual source)
    if uirevision is not None:
        layout_dict["uirevision"] = uirevision
    else:
        # Default: use "keep" for single plots
        layout_dict["uirevision"] = "keep"
    
    fig.update_layout(**layout_dict)
    
    return fig


def plot_spread_bps_timeline(
    nbbo: pl.DataFrame,
    show_churn: bool = False,
    symbol: str | None = None,
    uirevision: str | None = None,
) -> go.Figure:
    """
    Plot spread in basis points (bps) over time.
    
    Args:
        nbbo: NBBO DataFrame
        show_churn: Whether to overlay churn bar chart
        symbol: Optional symbol name for title
        
    Returns:
        Plotly figure
    """
    # Calculate spread in bps BEFORE downsampling (need accurate values)
    if "spread" not in nbbo.columns:
        nbbo = nbbo.with_columns([
            (pl.col("best_ask") - pl.col("best_bid")).alias("spread"),
        ])
    
    # Calculate mid price for bps calculation
    if "mid_price" not in nbbo.columns:
        nbbo = nbbo.with_columns([
            ((pl.col("best_bid") + pl.col("best_ask")) / 2).alias("mid_price"),
        ])
    
    # Spread in bps = (spread / mid_price) * 10000
    if "spread_bps" not in nbbo.columns:
        nbbo = nbbo.with_columns([
            ((pl.col("spread") / pl.col("mid_price")) * 10000).alias("spread_bps"),
        ])
    
    if show_churn:
        # Create subplot with secondary y-axis for churn
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Spread in bps
        # Downsample for better performance (adaptive)
        nbbo_viz = downsample_data(nbbo, "ts_event", max_points=None, data_type="nbbo")
        nbbo_pd = nbbo_viz.to_pandas()
        
        fig.add_trace(
            go.Scattergl(
                x=nbbo_pd["ts_event"],
                y=nbbo_pd["spread_bps"],
                mode="lines",
                name="Spread (bps)",
                line=dict(color="blue", width=1.5),
            ),
            secondary_y=False,
        )
        
        # Churn
        churn_df = calculate_churn(nbbo)
        churn_pd = churn_df.to_pandas()
        
        fig.add_trace(
            go.Bar(
                x=churn_pd["time_bucket"],
                y=churn_pd["churn"],
                name="Quote Churn",
                marker_color="rgba(255, 165, 0, 0.3)",
            ),
            secondary_y=True,
        )
        
        fig.update_yaxes(title_text="Spread (bps)", secondary_y=False)
        fig.update_yaxes(title_text="Churn (updates/min)", secondary_y=True)
    else:
        # Downsample for better performance (adaptive)
        nbbo_viz = downsample_data(nbbo, "ts_event", max_points=None, data_type="nbbo")
        nbbo_pd = nbbo_viz.to_pandas()
        
        # Ensure spread_bps exists (should already be calculated)
        if "spread_bps" not in nbbo_pd.columns:
            if "spread" not in nbbo_pd.columns:
                nbbo_pd["spread"] = nbbo_pd["best_ask"] - nbbo_pd["best_bid"]
            if "mid_price" not in nbbo_pd.columns:
                nbbo_pd["mid_price"] = (nbbo_pd["best_bid"] + nbbo_pd["best_ask"]) / 2
            nbbo_pd["spread_bps"] = (nbbo_pd["spread"] / nbbo_pd["mid_price"]) * 10000
        
        fig = go.Figure()
        fig.add_trace(go.Scattergl(
            x=nbbo_pd["ts_event"],
            y=nbbo_pd["spread_bps"],
            mode="lines",
            name="Spread (bps)",
            line=dict(color="blue", width=1.5),
        ))
        fig.update_yaxes(title_text="Spread (bps)")
    
    title = "Spread (bps) Over Time"
    if symbol:
        title = f"{symbol} - {title}"
    
    layout_dict = dict(
        title=title,
        xaxis_title="Time (NY)",
        height=400,
        hovermode="x unified",
    )
    
    # Only set uirevision if explicitly provided (use unique values for dual source)
    if uirevision is not None:
        layout_dict["uirevision"] = uirevision
    else:
        # Default: use "keep" for single plots
        layout_dict["uirevision"] = "keep"
    
    fig.update_layout(**layout_dict)
    
    return fig


def plot_churn_bar_chart(
    nbbo: pl.DataFrame,
    window: str = "1m",
    symbol: str | None = None,
) -> go.Figure:
    """
    Plot quote churn bar chart.
    
    Args:
        nbbo: NBBO DataFrame
        window: Time window for aggregation
        symbol: Optional symbol name for title
        
    Returns:
        Plotly figure
    """
    churn_df = calculate_churn(nbbo, window)
    churn_pd = churn_df.to_pandas()
    
    fig = px.bar(
        churn_pd,
        x="time_bucket",
        y="churn",
        title=f"{symbol} - Quote Churn" if symbol else "Quote Churn",
        labels={"time_bucket": "Time (NY)", "churn": "Quote Updates"},
    )
    
    fig.update_layout(height=400)
    return fig


def plot_spread_histogram(
    nbbo: pl.DataFrame,
    symbol: str | None = None,
) -> go.Figure:
    """
    Plot histogram of spread values.
    
    Args:
        nbbo: NBBO DataFrame
        symbol: Optional symbol name for title
        
    Returns:
        Plotly figure
    """
    nbbo_pd = nbbo.to_pandas()
    
    if "spread" not in nbbo_pd.columns:
        nbbo_pd["spread"] = nbbo_pd["best_ask"] - nbbo_pd["best_bid"]
    
    fig = px.histogram(
        nbbo_pd,
        x="spread",
        nbins=50,
        title=f"{symbol} - Spread Distribution" if symbol else "Spread Distribution",
        labels={"spread": "Spread ($)", "count": "Frequency"},
    )
    
    fig.update_layout(height=400)
    return fig


def get_highest_churn_minutes(
    nbbo: pl.DataFrame,
    top_n: int = 20,
    window: str = "1m",
) -> pl.DataFrame:
    """
    Get minutes with highest quote churn.
    
    Args:
        nbbo: NBBO DataFrame
        top_n: Number of minutes to return
        window: Time window for aggregation
        
    Returns:
        DataFrame with highest churn minutes
    """
    churn_df = calculate_churn(nbbo, window)
    
    highest = churn_df.sort("churn", descending=True).head(top_n)
    
    return highest


