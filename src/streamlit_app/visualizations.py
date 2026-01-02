"""Visualization functions for Streamlit app."""

from __future__ import annotations

import polars as pl
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots


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


def calculate_slippage(enriched_trades: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate slippage (trade price deviation from mid price).
    
    Args:
        enriched_trades: Enriched trades DataFrame with best_bid and best_ask
        
    Returns:
        DataFrame with slippage column added
    """
    if "best_bid" not in enriched_trades.columns or "best_ask" not in enriched_trades.columns:
        raise ValueError("enriched_trades must have best_bid and best_ask columns")
    
    enriched = enriched_trades.with_columns([
        ((pl.col("best_bid") + pl.col("best_ask")) / 2).alias("mid_price"),
    ]).with_columns([
        (pl.col("price") - pl.col("mid_price")).alias("slippage"),
        ((pl.col("price") - pl.col("mid_price")) / pl.col("mid_price") * 10000).alias("slippage_bps"),
    ])
    
    return enriched


def plot_price_panel(
    trades: pl.DataFrame | None,
    nbbo: pl.DataFrame | None,
    show_trades: bool = True,
    show_nbbo: bool = True,
    show_vwap: bool = False,
    symbol: str | None = None,
) -> go.Figure:
    """
    Plot price panel with bid/ask/mid and trade prints colored by at bid/at ask.
    
    Args:
        trades: Trades DataFrame (optional)
        nbbo: NBBO DataFrame (optional)
        show_trades: Whether to show trades
        show_nbbo: Whether to show NBBO bid/ask/mid
        show_vwap: Whether to show VWAP
        symbol: Optional symbol name for title
        
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
    
    # Add NBBO lines
    if nbbo is not None and len(nbbo) > 0 and show_nbbo:
        try:
            # Use all NBBO data - convert to pandas for plotting
            nbbo_pd = nbbo.to_pandas()
            
            # Check required columns
            if "best_bid" not in nbbo_pd.columns or "best_ask" not in nbbo_pd.columns:
                logger.warning(f"NBBO missing required columns. Available: {nbbo_pd.columns.tolist()}")
            else:
                # Best Bid
                fig.add_trace(go.Scatter(
                    x=nbbo_pd["ts_event"],
                    y=nbbo_pd["best_bid"],
                    mode="lines",
                    name="Best Bid",
                    line=dict(color="green", width=1.5),
                    hovertemplate="<b>Bid</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                ))
                
                # Best Ask
                fig.add_trace(go.Scatter(
                    x=nbbo_pd["ts_event"],
                    y=nbbo_pd["best_ask"],
                    mode="lines",
                    name="Best Ask",
                    line=dict(color="red", width=1.5),
                    hovertemplate="<b>Ask</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                ))
                
                # Mid Price
                if "mid_price" in nbbo_pd.columns:
                    mid_price = nbbo_pd["mid_price"]
                else:
                    mid_price = (nbbo_pd["best_bid"] + nbbo_pd["best_ask"]) / 2
                
                fig.add_trace(go.Scatter(
                    x=nbbo_pd["ts_event"],
                    y=mid_price,
                    mode="lines",
                    name="Mid Price",
                    line=dict(color="blue", width=1, dash="dash"),
                    hovertemplate="<b>Mid</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                ))
        except Exception as e:
            logger.error(f"Error adding NBBO traces: {e}")
            import traceback
            logger.debug(traceback.format_exc())
    
    # Add VWAP if requested
    if trades is not None and len(trades) > 0 and show_vwap:
        vwap_df = calculate_vwap(trades)
        vwap_pd = vwap_df.to_pandas()
        
        fig.add_trace(go.Scatter(
            x=vwap_pd["time_bucket"],
            y=vwap_pd["vwap"],
            mode="lines",
            name="VWAP",
            line=dict(color="purple", width=2),
            hovertemplate="<b>VWAP</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
        ))
    
    # Add trades colored by at bid/at ask
    if trades is not None and len(trades) > 0 and show_trades:
        try:
            # Enrich trades with NBBO if available - use all data
            enriched_pd = None
            if nbbo is not None and len(nbbo) > 0:
                try:
                    logger.info(f"Enriching {len(trades):,} trades with {len(nbbo):,} NBBO records...")
                    enriched = enrich_trades_with_nbbo(trades, nbbo)
                    enriched_pd = enriched.to_pandas()
                    logger.info(f"Enrichment complete. Plotting {len(enriched_pd):,} enriched trades.")
                except Exception as e:
                    logger.warning(f"Error enriching trades with NBBO: {e}. Plotting trades without enrichment.")
                    import traceback
                    logger.debug(traceback.format_exc())
                    enriched_pd = None
            
            # Color trades: at bid (green), at ask (red), within spread (gray), outside (orange/blue)
            if enriched_pd is not None and "price_location" in enriched_pd.columns:
                # At bid
                at_bid = enriched_pd[enriched_pd["price"] == enriched_pd["best_bid"]]
                if len(at_bid) > 0:
                    fig.add_trace(go.Scatter(
                        x=at_bid["ts_event"],
                        y=at_bid["price"],
                        mode="markers",
                        name="At Bid",
                        marker=dict(size=4, color="green", opacity=0.7, symbol="triangle-up"),
                        hovertemplate="<b>Trade @ Bid</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                    ))
                
                # At ask
                at_ask = enriched_pd[enriched_pd["price"] == enriched_pd["best_ask"]]
                if len(at_ask) > 0:
                    fig.add_trace(go.Scatter(
                        x=at_ask["ts_event"],
                        y=at_ask["price"],
                        mode="markers",
                        name="At Ask",
                        marker=dict(size=4, color="red", opacity=0.7, symbol="triangle-down"),
                        hovertemplate="<b>Trade @ Ask</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                    ))
                
                # Within spread (but not at bid/ask)
                within = enriched_pd[
                    (enriched_pd["price_location"] == "within_spread") &
                    (enriched_pd["price"] != enriched_pd["best_bid"]) &
                    (enriched_pd["price"] != enriched_pd["best_ask"])
                ]
                if len(within) > 0:
                    fig.add_trace(go.Scatter(
                        x=within["ts_event"],
                        y=within["price"],
                        mode="markers",
                        name="Within Spread",
                        marker=dict(size=3, color="gray", opacity=0.5),
                        hovertemplate="<b>Trade</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                    ))
                
                # Below bid
                below = enriched_pd[enriched_pd["price_location"] == "below_bid"]
                if len(below) > 0:
                    fig.add_trace(go.Scatter(
                        x=below["ts_event"],
                        y=below["price"],
                        mode="markers",
                        name="Below Bid",
                        marker=dict(size=3, color="blue", opacity=0.6),
                        hovertemplate="<b>Trade Below Bid</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                    ))
                
                # Above ask
                above = enriched_pd[enriched_pd["price_location"] == "above_ask"]
                if len(above) > 0:
                    fig.add_trace(go.Scatter(
                        x=above["ts_event"],
                        y=above["price"],
                        mode="markers",
                        name="Above Ask",
                        marker=dict(size=3, color="orange", opacity=0.6),
                        hovertemplate="<b>Trade Above Ask</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                    ))
            else:
                # Fallback: just plot all trades (without enrichment)
                try:
                    # Use all trades data
                    trades_pd = trades.to_pandas()
                    if len(trades_pd) > 0:
                        fig.add_trace(go.Scatter(
                            x=trades_pd["ts_event"],
                            y=trades_pd["price"],
                            mode="markers",
                            name="Trades",
                            marker=dict(size=3, color="black", opacity=0.5),
                            hovertemplate="<b>Trade</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                        ))
                except Exception as e:
                    logger.error(f"Error plotting trades fallback: {e}")
                    import traceback
                    logger.debug(traceback.format_exc())
        except Exception as e:
            logger.error(f"Error adding trade traces: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            # Try simple fallback
            try:
                # Use all trades data
                trades_pd = trades.to_pandas()
                if len(trades_pd) > 0 and "ts_event" in trades_pd.columns and "price" in trades_pd.columns:
                    fig.add_trace(go.Scatter(
                        x=trades_pd["ts_event"],
                        y=trades_pd["price"],
                        mode="markers",
                        name="Trades",
                        marker=dict(size=3, color="black", opacity=0.5),
                        hovertemplate="<b>Trade</b><br>Time: %{x}<br>Price: $%{y:.4f}<extra></extra>",
                    ))
            except Exception as e2:
                logger.error(f"Error in simple trade fallback: {e2}")
                import traceback
                logger.debug(traceback.format_exc())
    
    title = "Price Panel"
    if symbol:
        title = f"{symbol} - {title}"
    
    fig.update_layout(
        title=title,
        xaxis_title="Time (NY)",
        yaxis_title="Price ($)",
        hovermode="x unified",
        height=600,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    
    return fig


def plot_spread_bps_timeline(
    nbbo: pl.DataFrame,
    show_churn: bool = False,
    symbol: str | None = None,
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
    nbbo_pd = nbbo.to_pandas()
    
    # Calculate spread in bps
    if "spread" not in nbbo_pd.columns:
        nbbo_pd["spread"] = nbbo_pd["best_ask"] - nbbo_pd["best_bid"]
    
    # Calculate mid price for bps calculation
    if "mid_price" not in nbbo_pd.columns:
        nbbo_pd["mid_price"] = (nbbo_pd["best_bid"] + nbbo_pd["best_ask"]) / 2
    
    # Spread in bps = (spread / mid_price) * 10000
    nbbo_pd["spread_bps"] = (nbbo_pd["spread"] / nbbo_pd["mid_price"]) * 10000
    
    if show_churn:
        # Create subplot with secondary y-axis for churn
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        
        # Spread in bps
        fig.add_trace(
            go.Scatter(
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
        fig = go.Figure()
        fig.add_trace(go.Scatter(
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
    
    fig.update_layout(
        title=title,
        xaxis_title="Time (NY)",
        height=400,
        hovermode="x unified",
    )
    
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


def plot_slippage_histogram(
    enriched_trades: pl.DataFrame,
    symbol: str | None = None,
) -> go.Figure:
    """
    Plot histogram of slippage values.
    
    Args:
        enriched_trades: Enriched trades DataFrame
        symbol: Optional symbol name for title
        
    Returns:
        Plotly figure
    """
    enriched = calculate_slippage(enriched_trades)
    enriched_pd = enriched.to_pandas()
    
    fig = px.histogram(
        enriched_pd,
        x="slippage_bps",
        nbins=50,
        title=f"{symbol} - Slippage Distribution" if symbol else "Slippage Distribution",
        labels={"slippage_bps": "Slippage (bps)", "count": "Frequency"},
    )
    
    fig.update_layout(height=400)
    return fig


def get_worst_slippage_trades(
    enriched_trades: pl.DataFrame,
    top_n: int = 20,
) -> pl.DataFrame:
    """
    Get worst slippage trades (most negative slippage).
    
    Args:
        enriched_trades: Enriched trades DataFrame
        top_n: Number of worst trades to return
        
    Returns:
        DataFrame with worst slippage trades
    """
    enriched = calculate_slippage(enriched_trades)
    
    worst = enriched.sort("slippage_bps").head(top_n).select([
        "ts_event",
        "symbol",
        "price",
        "size",
        "best_bid",
        "best_ask",
        "mid_price",
        "slippage",
        "slippage_bps",
    ])
    
    return worst


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


def enrich_trades_with_nbbo(
    trades: pl.DataFrame,
    nbbo: pl.DataFrame,
) -> pl.DataFrame:
    """
    Enrich trades with NBBO data using ASOF join.
    
    Args:
        trades: Trades DataFrame
        nbbo: NBBO DataFrame
        
    Returns:
        Enriched trades DataFrame
    """
    # Ensure both have ts_event and symbol columns
    if "ts_event" not in trades.columns or "ts_event" not in nbbo.columns:
        raise ValueError("Both DataFrames must have 'ts_event' column")
    
    if "symbol" not in trades.columns or "symbol" not in nbbo.columns:
        raise ValueError("Both DataFrames must have 'symbol' column")
    
    # Sort by timestamp and symbol for ASOF join
    # Polars requires data to be sorted by the join key for ASOF joins
    # Use lazy evaluation for better performance
    import warnings
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message=".*Sortedness of columns cannot be checked.*")
        
        trades_sorted = trades.sort(["symbol", "ts_event"])
        nbbo_sorted = nbbo.sort(["symbol", "ts_event"])
        
        # Perform ASOF join (backward - get most recent NBBO at or before trade time)
        # Note: When using 'by' parameter, Polars expects data sorted by [by_cols, on_col]
        enriched = trades_sorted.join_asof(
            nbbo_sorted,
            on="ts_event",
            by="symbol",
            strategy="backward",
        )
    
    # Calculate derived metrics
    if "best_bid" in enriched.columns and "best_ask" in enriched.columns:
        enriched = enriched.with_columns([
            ((pl.col("price") - pl.col("best_bid")) / pl.col("best_bid") * 100).alias("price_vs_bid_pct"),
            ((pl.col("price") - pl.col("best_ask")) / pl.col("best_ask") * 100).alias("price_vs_ask_pct"),
            ((pl.col("price") - (pl.col("best_bid") + pl.col("best_ask")) / 2) / 
             ((pl.col("best_bid") + pl.col("best_ask")) / 2) * 100).alias("price_vs_mid_pct"),
            pl.when(pl.col("price") < pl.col("best_bid"))
              .then(pl.lit("below_bid"))
              .when(pl.col("price") > pl.col("best_ask"))
              .then(pl.lit("above_ask"))
              .otherwise(pl.lit("within_spread"))
              .alias("price_location"),
        ])
    
    return enriched
