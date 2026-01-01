#!/usr/bin/env python3
"""Test script to check what SIP data endpoints are available on Alpaca free tier."""

import os
import sys
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import requests
import yaml

def load_secrets():
    """Load Alpaca credentials from config.secrets.yaml."""
    secrets_path = "config.secrets.yaml"
    if not os.path.exists(secrets_path):
        print(f"Error: {secrets_path} not found")
        return None, None
    
    with open(secrets_path, "r") as f:
        secrets = yaml.safe_load(f)
    
    # Check stage_a_alpaca section
    alpaca_config = secrets.get("stage_a_alpaca", {})
    api_key = os.getenv("ALPACA_API_KEY") or alpaca_config.get("alpaca_api_key")
    secret_key = os.getenv("ALPACA_SECRET_KEY") or alpaca_config.get("alpaca_secret_key")
    
    return api_key, secret_key

def test_endpoint(session, base_url, symbol, endpoint_type, feed=None, days_ago=1):
    """Test an Alpaca API endpoint."""
    # Use a recent date
    test_date = date.today() - timedelta(days=days_ago)
    tz = ZoneInfo("America/New_York")
    
    # Market hours: 9:30 AM - 4:00 PM ET
    start_dt = datetime.combine(test_date, datetime.min.time().replace(hour=9, minute=30), tzinfo=tz)
    end_dt = datetime.combine(test_date, datetime.min.time().replace(hour=16, minute=0), tzinfo=tz)
    
    # Convert to UTC
    start_utc = start_dt.astimezone(ZoneInfo("UTC"))
    end_utc = end_dt.astimezone(ZoneInfo("UTC"))
    
    url = f"{base_url}/v2/stocks/{symbol}/{endpoint_type}"
    params = {
        "start": start_utc.isoformat(),
        "end": end_utc.isoformat(),
        "limit": 10,  # Just test with small limit
    }
    
    if feed:
        params["feed"] = feed
    
    try:
        response = session.get(url, params=params)
        
        status = response.status_code
        if status == 200:
            data = response.json()
            # Handle different response formats
            key = endpoint_type if endpoint_type in data else f"{endpoint_type}s"
            count = len(data.get(key, []))
            return {
                "status": "✓ Available",
                "status_code": status,
                "record_count": count,
                "feed": feed or "default",
                "url": url,
            }
        elif status == 404:
            return {
                "status": "✗ Not Found (404)",
                "status_code": status,
                "record_count": 0,
                "feed": feed or "default",
                "url": url,
            }
        elif status == 403:
            return {
                "status": "✗ Forbidden (403) - Requires paid subscription",
                "status_code": status,
                "record_count": 0,
                "feed": feed or "default",
                "url": url,
            }
        else:
            return {
                "status": f"✗ Error ({status})",
                "status_code": status,
                "record_count": 0,
                "feed": feed or "default",
                "url": url,
                "error": response.text[:200],
            }
    except Exception as e:
        return {
            "status": f"✗ Exception: {str(e)[:100]}",
            "status_code": None,
            "record_count": 0,
            "feed": feed or "default",
            "url": url,
        }

def main():
    print("=" * 80)
    print("Testing Alpaca SIP Data Availability (Free Tier)")
    print("=" * 80)
    
    # Load credentials
    api_key, secret_key = load_secrets()
    if not api_key or not secret_key:
        print("\nError: Could not load Alpaca credentials")
        print("Make sure config.secrets.yaml exists with your API keys")
        sys.exit(1)
    
    # Historical data uses data.alpaca.markets
    base_urls_to_test = [
        "https://data.alpaca.markets",  # Data API for historical data
    ]
    symbol = "AAPL"
    
    print(f"\nTesting with:")
    print(f"  Symbol: {symbol}")
    print(f"  API Key: {api_key[:10]}...")
    print(f"  Will test multiple feeds and dates")
    
    # Create session
    session = requests.Session()
    session.headers.update({
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": secret_key,
    })
    
    results = []
    feeds_to_test = [None, "iex", "sip"]
    days_to_test = [1, 7, 30]  # Try yesterday, week ago, month ago
    
    # Test each base URL
    for base_url in base_urls_to_test:
        print(f"\n{'=' * 80}")
        print(f"Testing Base URL: {base_url}")
        print(f"{'=' * 80}")
        
        # Test trades endpoints
        print("\n1. TRADES Endpoints:")
        print("-" * 80)
        
        trades_found = False
        for days_ago in days_to_test:
            test_date_actual = date.today() - timedelta(days=days_ago)
            print(f"\n  Testing with date: {test_date_actual} ({days_ago} days ago)")
            
            for feed in feeds_to_test:
                feed_name = feed or "default (IEX)"
                print(f"    Testing trades with feed='{feed_name}'...")
                result = test_endpoint(session, base_url, symbol, "trades", feed=feed, days_ago=days_ago)
                result["endpoint"] = "trades"
                result["test_date"] = test_date_actual
                result["base_url"] = base_url
                results.append(result)
                print(f"      Status: {result['status']}")
                if result.get("record_count", 0) > 0:
                    print(f"      Records returned: {result['record_count']}")
                    trades_found = True
                    break  # Found working endpoint
            if trades_found:
                break  # Found working date
        
        # Test quotes endpoints
        print("\n2. QUOTES/NBBO Endpoints:")
        print("-" * 80)
        
        quotes_found = False
        for days_ago in days_to_test:
            test_date_actual = date.today() - timedelta(days=days_ago)
            print(f"\n  Testing with date: {test_date_actual} ({days_ago} days ago)")
            
            for feed in feeds_to_test:
                feed_name = feed or "default (IEX)"
                print(f"    Testing quotes with feed='{feed_name}'...")
                result = test_endpoint(session, base_url, symbol, "quotes", feed=feed, days_ago=days_ago)
                result["endpoint"] = "quotes"
                result["test_date"] = test_date_actual
                result["base_url"] = base_url
                results.append(result)
                print(f"      Status: {result['status']}")
                if result.get("record_count", 0) > 0:
                    print(f"      Records returned: {result['record_count']}")
                    quotes_found = True
                    break  # Found working endpoint
            if quotes_found:
                break  # Found working date
    
    # Summary
    print("\n" + "=" * 80)
    print("Summary")
    print("=" * 80)
    
    print("\nAvailable Endpoints:")
    available = [r for r in results if r["status_code"] == 200]
    if available:
        for r in available:
            print(f"  ✓ {r['endpoint']} with feed='{r['feed']}' on {r['test_date']} - {r['record_count']} records")
            print(f"    URL: {r['url']}")
    else:
        print("  None available")
    
    print("\nUnavailable Endpoints:")
    unavailable = [r for r in results if r["status_code"] != 200]
    if unavailable:
        # Group by endpoint and feed
        seen = set()
        for r in unavailable:
            key = (r['endpoint'], r['feed'])
            if key not in seen:
                print(f"  {r['status']} - {r['endpoint']} with feed='{r['feed']}'")
                seen.add(key)
    
    print("\n" + "=" * 80)
    print("Recommendations:")
    print("=" * 80)
    
    trades_available = [r for r in results if r["endpoint"] == "trades" and r["status_code"] == 200]
    quotes_available = [r for r in results if r["endpoint"] == "quotes" and r["status_code"] == 200]
    
    if trades_available:
        best_trades = max(trades_available, key=lambda x: x["record_count"])
        print(f"\n✓ Trades: Use feed='{best_trades['feed']}' on {best_trades['base_url']}")
        print(f"  Base URL: {best_trades['base_url']}")
    else:
        print("\n✗ Trades: No endpoints available")
    
    if quotes_available:
        best_quotes = max(quotes_available, key=lambda x: x["record_count"])
        print(f"✓ Quotes: Use feed='{best_quotes['feed']}' on {best_quotes['base_url']}")
        print(f"  Base URL: {best_quotes['base_url']}")
    else:
        print("✗ Quotes: No endpoints available (may require paid subscription)")

if __name__ == "__main__":
    main()
