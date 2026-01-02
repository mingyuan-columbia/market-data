# Streamlit Market Data Visualization App

Interactive web application for visualizing trade and NBBO (National Best Bid and Offer) data.

## Features

- **Multi-source support**: Visualize data from TAQ, Alpaca SIP, Alpaca IEX, or CSV sources
- **Date and symbol selection**: Choose any date and symbol combination
- **Smart data availability checking**: Automatically suggests alternative dates and sources if data is not available
- **Interactive visualizations**:
  - Price timeline with NBBO bid/ask overlay
  - Trade volume over time
  - Trade size distribution
  - Spread analysis
  - Trade-NBBO consistency analysis
  - Price location distribution (below bid, within spread, above ask)

## Setup

### 1. Install Dependencies

```bash
pip install streamlit plotly
```

Or install all requirements:
```bash
pip install -r requirements.txt
```

### 2. Configure Data Sources

The app reads data source paths from `config.yaml`. Make sure your data sources are configured:

```yaml
stage_a:
  parquet_raw_root: /home/mingyuan/data/taq/parquet_raw

stage_a_alpaca:
  parquet_raw_root: /home/mingyuan/data/alpaca/parquet_raw

stage_a_alpaca_iex:
  parquet_raw_root: /home/mingyuan/data/alpaca_iex/parquet_raw

stage_a_csv:
  parquet_raw_root: /home/mingyuan/data/csv/parquet_raw
```

### 3. Set Up Password Protection

Create a `.streamlit/secrets.toml` file in the project root:

```toml
password = "your_secure_password_here"
```

**Important**: Add `.streamlit/secrets.toml` to `.gitignore` to avoid committing passwords.

### 4. Run the App

From the project root:

```bash
streamlit run src/streamlit_app/app.py --server.port 8500
```

Or if running from the `src/streamlit_app` directory:

```bash
streamlit run app.py --server.port 8500
```

## Remote Access

### Option 1: Cloudflare Tunnel (Quick Setup)

1. Install cloudflared:
```bash
# Linux
wget https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64
chmod +x cloudflared-linux-amd64
sudo mv cloudflared-linux-amd64 /usr/local/bin/cloudflared
```

2. Start Streamlit app:
```bash
streamlit run src/streamlit_app/app.py --server.port 8500
```

3. In another terminal, start tunnel:
```bash
cloudflared tunnel --url http://localhost:8500
```

4. Copy the provided URL (e.g., `https://random-subdomain.trycloudflare.com`) and access from any device.

**Security Note**: The app includes password protection. Make sure to set a strong password in `.streamlit/secrets.toml`.

### Option 2: VPN (Most Secure)

Set up a VPN (e.g., Tailscale, WireGuard) to access your local network securely.

### Option 3: Cloud Deployment

Deploy to Render, Railway, or similar services for persistent access.

## Usage

1. **Select Data Source**: Choose from available data sources (TAQ, Alpaca SIP, Alpaca IEX, CSV)

2. **Select Date**: Pick a trade date to analyze

3. **Select Symbol**: Choose a specific symbol or "All" to view all symbols

4. **View Visualizations**: Navigate through tabs:
   - **Overview**: Price timeline and volume charts
   - **Trade Analysis**: Trade size distribution and statistics
   - **NBBO Analysis**: Spread analysis and statistics
   - **Trade-NBBO Consistency**: Combined analysis showing trade prices relative to NBBO

## Data Structure

The app expects data organized as:

```
{data_root}/{data_source}/parquet_raw/
  ├── trades/
  │   └── trade_date={YYYY-MM-DD}/
  │       └── symbol={SYMBOL}/
  │           └── part_*.parquet
  ├── quotes/
  │   └── trade_date={YYYY-MM-DD}/
  │       └── symbol={SYMBOL}/
  │           └── part_*.parquet
  └── nbbo/
      └── trade_date={YYYY-MM-DD}/
          └── symbol={SYMBOL}/
              └── part_*.parquet
```

## Troubleshooting

### "No data sources configured"
- Check that `config.yaml` exists and contains data source configurations
- Verify that `parquet_raw_root` paths exist

### "Data not available"
- The app will suggest alternative dates and sources
- Check that parquet files exist for the selected date/symbol combination

### Password not working
- Ensure `.streamlit/secrets.toml` exists with `password` key
- Restart Streamlit after creating/modifying secrets file

## Development

### Adding New Visualizations

1. Add visualization function to `visualizations.py`
2. Import and use in `app.py`
3. Add to appropriate tab or create new tab

### Extending Data Sources

1. Add new data source configuration to `config.yaml`
2. Ensure data follows the expected structure
3. The app will automatically detect and include new sources

