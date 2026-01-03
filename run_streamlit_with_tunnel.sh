#!/bin/bash
# Run Streamlit app with Cloudflare Tunnel for external access

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Check if cloudflared is installed
if ! command -v cloudflared &> /dev/null; then
    echo "Error: cloudflared is not installed"
    echo "Run: ./setup_cloudflare_tunnel.sh to install it"
    exit 1
fi

# Activate virtual environment if it exists
if [ -d "mkdata" ]; then
    source mkdata/bin/activate
else
    echo "Error: Virtual environment 'mkdata' not found"
    echo "Please create it with: python3 -m venv mkdata"
    exit 1
fi

# Streamlit runs on port 8500 by default
STREAMLIT_PORT=8500

echo "=========================================="
echo "Starting Streamlit app with Cloudflare Tunnel"
echo "=========================================="
echo ""
echo "Streamlit will be available at: http://localhost:${STREAMLIT_PORT}"
echo "External access URL will be shown below..."
echo ""
echo "Press Ctrl+C to stop both services"
echo ""

# Function to cleanup on exit
cleanup() {
    echo ""
    echo "Shutting down..."
    kill $STREAMLIT_PID $TUNNEL_PID 2>/dev/null
    exit 0
}

trap cleanup SIGINT SIGTERM

# Start Streamlit in the background with CLOUDFLARE_TUNNEL environment variable
echo "Starting Streamlit app..."
CLOUDFLARE_TUNNEL=true python -m streamlit run src/streamlit_app/app.py --server.port ${STREAMLIT_PORT} --server.headless true &
STREAMLIT_PID=$!

# Wait a moment for Streamlit to start
sleep 3

# Check if Streamlit started successfully
if ! kill -0 $STREAMLIT_PID 2>/dev/null; then
    echo "Error: Streamlit failed to start"
    exit 1
fi

echo "✓ Streamlit app started (PID: $STREAMLIT_PID)"
echo ""

# Start Cloudflare Tunnel
echo "Starting Cloudflare Tunnel..."
echo "=========================================="
echo "Your public URL will appear below:"
echo "=========================================="
echo ""

# Run cloudflared tunnel (this will block and show the URL)
cloudflared tunnel --url http://localhost:${STREAMLIT_PORT}

