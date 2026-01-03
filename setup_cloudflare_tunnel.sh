#!/bin/bash
# Setup script for Cloudflare Tunnel
# This script helps set up cloudflared for exposing the Streamlit app

set -e

echo "Setting up Cloudflare Tunnel for Streamlit app..."

# Check if cloudflared is installed
if ! command -v cloudflared &> /dev/null; then
    echo "cloudflared is not installed. Installing..."
    
    # Detect OS
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        # Linux
        ARCH=$(uname -m)
        if [[ "$ARCH" == "x86_64" ]]; then
            ARCH="amd64"
        elif [[ "$ARCH" == "aarch64" ]]; then
            ARCH="arm64"
        fi
        
        echo "Downloading cloudflared for Linux ($ARCH)..."
        wget -q https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-${ARCH} -O cloudflared
        chmod +x cloudflared
        sudo mv cloudflared /usr/local/bin/
        echo "✓ cloudflared installed successfully"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        if command -v brew &> /dev/null; then
            echo "Installing via Homebrew..."
            brew install cloudflared
        else
            echo "Please install Homebrew first, or download cloudflared manually from:"
            echo "https://github.com/cloudflare/cloudflared/releases"
            exit 1
        fi
    else
        echo "Unsupported OS. Please install cloudflared manually from:"
        echo "https://github.com/cloudflare/cloudflared/releases"
        exit 1
    fi
else
    echo "✓ cloudflared is already installed"
fi

# Verify installation
cloudflared --version

echo ""
echo "Setup complete!"
echo ""
echo "Next steps:"
echo "1. Run: ./run_streamlit_with_tunnel.sh"
echo "2. The script will generate a Cloudflare Tunnel URL"
echo "3. Access your Streamlit app from anywhere using that URL"
echo ""
echo "Note: The tunnel URL will be displayed in the terminal output."

