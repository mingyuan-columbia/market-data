#!/bin/bash
# Run Streamlit app for market data visualization

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Activate virtual environment if it exists
if [ -d "mkdata" ]; then
    source mkdata/bin/activate
    # Use python -m streamlit for more reliable execution
    python -m streamlit run src/streamlit_app/app.py --server.port 8500 "$@"
else
    echo "Error: Virtual environment 'mkdata' not found"
    echo "Please create it with: python3 -m venv mkdata"
    exit 1
fi

