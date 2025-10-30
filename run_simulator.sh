#!/bin/bash
# =============================================================================
# D-1 Sales Simulator Runner
# =============================================================================
#
# This script simplifies the execution of the sales simulator.
#
# Usage:
#   ./run_simulator.sh
#   OR
#   echo "10" | ./run_simulator.sh
#
# =============================================================================

set -e  # Exit on error

# Change to script directory
cd "$(dirname "$0")"

# Check if .env exists
if [ ! -f .env ]; then
    echo "ERROR: .env file not found!"
    echo ""
    echo "Please create a .env file based on .env.example:"
    echo "  cp .env.example .env"
    echo ""
    echo "Then edit .env and add your NEON_ORG_ID and NEON_PROJECT_ID"
    exit 1
fi

# Run the simulator
uv run python d1_sales_simulator.py
