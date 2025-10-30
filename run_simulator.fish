#!/usr/bin/env fish
# =============================================================================
# D-1 Sales Simulator Runner (Fish Shell)
# =============================================================================
#
# This script simplifies the execution of the sales simulator.
#
# Usage:
#   ./run_simulator.fish
#   OR
#   echo "10" | ./run_simulator.fish
#
# =============================================================================

# Change to script directory
cd (dirname (status -f))

# Check if .env exists
if not test -f .env
    echo "ERROR: .env file not found!"
    echo ""
    echo "Please create a .env file based on .env.example:"
    echo "  cp .env.example .env"
    echo ""
    echo "Then edit .env and add your NEON_ORG_ID and NEON_PROJECT_ID"
    exit 1
end

# Run the simulator
uv run python d1_sales_simulator.py
