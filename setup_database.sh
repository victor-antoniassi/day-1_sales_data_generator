#!/bin/bash
# =============================================================================
# Database Setup Script for Chinook Sales Simulator
# =============================================================================
#
# This script automates the complete database setup process:
#   1. Validates environment configuration (.env file)
#   2. Tests database connectivity
#   3. Updates historical invoice data to align with D-1 workflow
#   4. Creates the simulate_new_sale function and required sequences
#
# USAGE:
#   ./setup_database.sh
#
# REQUIREMENTS:
#   - neonctl CLI installed and configured
#   - .env file with NEON_ORG_ID and NEON_PROJECT_ID
#   - Chinook database already deployed on Neon
#
# =============================================================================

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Change to script directory
cd "$(dirname "$0")"

echo -e "${BLUE}=============================================================${NC}"
echo -e "${BLUE}Chinook Database Setup${NC}"
echo -e "${BLUE}=============================================================${NC}"
echo ""

# Step 1: Check if .env exists
echo -e "${YELLOW}[1/5] Checking configuration...${NC}"
if [ ! -f .env ]; then
    echo -e "${RED}ERROR: .env file not found!${NC}"
    echo ""
    echo "Please create a .env file based on .env.example:"
    echo "  cp .env.example .env"
    echo ""
    echo "Then edit .env and add your NEON_ORG_ID and NEON_PROJECT_ID"
    exit 1
fi
echo -e "${GREEN}✓ Configuration file found${NC}"
echo ""

# Step 2: Check if neonctl is available
echo -e "${YELLOW}[2/5] Checking neonctl CLI...${NC}"
if ! command -v neonctl &> /dev/null; then
    echo -e "${RED}ERROR: neonctl not found!${NC}"
    echo ""
    echo "Please install neonctl: https://neon.com/docs/reference/neon-cli"
    exit 1
fi
echo -e "${GREEN}✓ neonctl CLI found ($(neonctl --version))${NC}"
echo ""

# Step 3: Test database connection
echo -e "${YELLOW}[3/5] Testing database connection...${NC}"
CONNECTION_STRING=$(neonctl connection-string)
if [ -z "$CONNECTION_STRING" ]; then
    echo -e "${RED}ERROR: Could not get connection string from neonctl${NC}"
    echo "Please check your .env configuration"
    exit 1
fi

# Test with a simple query
if ! echo "SELECT 1 AS test;" | psql "$CONNECTION_STRING" -t -A > /dev/null 2>&1; then
    echo -e "${RED}ERROR: Could not connect to database${NC}"
    echo "Please verify your Neon configuration"
    exit 1
fi
echo -e "${GREEN}✓ Database connection successful${NC}"
echo ""

# Step 4: Update historical data
echo -e "${YELLOW}[4/5] Updating historical invoice data...${NC}"
if [ ! -f update_historical_data.sql ]; then
    echo -e "${RED}ERROR: update_historical_data.sql not found${NC}"
    exit 1
fi

if psql "$CONNECTION_STRING" -f update_historical_data.sql; then
    echo -e "${GREEN}✓ Historical data update completed${NC}"
else
    echo -e "${RED}ERROR: Failed to update historical data${NC}"
    exit 1
fi
echo ""

# Step 5: Create simulate_new_sale function
echo -e "${YELLOW}[5/5] Creating simulate_new_sale function and sequences...${NC}"
if [ ! -f simulate_new_sale.sql ]; then
    echo -e "${RED}ERROR: simulate_new_sale.sql not found${NC}"
    exit 1
fi

if psql "$CONNECTION_STRING" -f simulate_new_sale.sql; then
    echo -e "${GREEN}✓ Function and sequences created successfully${NC}"
else
    echo -e "${RED}ERROR: Failed to create function${NC}"
    exit 1
fi
echo ""

# Verify installation
echo -e "${YELLOW}Verifying installation...${NC}"
FUNCTION_EXISTS=$(psql "$CONNECTION_STRING" -t -A -c "SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = 'public' AND routine_name = 'simulate_new_sale';")

if [ "$FUNCTION_EXISTS" -eq "1" ]; then
    echo -e "${GREEN}✓ simulate_new_sale function verified${NC}"
else
    echo -e "${RED}⨯ Function verification failed${NC}"
    exit 1
fi

# Success message
echo ""
echo -e "${BLUE}=============================================================${NC}"
echo -e "${GREEN}✓ DATABASE SETUP COMPLETE!${NC}"
echo -e "${BLUE}=============================================================${NC}"
echo ""
echo "You can now run the sales simulator:"
echo "  ./run_simulator.sh"
echo ""
echo "Or directly:"
echo "  echo \"10\" | uv run python d1_sales_simulator.py"
echo ""
