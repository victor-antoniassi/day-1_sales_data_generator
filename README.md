# Chinook Sales Simulator

This project is a synthetic sales data generator for the Chinook database. It's designed to simulate daily sales transactions (D-1) to provide a dynamic data source for ETL/ELT pipelines.

This project is fully cross-platform and will run on Windows, macOS, and Linux.

## Features

*   Generates a configurable number of sales for the previous day (D-1)
*   Distributes sales randomly throughout the 24-hour period
*   Ensures data integrity by using a single database transaction for the entire batch
*   Connects securely to a Neon database using `neonctl`
*   Structured logging with configurable log levels
*   Database state validation before execution
*   Concurrent-safe ID generation using PostgreSQL SEQUENCES
*   Batch processing with performance optimizations
*   Summary statistics (total revenue, average sale)

## Requirements

*   Python 3.11+
*   [uv](https://docs.astral.sh/uv/)
*   [Neon CLI](https://neon.com/docs/reference/neon-cli)
*   A running instance of the [Chinook database](https://neon.com/docs/import/import-sample-data#chinook-database) on Neon.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd chinook_db
    ```

2.  **Create a `.env` file:**
    Copy the example file and fill in your Neon credentials:
    ```bash
    cp .env.example .env
    ```

    Edit `.env` and add your credentials:
    ```
    NEON_ORG_ID=org-your-org-id-here
    NEON_PROJECT_ID=your-project-id-here
    ```

    Optional parameters you can configure:
    ```
    # Optional: specify database role (default: project default)
    # NEON_ROLE=your_role_name

    # Optional: specify branch (default: main)
    # NEON_BRANCH=main

    # Optional: logging level (default: INFO)
    # LOG_LEVEL=DEBUG
    ```

3.  **Install dependencies:**
    ```bash
    uv sync
    ```

4.  **Set up the database:**
    Run the automated setup command. This will:
    - Validate your configuration
    - Test database connectivity
    - Update historical invoice data to align with D-1 workflow
    - Create the `simulate_new_sale` function and required sequences

    ```bash
    uv run main.py setup
    ```

    The setup is **idempotent** and safe to run multiple times.

## Usage

All commands are run via the `main.py` script and are the same for Windows, macOS, and Linux.

### Interactive Mode (Recommended)

Run the simulator and wait for it to prompt you for the number of sales.

```bash
uv run main.py simulate
```

### Non-Interactive Mode

To pass the number of sales directly (useful for automation), you can pipe it to the command:

```bash
echo "10" | uv run main.py simulate
```

### Example Output

```
2025-10-30 14:05:56 - __main__ - INFO - === D-1 Sales Simulator Starting ===
2025-10-30 14:05:56 - __main__ - INFO - User requested 5 sales
2025-10-30 14:05:56 - __main__ - INFO - Database connection established
2025-10-30 14:05:56 - __main__ - INFO - Validating database state...
2025-10-30 14:05:56 - __main__ - INFO - Database state validation successful
2025-10-30 14:05:56 - __main__ - INFO - Generating 5 random timestamps for D-1...
2025-10-30 14:05:56 - __main__ - INFO - Processing 5 sales in single transaction...
2025-10-30 14:05:58 - __main__ - INFO - SUCCESS: 5 sales committed to the database
2025-10-30 14:05:58 - __main__ - INFO - Total revenue: 2.88
2025-10-30 14:05:58 - __main__ - INFO - Average sale: $2.58
```

## Project Structure

```
chinook_db/
├── main.py                     # Main cross-platform entrypoint (setup, simulate)
├── d1_sales_simulator.py       # Core simulator logic
├── simulate_new_sale.sql       # PostgreSQL function with SEQUENCE support
├── update_historical_data.sql  # Idempotent historical data alignment script
├── .env.example                # Configuration template with documentation
├── .env                        # Your actual credentials (gitignored)
├── pyproject.toml              # Python dependencies
└── README.md                   # This file
```

## Troubleshooting

**"Function 'simulate_new_sale' not found"**
- Run the automated setup: `uv run main.py setup`
- Or manually: `neonctl connection-string | xargs -I {} psql {} -f simulate_new_sale.sql`

**"neonctl not found"**
- Install Neon CLI: [https://neon.com/docs/reference/neon-cli](https://neon.com/docs/reference/neon-cli)

**"NEON_ORG_ID and NEON_PROJECT_ID must be set"**
- Create `.env` file from `.env.example` and add your credentials

**Need to reset historical data?**
- The setup command is idempotent. If you need to re-align historical dates, you can run it again:
  `uv run main.py setup`

**Want more detailed logs?**
- Add `LOG_LEVEL=DEBUG` to your `.env` file