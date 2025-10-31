# Chinook Sales Simulator

![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20macOS%20%7C%20Linux-lightgrey.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-16-blue.svg)

A synthetic sales data generator for the Chinook database, designed to simulate realistic daily transactions for data engineering practice and portfolio projects.

This project is fully cross-platform and will run on Windows, macOS, and Linux.

---

## What is This Project?

This project simulates a **real-world online sales system** that generates synthetic transaction data for practice and learning.

### Use Case

Perfect for **building data engineering portfolios** without needing access to real production databases. Great for:
- Learning data pipeline development (ETL/ELT)
- Testing data warehouse architectures
- Practicing with realistic transactional data
- Building end-to-end data engineering projects

### Key Concept: D-1 Batch Processing

**D-1** means "Day minus 1" — processing yesterday's data today.

This is a common pattern in production data engineering:
- **Today is November 1st** → Script generates sales for **October 31st**
- **Today is November 2nd** → Script generates sales for **November 1st**

**Why D-1?**
In real companies, sales data is typically processed in overnight batches. This simulator replicates that realistic workflow, making your portfolio projects more authentic.

### How It Fits Into a Data Pipeline

```
┌─────────────────┐         ┌──────────────┐         ┌─────────────────┐
│  This Simulator │  D-1    │  PostgreSQL  │  Batch  │   Data Pipeline │
│    (Python)     │─Sales──>│    (Neon)    │─Extract>│   (Databricks,  │
│                 │         │              │         │   Airflow, etc) │
└─────────────────┘         └──────────────┘         └─────────────────┘
                                                               │
                                                               ▼
                                                      ┌─────────────────┐
                                                      │   Data Lakehouse│
                                                      │  (Bronze/Silver/│
                                                      │       Gold)     │
                                                      └─────────────────┘
```

This project is the **data source** — the starting point for building complete data engineering solutions.

---

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

## Prerequisites - First Time Setup

**If this is your first time**, you'll need to install some tools and set up your Neon database. Follow the sections below before running the simulator.

### Required Tools

#### 1. Python 3.11+

**What it is**: The programming language this project is written in.

**Check if installed**:
```bash
python --version  # or python3 --version
```

**Install if needed**: [python.org/downloads](https://www.python.org/downloads/)

#### 2. uv (Python Package Manager)

**What it is**: A modern, fast Python package manager (faster and more reliable than pip).

**Why uv instead of pip?**: Better dependency resolution, faster installs, and built-in virtual environment management.

**Install**:
```bash
# macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"
```

**Verify**:
```bash
uv --version
```

**Learn more**: [docs.astral.sh/uv](https://docs.astral.sh/uv/)

#### 3. Node.js 18+ (Required for neonctl)

**What it is**: JavaScript runtime needed to run the Neon CLI tool.

**Check if installed**:
```bash
node --version
```

**Install if needed**: [nodejs.org](https://nodejs.org/) (Download LTS version)

#### 4. Neon CLI (neonctl)

**What it is**: Command-line tool to interact with your Neon database from the terminal.

**Install via npm**:
```bash
npm i -g neonctl
```

**Verify**:
```bash
neonctl --version
```

**Learn more**: [Neon CLI Docs](https://neon.com/docs/reference/neon-cli)

#### 5. PostgreSQL Client (psql)

**What it is**: Command-line tool to connect to PostgreSQL databases (needed to import Chinook data).

**Check if installed**:
```bash
psql --version
```

**Install if needed**:
- **macOS**: `brew install postgresql` (or download from [postgresql.org](https://www.postgresql.org/download/))
- **Linux**: `sudo apt install postgresql-client` (Debian/Ubuntu) or equivalent
- **Windows**: Download from [postgresql.org](https://www.postgresql.org/download/)

**Note**: You only need the **client** (`psql`), not the full PostgreSQL server.

---

## Setting Up Your Neon Database

**If you already have a Neon account with the Chinook database**, skip to the [Setup](#setup) section below.

### Step 1: Create a Neon Account and Project

1. Go to [neon.tech](https://neon.tech) and click **Sign Up** (free tier available)
2. Sign up with GitHub, Google, or email
3. Once logged in, click **New Project**
4. Configure your project:
   - **Name**: "chinook-simulator" (or any name you prefer)
   - **Region**: Choose the one closest to you
   - **PostgreSQL version**: 16 (recommended, or latest available)
5. Click **Create Project**

Your first project is created with a default database called `neondb`.

### Step 2: Import the Chinook Sample Database

The Chinook database contains realistic data for a digital media store (artists, albums, tracks, customers, invoices).

#### Get Your Connection String

1. In your Neon project dashboard, click the **Connect** button (top right)
2. Copy the connection string (it looks like: `postgresql://user:password@host/neondb`)
3. Keep this handy — you'll need it for the next commands

#### Import via Command Line

```bash
# 1. Create the chinook database
psql "<your-connection-string>" -c "CREATE DATABASE chinook;"

# 2. Download the official Chinook SQL file
wget https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/chinook.sql

# 3. Import the data (replace <your-connection-string> with your actual string)
#    Note: Change /neondb to /chinook at the end of your connection string
psql -d "<your-connection-string>/chinook" -f chinook.sql

# 4. Verify the import (should return 412)
psql "<your-connection-string>/chinook" -c 'SELECT COUNT(*) FROM "Invoice";'
```

**Expected output**: `412` (number of invoices in the sample data)

**Troubleshooting**:
- If `wget` is not installed, download the file manually from the URL in step 2
- On Windows without `wget`, use: `curl -O https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/chinook.sql`

### Step 3: Get Your Neon Credentials

You'll need two IDs to configure this project:

#### Finding Your Organization ID (NEON_ORG_ID)

**Via Neon Console**:
1. In the Neon Console, click on your organization name (top-left corner)
2. Go to **Settings** (in the organization menu)
3. Look under **General information**
4. Copy the **Organization ID** (format: `org-word-word-12345678`)

**Via CLI** (alternative method):
```bash
neonctl orgs list
# Your org ID will be shown in the output
```

#### Finding Your Project ID (NEON_PROJECT_ID)

**Via Neon Console**:
1. Open your project in the Neon Console
2. Click **Settings** (in the left sidebar)
3. Under **General**, find and copy the **Project ID**

**Via CLI** (alternative method):
```bash
neonctl projects list
# Your project ID will be shown in the output
```

**Save these IDs** — you'll need them in the next step!

---

## Setup

**Prerequisites**: Make sure you've completed the [Prerequisites](#prerequisites---first-time-setup) and [Setting Up Your Neon Database](#setting-up-your-neon-database) sections above.

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/victor-antoniassi/day-1_sales_data_generator.git
    cd day-1_sales_data_generator
    ```

2.  **Create a `.env` file:**
    Copy the example file and fill in your Neon credentials (from [Step 3](#step-3-get-your-neon-credentials) above):
    ```bash
    cp .env.example .env
    ```

    Edit `.env` and add the credentials you found earlier:
    ```
    NEON_ORG_ID=org-your-actual-org-id
    NEON_PROJECT_ID=your-actual-project-id
    ```

    **Reminder**: See [Finding Your Organization ID](#finding-your-organization-id-neon_org_id) and [Finding Your Project ID](#finding-your-project-id-neon_project_id) if you skipped those steps.

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

## Glossary

Quick reference for technical terms used in this project:

| Term | Definition |
|------|------------|
| **D-1** | "Day minus 1" — yesterday. In data engineering, processing yesterday's data today (common batch pattern). |
| **OLTP** | Online Transaction Processing — database optimized for day-to-day operations (inserts, updates). Opposite of OLAP (analytics). |
| **ETL/ELT** | Extract, Transform, Load / Extract, Load, Transform — processes for moving data between systems. |
| **Synthetic Data** | Artificially generated data that mimics real data patterns, used for testing and development. |
| **Idempotent** | Safe to run multiple times without unintended side effects. Running the same operation twice produces the same result. |
| **Batch Processing** | Processing data in groups (batches) rather than one record at a time. Common in overnight data pipelines. |
| **Sequence** | PostgreSQL feature for generating unique, sequential IDs safely in concurrent environments. |
| **Lakehouse** | Modern data architecture combining data lake (storage) and data warehouse (analytics) features. |
| **Medallion Architecture** | Data organization pattern with Bronze (raw), Silver (cleaned), and Gold (aggregated) layers. |

---

## Troubleshooting

**"Function 'simulate_new_sale' not found"**
- Run the automated setup: `uv run main.py setup`
- Or manually: `neonctl connection-string | xargs -I {} psql {} -f simulate_new_sale.sql`

**"neonctl not found"**
- Install Neon CLI: `npm i -g neonctl`
- See [Prerequisites](#prerequisites---first-time-setup) section for details

**"NEON_ORG_ID and NEON_PROJECT_ID must be set"**
- Create `.env` file from `.env.example` and add your credentials
- See [Setting Up Your Neon Database](#setting-up-your-neon-database) section for how to find these IDs

**"psql: command not found"**
- Install PostgreSQL client (see [Prerequisites](#prerequisites---first-time-setup))
- You only need the client, not the full PostgreSQL server

**Need to reset historical data?**
- The setup command is idempotent. If you need to re-align historical dates, you can run it again:
  `uv run main.py setup`

**Want more detailed logs?**
- Add `LOG_LEVEL=DEBUG` to your `.env` file

**Import failed with "database chinook already exists"**
- That's okay! It means you already imported it. Skip to the verification step.

**Verification shows wrong number of invoices**
- Expected: 412 invoices
- If different, try dropping and recreating: `psql "<connection-string>" -c "DROP DATABASE chinook;"` then repeat Step 2 of [Setting Up Your Neon Database](#setting-up-your-neon-database)