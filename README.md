# Chinook Sales Simulator

![Python](https://img.shields.io/badge/python-3.11+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/postgresql-16-blue.svg)
![Platform](https://img.shields.io/badge/platform-Windows%20%7C%20macOS%20%7C%20Linux-lightgrey.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)


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

*   Generates a configurable number of **inserts, updates, and deletes** for the previous day (D-1)
*   Simulates realistic transaction types:
    *   **Inserts**: New sales with 1 to 5 items.
    *   **Updates**: Adds a new track to an existing D-1 invoice.
    *   **Deletes**: Cancels a D-1 invoice by setting its total to 0 and removing its items.
*   Distributes new sales randomly throughout the 24-hour period
*   Ensures data integrity by using a single database transaction for the entire batch
*   Connects securely to a Neon database using `neonctl`
*   Structured logging with configurable log levels
*   Database state validation before execution
*   Concurrent-safe ID generation using PostgreSQL SEQUENCES
*   Batch processing with performance optimizations
*   Summary statistics (total revenue, average sale for new invoices)

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

### Step 2: Choose Your Database Name

**Important decision**: You need to choose a name for the database where Chinook data will be stored.

**Recommended name**: `chinook_db` (we'll use this throughout the guide)

**Why it matters**: You'll need to use this EXACT name in:
- The database creation command below
- The data import command
- Your `.env` configuration file (later in Setup)

**Note**: You can choose any name you prefer (e.g., `chinook`, `music_store`, `sales_db`), just be consistent everywhere.

### Step 3: Import the Chinook Sample Database

The Chinook database contains realistic data for a digital media store (artists, albums, tracks, customers, invoices).

#### Get Your Connection String

1. In your Neon project dashboard, click the **Connect** button (top right)
2. Copy the connection string (it looks like: `postgresql://user:password@host/neondb`)
3. Keep this handy — you'll need it for the next commands

#### Understanding Connection Strings (Important!)

Your Neon connection string has this format:
```
postgresql://username:password@ep-cool-name-123456.us-east-2.aws.neon.tech/neondb
                                                                           ^^^^^^^
                                                                      database name
```

To connect to a different database, **replace the database name** at the end:
```
postgresql://username:password@ep-cool-name-123456.us-east-2.aws.neon.tech/chinook_db
                                                                           ^^^^^^^^^^
```

#### Import via Command Line

```bash
# 1. Create the chinook_db database (using your original connection string with /neondb)
psql "postgresql://username:password@host.neon.tech/neondb" -c "CREATE DATABASE chinook_db;"

# 2. Download the official Chinook SQL file
wget https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/chinook.sql

# 3. Import the data (replace /neondb with /chinook_db in your connection string)
psql "postgresql://username:password@host.neon.tech/chinook_db" -f chinook.sql

# 4. Verify the import (should return 412)
psql "postgresql://username:password@host.neon.tech/chinook_db" -c 'SELECT COUNT(*) FROM "Invoice";'
```

**Remember**: Replace `username:password@host.neon.tech` with your actual Neon credentials.

**Expected output**: `412` (number of invoices in the sample data)

**Troubleshooting**:
- If `wget` is not installed, download the file manually from the URL in step 2
- On Windows without `wget`, use: `curl -O https://raw.githubusercontent.com/neondatabase/postgres-sample-dbs/main/chinook.sql`

### Step 4: Get Your Neon Credentials

You'll need three values to configure this project:
- **NEON_ORG_ID**: Your organization ID
- **NEON_PROJECT_ID**: Your project ID
- **NEON_DATABASE**: The database name you chose in Step 2 (e.g., `chinook_db`)

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
    Copy the example file and fill in your Neon credentials (from [Step 4](#step-4-get-your-neon-credentials) above):
    ```bash
    cp .env.example .env
    # Windows (CMD) users: use 'copy' instead of 'cp'
    ```

    Edit `.env` and add the credentials you found earlier:
    ```
    NEON_ORG_ID=org-your-actual-org-id
    NEON_PROJECT_ID=your-actual-project-id
    NEON_DATABASE=chinook_db
    ```

    **Critical**: The `NEON_DATABASE` value MUST match the database name you created in [Step 2](#step-2-choose-your-database-name). If you used a different name than `chinook_db`, use that name here.

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
    - Validate your configuration and test connectivity
    - Update historical invoice data to align with D-1 workflow
    - Create all required simulation functions (`INSERT`, `UPDATE`, `DELETE`) and sequences

    ```bash
    uv run main.py setup
    ```

    The setup is **idempotent** and safe to run multiple times.

## Usage

All commands are run via the `main.py` script. The simulator now supports generating a mix of inserts, updates, and deletes in a single run.

The script will prompt you to enter three numbers separated by spaces:
1.  Number of **new sales** (Inserts)
2.  Number of **sale updates** (Updates)
3.  Number of **sale cancellations** (Deletes)

### Interactive Mode (Recommended)

Run the simulator and wait for it to prompt you for input.

```bash
uv run main.py simulate
```

You will see a prompt like this. Enter three numbers and press Enter:
```
Enter the number of INSERTS, UPDATES, and DELETES for D-1 (e.g., '100 5 2'): 100 5 2
```

### Non-Interactive Mode

To pass the numbers directly (useful for automation), you can `echo` a string with the three values and pipe it to the command.

```bash
# Format: "INSERTS UPDATES DELETES"
echo "100 5 2" | uv run main.py simulate
```

### Example Output

```
2025-11-12 10:30:00 - __main__ - INFO - === D-1 Sales Simulator Starting ===
Enter the number of INSERTS, UPDATES, and DELETES for D-1 (e.g., '100 5 2'): 100 5 2
2025-11-12 10:30:05 - d1_sales_simulator - INFO - Database connection established
2025-11-12 10:30:05 - d1_sales_simulator - INFO - Validating database state...
2025-11-12 10:30:05 - d1_sales_simulator - INFO - Database state validation successful
2025-11-12 10:30:05 - d1_sales_simulator - INFO - Starting batch operation in a SINGLE TRANSACTION...
2025-11-12 10:30:05 - d1_sales_simulator - INFO - Starting batch operation for D-1 (2025-11-11)
2025-11-12 10:30:05 - d1_sales_simulator - INFO - Requested: 100 Inserts, 5 Updates, 2 Deletes
2025-11-12 10:30:05 - d1_sales_simulator - INFO - Processing 2 DELETES...
2025-11-12 10:30:06 - d1_sales_simulator - INFO - Processing 5 UPDATES...
2025-11-12 10:30:07 - d1_sales_simulator - INFO - Processing 100 INSERTS...
2025-11-12 10:30:09 - d1_sales_simulator - INFO - Batch successfully prepared. Committing transaction...
2025-11-12 10:30:09 - d1_sales_simulator - INFO - SUCCESS: All operations committed to the database.
2025-11-12 10:30:09 - d1_sales_simulator - INFO - Summary: 100 new sales inserted, 5 updated, 2 deleted.
2025-11-12 10:30:09 - d1_sales_simulator - INFO - Total new revenue: $101.97
2025-11-12 10:30:09 - d1_sales_simulator - INFO - Average new sale: $1.02
```

## Project Structure

```
chinook_db/
├── main.py                     # Main cross-platform entrypoint (setup, simulate)
├── d1_sales_simulator.py       # Core simulator logic
├── simulate_new_sale.sql       # PostgreSQL function for INSERTS
├── simulate_modifications.sql  # PostgreSQL functions for UPDATES and DELETES
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

**"Function 'simulate_new_sale' not found" (or update/delete function)**
- Your database setup is likely incomplete or outdated. Run the automated setup again:
  `uv run main.py setup`

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
