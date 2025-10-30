# Chinook Sales Simulator

This project is a synthetic sales data generator for the Chinook database. It's designed to simulate daily sales transactions (D-1) to provide a dynamic data source for ETL/ELT pipelines.

## Features

*   Generates a configurable number of sales for the previous day (D-1).
*   Distributes sales randomly throughout the 24-hour period.
*   Ensures data integrity by using a single database transaction for the entire batch.
*   Connects securely to a Neon database using `neonctl`.

## Requirements

*   Python 3.11+
*   [uv](https://docs.astral.sh/uv/)
*   [Neon CLI](https://neon.com/docs/reference/neon-cli)
*   A running instance of the [Chinook database](https://neon.com/docs/import/import-sample-data) on Neon.

## Setup

1.  **Clone the repository:**
    ```bash
    git clone <repository-url>
    cd chinook_db
    ```

2.  **Create a `.env` file:**
    Create a `.env` file in the root of the project with your Neon credentials:
    ```
    NEON_ORG_ID=<your-neon-org-id>
    NEON_PROJECT_ID=<your-neon-project-id>
    ```
    
    *Note:* Other `PG*` variables (e.g., `PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`) are part of the connection string provided by Neon and are used by `neonctl`. You typically obtain these from your Neon project dashboard.

3.  **Install dependencies:**
    ```bash
    uv sync
    ```

4.  **Set up the database function:**
    This project requires a PostgreSQL function to be created in your Chinook database. Run the following command to apply the function:
    ```bash
    neonctl connection-string | xargs -I {} psql {} -f simulate_new_sale.sql
    ```

## Usage

To run the simulation, execute the following command, replacing `<number_of_sales>` with the desired amount:

```bash
echo "<number_of_sales>" | uv run python d1_sales_simulator.py
```

For example, to generate 10 sales:

```bash
echo "10" | uv run python d1_sales_simulator.py
```

## Expected Results

When you run the simulator, you should expect:

*   **Timestamps:** All sales will have timestamps within the previous day (D-1), randomly distributed across the 24-hour period.
*   **Invoice Items:** Each invoice will contain 1-5 randomly selected items (tracks).
*   **Calculated Totals:** Invoice totals are automatically calculated based on the sum of track prices.
*   **Referential Integrity:** All foreign keys (customers, tracks) reference existing records in the database.
*   **No Duplicates:** No invoice will contain duplicate tracks.

## Validation

After running the simulator, you can verify the results with these queries:

**Count D-1 sales created:**
```sql
SELECT COUNT(*) as d1_invoices
FROM "Invoice"
WHERE "InvoiceDate" >= CURRENT_DATE - INTERVAL '1 day'
  AND "InvoiceDate" < CURRENT_DATE;
```

**Verify invoice totals are correct:**
```sql
SELECT
  i."InvoiceId",
  i."Total" as recorded_total,
  COALESCE(SUM(il."UnitPrice" * il."Quantity"), 0) as calculated_total
FROM "Invoice" i
LEFT JOIN "InvoiceLine" il ON i."InvoiceId" = il."InvoiceId"
WHERE i."InvoiceDate" >= CURRENT_DATE - INTERVAL '1 day'
  AND i."InvoiceDate" < CURRENT_DATE
GROUP BY i."InvoiceId", i."Total"
LIMIT 10;
```

**Check timestamp distribution by hour:**
```sql
SELECT
  EXTRACT(HOUR FROM "InvoiceDate") as hour,
  COUNT(*) as sales_count
FROM "Invoice"
WHERE "InvoiceDate" >= CURRENT_DATE - INTERVAL '1 day'
  AND "InvoiceDate" < CURRENT_DATE
GROUP BY EXTRACT(HOUR FROM "InvoiceDate")
ORDER BY hour;
```