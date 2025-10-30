"""
D-1 Sales Simulator for the Chinook Database.

This script orchestrates the generation of synthetic sales data for the day
before the script is run (D-1). It is designed to be used as a data source
for a D-1 batch data pipeline.

The script prompts the user for the number of sales to generate, then
securely connects to a Neon DB instance using neonctl. It calls a
PostgreSQL function (`simulate_new_sale`) for each sale, wrapping the entire
process in a single transaction to ensure atomicity.

Usage:
    echo "<num_sales>" | uv run python d1_sales_simulator.py
"""

import subprocess
import datetime
import random
import logging
import sys
from typing import List, Tuple
import psycopg
from psycopg import sql
from dotenv import dotenv_values

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def get_connection_string() -> str:
    """
    Retrieves the database connection string from neonctl.

    This function requires the neonctl CLI to be installed and configured.
    It reads the Neon organization and project IDs from a .env file.

    Returns:
        str: The database connection string.

    Raises:
        ValueError: If NEON_ORG_ID or NEON_PROJECT_ID are not set in the
                    .env file.
        RuntimeError: If neonctl is not found or fails to execute.
    """
    config = dotenv_values()
    org_id = config.get("NEON_ORG_ID")
    project_id = config.get("NEON_PROJECT_ID")

    if not org_id or not project_id:
        raise ValueError(
            "NEON_ORG_ID and NEON_PROJECT_ID must be set in the .env file."
        )

    # Optional: Allow custom role or branch from .env
    role = config.get("NEON_ROLE", "")  # Empty means use default
    branch = config.get("NEON_BRANCH", "")  # Empty means use default

    command = [
        "neonctl",
        "connection-string",
        "--org-id",
        org_id,
        "--project-id",
        project_id,
    ]

    # Add optional parameters if configured
    if role:
        command.extend(["--role-name", role])
    if branch:
        command.extend(["--branch", branch])

    try:
        result = subprocess.run(
            command, capture_output=True, text=True, check=True
        )
        logger.debug("Connection string obtained successfully from neonctl")
        return result.stdout.strip()
    except FileNotFoundError:
        raise RuntimeError("neonctl not found. Please install and configure it.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Error getting connection string from neonctl: {e.stderr}"
        )


def validate_database_state(conn: psycopg.Connection) -> None:
    """
    Validates that the required database objects exist.

    Checks for:
    - simulate_new_sale function
    - invoice_id_seq sequence
    - invoice_line_id_seq sequence

    Args:
        conn: Active database connection

    Raises:
        RuntimeError: If required database objects are missing
    """
    logger.info("Validating database state...")

    with conn.cursor() as cur:
        # Check for the simulate_new_sale function
        cur.execute("""
            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_schema = 'public'
            AND routine_name = 'simulate_new_sale'
        """)
        if not cur.fetchone():
            raise RuntimeError(
                "Function 'simulate_new_sale' not found in database. "
                "Please run the simulate_new_sale.sql script first."
            )
        logger.debug("Function 'simulate_new_sale' found")

        # Check for required sequences
        cur.execute("""
            SELECT sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema = 'public'
            AND sequence_name IN ('invoice_id_seq', 'invoice_line_id_seq')
        """)
        sequences = [row[0] for row in cur.fetchall()]

        if 'invoice_id_seq' not in sequences:
            raise RuntimeError(
                "Sequence 'invoice_id_seq' not found. "
                "Please run the simulate_new_sale.sql script first."
            )
        logger.debug("Sequence 'invoice_id_seq' found")

        if 'invoice_line_id_seq' not in sequences:
            raise RuntimeError(
                "Sequence 'invoice_line_id_seq' not found. "
                "Please run the simulate_new_sale.sql script first."
            )
        logger.debug("Sequence 'invoice_line_id_seq' found")

    logger.info("Database state validation successful")


def generate_random_timestamp_d1() -> datetime.datetime:
    """
    Generates a random timestamp within the previous day (D-1).

    The timestamp will be between 00:00:00 and 23:59:59 of the day
    before the current date.

    Returns:
        datetime.datetime: A random timestamp from yesterday.
    """
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)

    start_of_yesterday = datetime.datetime.combine(yesterday, datetime.time.min)
    end_of_yesterday = datetime.datetime.combine(yesterday, datetime.time.max)

    time_diff_seconds = int((end_of_yesterday - start_of_yesterday).total_seconds())
    random_seconds = random.randint(0, time_diff_seconds)

    return start_of_yesterday + datetime.timedelta(seconds=random_seconds)


def generate_timestamps_batch(num_sales: int) -> List[datetime.datetime]:
    """
    Generates a batch of random timestamps for D-1.

    Pre-generating all timestamps improves performance by separating
    random generation from database operations.

    Args:
        num_sales: Number of timestamps to generate

    Returns:
        List of datetime objects sorted chronologically
    """
    logger.info(f"Generating {num_sales} random timestamps for D-1...")
    timestamps = [generate_random_timestamp_d1() for _ in range(num_sales)]
    # Sort timestamps chronologically for more realistic data patterns
    timestamps.sort()
    logger.debug(f"Generated timestamps range: {timestamps[0]} to {timestamps[-1]}")
    return timestamps


def process_sales_batch(
    cur: psycopg.Cursor,
    timestamps: List[datetime.datetime]
) -> List[Tuple[int, float]]:
    """
    Processes a batch of sales using pre-generated timestamps.

    Args:
        cur: Database cursor
        timestamps: List of timestamps for the sales

    Returns:
        List of tuples (invoice_id, total) for each generated sale
    """
    results = []
    total_sales = len(timestamps)

    logger.info(f"Processing {total_sales} sales in single transaction...")

    for i, timestamp in enumerate(timestamps, 1):
        # Execute the PostgreSQL function to simulate one sale
        cur.execute(
            "SELECT generated_invoice_id, generated_total "
            "FROM simulate_new_sale(%s);",
            (timestamp,),
        )
        result = cur.fetchone()

        if result:
            invoice_id, total = result
            results.append((invoice_id, float(total)))

            # Log progress every 10% or for small batches
            if total_sales <= 10 or i % max(1, total_sales // 10) == 0:
                logger.info(
                    f"Progress: {i}/{total_sales} ({i*100//total_sales}%) - "
                    f"Pending InvoiceId={invoice_id}, "
                    f"Total=${float(total):.2f}, "
                    f"Timestamp={timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                )
        else:
            logger.warning(
                f"Sale {i}/{total_sales}: Function did not return data"
            )

    return results


def main() -> None:
    """
    Main function to run the sales simulation.

    Orchestrates the process of getting user input, connecting to the
    database, validating state, and generating the specified number of
    sales within a single database transaction.
    """
    try:
        # Get configuration and connection
        config = dotenv_values()
        log_level = config.get("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, log_level, logging.INFO))

        logger.info("=== D-1 Sales Simulator Starting ===")

        conn_string = get_connection_string()
        logger.info("Connection string obtained successfully")

        # Get user input
        try:
            num_sales_str = input(
                "How many sales do you want to generate for D-1? "
            )
            if not num_sales_str.isdigit() or int(num_sales_str) <= 0:
                logger.error("Invalid input: must be a positive integer")
                sys.exit(1)
            num_sales = int(num_sales_str)
            logger.info(f"User requested {num_sales} sales")
        except ValueError:
            logger.error("Invalid input format")
            sys.exit(1)

        # Connect and process
        with psycopg.connect(conn_string) as conn:
            logger.info("Database connection established")

            # Validate database state
            validate_database_state(conn)

            # Pre-generate all timestamps for batch processing
            timestamps = generate_timestamps_batch(num_sales)

            # Process sales in a single transaction
            try:
                with conn.cursor() as cur:
                    logger.info(
                        f"Starting batch generation of {num_sales} sales "
                        "in a SINGLE TRANSACTION..."
                    )

                    results = process_sales_batch(cur, timestamps)

                    # Commit the transaction
                    logger.info("Batch successfully prepared. Committing transaction...")
                    conn.commit()

                    logger.info(
                        f"SUCCESS: {len(results)} sales committed to the database"
                    )

                    # Summary statistics
                    if results:
                        total_revenue = sum(total for _, total in results)
                        avg_revenue = total_revenue / len(results)
                        logger.info(f"Total revenue: ${total_revenue:.2f}")
                        logger.info(f"Average sale: ${avg_revenue:.2f}")

            except (Exception, psycopg.Error) as e:
                # If any error occurs, roll back the entire transaction
                logger.error(f"Error during batch generation: {e}", exc_info=True)
                logger.info("Rolling back all changes for this batch...")
                conn.rollback()
                logger.info("Rollback complete. No sales were saved.")
                sys.exit(1)

    except (ValueError, RuntimeError) as e:
        # Catches configuration or execution errors
        logger.error(f"Configuration/execution error: {e}")
        sys.exit(1)
    except psycopg.Error as e:
        # Catches database connection errors
        logger.error(f"Database connection error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(130)
    except Exception as e:
        # Catches any other unexpected errors
        logger.error(f"Unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
