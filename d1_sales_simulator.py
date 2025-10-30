"""
D-1 Sales Simulator for the Chinook Database.

This script contains the core logic for generating synthetic sales data.
It is designed to be called from the main.py orchestrator.
"""

import datetime
import random
import logging
import sys
from typing import List, Tuple

import psycopg
from psycopg import sql
from dotenv import dotenv_values

# Logger will be configured by the entry point (main.py)
logger = logging.getLogger(__name__)


def validate_database_state(conn: psycopg.Connection) -> None:
    """
    Validates that the required database objects exist.
    """
    logger.info("Validating database state...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_schema = 'public'
            AND routine_name = 'simulate_new_sale'
        """)
        if not cur.fetchone():
            raise RuntimeError(
                "Function 'simulate_new_sale' not found. Please run 'uv run main.py setup'."
            )
        logger.debug("Function 'simulate_new_sale' found")

        cur.execute("""
            SELECT sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema = 'public'
            AND sequence_name IN ('invoice_id_seq', 'invoice_line_id_seq')
        """)
        sequences = [row[0] for row in cur.fetchall()]
        if 'invoice_id_seq' not in sequences or 'invoice_line_id_seq' not in sequences:
            raise RuntimeError(
                "Required sequences not found. Please run 'uv run main.py setup'."
            )
        logger.debug("Sequences 'invoice_id_seq' and 'invoice_line_id_seq' found")

    logger.info("Database state validation successful")


def generate_random_timestamp_d1() -> datetime.datetime:
    """
    Generates a random timestamp within the previous day (D-1).
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
    """
    logger.info(f"Generating {num_sales} random timestamps for D-1...")
    timestamps = [generate_random_timestamp_d1() for _ in range(num_sales)]
    timestamps.sort()
    if timestamps:
        logger.debug(f"Generated timestamps range: {timestamps[0]} to {timestamps[-1]}")
    return timestamps


def process_sales_batch(
    cur: psycopg.Cursor,
    timestamps: List[datetime.datetime]
) -> List[Tuple[int, float]]:
    """
    Processes a batch of sales using pre-generated timestamps.
    """
    results = []
    total_sales = len(timestamps)
    logger.info(f"Processing {total_sales} sales in single transaction...")

    for i, timestamp in enumerate(timestamps, 1):
        cur.execute(
            "SELECT generated_invoice_id, generated_total FROM simulate_new_sale(%s);",
            (timestamp,),
        )
        result = cur.fetchone()
        if result:
            invoice_id, total = result
            results.append((invoice_id, float(total)))
            if total_sales <= 10 or i % max(1, total_sales // 10) == 0:
                logger.info(
                    f"Progress: {i}/{total_sales} ({i*100//total_sales}%) - "
                    f"Pending InvoiceId={invoice_id}, Total=${float(total):.2f}"
                )
        else:
            logger.warning(f"Sale {i}/{total_sales}: Function did not return data")
    return results


def start_simulation(conn_string: str) -> None:
    """
    Main function to run the sales simulation.

    Orchestrates getting user input, connecting to the database, validating,
    and generating sales in a single transaction.
    """
    try:
        config = dotenv_values()
        log_level = config.get("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, log_level, logging.INFO))

        logger.info("=== D-1 Sales Simulator Starting ===")

        try:
            num_sales_str = input("How many sales do you want to generate for D-1? ")
            if not num_sales_str.isdigit() or int(num_sales_str) <= 0:
                logger.error("Invalid input: must be a positive integer")
                sys.exit(1)
            num_sales = int(num_sales_str)
            logger.info(f"User requested {num_sales} sales")
        except (ValueError, EOFError):
            logger.error("Invalid input format or no input provided.")
            sys.exit(1)

        with psycopg.connect(conn_string) as conn:
            logger.info("Database connection established")
            validate_database_state(conn)
            timestamps = generate_timestamps_batch(num_sales)

            with conn.cursor() as cur:
                try:
                    logger.info(f"Starting batch generation of {num_sales} sales in a SINGLE TRANSACTION...")
                    results = process_sales_batch(cur, timestamps)
                    logger.info("Batch successfully prepared. Committing transaction...")
                    conn.commit()
                    logger.info(f"SUCCESS: {len(results)} sales committed to the database")

                    if results:
                        total_revenue = sum(total for _, total in results)
                        avg_revenue = total_revenue / len(results)
                        logger.info(f"Total revenue: ${total_revenue:.2f}")
                        logger.info(f"Average sale: ${avg_revenue:.2f}")

                except (Exception, psycopg.Error) as e:
                    logger.error(f"Error during batch generation: {e}", exc_info=True)
                    logger.info("Rolling back all changes for this batch...")
                    conn.rollback()
                    logger.info("Rollback complete. No sales were saved.")
                    sys.exit(1)

    except psycopg.Error as e:
        logger.error(f"Database connection error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user.")
        sys.exit(130)
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)