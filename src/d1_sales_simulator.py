"""
D-1 Sales Simulator for the Chinook Database.

This script contains the core logic for generating and modifying synthetic sales data.
It is designed to be called from the main.py orchestrator.
"""

import datetime
import random
import logging
import sys
import os
import toml
from typing import List, Tuple, Dict, Any

import psycopg
from psycopg import sql

# Logger will be configured by the entry point (main.py)
logger = logging.getLogger(__name__)

# Project root directory, assuming src is one level down
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOGS_DIR = os.path.join(PROJECT_ROOT, "simulation_logs")


def validate_database_state(conn: psycopg.Connection) -> None:
    """
    Validates that the required database objects (functions, sequences) exist.
    """
    logger.info("Validating database state...")
    with conn.cursor() as cur:
        cur.execute("""
            SELECT routine_name
            FROM information_schema.routines
            WHERE routine_schema = 'public'
            AND routine_name IN ('simulate_new_sale', 'simulate_update_sale', 'simulate_delete_sale')
        """)
        found_functions = {row[0] for row in cur.fetchall()}
        required_functions = {'simulate_new_sale', 'simulate_update_sale', 'simulate_delete_sale'}

        if not required_functions.issubset(found_functions):
            missing = required_functions - found_functions
            raise RuntimeError(
                f"Missing required functions: {', '.join(missing)}. Please run 'uv run main.py setup'."
            )
        logger.debug("All required functions (INSERT, UPDATE, DELETE) found.")

        cur.execute("""
            SELECT sequence_name
            FROM information_schema.sequences
            WHERE sequence_schema = 'public'
            AND sequence_name IN ('invoice_id_seq', 'invoice_line_id_seq')
        """)
        sequences = {row[0] for row in cur.fetchall()}
        if 'invoice_id_seq' not in sequences or 'invoice_line_id_seq' not in sequences:
            raise RuntimeError(
                "Required sequences not found. Please run 'uv run main.py setup'."
            )
        logger.debug("Required sequences 'invoice_id_seq' and 'invoice_line_id_seq' found.")

    logger.info("Database state validation successful")


def get_d1_date_range() -> Tuple[datetime.datetime, datetime.datetime]:
    """Returns the start and end datetimes for the previous day (D-1)."""
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    start_of_yesterday = datetime.datetime.combine(yesterday, datetime.time.min)
    end_of_yesterday = datetime.datetime.combine(yesterday, datetime.time.max)
    return start_of_yesterday, end_of_yesterday


def _perform_deletes(cur: psycopg.Cursor, num_deletes: int, simulation_date: datetime.datetime) -> List[Dict[str, Any]]:
    """Executes delete operations and returns a log of them."""
    if num_deletes == 0:
        return []
    logger.info(f"Processing {num_deletes} DELETES...")
    deleted_ops = []
    for i in range(num_deletes):
        cur.execute("SELECT simulate_delete_sale(%s);", (simulation_date.date(),))
        deleted_id = cur.fetchone()[0]
        if deleted_id:
            deleted_ops.append({
                "type": "delete",
                "invoice_id": deleted_id,
                "timestamp": simulation_date.isoformat()
            })
            logger.info(f"Progress: {i+1}/{num_deletes} deletes executed for InvoiceId {deleted_id}.")
        else:
            logger.warning(f"Progress: {i+1}/{num_deletes} - No suitable invoice found to delete.")
    return deleted_ops


def _perform_updates(cur: psycopg.Cursor, num_updates: int, simulation_date: datetime.datetime) -> List[Dict[str, Any]]:
    """Executes update operations and returns a log of them."""
    if num_updates == 0:
        return []
    logger.info(f"Processing {num_updates} UPDATES...")
    updated_ops = []
    for i in range(num_updates):
        cur.execute("SELECT simulate_update_sale(%s);", (simulation_date.date(),))
        updated_id = cur.fetchone()[0]
        if updated_id:
            updated_ops.append({
                "type": "update",
                "invoice_id": updated_id,
                "timestamp": simulation_date.isoformat()
            })
            logger.info(f"Progress: {i+1}/{num_updates} updates executed for InvoiceId {updated_id}.")
        else:
            logger.warning(f"Progress: {i+1}/{num_updates} - No suitable invoice found to update.")
    return updated_ops


def _perform_inserts(cur: psycopg.Cursor, num_inserts: int) -> List[Dict[str, Any]]:
    """Generates and inserts new sales, returning a log of them."""
    if num_inserts == 0:
        return []
    
    logger.info(f"Processing {num_inserts} INSERTS...")
    start_date, end_date = get_d1_date_range()
    time_diff_seconds = int((end_date - start_date).total_seconds())
    
    inserted_ops = []
    for i in range(num_inserts):
        random_seconds = random.randint(0, time_diff_seconds)
        timestamp = start_date + datetime.timedelta(seconds=random_seconds)
        
        cur.execute(
            "SELECT generated_invoice_id, generated_total FROM simulate_new_sale(%s);",
            (timestamp,),
        )
        result = cur.fetchone()
        if result:
            invoice_id, total = result
            inserted_ops.append({
                "type": "insert",
                "invoice_id": invoice_id,
                "total": float(total),
                "timestamp": timestamp.isoformat()
            })
            if num_inserts <= 10 or (i+1) % max(1, num_inserts // 10) == 0:
                logger.info(
                    f"Progress: {i+1}/{num_inserts} ({ (i+1)*100//num_inserts }%) - "
                    f"Pending InvoiceId={invoice_id}, Total=${float(total):.2f}"
                )
        else:
            logger.warning(f"Insert {i+1}/{num_inserts}: Function did not return data")
    return inserted_ops


def process_operations_batch(
    cur: psycopg.Cursor,
    num_inserts: int,
    num_updates: int,
    num_deletes: int
) -> List[Dict[str, Any]]:
    """
    Processes a batch of deletes, updates, and inserts, returning a log of all operations.
    """
    start_date, _ = get_d1_date_range()
    simulation_date_for_mods = start_date # Use the start of D-1 for modification context

    logger.info(f"Starting batch operation for D-1 ({start_date.date()})")
    logger.info(f"Requested: {num_inserts} Inserts, {num_updates} Updates, {num_deletes} Deletes")

    all_operations = []
    # Order of operations: Deletes -> Updates -> Inserts
    all_operations.extend(_perform_deletes(cur, num_deletes, simulation_date_for_mods))
    all_operations.extend(_perform_updates(cur, num_updates, simulation_date_for_mods))
    all_operations.extend(_perform_inserts(cur, num_inserts))
    
    return all_operations


def write_log_file(start_time: datetime.datetime, operations: List[Dict[str, Any]], d1_date: datetime.date):
    """Writes the simulation operations to a TOML log file."""
    os.makedirs(LOGS_DIR, exist_ok=True)
    
    log_filename = f"simulation_{start_time.strftime('%Y-%m-%d_%H-%M-%S')}.toml"
    log_filepath = os.path.join(LOGS_DIR, log_filename)

    summary = {
        "inserts": sum(1 for op in operations if op['type'] == 'insert'),
        "updates": sum(1 for op in operations if op['type'] == 'update'),
        "deletes": sum(1 for op in operations if op['type'] == 'delete'),
    }

    log_data = {
        "simulation_summary": {
            "d1_date": d1_date.isoformat(),
            "simulation_timestamp_utc": start_time.isoformat(),
            "counts": summary
        },
        "operations": operations
    }

    try:
        with open(log_filepath, "w", encoding="utf-8") as f:
            toml.dump(log_data, f)
        logger.info(f"Simulation log saved to: {log_filepath}")
    except Exception as e:
        logger.error(f"Failed to write log file: {e}")


def start_simulation(conn_string: str, num_inserts: int, num_updates: int, num_deletes: int) -> None:
    """
    Main function to run the sales simulation.
    Orchestrates DB connection, validation, data generation, and logging.
    """
    simulation_start_time = datetime.datetime.now(datetime.timezone.utc)
    d1_date, _ = get_d1_date_range()
    
    try:
        logger.info("=== D-1 Sales Simulator Starting ===")
        
        with psycopg.connect(conn_string) as conn:
            logger.info("Database connection established")
            validate_database_state(conn)

            with conn.cursor() as cur:
                try:
                    logger.info("Starting batch operation in a SINGLE TRANSACTION...")
                    
                    operations = process_operations_batch(cur, num_inserts, num_updates, num_deletes)
                    
                    logger.info("Batch successfully prepared. Committing transaction...")
                    conn.commit()
                    logger.info("SUCCESS: All operations committed to the database.")
                    
                    # Write log file ONLY after successful commit
                    write_log_file(simulation_start_time, operations, d1_date.date())

                    # Final summary
                    inserts_results = [op for op in operations if op['type'] == 'insert']
                    logger.info(f"Summary: {len(inserts_results)} new sales inserted, "
                                f"{sum(1 for op in operations if op['type'] == 'update')} updated, "
                                f"{sum(1 for op in operations if op['type'] == 'delete')} deleted.")

                    if inserts_results:
                        total_revenue = sum(op.get('total', 0) for op in inserts_results)
                        avg_revenue = total_revenue / len(inserts_results)
                        logger.info(f"Total new revenue: ${total_revenue:.2f}")
                        logger.info(f"Average new sale: ${avg_revenue:.2f}")

                except (Exception, psycopg.Error) as e:
                    logger.error(f"Error during batch operation: {e}", exc_info=True)
                    logger.info("Rolling back all changes for this batch...")
                    conn.rollback()
                    logger.info("Rollback complete. No changes were saved.")
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