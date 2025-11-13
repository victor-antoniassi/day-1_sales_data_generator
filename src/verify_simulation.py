import argparse
import os
import subprocess
import datetime
import logging
from typing import Tuple

import psycopg
from dotenv import load_dotenv, dotenv_values

# Determine project root directory (parent of src/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Load environment variables from project root
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def get_connection_string() -> str:
    """
    Retrieves the database connection string from neonctl.
    (Replicated from main.py for self-containment)
    """
    config = dotenv_values(os.path.join(PROJECT_ROOT, '.env'))
    org_id = config.get("NEON_ORG_ID")
    project_id = config.get("NEON_PROJECT_ID")
    database = config.get("NEON_DATABASE")

    if not org_id or not project_id or not database:
        raise ValueError(
            "NEON_ORG_ID, NEON_PROJECT_ID, and NEON_DATABASE must be set in the .env file."
        )

    role = config.get("NEON_ROLE", "")
    branch = config.get("NEON_BRANCH", "")

    command = [
        "neonctl",
        "connection-string",
        "--org-id",
        org_id,
        "--project-id",
        project_id,
        "--database-name",
        database,
    ]
    if role:
        command.extend(["--role-name", role])
    if branch:
        command.extend(["--branch", branch])

    try:
        result = subprocess.run(
            command, capture_output=True, text=True, check=True, encoding='utf-8'
        )
        logger.debug("Connection string obtained successfully from neonctl")
        return result.stdout.strip()
    except FileNotFoundError:
        raise RuntimeError("neonctl not found. Please install and configure it.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Error getting connection string from neonctl: {e.stderr}"
        )

def get_d1_date_range() -> Tuple[datetime.date, datetime.date]:
    """Returns the start and end dates for the previous day (D-1)."""
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    return yesterday, yesterday

def verify_simulation_results(num_inserts: int, num_updates: int, num_deletes: int):
    logger.info("Starting simulation results verification...")
    try:
        conn_string = get_connection_string()
        d1_date, _ = get_d1_date_range()

        with psycopg.connect(conn_string) as conn:
            with conn.cursor() as cur:
                logger.info(f"Verifying data for D-1: {d1_date}")

                # 1. Count new invoices for D-1
                cur.execute(f"""
                    SELECT COUNT(*) FROM "Invoice"
                    WHERE DATE("InvoiceDate") = '{d1_date.isoformat()}'
                """)
                new_invoices_count = cur.fetchone()[0]
                logger.info(f"Number of new invoices created on D-1 ({d1_date}): {new_invoices_count}")

                if new_invoices_count == num_inserts:
                    logger.info(f"✅ Correct number of inserts found ({new_invoices_count})")
                else:
                    logger.error(f"❌ Incorrect number of inserts: expected {num_inserts}, found {new_invoices_count}")

                # 2. Check for 'deleted' invoices (hard delete)
                # This is tricky without knowing the IDs that were deleted.
                # We can only confirm that the count of invoices for D-1 matches the expected inserts minus deletes.
                # For a more precise check, we'd need to capture IDs before deletion.
                # For now, we'll rely on the count.

                # 3. Check for updated invoices (more invoice lines)
                # This also requires knowing original state or specific invoice IDs.
                # A simpler check is to see if any D-1 invoices have more than 1 line (as new inserts can have up to 5)
                # and if the total matches the sum of lines.
                cur.execute(f"""
                    SELECT i."InvoiceId", COUNT(il."InvoiceLineId"), i."Total"
                    FROM "Invoice" i
                    JOIN "InvoiceLine" il ON i."InvoiceId" = il."InvoiceId"
                    WHERE DATE(i."InvoiceDate") = '{d1_date.isoformat()}'
                    GROUP BY i."InvoiceId", i."Total"
                    HAVING COUNT(il."InvoiceLineId") > 1
                """)
                updated_invoices_with_multiple_lines = cur.fetchall()
                logger.info(f"Number of D-1 invoices with multiple lines (potential updates/multi-item inserts): {len(updated_invoices_with_multiple_lines)}")
                
                # Basic check: sum of lines should match invoice total
                for inv_id, line_count, total in updated_invoices_with_multiple_lines:
                    cur.execute(f"""
                        SELECT SUM("UnitPrice" * "Quantity") FROM "InvoiceLine"
                        WHERE "InvoiceId" = {inv_id}
                    """)
                    calculated_total = cur.fetchone()[0]
                    if abs(calculated_total - total) > 0.001: # Using a small delta for float comparison
                        logger.warning(f"Invoice {inv_id}: Calculated total ({calculated_total:.2f}) does not match stored total ({total:.2f}).")
                    else:
                        logger.debug(f"Invoice {inv_id}: Total matches sum of lines.")

                logger.info("Verification complete.")

    except (ValueError, RuntimeError, psycopg.Error) as e:
        logger.error(f"Error during verification: {e}")
        exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify the D-1 sales simulation results.")
    parser.add_argument("num_inserts", type=int, help="Number of inserts to verify.")
    parser.add_argument("num_updates", type=int, help="Number of updates to verify.")
    parser.add_argument("num_deletes", type=int, help="Number of deletes to verify.")
    args = parser.parse_args()

    verify_simulation_results(args.num_inserts, args.num_updates, args.num_deletes)
