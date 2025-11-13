import os
import subprocess
import logging
import toml
from pathlib import Path
from typing import Dict, Any, List, Optional
from decimal import Decimal

import psycopg
from dotenv import load_dotenv, dotenv_values


# Determine project root directory (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load environment variables from project root
load_dotenv(PROJECT_ROOT / '.env')

# Configure logging
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

LOGS_DIR = PROJECT_ROOT / "simulation_logs"

def get_connection_string() -> str:
    """Retrieves the database connection string from neonctl."""
    config = dotenv_values(PROJECT_ROOT / '.env')
    required_vars = ["NEON_ORG_ID", "NEON_PROJECT_ID", "NEON_DATABASE"]
    if not all(k in config for k in required_vars):
        raise ValueError(f"Missing one or more required .env variables: {', '.join(required_vars)}")

    command = [
        "neonctl", "connection-string",
        "--org-id", config["NEON_ORG_ID"],
        "--project-id", config["NEON_PROJECT_ID"],
        "--database-name", config["NEON_DATABASE"],
    ]
    if config.get("NEON_ROLE"):
        command.extend(["--role-name", config["NEON_ROLE"]])
    if config.get("NEON_BRANCH"):
        command.extend(["--branch", config["NEON_BRANCH"]])

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True, encoding='utf-8')
        return result.stdout.strip()
    except FileNotFoundError:
        raise RuntimeError("neonctl not found. Please install and configure it.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Error getting connection string from neonctl: {e.stderr}")

def find_latest_log_file() -> Optional[Path]:
    """Finds the most recent simulation log file."""
    if not LOGS_DIR.exists():
        logger.error(f"Log directory not found: {LOGS_DIR}")
        return None
    
    log_files = list(LOGS_DIR.glob("simulation_*.toml"))
    if not log_files:
        logger.error(f"No simulation logs found in {LOGS_DIR}")
        return None
        
    latest_log = max(log_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"Found latest log file: {latest_log.name}")
    return latest_log

def verify_simulation_results():
    """
    Connects to the database, reads the latest simulation log, and verifies
    that the operations recorded in the log are reflected in the database state.
    """
    logger.info("=== Starting Log-Based Simulation Verification ===")
    
    latest_log_path = find_latest_log_file()
    if not latest_log_path:
        return

    try:
        log_data = toml.load(latest_log_path)
        operations = log_data.get("operations", [])
        summary = log_data.get("simulation_summary", {})
        logger.info(f"Verifying {len(operations)} operations for D-1: {summary.get('d1_date')}")

    except Exception as e:
        logger.error(f"Failed to read or parse log file {latest_log_path}: {e}")
        return

    try:
        conn_string = get_connection_string()
        with psycopg.connect(conn_string) as conn:
            with conn.cursor() as cur:
                # Separate operations by type
                inserts = [op for op in operations if op['type'] == 'insert']
                updates = [op for op in operations if op['type'] == 'update']
                deletes = [op for op in operations if op['type'] == 'delete']

                # --- Verification counters ---
                success_count = 0
                fail_count = 0

                # 1. Verify INSERTS
                logger.info(f"--- Verifying {len(inserts)} Inserts ---")
                for op in inserts:
                    cur.execute('SELECT "Total" FROM "Invoice" WHERE "InvoiceId" = %s', (op['invoice_id'],))
                    result = cur.fetchone()
                    if not result:
                        logger.error(f"❌ INSERT FAIL: InvoiceId {op['invoice_id']} not found.")
                        fail_count += 1
                    elif abs(result[0] - Decimal(str(op['total']))) > Decimal('0.001'):
                        logger.error(f"❌ INSERT FAIL: InvoiceId {op['invoice_id']} has wrong total. Expected: {op['total']:.2f}, Found: {result[0]:.2f}")
                        fail_count += 1
                    else:
                        logger.debug(f"✅ INSERT OK: InvoiceId {op['invoice_id']}")
                        success_count += 1
                
                # 2. Verify DELETES
                logger.info(f"--- Verifying {len(deletes)} Deletes ---")
                for op in deletes:
                    cur.execute('SELECT 1 FROM "Invoice" WHERE "InvoiceId" = %s', (op['invoice_id'],))
                    if cur.fetchone():
                        logger.error(f"❌ DELETE FAIL: InvoiceId {op['invoice_id']} still exists.")
                        fail_count += 1
                    else:
                        logger.debug(f"✅ DELETE OK: InvoiceId {op['invoice_id']} correctly deleted.")
                        success_count += 1

                # 3. Verify UPDATES
                logger.info(f"--- Verifying {len(updates)} Updates ---")
                for op in updates:
                    cur.execute('SELECT 1 FROM "Invoice" WHERE "InvoiceId" = %s', (op['invoice_id'],))
                    if not cur.fetchone():
                        logger.error(f"❌ UPDATE FAIL: InvoiceId {op['invoice_id']} not found (it may have been deleted later).")
                        fail_count += 1
                    else:
                        # Basic check: just confirm it exists. A deeper check would require pre-update state.
                        logger.debug(f"✅ UPDATE OK: InvoiceId {op['invoice_id']} exists.")
                        success_count += 1

                # --- Final Summary ---
                logger.info("--- Verification Summary ---")
                if fail_count == 0:
                    logger.info(f"✅ SUCCESS: All {success_count} verified operations are consistent with the log.")
                else:
                    logger.error(f"❌ FAILURE: {fail_count} inconsistencies found.")
                    logger.info(f"({success_count} operations were consistent).")

    except (ValueError, RuntimeError, psycopg.Error) as e:
        logger.error(f"An error occurred during verification: {e}")
        exit(1)

if __name__ == "__main__":
    verify_simulation_results()

