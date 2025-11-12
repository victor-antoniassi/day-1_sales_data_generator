"""
Main entry point for the Chinook Sales Simulator.

This script provides a cross-platform command-line interface to manage and
run the sales simulator, replacing the previous platform-specific shell scripts.

Usage:
    uv run main.py setup
    uv run main.py simulate
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
from typing import List

from dotenv import load_dotenv, dotenv_values

# Load environment variables before configuring logging
load_dotenv()

# Configure structured logging with level from .env
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, log_level, logging.INFO),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Color constants for terminal output ---
RED = '\033[0;31m'
GREEN = '\033[0;32m'
YELLOW = '\033[1;33m'
BLUE = '\033[0;34m'
NC = '\033[0m'  # No Color


def get_connection_string() -> str:
    """
    Retrieves the database connection string from neonctl.

    This function requires the neonctl CLI to be installed and configured.
    It reads the Neon organization ID, project ID, and database name from a .env file.

    Returns:
        str: The database connection string.

    Raises:
        ValueError: If NEON_ORG_ID, NEON_PROJECT_ID, or NEON_DATABASE are not set
                    in the .env file.
        RuntimeError: If neonctl is not found or fails to execute.
    """
    config = dotenv_values()
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


def run_psql_command(
    conn_string: str, 
    command: str = None, 
    file_path: str = None, 
    quiet: bool = False
) -> subprocess.CompletedProcess:
    """Runs a command or file using psql."""
    if not shutil.which('psql'):
        raise RuntimeError("psql command not found. Is PostgreSQL client installed and in your PATH?")

    base_command = ['psql', conn_string]
    if quiet:
        base_command.extend(['-t', '-A'])

    if file_path:
        base_command.extend(['-f', file_path])
    
    return subprocess.run(
        base_command,
        capture_output=True,
        text=True,
        check=True,
        input=command,
        encoding='utf-8'
    )


def setup():
    """Orchestrates the database setup process."""
    logger.info(f"{BLUE}============================================================={NC}")
    logger.info(f"{BLUE}Chinook Database Setup (Python){NC}")
    logger.info(f"{BLUE}============================================================={NC}\n")

    # 1. Check for .env file
    logger.info(f"{YELLOW}[1/7] Checking configuration...{NC}")
    if not os.path.exists('.env'):
        logger.error(f"{RED}ERROR: .env file not found!{NC}")
        logger.info("Please create a .env file based on .env.example and add your credentials.")
        sys.exit(1)
    logger.info(f"{GREEN}✓ Configuration file found{NC}\n")

    # 2. Check for neonctl
    logger.info(f"{YELLOW}[2/7] Checking neonctl CLI...{NC}")
    if not shutil.which('neonctl'):
        logger.error(f"{RED}ERROR: neonctl not found!{NC}")
        logger.info("Please install neonctl: https://neon.com/docs/reference/neon-cli")
        sys.exit(1)
    logger.info(f"{GREEN}✓ neonctl CLI found{NC}\n")

    # 3. Test database connection
    logger.info(f"{YELLOW}[3/7] Testing database connection...{NC}")
    try:
        conn_string = get_connection_string()
        run_psql_command(conn_string, command="SELECT 1;", quiet=True)
        logger.info(f"{GREEN}✓ Database connection successful{NC}\n")
    except (ValueError, RuntimeError, subprocess.CalledProcessError) as e:
        logger.error(f"{RED}ERROR: Could not connect to database.{NC}")
        logger.error(e)
        sys.exit(1)

    # 4. Update historical data
    logger.info(f"{YELLOW}[4/7] Updating historical invoice data...{NC}")
    try:
        result = run_psql_command(conn_string, file_path='update_historical_data.sql')
        # psql -f sends output to stdout, so we log it.
        for line in result.stdout.splitlines():
            logger.info(line)
        logger.info(f"{GREEN}✓ Historical data update completed{NC}\n")
    except (RuntimeError, subprocess.CalledProcessError) as e:
        logger.error(f"{RED}ERROR: Failed to update historical data.{NC}")
        logger.error(e.stderr)
        sys.exit(1)

    # 5. Create simulate_new_sale function
    logger.info(f"{YELLOW}[5/7] Creating simulate_new_sale function and sequences...{NC}")
    try:
        result = run_psql_command(conn_string, file_path='simulate_new_sale.sql')
        for line in result.stdout.splitlines():
            logger.info(line)
        logger.info(f"{GREEN}✓ INSERT function and sequences created successfully{NC}\n")
    except (RuntimeError, subprocess.CalledProcessError) as e:
        logger.error(f"{RED}ERROR: Failed to create INSERT function.{NC}")
        logger.error(e.stderr)
        sys.exit(1)

    # 6. Create modification functions
    logger.info(f"{YELLOW}[6/7] Creating simulation modification functions (UPDATE/DELETE)...{NC}")
    try:
        result = run_psql_command(conn_string, file_path='simulate_modifications.sql')
        for line in result.stdout.splitlines():
            logger.info(line)
        logger.info(f"{GREEN}✓ UPDATE/DELETE functions created successfully{NC}\n")
    except (RuntimeError, subprocess.CalledProcessError) as e:
        logger.error(f"{RED}ERROR: Failed to create modification functions.{NC}")
        logger.error(e.stderr)
        sys.exit(1)
        
    # 7. Verify installation
    logger.info(f"{YELLOW}[7/7] Verifying all simulation functions...{NC}")
    try:
        # Check for all three functions
        sql_verify_all = """
        SELECT routine_name
        FROM information_schema.routines
        WHERE routine_schema = 'public'
        AND routine_name IN ('simulate_new_sale', 'simulate_update_sale', 'simulate_delete_sale');
        """
        result = run_psql_command(conn_string, command=sql_verify_all, quiet=True)
        found_functions = set(result.stdout.strip().split())
        
        required_functions = {'simulate_new_sale', 'simulate_update_sale', 'simulate_delete_sale'}
        
        if found_functions == required_functions:
            for func in sorted(list(found_functions)):
                logger.info(f"{GREEN}✓ {func} function verified{NC}")
            logger.info(f"{GREEN}✓ All simulation functions verified successfully{NC}\n")
        else:
            missing = required_functions - found_functions
            logger.error(f"{RED}⨯ Function verification failed. Missing: {', '.join(missing)}{NC}")
            sys.exit(1)
            
    except (subprocess.CalledProcessError, ValueError) as e:
        logger.error(f"{RED}⨯ Function verification failed.{NC}")
        logger.error(e)
        sys.exit(1)

    logger.info(f"{BLUE}============================================================={NC}")
    logger.info(f"{GREEN}✓ DATABASE SETUP COMPLETE!{NC}")
    logger.info(f"{BLUE}============================================================={NC}\n")
    logger.info("You can now run the sales simulator:")
    logger.info("  uv run main.py simulate")


def simulate():
    """Orchestrates the execution of the sales simulator."""
    if not os.path.exists('.env'):
        logger.error(f"{RED}ERROR: .env file not found!{NC}")
        logger.info("Please run 'uv run main.py setup' first.")
        sys.exit(1)
    
    try:
        from d1_sales_simulator import start_simulation
        
        conn_string = get_connection_string()

        try:
            if sys.stdin.isatty():
                prompt = "Enter the number of INSERTS, UPDATES, and DELETES for D-1 (e.g., '100 5 2'): "
                user_input = input(prompt)
            else:
                user_input = sys.stdin.readline().strip()

            parts = user_input.split()
            if len(parts) != 3:
                raise ValueError("Invalid input: Please provide three numbers for inserts, updates, and deletes.")

            num_inserts = int(parts[0])
            num_updates = int(parts[1])
            num_deletes = int(parts[2])

            if num_inserts < 0 or num_updates < 0 or num_deletes < 0:
                raise ValueError("Invalid input: all numbers must be zero or positive.")

        except (ValueError, EOFError) as e:
            logger.error(f"Invalid input: {e}")
            sys.exit(1)

        start_simulation(conn_string, num_inserts, num_updates, num_deletes)

    except (ValueError, RuntimeError) as e:
        logger.error(f"A problem occurred: {e}")
        sys.exit(1)
    except ImportError:
        logger.error("Could not import 'start_simulation' from d1_sales_simulator.py")
        sys.exit(1)


def main():
    """Main entry point and command router."""
    parser = argparse.ArgumentParser(
        description="A cross-platform manager for the Chinook Sales Simulator."
    )
    subparsers = parser.add_subparsers(dest='command', required=True)

    # Setup command
    parser_setup = subparsers.add_parser(
        'setup', help="Sets up the database, creating functions and aligning data."
    )
    parser_setup.set_defaults(func=setup)

    # Simulate command
    parser_simulate = subparsers.add_parser(
        'simulate', help="Runs the D-1 sales simulator."
    )
    parser_simulate.set_defaults(func=simulate)

    args = parser.parse_args()
    args.func()


if __name__ == "__main__":
    main()
