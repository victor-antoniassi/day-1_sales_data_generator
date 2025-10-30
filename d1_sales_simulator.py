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
import psycopg
from dotenv import dotenv_values

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

    command = [
        "neonctl",
        "connection-string",
        "--org-id",
        org_id,
        "--project-id",
        project_id,
    ]
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, check=True
        )
        return result.stdout.strip()
    except FileNotFoundError:
        raise RuntimeError("neonctl not found. Please install and configure it.")
    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Error getting connection string from neonctl: {e.stderr}"
        )

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

def main() -> None:
    """
    Main function to run the sales simulation.

    Orchestrates the process of getting user input, connecting to the
    database, and generating the specified number of sales within a single
    database transaction.
    """
    try:
        conn_string = get_connection_string()
        print("Connection string obtained successfully.")

        try:
            num_sales_str = input(
                "How many sales do you want to generate for D-1? "
            )
            if not num_sales_str.isdigit() or int(num_sales_str) <= 0:
                print("Please enter a positive integer.")
                return
            num_sales = int(num_sales_str)
        except ValueError:
            print("Invalid input. Please enter a positive integer.")
            return

        # The 'with' statement ensures the connection is properly closed.
        with psycopg.connect(conn_string) as conn:
            # psycopg 3 automatically begins a transaction on the first execute.
            # We will manually control the commit or rollback.
            try:
                # The 'with' statement ensures the cursor is properly closed.
                with conn.cursor() as cur:
                    print(
                        f"\nStarting the generation of {num_sales} sales "
                        "IN A SINGLE TRANSACTION..."
                    )

                    for i in range(num_sales):
                        timestamp = generate_random_timestamp_d1()

                        # Execute the PostgreSQL function to simulate one sale.
                        cur.execute(
                            "SELECT generated_invoice_id, generated_total "
                            "FROM simulate_new_sale(%s);",
                            (timestamp,),
                        )
                        result = cur.fetchone()

                        if result:
                            invoice_id, total = result
                            # The 'Pending' status indicates the transaction
                            # has not yet been committed.
                            print(
                                f"  (Pending) Sale {i + 1}/{num_sales}: "
                                f"InvoiceId={invoice_id}, "
                                f"Total=${float(total):.2f}, "
                                f"Timestamp={timestamp.strftime('%Y-%m-%d %H:%M:%S')}"
                            )
                        else:
                            print(
                                f"  (Pending) Sale {i + 1}/{num_sales}: "
                                "Function did not return data."
                            )

                # If the loop completes without errors, commit the transaction.
                print("\nBatch successfully prepared. Committing transaction...")
                conn.commit()
                print(
                    f"SUCCESS: {num_sales} sales were committed to the database."
                )

            except (Exception, psycopg.Error) as e:
                # If any error occurs, roll back the entire transaction.
                print(
                    f"\nERROR: An error occurred during batch generation: {e}"
                )
                print("Rolling back all changes for this batch...")
                conn.rollback()
                print("Rollback complete. No sales were saved.")

    except (ValueError, RuntimeError) as e:
        # Catches configuration or execution errors.
        print(f"An error occurred: {e}")
    except psycopg.Error as e:
        # Catches database connection errors.
        print(f"A database connection error occurred: {e}")
    except Exception as e:
        # Catches any other unexpected errors.
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    main()