"""
# SFTP to Snowflake DAG Template

This DAG provides a skeleton for downloading files from an SFTP server,
transforming them, and loading them into Snowflake.
"""

from __future__ import annotations

import logging
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.sdk import dag, task

# Utilities
from utils import create_sftp_connection, get_snowflake_connection, list_files

# -------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------
log = logging.getLogger(__name__)

# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
STAGING_AREA = Path("staging/sftp")
PROCESSED_LOG_FILE = STAGING_AREA / "processed_dates.txt"
SNOWFLAKE_TABLE = "SFTP_API_EVANS_E"

# Ensure base directories/files exist
STAGING_AREA.mkdir(parents=True, exist_ok=True)
PROCESSED_LOG_FILE.touch(exist_ok=True)


@dag(
    dag_id="sftp_template_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["template", "sftp", "snowflake", "best-practice"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def sftp_template_pipeline():
    """
    ### SFTP to Snowflake ELT Best Practices
    """

    # ---------------------------------------------------------------
    # Extract
    # ---------------------------------------------------------------
    @task
    def extract_from_sftp(data_interval_start=None):
        """
        Connects to SFTP, checks if the date has been processed,
        and downloads files.
        """
        date_str = (data_interval_start - timedelta(days=7)).strftime('%Y-%m-%d')
        local_dir = STAGING_AREA / date_str

        # --- Duplicate Check ---
        with open(PROCESSED_LOG_FILE, "r") as f:
            processed_dates = f.read().splitlines()

        if date_str in processed_dates:
            log.info(f"Date {date_str} already processed. Skipping extract.")
            return [], date_str

        local_dir.mkdir(parents=True, exist_ok=True)
        remote_path = f"/course/ITM327/news/{date_str}"
        downloaded_files = []

        log.info(f"Connecting to SFTP path: {remote_path}")

        try:
            sftp = create_sftp_connection()
            file_list = list_files(sftp, remote_path)

            if not file_list:
                log.warning(f"No files found at {remote_path}")
                return [], date_str

            for filename in file_list:
                remote_file = os.path.join(remote_path, filename)
                local_file = local_dir / filename

                log.info(f"Downloading {remote_file} → {local_file}")
                sftp.get(remote_file, str(local_file))
                downloaded_files.append(str(local_file))

            sftp.close()

        except Exception as e:
            log.error(f"SFTP extract failed: {e}")
            raise

        log.info(f"Downloaded {len(downloaded_files)} files")
        return downloaded_files, date_str

    # ---------------------------------------------------------------
    # Transform
    # ---------------------------------------------------------------
    @task
    def transform_data(extract_result: tuple):
        """
        Reads downloaded files, combines, and transforms them.
        """
        local_files, date_str = extract_result

        if not local_files:
            log.info("No files received. Skipping transform.")
            return None, date_str

        all_dfs = []

        for file_path in local_files:
            log.info(f"Reading file: {file_path}")
            try:
                df = pd.read_csv(file_path)
                all_dfs.append(df)
            except pd.errors.EmptyDataError:
                log.warning(f"Empty file skipped: {file_path}")
            except Exception as e:
                log.error(f"Failed reading {file_path}: {e}")
                raise

        if not all_dfs:
            log.warning("No valid data found after reading files.")
            return None, date_str

        combined_df = pd.concat(all_dfs, ignore_index=True)

        # --- Transformations ---
        log.info("Applying transformations")

        # Normalize column names
        combined_df.columns = (
            combined_df.columns.str.strip()
            .str.upper()
            .str.replace(" ", "_")
        )

        # Add load metadata
        combined_df["LOAD_DATE"] = date_str
        combined_df["LOADED_AT"] = datetime.utcnow()

        log.info(f"Transformation complete: {len(combined_df)} rows")
        return combined_df, date_str

    # ---------------------------------------------------------------
    # Load
    # ---------------------------------------------------------------
    @task
    def load_to_snowflake(transform_result: tuple):
        """
        Loads the transformed DataFrame to Snowflake
        and logs the processed date.
        """
        df, date_str = transform_result

        if df is None or df.empty:
            log.info("Empty DataFrame. Skipping Snowflake load.")
            return date_str

        log.info(f"Loading {len(df)} rows into {SNOWFLAKE_TABLE}")

        conn = None
        try:
            conn = get_snowflake_connection()

            from snowflake.connector.pandas_tools import write_pandas

            success, n_chunks, n_rows, _ = write_pandas(
                conn, df, SNOWFLAKE_TABLE
            )

            if not success:
                raise Exception("write_pandas reported failure")

            log.info(f"Loaded {n_rows} rows into Snowflake")

            # --- Log successful date ---
            with open(PROCESSED_LOG_FILE, "a") as f:
                f.write(f"{date_str}\n")

            log.info(f"Logged processed date: {date_str}")

        except Exception as e:
            log.error(f"Snowflake load failed: {e}")
            raise

        finally:
            if conn:
                conn.close()
                log.info("Snowflake connection closed")

        return date_str

    # ---------------------------------------------------------------
    # Cleanup
    # ---------------------------------------------------------------
    @task
    def cleanup_staging_files(processed_date: str):
        """
        Removes staging files for the processed date.
        """
        if not processed_date:
            log.warning("No processed date provided. Skipping cleanup.")
            return

        local_dir = STAGING_AREA / processed_date

        if local_dir.exists():
            log.info(f"Cleaning staging directory: {local_dir}")
            shutil.rmtree(local_dir)
        else:
            log.warning(f"Staging directory not found: {local_dir}")

    # ---------------------------------------------------------------
    # DAG Flow
    # ---------------------------------------------------------------
    extract_result = extract_from_sftp()
    transform_result = transform_data(extract_result)
    loaded_date = load_to_snowflake(transform_result)
    cleanup_staging_files(loaded_date)


# Instantiate DAG
sftp_template_dag = sftp_template_pipeline()
