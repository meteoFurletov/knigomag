from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from googleapiclient.http import MediaIoBaseDownload
from airflow.providers.postgres.hooks.postgres import PostgresHook

import io
import os
import logging
import pandas as pd
from datetime import timedelta

from gdrive_etl_store_dag.processing import (
    process_store_data,
)


# Define the shared directory for storing Parquet files
SHARED_DIR = "/usr/local/airflow/data/etl/"

# Ensure the directory exists
os.makedirs(SHARED_DIR, exist_ok=True)


@dag(
    dag_id="gdrive_etl_store_dag",
    owner_links={"meteofurletov": "https://t.me/meteoFurletov"},
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["google_drive", "store"],
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
)
def gdrive_etl_store_dag():
    """
    DAG to load store data from Google Drive, process them and load into PostgreSQL.
    """

    @task(owner="meteofurletov")
    def extract_store_data(folder_id: str) -> list:
        """
        Downloads files from Google Drive folder and its subfolders,
        saves each as a separate CSV file locally.

        Args:
            folder_id: Google Drive folder ID

        Returns:
            List of paths to the extracted CSV files.
        """
        drive_hook = GoogleDriveHook(gcp_conn_id="google_drive")
        all_files = []
        extracted_file_paths = []

        def list_files_recursively(current_folder_id):
            query = f"'{current_folder_id}' in parents and trashed = false"
            try:
                response = (
                    drive_hook.get_conn()
                    .files()
                    .list(q=query, fields="files(id, name, mimeType)")
                    .execute()
                )
                files = response.get("files", [])
                for file in files:
                    if file["mimeType"] == "application/vnd.google-apps.folder":
                        list_files_recursively(file["id"])
                    else:
                        if "Касса_Отчет" in file.get("name", "") and file.get(
                            "mimeType"
                        ) in [
                            "application/vnd.google-apps.spreadsheet",
                            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        ]:
                            all_files.append(file)
            except Exception as e:
                logging.error(
                    f"Error listing files in folder {current_folder_id}: {str(e)}"
                )

        # Start recursion from the main folder
        list_files_recursively(folder_id)
        logging.info(f"Found {len(all_files)} files matching the criteria.")

        for file in all_files:
            file_id = file.get("id")
            file_name = file.get("name")

            if not file_id or not file_name:
                logging.warning(f"Skipping file with missing ID or name: {file}")
                continue

            try:
                fh = io.BytesIO()
                if file["mimeType"] == "application/vnd.google-apps.spreadsheet":
                    request = (
                        drive_hook.get_conn()
                        .files()
                        .export_media(
                            fileId=file_id,
                            mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        )
                    )
                else:
                    request = drive_hook.get_conn().files().get_media(fileId=file_id)
                downloader = MediaIoBaseDownload(fh, request)

                done = False
                while not done:
                    status, done = downloader.next_chunk()
                    if status:
                        logging.info(
                            f"Download {int(status.progress() * 100)}% for file {file_name}"
                        )

                fh.seek(0)
                df = pd.read_excel(fh)
                logging.info(f"Read Excel content for file {file_name}")

                # Define the extracted CSV file path
                sanitized_file_name = file_name.replace(" ", "_").replace("/", "_")
                extract_path = os.path.join(SHARED_DIR, f"e/{sanitized_file_name}.csv")

                # Save DataFrame as CSV
                df.to_csv(extract_path, index=False)
                logging.info(f"Extracted data saved to {extract_path}")
                extracted_file_paths.append(extract_path)

            except Exception as e:
                logging.error(f"Error processing {file_name}: {str(e)}")

        logging.info(f"Successfully extracted {len(extracted_file_paths)} files.")
        return extracted_file_paths

    @task(owner="meteofurletov")
    def transform_store_data(extract_paths: list) -> str:
        """
        Transforms the extracted data and saves it as a single CSV file.

        Args:
            extract_paths: List of paths to the extracted CSV files.

        Returns:
            Path to the transformed CSV file.
        """
        try:
            data_dict = {}
            for path in extract_paths:
                df = pd.read_csv(path)
                data_dict[path] = df
            processed_data = process_store_data(data_dict)
            logging.info("Data transformation complete.")

            # Save the concatenated DataFrame as a single CSV file
            transformed_path = os.path.join(SHARED_DIR, "t/transformed_data.parquet")
            processed_data.to_parquet(transformed_path, index=False)
            logging.info(f"Transformed data saved to {transformed_path}")

            return transformed_path

        except Exception as e:
            logging.error(f"Error in transformation for {extract_paths}: {str(e)}")
            raise

    @task(owner="meteofurletov")
    def load_store_data(transform_path: str) -> None:
        """
        Loads the transformed data into the PostgreSQL database.

        Args:
            transform_path: Path to the transformed CSV file.
        """
        try:
            df = pd.read_parquet(transform_path)
            logging.info(f"Loaded transformed data from {transform_path}")

            pg_hook = PostgresHook(postgres_conn_id="postgres_knigomag")

            insert_query = """
            INSERT INTO staging.store_data (date, revenue, balance, seller_name, store_id)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (store_id, date) 
            DO UPDATE SET
                revenue = EXCLUDED.revenue,
                balance = EXCLUDED.balance,
                seller_name = EXCLUDED.seller_name;
            """

            # Convert DataFrame to list of native Python tuples
            data = list(df.itertuples(index=False, name=None))

            connection = pg_hook.get_conn()
            cursor = connection.cursor()
            try:
                cursor.executemany(insert_query, data)
                connection.commit()
                logging.info(
                    f"Data successfully loaded into PostgreSQL from {transform_path}."
                )
            except Exception as e:
                connection.rollback()
                logging.error(
                    f"Error executing batch insert from {transform_path}: {str(e)}"
                )
                raise
            finally:
                cursor.close()
                connection.close()

        except Exception as e:
            logging.error(f"Error in loading data from {transform_path}: {str(e)}")
            raise

    # Define the Google Drive folder ID
    FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID")

    # Task Dependencies using Dynamic Task Mapping
    extracted_file_paths = extract_store_data(folder_id=FOLDER_ID)

    transformed_path = transform_store_data(extract_paths=extracted_file_paths)

    load_store_data(transform_path=transformed_path)


# Instantiate the DAG
dag = gdrive_etl_store_dag()
