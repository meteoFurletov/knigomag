from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pendulum import datetime
import logging


@dag(
    dag_id="test_pg_connector_dag",
    owner_links={"meteofurletov": "https://t.me/meteoFurletov"},
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "postgres"],
)
def test_postgres_connection_dag():
    """
    DAG to test PostgreSQL connection.
    """

    @task(owner="meteofurletov")
    def execute_query():
        """
        Executes a simple SQL query against the PostgreSQL database.
        """
        # Initialize PostgresHook with the connection ID
        pg_hook = PostgresHook(postgres_conn_id="postgres_knigomag")

        # Define your SQL query
        sql = "SELECT 1;"

        # Execute the query and fetch the result
        result = pg_hook.get_first(sql)

        # Log the result
        logging.info(f"Query Result: {result}")

        return result

    execute_query()


# Instantiate the DAG
dag = test_postgres_connection_dag()
