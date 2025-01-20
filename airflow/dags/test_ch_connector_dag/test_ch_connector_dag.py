from pendulum import datetime
from airflow.decorators import dag, task
from apache.airflow.providers.clickhouse.operators.ClickhouseOperator import (
    ClickhouseOperator,
)

# Default arguments for the DAG and tasks
default_args = {
    "owner": "meteofurletov",
}


@dag(
    dag_id="test_ch_connector_dag",
    default_args=default_args,
    owner_links={"meteofurletov": "https://t.me/meteoFurletov"},
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["example", "clickhouse"],
)
def test_clickhouse_connection_dag():
    """
    DAG to test ClickHouse connection with a single task using ClickhouseOperator.
    """

    execute_query = ClickhouseOperator(
        task_id="execute_query",
        sql="SELECT * FROM system.tables LIMIT 5;",
        click_conn_id="clickhouse_knigomag",
        do_xcom_push=True,  # Enable XCom push if needed
    )

    execute_query


# Instantiate the DAG
test_clickhouse_dag = test_clickhouse_connection_dag()
