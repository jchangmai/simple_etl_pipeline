from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models import Variable

# Target Snowflake stage and table configurations
STAGE_NAME = "@KOMODO.RAW_KOMODO.RAW_DATA_STAGE"
TABLE_NAME = "KOMODO.RAW_KOMODO.RAW_USERS"

def load_data_into_snowflake():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # 1. Upload CSV file(s) from the data folder into your stage
    put_query = f"PUT 'file:///opt/airflow/data/*.csv' {STAGE_NAME} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;"
    
    # 2. Copy the staged CSV data into your table
    copy_query = f"""
    COPY INTO {TABLE_NAME}
    FROM {STAGE_NAME}
    FILE_FORMAT = (FORMAT_NAME = 'KOMODO.RAW_KOMODO.CSV_FORMAT')
    ON_ERROR = 'CONTINUE';
    """
    
    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(put_query)
            cur.execute(copy_query)

def check_snowflake_data():
    """Checks whether data exists in the Snowflake table and chooses the next task."""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    
    # Query row count from target Snowflake table
    result = hook.get_first(f"SELECT COUNT(*) FROM {TABLE_NAME};")
    row_count = result[0] if result else 0
    print(f"Row count in {TABLE_NAME}: {row_count}")
    
    if row_count > 0:
        print("Data successfully verified in Snowflake. Branching to proceed_with_dbt.")
        return 'proceed_with_dbt'
    else:
        print("No data found in Snowflake table. Branching to no_data_found.")
        return 'no_data_found'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    'dl_ing_dummy_data',
    default_args=default_args,
    description='A DAG to load dummy data into Snowflake and run dbt job',
    schedule_interval=None,
    catchup=False,
) as dag:

    # 1. Load data task
    load_data_task = PythonOperator(
        task_id='load_data_into_snowflake',
        python_callable=load_data_into_snowflake,
    )

    # 2. Branching operator: checks if data was uploaded
    check_data_task = BranchPythonOperator(
        task_id='check_data_in_snowflake',
        python_callable=check_snowflake_data,
    )

    # 3. Branch A: Proceed if data exists — trigger dbt Cloud job
    proceed_with_dbt = DbtCloudRunJobOperator(
        task_id='proceed_with_dbt',
        job_id=Variable.get("dbt_job_id"),
        account_id=Variable.get("dbt_account_id"),
        check_interval=10,
        timeout=600,
    )

    # 4. Branch B: End/Skip if no data exists
    no_data_found = EmptyOperator(
        task_id='no_data_found',
    )

    # DAG Dependency flow
    load_data_task >> check_data_task >> [proceed_with_dbt, no_data_found]
