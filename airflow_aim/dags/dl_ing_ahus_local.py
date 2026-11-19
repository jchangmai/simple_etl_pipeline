from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from airflow.models import Variable
import pendulum
import os
import re
import json
import csv
import shutil

# Configuration Constants
SNOWFLAKE_CONNECTION = 'snowflake_default'
LOCAL_STORAGE_ROOT = '/opt/airflow/data'

dag_id = 'DL_ING_aHUS_Prescriptive_Internal'
indication_type = 'aHUS'
alert_type = 'prescriptive'
entity = 'ahus_prescriptive_internal'
raw_table = 'raw_ahus_prescriptive_alert_internal'
trusted_table = 'ahus_prescriptive_alert_internal'
trusted_db = 'komodo'
staging_table_alert = 'stg_alerts_ahus_prescriptive_internal'
staging_table_patient = 'stg_patient_id_ahus_prescriptive_internal'

file_pattern = Variable.get('AHUS_INTERNAL_FILE_PATTERN', default_var=r'AHUS_PRESCRIPTIVE_INTERNAL_\d{8}.*\.csv')
header = 'ALERT_ID,ALERTNAME,ALERT,PATIENT_ID,CODE_DESCRIPTION,NPI,FIRSTNAME,MIDDLENAME,LASTNAME,HCPSPECIALTY1,HCPSPECIALTY2,HCPADDRESS,HCO_NAME,CITY,STATE,ZIP,EMAIL,PHONE,PAYERNAME,PAYERTYPE,YEAR,WEEK,DATE_1,DATE_KIDNEY_TRANSPLANT,DATE_THROMBOCYTOPENIA,DATE_HEMOLYSIS,DATE_BMT,DATE_DIALYSIS,DATE_PLASMA_EXCHANGE,DATE_SOL_ULT,DATE_9,DATE_10,DATE_11,DATE_TMA_ENCOUNTER,ADULT,TOTAL_DIALYSIS_ENCOUNTERS,TOTAL_RENALDYSFUNCTIONS_ENCOUNTERS,TMA_ENCOUNTERS_R12M,LUPUS_NEPHRITIS,QUEST_LOCAL_RESULT_NAME,ADAMTS13_LAB_RESULT,QUEST_RESULT_UNIT,QUEST_REFERENCE_RANGE_LOW,QUEST_REFERENCE_RANGE_HIGH,QUEST_REFERENCE_RANGE_ALPHA,QUEST_REFERENCE_INTERPRETATION,QUEST_COMMENTS,INDICATION_MAPPING,ON_LABEL_USAGE,DATE_LAST_ENCOUNTER,LAST_CLAIM_DATE,INSIGHTS_TEXT,SEGMENT_US__C,FILE_DATE,DIAGNOSED_AHUS,TERRITORY_NAME,DATE_OF_MOST_RECENT_EVENT,WHAT_WAS_THE_LAST_TRIGGER,HEMATOLOGIST,NEPHROLOGIST,RHEUMATOLOGIST,CKD_STAGE,PATIENT_TYPE,ALERT_TYPE_DETAIL'
schedule_interval = None

dag_tags = ['AIM', 'Ingestion', 'AIM_Internal_Alerts']

# Local folder equivalents of S3 paths
local_raw = f'{LOCAL_STORAGE_ROOT}/commercial_ds/raw/ahus_prescriptive'
all_sources = json.loads(Variable.get(
    'AIM_SOURCE_LOCATION_INTERNAL_ALERTS',
    default_var=json.dumps({dag_id: f'{LOCAL_STORAGE_ROOT}/source'})
))
local_source = all_sources.get(dag_id)
local_success = f'{LOCAL_STORAGE_ROOT}/commercial_ds/archive/ahus_prescriptive/success'
local_error = f'{LOCAL_STORAGE_ROOT}/commercial_ds/archive/ahus_prescriptive/failure'

# dbt Cloud job ID (Update 33333 to your actual dbt Cloud Job ID)
dbtcloudjobs = json.loads(Variable.get(
    'DBTCLOUD_JOBS_JSON',
    default_var=json.dumps({dag_id: 70506183146557})
))
job_id = dbtcloudjobs[dag_id]


# --- Helper Functions ---

def get_snowflake_env_vars(conn_id, SNOWFLAKE_SCHEMA):
    return {
        "env": {
            "DBT_SNOWFLAKE_SCHEMA": SNOWFLAKE_SCHEMA
        }
    }

def verify_row_dq(row):
    if len(row) != 65:
        return False
    alert_id, patient_id, npi, file_date = row[0], row[3], row[5], row[53]
    if not alert_id or not patient_id or not npi or len(file_date) != 8:
        return False
    return True


# --- Core DAG Functions ---

def refresh_external_tables(sf_hook):
    sf_hook.run("REMOVE @komodo.raw_komodo.ahus_internal_stage;")
    sf_hook.run(f"TRUNCATE TABLE raw_komodo.{raw_table};")
    sf_hook.run(f"PUT file://{local_raw}/*.csv @komodo.raw_komodo.ahus_internal_stage AUTO_COMPRESS=TRUE;")
    result = sf_hook.get_first(f"""
        COPY INTO raw_komodo.{raw_table}
        FROM @komodo.raw_komodo.ahus_internal_stage
        FILE_FORMAT = (FORMAT_NAME = 'komodo.raw_komodo.csv_format');
    """)
    print(result)


def delete_from_local():
    os.makedirs(local_raw, exist_ok=True)
    for f in os.listdir(local_raw):
        file_path = os.path.join(local_raw, f)
        if os.path.isfile(file_path):
            os.remove(file_path)


def checkHeaderFromStringPartialLocal(data_file, header):
    print('--- Inside checkHeaderFromStringPartialLocal ---')
    with open(data_file, 'r', encoding='utf-8-sig') as f:
        file_header = f.readline().replace(chr(65279), '').strip().upper()

    expected_header = header.replace('"', '').replace(chr(65279), '').strip().upper()

    start_fields = [x.strip() for x in file_header.split(',')]
    expected_fields = [x.strip() for x in expected_header.split(',')]

    for i in range(min(len(start_fields), len(expected_fields))):
        if start_fields[i] != expected_fields[i]:
            print(f"Mismatch at position {i+1}: Expected '{expected_fields[i]}', Found '{start_fields[i]}'")

    if len(start_fields) != len(expected_fields):
        print(f"Column count mismatch: Expected {len(expected_fields)}, Found {len(start_fields)}")

    if file_header.startswith(expected_header):
        print('Header row matches.')
        return True
    else:
        print('Header row does not match.')
        return False


def check_dq(file_path):
    good_rows, bad_rows = 0, 0
    with open(file_path, 'r', encoding='utf-8-sig') as f:
        csvreader = csv.reader(f)
        next(csvreader)  # Skip header
        for row in csvreader:
            if not verify_row_dq(row):
                print('DQ Check failed: ' + str(row))
                bad_rows += 1
            else:
                good_rows += 1
    return good_rows, bad_rows


def copy_local_file(source_path, destination_path):
    os.makedirs(os.path.dirname(destination_path), exist_ok=True)
    shutil.copy2(source_path, destination_path)
    print('Copied file from ' + source_path + ' to ' + destination_path)


def get_file_date(file_name):
    if file_name == "":
        print("file_name not populated")
    else:
        mobj1 = re.search(r'\d{8}', file_name)
        file_key = mobj1.group()
        print(f"get_file_date: file_date for file : {file_name} ---------- {file_key}")
        return file_key


def download_files(max_file_key, source_dir, file_pattern):
    download_list = []
    pattern = re.compile("^" + file_pattern + "$")

    os.makedirs(source_dir, exist_ok=True)
    files = os.listdir(source_dir)
    print('No of files found in local source: ' + str(len(files)))

    for name in files:
        if pattern.match(name):
            file_key = get_file_date(name)
            if max_file_key < file_key:
                download_list.append(name)
                print('File added to download list: ' + name)
            else:
                print(f'File Already Processed - MAX_FILE_KEY: {max_file_key} - file_key: {file_key}')
        else:
            print('Filename NOT match: ' + name)

    return download_list


def ingest_files(**kwargs):
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION)
    result = sf_hook.get_first(f"select COALESCE(MAX(file_date), '') from {trusted_db}.komodo.{trusted_table};")
    max_key = str(result[0])
    print('max_key: ' + max_key)

    file_list = download_files(max_key, local_source, file_pattern)
    print('Discovered files: ' + str(file_list))

    header_check_fail_list = []
    dq_check_fail_list = []
    valid_list = []

    if file_list:
        delete_from_local()
        for file_key in file_list:
            full_path = os.path.join(local_source, file_key)
            if checkHeaderFromStringPartialLocal(full_path, header):
                good_rows, bad_rows = check_dq(full_path)
                if bad_rows == 0:
                    valid_list.append(file_key)
                else:
                    dq_check_fail_list.append(file_key)
            else:
                header_check_fail_list.append(file_key)
    else:
        print('No new files found locally!!')

    for file_key in valid_list:
        src_path = os.path.join(local_source, file_key)
        copy_local_file(src_path, os.path.join(local_raw, file_key))
        copy_local_file(src_path, os.path.join(local_success, file_key))
        os.remove(src_path)

    if valid_list:
        refresh_external_tables(sf_hook)

    for file_key in header_check_fail_list + dq_check_fail_list:
        src_path = os.path.join(local_source, file_key)
        copy_local_file(src_path, os.path.join(local_error, file_key))
        os.remove(src_path)

    status = {
        "valid": {"count": len(valid_list), "files": valid_list},
        "header_check_failed": {"count": len(header_check_fail_list), "files": header_check_fail_list},
        "dq_check_failed": {"count": len(dq_check_fail_list), "files": dq_check_fail_list}
    }

    kwargs['ti'].xcom_push(key="status", value=status)


def notify_failed_files(**kwargs):
    ti = kwargs['ti']
    status = ti.xcom_pull(dag_id=dag_id, task_ids='Ingest_Source_Files', key='status')
    if (status['header_check_failed']['count'] != 0) or (status['dq_check_failed']['count'] != 0):
        email_body = (
            f"[DQ/HEADER FAILURE NOTIFICATION]\n"
            f"Header Failed ({status['header_check_failed']['count']}): {status['header_check_failed']['files']}\n"
            f"DQ Failed ({status['dq_check_failed']['count']}): {status['dq_check_failed']['files']}"
        )
        ti.xcom_push(key="email_body", value=email_body)
        return ['Send_Email_On_DQ_Failure']
    return []


def notify_missing_files(**kwargs):
    ti = kwargs['ti']
    status = ti.xcom_pull(dag_id=dag_id, task_ids='Ingest_Source_Files', key='status')
    if status['valid']['count'] > 0:
        return ['Load_Trusted_Table']
    elif (status['header_check_failed']['count'] == 0) and (status['dq_check_failed']['count'] == 0):
        email_body = f"[MISSING FILES NOTIFICATION] No new files found for {indication_type} {alert_type} alerts."
        ti.xcom_push(key="email_body", value=email_body)
        return ['Send_Email_On_Missing_Files']
    return []


def log_notification(source_task_id, **kwargs):
    msg = kwargs['ti'].xcom_pull(key="email_body", task_ids=source_task_id)
    print("=" * 60)
    print("SIMULATED EMAIL NOTIFICATION:")
    print(msg)
    print("=" * 60)


def failed_notification(context):
    print("=" * 60)
    print(f"AIRFLOW DAG FAILED | DAG: {context['dag'].dag_id} | Task: {context['task_instance'].task_id}")
    print("=" * 60)


default_args = {
    'owner': 'local_practice',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2026, 1, 1, tz="America/New_York"),
    'provide_context': True
}

with DAG(dag_id=dag_id, tags=dag_tags, schedule_interval=schedule_interval, catchup=False, default_args=default_args, on_failure_callback=failed_notification) as dag:

    ingest_source_files = PythonOperator(
        task_id='Ingest_Source_Files',
        python_callable=ingest_files,
        do_xcom_push=False
    )

    dq_check_status = BranchPythonOperator(
        task_id='DQ_Check_Status',
        python_callable=notify_failed_files
    )

    dq_fail_notification = PythonOperator(
        task_id='Send_Email_On_DQ_Failure',
        python_callable=log_notification,
        op_kwargs={'source_task_id': 'DQ_Check_Status'}
    )

    valid_file_status = BranchPythonOperator(
        task_id='Valid_File_Status',
        python_callable=notify_missing_files
    )

    missing_file_notification = PythonOperator(
        task_id='Send_Email_On_Missing_Files',
        python_callable=log_notification,
        op_kwargs={'source_task_id': 'Valid_File_Status'}
    )

    load_trusted_table = DbtCloudRunJobOperator(
        task_id="Load_Trusted_Table",
        dbt_cloud_conn_id='dbtcloud_conn_aim',
        steps_override=["dbt run -m ahus_prescriptive_alert_internal"],
        additional_run_config=get_snowflake_env_vars(SNOWFLAKE_CONNECTION, SNOWFLAKE_SCHEMA='komodo'),
        job_id=job_id,
        wait_for_termination=True,
        timeout=600,
        dag=dag
    )

    load_staging_table = DbtCloudRunJobOperator(
        task_id="Load_Staging_Table",
        dbt_cloud_conn_id='dbtcloud_conn_aim',
        steps_override=["dbt run -m stg_alerts_ahus_prescriptive_internal stg_patient_id_ahus_prescriptive_internal"],
        additional_run_config=get_snowflake_env_vars(SNOWFLAKE_CONNECTION, SNOWFLAKE_SCHEMA='raw_komodo'),
        job_id=job_id,
        wait_for_termination=True,
        timeout=600,
        dag=dag
    )

    load_komodo_table = DbtCloudRunJobOperator(
        task_id="Load_Komodo_Table",
        dbt_cloud_conn_id='dbtcloud_conn_aim',
        steps_override=["dbt run --vars '{alert_staging_table: " + staging_table_alert + ", patient_staging_table: " + staging_table_patient + ", indication_alert: " + indication_type + " " + alert_type +"}' -m komodo_alerts master_patient_id"],
        additional_run_config=get_snowflake_env_vars(SNOWFLAKE_CONNECTION, SNOWFLAKE_SCHEMA='komodo'),
        job_id=job_id,
        wait_for_termination=True,
        timeout=600,
        dag=dag
    )

    ingest_source_files >> valid_file_status >> [missing_file_notification, load_trusted_table]
    ingest_source_files >> dq_check_status >> dq_fail_notification
    load_trusted_table >> load_staging_table >> load_komodo_table