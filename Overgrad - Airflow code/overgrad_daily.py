from datetime import datetime, timedelta, date

from airflow import DAG

from airflow.hooks.base_hook import BaseHook

from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import BranchPythonOperator, PythonOperator

from operators.tools.s3_tools import DataFrameToJsonAndS3
from spswarehouse_airflow.warehouse import create_warehouse
from utils import (
    S3_BUCKET,
    get_s3_key,
    create_post_s3_tasks,
)

import json
import pandas as pd
import logging
import requests
import time
from requests.exceptions import HTTPError

admissions_create_sql = """
CREATE TABLE IF NOT EXISTS overgrad.admissions_changelog (
  id INTEGER,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  student_object VARIANT,
  university_object VARIANT,
  applied_on DATE,
  application_source VARCHAR,
  due_date_object VARIANT,
  status VARCHAR,
  status_updated_at TIMESTAMP,
  waitlisted BOOLEAN,
  deferred BOOLEAN,
  academic_fit VARCHAR,
  probability_of_acceptance NUMBER(8,4),
  award_letter_object VARIANT
)
"""
admissions_insert_sql = """
INSERT INTO overgrad.admissions_changelog
SELECT
  _id,
  TO_TIMESTAMP(_created_at),
  TO_TIMESTAMP(_updated_at),
  PARSE_JSON(_student),
  PARSE_JSON(_university),
  TO_DATE(_applied_on),
  _application_source,
  PARSE_JSON(_due_date),
  _status,
  TO_TIMESTAMP(_status_updated_at),
  _waitlisted = 'true',
  _deferred = 'true',
  _academic_fit,
  TRY_TO_NUMBER(_probability_of_acceptance, 8, 4),
  PARSE_JSON(_award_letter)
FROM overgrad.admissions_latest
"""

followings_create_sql = """
CREATE TABLE IF NOT EXISTS overgrad.followings_changelog (
  id INTEGER,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  student_object VARIANT,
  university_object VARIANT,
  rank INTEGER,
  academic_fit VARCHAR,
  probability_of_acceptance NUMBER(8,4),
  added_by VARCHAR
)
"""

followings_insert_sql = """
INSERT INTO overgrad.followings_changelog
SELECT
  _id,
  TO_TIMESTAMP(_created_at),
  TO_TIMESTAMP(_updated_at),
  PARSE_JSON(_student),
  PARSE_JSON(_university),
  TO_NUMBER(_rank),
  _academic_fit,
  TRY_TO_NUMBER(_probability_of_acceptance, 8, 4),
  _added_by
FROM overgrad.followings_latest
"""


api_pool = 'overgrad_api'
apikey = BaseHook.get_connection('overgrad_api').password
base_uri = 'https://overgrad.com/api/v1/'
endpoints = {
    'admissions': {
        'table_type': 'changelog',
        'create_sql': admissions_create_sql,
        'insert_sql': admissions_insert_sql
    },
    'followings': {
        'table_type': 'changelog',
        'create_sql': followings_create_sql,
        'insert_sql': followings_insert_sql,
    },
    'schools': {'table_type': 'snapshot'},
    'students': {'table_type': 'snapshot'},
}
id_col = 'id' # Overgrad has a standard id column in all endpoints. Recode if this changes
start_date = datetime(2020, 9, 16)
vendor_name = 'overgrad'
version = '1.0'
this_dag = 'overgrad_daily_v' + version

default_args = {
    'owner': 'howard',
    'depends_on_past': False,
    'wait_for_downstream': False,
    'start_date': start_date,
    'email': ['warehouse@summitps.org'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 5,
    'retry_exponential_backoff': True,
    'retry_delay': timedelta(minutes=2),
}


dag = DAG(
    this_dag,
    max_active_runs=1,
    default_args=default_args,
    description="Overgrad API scrape. Note that this is not backfillable, and should never be rerun.",
    schedule_interval='0 4 * * *',
    catchup=True,
)

def overgrad_get(uri, headers, params):
    try:
        results = requests.get(uri, headers=headers, params=params)
        results.raise_for_status()
    except HTTPError as exception:
        error = results.headers
#         logging.info('request limit reached, sleeping')
        if error['X-Ratelimit-Limit'] == '60':
            logging.info(f"one minute limit reached on page {str(params['page'])}, sleep for {error['X-Ratelimit-Reset']}")
            time.sleep(int(error['X-Ratelimit-Reset']) + 5)
        elif error['X-Ratelimit-Limit'] == '1000':
            logging.info(f"one hour limit reached on page {params['page']}, sleep for {error['X-Ratelimit-Reset']}")
            time.sleep(int(error['X-Ratelimit-Reset']) + 120)
        else:
            raise exception
        
        results = requests.get(uri, headers=headers, params=params)

    return results

def get_all_pages(uri, apikey, table_type, **kwargs):
    headers = {'apikey': apikey}
#     start_time = datetime.now()
    
    ds = kwargs['ds']
    tomorrow = kwargs['tomorrow_ds']
    
    s3_key = kwargs['s3_key']
    
    if table_type == 'changelog':
        if ds == start_date.date().isoformat():
            params = {'updated_before': tomorrow}
        else:
            params = {'updated_before': tomorrow, 'updated_after': ds}
    else:
        params = {}

    params['page'] = 1
    first_page = overgrad_get(uri, headers=headers, params=params).json()
    results = first_page['data']
    if first_page['total_pages'] == 1:
        pass
    else:
        total_pages = first_page['total_pages']
        for i in range(2, total_pages+1):
            params['page'] = i
            current_page = overgrad_get(uri, headers=headers, params=params)
            # These end points return a list of dictionaries, so extend the list
            results.extend(current_page.json()['data'])
            
    df = pd.DataFrame(results)
    uploader = DataFrameToJsonAndS3(
        df=df,
        id_colname=id_col,
        as_of=ds,
    )
    uploader.upload(s3_key=s3_key)

def check_for_changes(endpoint_name):
    Warehouse = create_warehouse()
    data = Warehouse.read_sql(f"SELECT COUNT(1) FROM overgrad.{endpoint_name}_latest")
    if data.iloc[0,0] == 0:
        return f"{endpoint_name}_no_changes"
    else:
        return f"{endpoint_name}_create_changelog"
    
success_task = DummyOperator(
    dag=dag,
    task_id='success_task',
    trigger_rule='none_failed',
)

for endpoint_name, endpoint_vars in endpoints.items():
    uri = base_uri + endpoint_name
    api_get_and_load_to_s3 = PythonOperator(
        dag=dag,
        task_id=f'{endpoint_name}_api_get_and_load_to_s3',
        pool=api_pool,
        python_callable=get_all_pages,
        provide_context=True,
        op_kwargs={
            'uri': uri,
            'apikey': apikey,
            'table_type': endpoint_vars['table_type'],
            's3_key': get_s3_key(vendor_name,
                                 endpoint_name,
                                 '{{ ds }}',
                                ),
        },
    )
    
    post_s3_tasks = create_post_s3_tasks(
        dag=dag,
        s3_key=get_s3_key(vendor_name, endpoint_name, '{{ ds }}'),
        task_prefix=f'{endpoint_name}',
        qualified_table_name=f'"MAIN"."{vendor_name.upper()}"."{endpoint_name.upper()}"',
        qualified_stage=f"{vendor_name}.DATA_LAKE".upper(),
    )
    
    api_get_and_load_to_s3 >> post_s3_tasks[0]
    
    if endpoint_vars['table_type'] == 'snapshot':
        post_s3_tasks[-1] >> success_task
    elif endpoint_vars['table_type'] == 'changelog':
        changes_today = BranchPythonOperator(
            dag=dag,
            task_id=f"{endpoint_name}_has_changes",
            python_callable=check_for_changes,
            op_kwargs={
                'endpoint_name': endpoint_name,
            },
        )
        
        no_changes = DummyOperator(
            dag=dag,
            task_id=f"{endpoint_name}_no_changes",
        )
        
        create_changelog = SnowflakeOperator(
            dag=dag,
            task_id=f"{endpoint_name}_create_changelog",
            sql=endpoint_vars['create_sql'],
        )
        
        insert_changelog = SnowflakeOperator(
            dag=dag,
            task_id=f"{endpoint_name}_insert_changelog",
            sql=endpoint_vars['insert_sql'],
        )
        
        post_s3_tasks[-1] >> changes_today >> [no_changes, create_changelog]
        create_changelog >> insert_changelog >> success_task
        no_changes >> success_task
            
    else:
        raise Exception("bad table_type")
    