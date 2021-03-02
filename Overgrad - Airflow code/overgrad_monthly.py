# Used to retrieve endpoints where only one id can be retrieved at a time.

from datetime import datetime, timedelta, date

from airflow import DAG

from airflow.hooks.base_hook import BaseHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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

def university_id_finder():
    Warehouse = create_warehouse()
    
    university_id_list = Warehouse.read_sql("""
    SELECT DISTINCT PARSE_JSON(university_object):id AS id
    FROM overgrad.admissions_changelog
    UNION
    SELECT DISTINCT PARSE_JSON(university_object):id AS id
    FROM overgrad.followings_changelog
    """)
    
    Warehouse.close()
    return university_id_list

api_pool = 'overgrad_api'
apikey = BaseHook.get_connection('overgrad_api').password
base_uri = 'https://overgrad.com/api/v1/'
endpoints = {
    'universities': {'id_list_function': university_id_finder},
}
id_col = 'id' # Overgrad has a standard id column in all endpoints. Recode if this changes
start_date = datetime(2020, 8, 17)
vendor_name = 'overgrad'
version = '1.0'
this_dag = 'overgrad_monthly_v' + version

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
    description="Overgrad API scrape of university endpoint.",
    schedule_interval='0 0 1 * *',
    catchup=False,
)

def overgrad_get(uri, headers, params={}):
    try:
        results = requests.get(uri, headers=headers, params=params)
        results.raise_for_status()
    except HTTPError as exception:
        error = results.headers
#         logging.info('request limit reached, sleeping')
        if error['X-Ratelimit-Limit'] == '60':
            logging.info(f"one minute limit reached, sleep for {error['X-Ratelimit-Reset']}")
            time.sleep(int(error['X-Ratelimit-Reset']) + 5)
        elif error['X-Ratelimit-Limit'] == '1000':
            logging.info(f"one hour limit reached, sleep for {error['X-Ratelimit-Reset']}")
            time.sleep(int(error['X-Ratelimit-Reset']) + 120)
        else:
            raise exception
        
        results = requests.get(uri, headers=headers, params=params)

    return results

def get_all_records(uri, apikey, id_list_function, id_list_column='id', **kwargs):
    headers = {'apikey': apikey}
#     start_time = datetime.now()
    
    ds = kwargs['next_ds']
    
    s3_key = kwargs['s3_key']
    
    results =[]
    id_list = id_list_function()

    for id_number in id_list[id_list_column]:
        current_results = overgrad_get(f'{uri}/{id_number}', headers=headers)
        # The universities endpoint returns a single dictionary, so append to list
        results.append(current_results.json()['data'])


    df = pd.DataFrame(results)
    uploader = DataFrameToJsonAndS3(
        df=df,
        id_colname=id_col,
        as_of=ds,
    )
    uploader.upload(s3_key=s3_key)

success_task = DummyOperator(
    dag=dag,
    task_id='success_task',
)

for endpoint_name, endpoint_vars in endpoints.items():
    uri = base_uri + endpoint_name
    api_get_and_load_to_s3 = PythonOperator(
        dag=dag,
        task_id=f'{endpoint_name}_api_get_and_load_to_s3',
        pool=api_pool,
        python_callable=get_all_records,
        provide_context=True,
        op_kwargs={
            'uri': uri,
            'apikey': apikey,
            'id_list_function': endpoint_vars['id_list_function'],
            's3_key': get_s3_key(vendor_name,
                                 endpoint_name,
                                 '{{ next_ds }}',
                                ),
        },
    )
    
    post_s3_tasks = create_post_s3_tasks(
        dag=dag,
        s3_key=get_s3_key(vendor_name, endpoint_name, '{{ next_ds }}'),
        task_prefix=f'{endpoint_name}',
        qualified_table_name=f'"MAIN"."{vendor_name.upper()}"."{endpoint_name.upper()}"',
        qualified_stage=f"{vendor_name}.DATA_LAKE".upper(),
        airflow_date_macro='next_ds',
    )
    
    api_get_and_load_to_s3 >> post_s3_tasks[0]
    post_s3_tasks[-1] >> success_task    