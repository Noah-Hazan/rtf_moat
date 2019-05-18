from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow import models
from google.cloud import bigquery
import base64
import logging
from datetime import datetime, timedelta
import requests

from moat_helper import build_query, moat_req

default_args = {
    'owner': 'kyle.randolph',
    'depends_on_past': False, 
    'start_date': datetime(2019, 5, 16), 
    'email_on_failure': True,
    'email': ['kyle.randolph@essence.global.com'],
    'retries': 1, 
    'retry_delay': timedelta(minutes=2), 
}

# get moat & push GCS -> GCS to CSV -> CSV Import BQ -> Confirm x-fer and delete csv -> Batch Assemble

def test_moat_request_task():
    auth_header = 'Bearer {}'.format(TOKEN)
    resp = requests.get('https://api.moat.com/1/account.json',
                        headers={'Authorization': auth_header})
    print(resp.json())


# Function below to try imports from helper file

"""
def moat_request_task(campaign_id,brandId):
    q = build_query(campaign_id,brandId,tile_type):
    resp = moat_req(q)
    print(resp.json)
"""

dag = DAG('kyle_moat_test_dag',
            default_args=default_args,
            description='Test Account Fetch',
            schedule_interval='@once',
            start_date=datetime(2018, 3, 20), catchup=False)

start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
moat_account_task = PythonOperator(task_id = 'moat_accounts', python_callable = test_moat_request_task, dag = dag)
end_task = DummyOperator(task_id="End", retries=0, dag=dag)

start_task >> moat_account_task >> end_task



"""
campaigns = ["campaign1","campaign2"]

moat_vid_tiles = ["Moat_Video_Tile_" + str(x) for x in range(0,13)]

moat_disp_tiles = ["Moat_Disp_Tile_" + str(x) for x in range(0,5)]


for campaign in campaigns:
    vid_tasks = []
    disp_tasks = []
    
    for tile in moat_vid_tiles:
        task_name = "dummy_{}_{}".format(campaign,tile)
        vid_dummy = DummyOperator(task_id=task_name, retries=0, dag=dag)
        vid_dummy.doc_md = "Writing Vid Tile Data"
        vid_tasks.append(vid_dummy)
    for tile in moat_disp_tiles:
        task_name = "dummy_{}_{}".format(campaign,tile)
        disp_dummy = DummyOperator(task_id=task_name, retries=0, dag=dag)
        disp_dummy.doc_md = "Writing Disp Tile Data"
        disp_tasks.append(disp_dummy)
    
    dummy_start >> vid_tasks >> dummy_middle >> disp_tasks >> dummy_end
"""