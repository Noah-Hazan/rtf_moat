from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from google.cloud import bigquery, storage
import base64
import logging
from datetime import datetime, timedelta
import requests
import logging
import json

# Load Stuff
TOKEN = Variable.get('rtf_moat_token')

with open("/home/airflow/gcs/dags/RTF/moat_config_pixel.json") as json_file:
        config = json.load(json_file)

default_args = {
    'owner': 'kyle.randolph',
    'depends_on_past': False, 
    'start_date': datetime(2019, 5, 16), 
    'email_on_failure': True,
    'email': ['kyle.randolph@essence.global.com'],
    'retries': 1, 
    'retry_delay': timedelta(minutes=1),
    'provide_context':True # this makes xcom work with
}

dag = DAG('kyle_moat_test_dag',
            default_args=default_args,
            description='Test Account Fetch',
            schedule_interval='@once',
            start_date=datetime(2018, 3, 20), 
            catchup=False)


## New Plan:
##  1. upload api resp string to GCS (filename should be passed dynamically
##  2. Pass file name and location to cleaning task. Task should pull file and transform to newline json and map headers & store
##  3. Push file into BQ
##  4. Confirm BQ and del temp
##  5. Email stats of all events?

def upload_blob(project_id, bucket_name, source, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(source)

    print('File uploaded to {}.'.format(destination_blob_name))

def test_moat_request_task(**kwargs):
    #print(config)
    auth_header = 'Bearer {}'.format(TOKEN)
    resp = requests.get('https://api.moat.com/1/account.json',
                        headers={'Authorization': auth_header})

    print("Try to Upload")
    resp = upload_blob('essence-analytics-dwh','rtf_temp',resp.text,'test_response.json')
    
    return "fuck you kyle it works"


# Function below to try imports from helper file
def pull_xcom(**kwargs):
    ti = kwargs['ti']
    msg = ti.xcom_pull(task_ids='moat_accounts') #xcom puller needs to know what downstream task to pull from w/ task_id

    resp = upload_blob('essence-analytics-dwh','rtf_temp',msg,'xcom_text.json')

"""
def moat_request_task(campaign_id,brandId):
    q = build_query(campaign_id,brandId,tile_type):
    resp = moat_req(q)
    print(resp.json)
"""



start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
moat_account_task = PythonOperator(task_id = 'moat_accounts', python_callable = test_moat_request_task, dag = dag)

xcom_pull_task = PythonOperator(task_id = 'puller', python_callable = pull_xcom, dag = dag)

end_task = DummyOperator(task_id= "End", retries=0, dag=dag)

start_task >> moat_account_task >> xcom_pull_task >> end_task



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