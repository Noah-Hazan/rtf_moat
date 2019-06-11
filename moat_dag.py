## New Plan:
##  1. upload api resp string to GCS (filename should be passed dynamically
##  2. Pass file name and location to cleaning task. Task should pull file and transform to newline json and map headers & store
##  3. Push file into BQ
##  4. Confirm BQ and del temp
##  5. Email stats of all events?

# ToDo:
# temp folder path should be read from a config file (defined once)
# when importing class does it read from main global or class file global (token)

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
from RTF.moat_helper import MoatTile

# Load Stuff
TOKEN = Variable.get('rtf_moat_token')

with open("/home/airflow/gcs/dags/RTF/moat_config_pixel.json") as json_file:
        config = json.load(json_file)

default_args = {
    'owner': 'Reporting Task Force',
    'depends_on_past': False, 
    'start_date': datetime(2019, 5, 16), 
    'email_on_failure': True,
    'email': ['kyle.randolph@essence.global.com'],
    'retries': 1, 
    'retry_delay': timedelta(minutes=1),
    'provide_context':True # this makes xcom work with
}

dag = DAG('rtf_moat_dag',
            default_args=default_args,
            description='moat_api',
            schedule_interval='@once',
            start_date=datetime(2018, 3, 20), 
            catchup=False)

tiles = [(2506,"google_display",[22443077],"disp",False),
            (2698,"google_video",[22443077],"vid",False),
            (6195541,"tw_display",[21017248,20969946,20970333,20969992],"disp",True),
            (6195543,"tw_display",[20970543],"vid",True),
            (8268,"fb_video",[23843331336980586],"vid",True),
            (6188035,"ig_disp",[23843331350810586],"vid",True),
            (6195503,"fb_video",[23843331338570586,23843331340260586],"vid",True)]

## need to add youtube


def moat_request_task(**context):
    tile_id = context['tile_id']
    tile_name = context['tile_name']
    campaign_ids = context['campaign_ids']
    tile_type = context['tile_type']
    social = context['social']
    tile = MoatTile(tile_id,tile_name,campaign_ids,tile_type,social)

    tile.get_data('20190505','20190511')
    
    filename = tile.name + tile.start_date + '_' + tile.start_date + ".json"
    if tile.data != []:
        upload_blob('essence-analytics-dwh', ## store these paths in a config file
                    'rtf_temp',
                    json.dumps(tile.data),
                    filename)
    return filename

def upload_blob(project_id, bucket_name, source, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(source)
    print('File uploaded to {}.'.format(destination_blob_name))
    return

def xcom_write(**kwargs):
    ti = kwargs['ti']
    task_id = kwargs['task_id']
    filename = task_id + ".json"
    msg = ti.xcom_pull(task_ids=task_id) #xcom puller needs to know what downstream task to pull from w/ task_id
    

################ Define DAG ################
start_task = DummyOperator(task_id="Start", retries=0, dag=dag)
end_task = DummyOperator(task_id= "End", retries=0, dag=dag)

for tile in tiles:
        task_id = tile[1] + "_" + str(tile[0])        
        args = dict(zip(('tile_id','tile_name','campaign_ids','tile_type','social'),tile))
        moat_account_task = PythonOperator(task_id = task_id, 
                                            python_callable = moat_request_task,
                                            op_kwargs = args,
                                            dag = dag)

        

        write_to_gcs_task = PythonOperator(task_id = task_id + "_gcs",
                                            python_callable = xcom_write, 
                                            op_kwargs = {'task_id':task_id},
                                            dag = dag)
        start_task >> moat_account_task >> write_to_gcs_task >> end_task

