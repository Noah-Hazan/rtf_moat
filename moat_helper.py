"""
TODO:
- Write method to write arbitary data to bq
- Try/Fail Logic
- Synchronous?
"""

import requests
import pandas as pd
import os
import json
from time import sleep
import logging
from airflow.models import Variable

TOKEN = Variable.get('rtf_moat_token')

class MoatTile:
    with open("/home/airflow/gcs/dags/RTF/moat_config_pixel.json") as json_file:
        config = json.load(json_file)
        
    def __init__(self, tile_id, name, campaigns, tile_type, social=None, **kwargs):
        self.brandid = tile_id
        self.name = name
        self.campaigns = campaigns
        self.tile_type = tile_type
        
        if social:
            self.campaign_level = "level2"
            self.base_metrics = ['date','level2','level3']
        else:
            self.campaign_level = "level1"
            self.base_metrics = ['date','level1','level3','level4']
            
        if self.tile_type == "disp":
            self.metrics = self.base_metrics + MoatTile.config['metrics']['disp_metrics']
        else:
            self.metrics = self.base_metrics + MoatTile.config['metrics']['vid_metrics']
    
    def get_data(self, start_date, end_date):
        auth_header = 'Bearer {}'.format(TOKEN)        
        query = {
                'metrics': ','.join(self.metrics),
                'start': start_date,
                'end': end_date,
                'brandId':self.brandid, ## this is the tile ID 
                }         
        
        self.data = []
        
        for campaign in self.campaigns:
            query[self.campaign_level] = campaign
            logging.info("Getting Data for {}\n {}-{}".format(campaign,start_date,end_date))
            
            try:
                resp = requests.get('https://api.moat.com/1/stats.json',
                                    params=query,
                                    headers={'Authorization': auth_header,
                                                'User-agent': 'Essence Global 1.0'}
                                   )
            
                if resp.status_code == 200:
                    r = resp.json()
                    details = r.get('results').get('details')
                    self.data.extend(details)
                    logging.info("Stored {} entries for {}".format(len(details),campaign))
                elif resp.status_code == 400:
                    logging.error("Ya Goofed. Query:\n{}".format(query))            
                   
            except Exception as e:
                logging.error("Request Failure {}".format(e))
                pass
            
            sleep(11)