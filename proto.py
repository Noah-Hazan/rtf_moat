import requests
import pandas as pd
import os
import json
from time import sleep
import logging

TOKEN = os.environ['MOAT_TOKEN']

class MoatTile:
    with open("moat_config_pixel.json", encoding='utf-8') as data_file:
        config = json.loads(data_file.read())
        
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
            logging.info("Getting Data for {}\n {}-{}".format(x,start_date,end_date))
            
            try:
                resp = requests.get('https://api.moat.com/1/stats.json',
                                    params=query,
                                    headers={'Authorization': auth_header,
                                                'User-agent': 'Essence Global 1.0'}
                                   )
            
                if resp.status_code == 200:
                    r = resp.json()
                    details = r.get('results').get('details')
                    self.data.append(details)
                    logging.info("Stored {} entries for {}".format(len(details),campaign))
                elif resp.status_code == 400:
                    logging.error("Ya Goofed. Query:\n{}".format(query))            
                   
            except Exception as e:
                logging.error("Request Failure {}".format(e))
                pass
            
            sleep(11)

## cm 
cm_disp = MoatTile(2506,"Google Display",[22443077],"disp",False)
cm_vid = MoatTile(2698,"Google Display",[22443077],"vid",False)

## twitter
tw_disp = MoatTile(6195541,"Twitter Display",[21017248,20969946,20970333,20969992],"disp",True)
tw_vid = MoatTile(6195543,"Twitter Video",[20970543],"vid",True)

## facebook 
fb_dis = MoatTile(6195503,"FB Video",[23843331338570586,23843331340260586],"vid",True)
fb_vib = MoatTile(8268,"FB Video",[23843331336980586],"vid",True)
ig_disp = MoatTile(6188035,"IG Disp",[23843331350810586],"vid",True)