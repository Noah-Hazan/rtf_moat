"""
TODO:
- Write method to write arbitary data to bq
- Try/Fail Logic
- Synchronous?
"""

import datetime
import json
import os
import requests
import sys
from time import sleep

## Load Token from GCS or Environment Variable
TOKEN = os.environ['MOAT_TOKEN'] ## add error handling if token isn't present

## Load path to Config file
## config_path = models.Variables.get("path_to_moat_config")

with open("moat_config_pixel.json", encoding='utf-8') as data_file:
        config = json.loads(data_file.read())

moat_metrics = config['metrics']
moat_tiles = config['tiles']

def build_query(level1Id,brandId,tile_type):
    metrics = moat_metrics[tile_type]        
    start_date = 20190512
    end_date = start_date
    metrics  = metrics + ["level1","level2","Level3"]
    
    query = {
    'metrics': ','.join(metrics),
    'start': start_date,
    'end': end_date,
    'brandId':brandId, ## this is the tile ID 
    'level1Id':level1Id ## this is the campaign
    }   
    
    return query


def moat_req(token,query):  
    auth_header = 'Bearer {}'.format(TOKEN)
    resp = requests.get('https://api.moat.com/1/stats.json',
                        params=query,
                        headers={'Authorization': auth_header})
    if resp.status_code == 200:
        return resp
    else:
        raise Exception("{} Error".format(resp.resp.status_code))


def main():
    print("Lets Go Dude")
    query = build_query(22443077,2698,"vid_metrics")
    resp = moat_req(TOKEN,query)
    r = resp.json()
    #df = pd.DataFrame(r.get('results').get('details'))
    return r

if __name__ == "__main__":
    main()
