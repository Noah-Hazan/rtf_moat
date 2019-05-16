"""
TODO:
- Function to load metrics config JSON
- Figure out how to handle Tiles and their tile types
- Try/Fail Logic
- Synchronous?
"""

import argparse
import datetime
import json
import logging
import os
import requests
import sys
from time import sleep
import pandas as pd

TOKEN = os.environ['MOAT_TOKEN']

with open("moat_config.json", encoding='utf-8') as data_file:
        config = json.loads(data_file.read())

"""
logging.basicConfig(level=logging.INFO)

parser = argparse.ArgumentParser(description='Update Moat')

parser.add_argument('-s','--startdate',type=str, help='YYYY-MM-DD')
parser.add_argument('-e','--enddate',type=str, help='YYYY-MM-DD')
parser.add_argument('-p','--prod', help='send to prod database (cloud)',action='store_true')

args = parser.parse_args()

logging.debug(args)

if args.startdate:
    START_DATE = args.startdate
    END_DATE = args.enddate
else:
    START_DATE = END_DATE = (datetime.datetime.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')

with open('moat_config.json', encoding='utf-8') as data_file:
        config = json.loads(data_file.read())

with open('config.json', encoding='utf-8') as data_file:
        db_creds = json.loads(data_file.read())


dimensions = config['request_dimensions']
"""

moat_metrics = config['metrics']
moat_tiles = config['tiles']

def build_query(level1Id,brandId,tile_type):
    metrics = moat_metrics[tile_type]        
    START_DATE = 20190512
    END_DATE = START_DATE
    metrics  = metrics + ["level1","level2","Level3"]
    
    query = {
    'metrics': ','.join(metrics),
    'start': START_DATE,
    'end': END_DATE,
    'brandId':brandId, ## this is the tile ID 
    'level1Id':level1Id ## this is the campaign
    }   
    
    return query


def moat_req(token,query):  
    auth_header = 'Bearer {}'.format(token)
    resp = requests.get( 'https://api.moat.com/1/stats.json',
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
    df = pd.DataFrame(r.get('results').get('details'))
    return df

def test():
    print("Lets Go Dude")
    query = build_query(22443077,2698,"vid_metrics")
    resp = moat_req(TOKEN,query)
    r = resp.json()
    df = pd.DataFrame(r.get('results').get('details'))
    return df


if __name__ == "__main__":
    test()
