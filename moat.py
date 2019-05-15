import argparse
import datetime
import urllib.request
import urllib.error
import base64
import logging
import json
from time import sleep
import sys
import db_helper
from db_helper import dbConnection

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

if args.prod:
    creds = db_creds['production']
else:
    creds = db_creds['dev']

dimensions = config['request_dimensions']

db = db_helper.dbConnection(creds['host'],
                        creds['db'],
                        creds['user'],
                        creds['password'],
                        3306)

def get_data(email,password,start_date,end_date,campaign_id,media_type):
    logging.info("Start Date {}, End Date {}".format(start_date,end_date))    
    columns = dimensions[media_type]    
    base_url = 'https://api.moat.com/1/stats.json?start={}&end={}&level1={}&columns='
    url = base_url + ','.join(columns)   
    tile = email[13:email.find('@')]             
    req = urllib.request.Request(url.format(start_date,end_date,campaign_id))

    base64string = base64.b64encode('{}:{}'.format(email,password).encode())

    req.add_header("Authorization", "Basic {}".format(base64string.decode()))
    
    tries = 0
    
    parsed = None
    while tries < 4:
            try:
                result = urllib.request.urlopen(req)
                parsed = json.loads(result.read())
                break        
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    sleep_time = 2**(tries+1)
                    logging.error('429 Error, Sleep {}s. {} tries remaining'.format(sleep_time,4-tries))
                    sleep(sleep_time)
                    tries += 1
                    continue
            except Exception as e:
                print(e)    
    
    
    if parsed['results']['details'] == [] or parsed is None:
        logging.info('{} Not Found in {} Tile'.format(campaign_id,tile))
        return
    else:
        logging.info('{} Found in {} Tile'.format(campaign_id,tile))                
        return parsed['results']['details']
    

def main():
    logging.info('Start Video')
    tiles = [{'tile':'nbcu_essence_DCM_Display@moat.com','type':'display'},
            {'tile':'nbcu_essence_dcm_video@moat.com','type':'video'}]
    campaigns = db.get_campaigns()
    logging.debug(campaigns)
    for campaign in campaigns:
        campaign_id = campaign['id']
        logging.debug('Campaign {}'.format(campaign_id))
        for tile in tiles:
            response = get_data(tile['tile'],'fathoG6300!',START_DATE,END_DATE,str(campaign_id),tile['type'])            
            logging.debug(response)
            sleep(2)
            if tile['type'] == 'video' and response != None:
                logging.info('Writing to Moat Video Table')
                rows = db.write_moat_video(response)
                
            elif tile['type'] == 'display' and response != None:
                logging.info('Writing to Moat display Table')
                rows = db.write_moat_display(response)
            else:
                pass
            
            logging.info('Rows Affected {}'.format(rows))

    return

if __name__ == "__main__":
    main()