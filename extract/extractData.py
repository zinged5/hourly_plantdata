from datetime import datetime, timedelta
import pandas as pd
import requests
import logging
import configparser
import os

config = configparser.RawConfigParser()
cwd=os.getcwd()
configFilePath = f'{cwd}/config.ini'
config.read(configFilePath)
LIST_URL=config.defaults()['list_url']
PLANT_URL=config.defaults()['plant_url']

def get_hourly_generations(start,end,target,plant_id=None):
    logging.log(10, f'starting get_hourly_generations at {datetime.now()}')
    try:
        timestamp = datetime.now()
        timestamp = timestamp.strftime("%m.%d.%Y_%H.%M.%S")
        request = requests.get(url=LIST_URL)
        df_plant_ids = pd.DataFrame(request.json()["body"]["powerPlantList"], index=None)
        if plant_id is not None:
            params = {'startDate': start, 'endDate': end, 'powerPlantId': plant_id}
            response = requests.get(PLANT_URL, params=params)
            df_generation = pd.DataFrame(response.json()['body']['hourlyGenerations'])
            timestamp = datetime.now()
            timestamp = timestamp.strftime("%m.%d.%Y_%H.%M.%S")
            target = f'{target}/hourlyfile_{timestamp}_{str(plant_id)}.csv'
            df_generation.to_csv(target,index=False)
            return df_generation
        else:
            plant_ids=df_plant_ids['id']

    except:
        logging.log(30,f'Something went wrong during fetching data from {LIST_URL} at {datetime.now()}')
        return
    if plant_ids is not None:
        try:
            for id in plant_ids:
                params = {'startDate': start, 'endDate': end, 'powerPlantId': id}
                response = requests.get(PLANT_URL, params=params)
                print(params)
                df_generation = pd.DataFrame(response.json()['body']['hourlyGenerations'])
                # you could use parquet : df_generation.to_parquet(target,compression='snappy')
                df_generation.to_csv(f'{target}/hourlyfile_{timestamp}_{str(id)}.csv', index=False)
                logging.log(10, f'data imported for plant_id {id} at {target}')
        except:
            logging.log(30,f'Something went wrong while fetching the data from {PLANT_URL} querying {id} ')
            return

