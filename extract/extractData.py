import timeit
from datetime import datetime, timedelta
import pandas as pd
import fastparquet
import requests
import logging
import configparser
import os

config = configparser.RawConfigParser()
cwd=os.getcwd()
configFilePath = f'{cwd}/config.ini'
config.read(configFilePath)


def get_hourly_generations(start,end,plant_id=989):
    logging.log(10,f'starting get_hourly_generations at {datetime.now()}')
    try:
        url=config.defaults()['list_url']
        request = requests.get(url=url)
        df_plant_ids = pd.DataFrame(request.json()["body"]["powerPlantList"], index=None)
        plant_ids=df_plant_ids['id']
    except:
        logging.log(30,f'Something went wrong during fetching data from {url} at {datetime.now()}')
        return
    if plant_ids is not None:
        try:
            for id in plant_ids:
                url=config.defaults()['plant_url']
                params = {'startDate': start, 'endDate': end, 'powerPlantId': id}
                response = requests.get(url, params=params)
                df_generation = pd.DataFrame(response.json()['body']['hourlyGenerations'])
                # target=f'/Users/amy/Documents/df_generation_{str(id)}.parquet'
                target = f'/Users/amy/Documents/df_generation_{str(id)}.csv'
                # df_generation.to_parquet(target,compression='snappy')
                logging.log(10, f'data imported for plant_id {id} at {target}')
                # df_generation.to_csv(target,index=False)
            return df_generation
        except:
            logging.log(30,f'Something went wrong fetching the data from {url} querying {id} ')
            return

