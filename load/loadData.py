from datetime import datetime, timedelta

import pandas as pd
import logging
from extract import extractData
from transform import transformData

def load_hourlyGenerations(data,target):
    return data


target = f'/Users/amy/Documents/hourlyGeneration_{str(id)}.csv'
start=str(pd.Timestamp(datetime.date(datetime.today()- timedelta(days=1))))

end=str(pd.Timestamp(datetime.date(datetime.today())))

raw_data=extractData.get_hourly_generations(start, end,plant_id=989)
    # transform raw data
transformed_data=transformData.transform_hourly_generations(raw_data)
    # load transformed data to csv
load=load_hourlyGenerations(transformed_data,target)

print(load)