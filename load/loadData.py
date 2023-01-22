from datetime import datetime, timedelta

import pandas as pd
import logging
import extract as e,transform as t

def load_hourlyGenerations(data):
    return data

start=str(pd.Timestamp(datetime.date(datetime.today()- timedelta(days=1))))

end=str(pd.Timestamp(datetime.date(datetime.today())))

raw_data=e.extractData.get_hourly_generations(start, end)
    # transform raw data
transformed_data=t.transformData.transform_hourly_generations(raw_data)
    # load transformed data to csv
load=load_hourlyGenerations(transformed_data)

print(load)