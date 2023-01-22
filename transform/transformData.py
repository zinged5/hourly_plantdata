import timeit
from datetime import datetime, timedelta
import pandas as pd
import logging

# data='/Users/amy/Documents/df_generation_1.csv'
def transform_hourly_generations(data):
    df=data
    # drop null rows
    df.dropna(how='all')
    # add date,hour,min,sec columns for easy slice/dice of data
    df['datetime'] =df['date']
    df['date']=df['datetime'].apply(lambda x: datetime_converter(x,'date'))
    df['hour']=df['datetime'].apply(lambda x: datetime_converter(x,'hour'))
    df['min'] =df['datetime'].apply(lambda x: datetime_converter(x,'min'))
    df['sec']=df['datetime'].apply(lambda x: datetime_converter(x,'sec'))
    return df

# method to convert datetime values
def datetime_converter(df,type):
    try:
        df = df.split('T')
        if len(df)<=1:
            logging.log(30,f'failed to parse {df} to {type}')
        else:
            if type=='hour':
                out = df[1].split(':')[0]
            elif type=='min':
                out = df[1].split(':')[1]
            elif type=='sec':
                out = df[1].split(':')[2].split('.')[0]
            elif type =='date':
                out = df[0]
            return out
    except:
        logging.log(30, f'couldnt parse date from {df}')
        return


