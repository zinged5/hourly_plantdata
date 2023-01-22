import time
import timeit
from datetime import datetime, timedelta


import pandas as pd
import logging

# data='/Users/amy/Documents/df_generation_1.csv'
def transform_hourly_generations(data):
    df=pd.read_csv(data)
    if df is not None:
        ind = data.rindex('/')
        filename = data[ind + 1:].replace('.csv', '').strip()
        try:
            # drop null rows
            df.dropna(how='all')
            # add date,hour,min,sec columns for easy slice/dice of data
            df['datetime'] =df['date']
            df['date']=df['datetime'].apply(lambda x: datetime_converter(x,'date'))
            df['hour']=df['datetime'].apply(lambda x: datetime_converter(x,'hour'))
            df['min'] =df['datetime'].apply(lambda x: datetime_converter(x,'min'))
            df['sec']=df['datetime'].apply(lambda x: datetime_converter(x,'sec'))
            df['plant_id']=filename.split('_')[1]
            df=df.reindex(columns=['plant_id','datetime','date','hour','min','sec','fueloil','gasOil','blackCoal','lignite','geothermal','naturalGas','river','dammedHydro','lng','biomass','naphta','importCoal','asphaltiteCoal','wind','nucklear','sun','importExport','wasteheat','total'])
            return df
        except:
            logging.log(30,f'failed to transform {data}')

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


