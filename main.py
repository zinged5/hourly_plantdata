import logging
import os
import timeit
from pathlib import Path

import pandas as pd
from datetime import datetime, timedelta
import load.loadData
from  extract import extractData
from transform import transformData
from load import loadData

# start/end date parametized
start=str(pd.Timestamp(datetime.date(datetime.today()- timedelta(days=1))))

end=str(pd.Timestamp(datetime.date(datetime.today())))



if __name__ == '__main__':
    # start timer
    starttime = timeit.timeit()
    try:
        # extract raw data, save it to parquet
        # raw_data=extractData.get_hourly_generations(start,end)
        # transform raw data
        dir=f'/Users/amy/Documents/hourlyGenerationFiles'
        path = os.walk(dir, topdown=True)
        filelist =[]
        for root, dirs, files in os.walk(dir, topdown=True):
            if root == dir:
                for file in files:
                    if file.endswith('.csv'):
                        filelist.append(f'{root}/{file}')
        if filelist is not None:
            for file in filelist:
                logging.log(10,f'starting to process {file} at {datetime.now()}')
                # check if file is empty
                file_size = os.path.getsize(file)
                if file_size>1:
                 # transform data
                    df=transformData.transform_hourly_generations(file)

                    # load transformed data to csv
                    datestamp=datetime.today()
                    target = file.replace('hourlyGenerationFiles/', f'hourlyGenerationFiles/transformed/{datestamp.strftime("%Y")}/{datestamp.strftime("%m")}/{datestamp.strftime("%m")}/')
                    load.loadData.load_hourlyGenerations(df, target)

                     # create dir if it doesn't exist
                    processed_dir=file.replace('hourlyGenerationFiles/',f'hourlyGenerationFiles/processed/{datestamp.strftime("%Y")}/{datestamp.strftime("%m")}/{datestamp.strftime("%m")}/')
                    processed_dir = Path(processed_dir)
                    processed_dir.parent.mkdir(parents=True, exist_ok=True)

                     # move file to processed
                    d=Path(file).rename(processed_dir)
                else:
                    logging.log(30,f'{file} is empty')
        # stop timer
    except:
        logging.log(30, f'process failed at {datetime.now()}')
    endtime = timeit.timeit()
    logging.log(10,f'process took {endtime - starttime}')

