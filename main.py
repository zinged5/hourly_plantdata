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
# start=str(pd.Timestamp('2022-01-01'))
# end=str(pd.Timestamp('2022-02-01'))
def main():
    # start timer
    starttime = timeit.timeit()
    try:
        dir = f'/Users/amy/Documents/hourlyGenerationFiles'
        # extract raw data
        raw_data = extractData.get_hourly_generations(start, end, target=dir)
        path = os.walk(dir, topdown=True)
    except:
        logging.log(30, 'failed to extract hourlyGeneration files from API')
    try:
        dir = f'/Users/amy/Documents/hourlyGenerationFiles'
        filelist = set()
        for root, dirs, files in os.walk(dir, topdown=True):
            if root == dir:
                for file in files:
                    if file.endswith('.csv'):
                        filelist.add(f'{root}/{file}')
        if filelist is not None:
            for file in filelist:
                print(file)
                logging.log(10, f'starting to process {file} at {datetime.now()}')
                # check if file is empty
                file_size = os.path.getsize(file)
                if file_size > 1:
                    # transform data
                    df = transformData.transform_hourly_generations(file)

                    # load transformed data to csv
                    datestamp = datetime.today()
                    target = file.replace('hourlyGenerationFiles/',
                                          f'hourlyGenerationFiles/transformed/{datestamp.strftime("%Y")}/{datestamp.strftime("%m")}/{datestamp.strftime("%m")}/')
                    load.loadData.load_hourlyGenerations(df, target)

                    # create dir if it doesn't exist
                    processed_dir = file.replace('hourlyGenerationFiles/',
                                                 f'hourlyGenerationFiles/processed/{datestamp.strftime("%Y")}/{datestamp.strftime("%m")}/{datestamp.strftime("%m")}/')
                    processed_dir = Path(processed_dir)
                    processed_dir.parent.mkdir(parents=True, exist_ok=True)

                    # move file to processed
                    Path(file).rename(processed_dir)
                else:
                    logging.log(30, f'{file} is empty')
                    # create dir if it doesn't exist
                    datestamp = datetime.today()
                    failed_dir = file.replace('hourlyGenerationFiles/',
                                              f'hourlyGenerationFiles/failed/{datestamp.strftime("%Y")}/{datestamp.strftime("%m")}/{datestamp.strftime("%m")}/')
                    failed_dir = Path(failed_dir)
                    failed_dir.parent.mkdir(parents=True, exist_ok=True)
                    # move file to failed
                    Path(file).rename(failed_dir)

    except:
        logging.log(30, f'process failed at {datetime.now()}')
        # stop the timer
        endtime = timeit.timeit()
        logging.log(10, f'process took {endtime - starttime}')

if __name__ == '__main__':
    main()



