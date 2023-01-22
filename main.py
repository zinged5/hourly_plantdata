import logging
import timeit
import pandas as pd
from datetime import datetime, timedelta
from  extract import extractData
from transform import transformData
from load import loadData

# start/end date parametized
start=str(pd.Timestamp(datetime.date(datetime.today()- timedelta(days=1))))

end=str(pd.Timestamp(datetime.date(datetime.today())))



if __name__ == '__main__':
    # start timer
    starttime = timeit.timeit()
    # extract raw data, save it to parquet
    raw_data=extractData.get_hourly_generations(start, end)
    # transform raw data
    transformed_data=transfromData.transform_hourly_generations(raw_data)
    # load transformed data to csv
    loadData.load_hourlyGenerations(transformed_data)
    # stop timer
    endtime = timeit.timeit()
    logging.log(10,f'process took {endtime - starttime}')

