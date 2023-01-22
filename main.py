import logging
import timeit
import pandas as pd
from datetime import datetime, timedelta
import extract,transform,load

# start/end date parametized
start=str(pd.Timestamp(datetime.date(datetime.today()- timedelta(days=1))))

end=str(pd.Timestamp(datetime.date(datetime.today())))



if __name__ == '__main__':
    # start timer
    starttime = timeit.timeit()
    # extract raw data, save it to parquet
    raw_data=extract.get_hourly_generations(start, end)
    # transform raw data
    transformed_data=transform.transform_hourly_generations(raw_data)
    # load transformed data to csv
    load.load_hourlyGenerations(transformed_data)
    # stop timer
    endtime = timeit.timeit()
    logging.log(10,f'process took {endtime - starttime}')

