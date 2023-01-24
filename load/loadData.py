from datetime import datetime
from pathlib import Path
import logging

def load_hourlyGenerations(data,target):
    df=data
    if df is not None:
        timestamp = datetime.now()
        timestamp = timestamp.strftime("%m.%d.%Y_%H.%M.%S")
        try:
            target=Path(target)
            target.parent.mkdir(parents=True,exist_ok=True)
            df.to_csv(target,index=False)
        except:
            logging.log(30,f'failed to load hourlyGenerations file')
    return




