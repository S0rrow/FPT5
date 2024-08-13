import pandas as pd
import datetime

#import utils
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from src import utils

def save_dataframe(dic):
    try:
        dic_df = pd.DataFrame(dic)
        timedate = datetime.datetime.now().strftime("%Y%m%d")
        dic_df.to_json(f'rocketpunch_{timedate}.json', force_ascii=False, orient = 'records', indent=4)
        utils.log("save_json module succeeded",flag=4) # info
    except Exception as e:
        utils.log(f"save_json module failed : {e}",flag=1) # error