import json, os, requests
from time import gmtime, strftime
import streamlit as st
import pandas as pd

@st.cache_data
def get_dataframe(url:str, query:str)->pd.DataFrame:
    logger = Logger()
    try:
        query_result = json.loads(requests.post(url, data=json.dumps({"query":f"{query}"})).text)
        df = pd.DataFrame(query_result)
        return df
    except Exception as e:
        logger.log(f"Exception occurred during api call: {e}", 1)
        return None

class Logger():
    path = None

    def __init__(self, path="./logs"):
        self.path = path
        
    def log(self, msg, flag=None, path="./logs"):
        '''
            print message strings to given level of depth.
            flag value determines the level, where 0 = debug, 1 = error, 2 = warn, 3 = status, 4 = info.
        '''
        if flag is None:
            flag = 4
        head = ["DEBUG", "ERROR", "WARN", "STATUS", "INFO"]
        now = strftime("%Y-%m-%d %H:%M:%S", gmtime())

        if not os.path.isdir(self.path):
            os.mkdir(self.path)

        msg.replace("\n", " ")
        msg.replace("  ", " ")
        log_file = f"{self.path}/{head[flag]}.log"
        log_message = f"[{now}][{head[flag]}] > {msg}\n"
        
        with open(log_file, "a") as f:
            f.write(log_message)

    def get_time(self):
        return strftime("%Y-%m-%d_%H%M%S", gmtime())
