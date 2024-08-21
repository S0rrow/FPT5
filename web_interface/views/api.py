import pandas as pd
import streamlit as st
import json, requests

@st.cache_data
def get_dataframe(url:str, query:str, _logger)->pd.DataFrame:
    '''
        Send query as post method to the url, and return query results in pandas dataframe format.
        - url: API endpoint
        - query: sql query to execute in database
    '''
    try:
        _logger.log(f"getting dataframe...", name=__name__)
        query_result = json.loads(requests.post(url, data=json.dumps({"query":f"{query}"})).text)
        df = pd.DataFrame(query_result)
        return df
    except Exception as e:
        _logger.log(f"Exception occurred while getting dataframe: {e}", flag=1, name=__name__)
        return None