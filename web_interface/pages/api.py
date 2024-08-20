import pandas as pd
import streamlit as st
import json, requests

@st.cache_data
def get_dataframe(url:str, query:str)->pd.DataFrame:
    '''
        Send query as post method to the url, and return query results in pandas dataframe format.
        - url: API endpoint
        - query: sql query to execute in database
    '''
    try:
        query_result = json.loads(requests.post(url, data=json.dumps({"query":f"{query}"})).text)
        df = pd.DataFrame(query_result)
        return df
    except Exception as e:
        return None