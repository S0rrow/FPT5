import streamlit as st
import pandas as pd
import requests, json
from utils import Logger
import altair as alt

logger = Logger()

def home():
    '''
    show initial page
    '''
    st.header("This is /home")

@st.cache_data
def get_dataframe(url:str, query:str):
    st.header("Job Informations")
    flag = -1
    try:
        query_result = json.loads(requests.post(url, data=json.dumps({"query":f"{query}"})).text)
        flag = 0
        st.subheader("query result from API")
        df = pd.DataFrame(query_result)
        flag = 1
        st.dataframe(df)
    except Exception as e:
        st.subheader("Something went wrong :(")
        logger.log(f"Exception occured while retrieving data from API on flag #{flag}: {e}", 1)