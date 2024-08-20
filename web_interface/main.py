import os
import streamlit as st
from utils import Logger
from pages import get_dataframe, home

import json

with open("./config.json", "r") as f:
    config = json.load(f)

logger = Logger()

def main():
    flag = -1
    try:
        if flag == -1:
            flag = 0
            home()
        
        # sidebar layout
        
        if st.sidebar.button("get_dataframe"):
            flag = 1
            url = "http://127.0.0.1:8000/query"
            table = "job_information"
            query = f"SELECT * from {table}"
            get_dataframe(url=url, query=query)
        
        if st.sidebar.button("button2"):
            st.subheader("점심뭐먹지")
            
    except Exception as e:
        logger.log(f"Exception occured during flag #{flag}: {e}", 1)


if __name__=="__main__":
    main()