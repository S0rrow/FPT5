import streamlit as st
from pages.api import get_dataframe

def display_job_informations(logger, authenticator):
    '''
        display job informations retreived from given url
    '''
    # if not url:
    #     url = "http://127.0.0.1:8000/query"
    # if not query:
    #     query = f"SELECT * from job_information"
    # df = get_dataframe(url=url, query=query)
    st.header("Job Informations")
    st.subheader("query result from API")
    # if df.empty:
    #     st.write(f"dataframe is empty")
    # st.dataframe(df, use_container_width=True)