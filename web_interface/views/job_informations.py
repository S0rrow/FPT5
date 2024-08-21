import streamlit as st
from views.api import get_dataframe

def display_job_informations(logger, url:str=None, query:str=None):
    '''
        display job informations retreived from given url
    '''
    try:
        logger.log(f"url:{url}, query:{query}", name=__name__)
        if not url:
            url = "http://127.0.0.1:8000/query"
        if not query:
            query = f"SELECT * from job_information"
        df = get_dataframe(url=url, query=query, _logger=logger)
        st.header("Job Informations")
        st.subheader("query result from API")
        st.dataframe(df, use_container_width=True)
    except Exception as e:
        logger.log(f"Exception occurred while rendering job informations: {e}", name=__name__)