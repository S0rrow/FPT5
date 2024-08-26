import streamlit as st
from ..utils import Logger
def display_error_page(logger:Logger):
    logger.log(f"error page displalying...", name=__name__)
    st.title("Something went wrong...")
    st.write("""
             You are currently seeing error page. 
             
             Please check logic flow and session status configuration.
             """)
    