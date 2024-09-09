import streamlit as st
from .utils import Logger

def display_error_page(logger:Logger):
    method_name = __name__ + ".display_error_page"
    st.title("Something went wrong...")
    st.write("""
             You are currently seeing error page. 
             
             Please check logic flow and session status configuration.
             """)
    logger.log(f"action:load, element:error_page", flag=4, name=method_name)