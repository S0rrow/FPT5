import streamlit as st

def display_error_page(logger):
    logger.log(f"error page displalying...", name=__name__)
    st.title("Something went wrong...")
    st.write("""
             You are currently seeing error page. 
             
             Please check logic flow and session status configuration.
             """)
    