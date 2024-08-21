import streamlit as st
from google_auth_oauthlib.flow import Flow

def display_login_page(logger, authenticator):
    logger.log(f"rendering login button...",name=__name__)
    authenticator.check_authentification()
    if st.session_state.get('connected', False):
        st.experimental_rerun()
    else:
        authenticator.login(color='white', justify_content="left")