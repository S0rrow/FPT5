import os, json
import streamlit as st
from streamlit_navigation_bar import st_navbar
from streamlit_google_auth import Authenticate
from utils import Logger
import pages as pg
from time import strftime, gmtime

# init
flag = -1
parent_path = os.path.dirname(os.path.abspath(__file__))
logger = Logger()
try:
    flag = 0
    st.set_page_config(
        page_title="TechMap IT",
        layout='wide',
        page_icon=":shark:",
        initial_sidebar_state="auto",
        menu_items=None
    )

    # navigation bar
    flag = 1
    pages = []
    if st.session_state.get('connected', False):
        pages = ['Home','Job Informations', 'User Information']
    else:
        pages = ['Home','Job Informations', 'Login']
    logger.log(f"flag #{flag} | pages:{pages}", 0)

    flag = 2
    logo_path = f"{parent_path}/logo.png"
    page = st_navbar(
        pages=pages,
        # logo_page=logo_path,
        # options=options
    )
    st.write(page)
    # authenticator init
    flag = 3
    authenticator = Authenticate(
        secret_credentials_path = f"{parent_path}/auth.json",
        cookie_name='oauth_connectivity',
        cookie_key=f'{strftime("%Y%m%d%H%M%S", gmtime())}',
        redirect_uri = 'http://localhost:8501',
    )
    authenticator.check_authentification()

    # call page functions
    flag = 4
    functions = {
        "Home": pg.display_home_page,
        "Job Informations": pg.display_job_informations,
        "User Information": pg.display_user_information,
        "Login": pg.display_login_page
    }
    logger.log(f"flag #{flag} | page:{page}", 0)
    loc = functions.get(page)
    flag = 5
    if loc:
        logger.log(f"flag #{flag} | function called.", 0)
        loc(logger, authenticator)
except Exception as e:
    logger.log(f"Exception occurred at flag #{flag}: {e}", 1)