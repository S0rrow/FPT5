import json
import streamlit as st
from streamlit_navigation_bar import st_navbar
from streamlit_google_auth import Authenticate
from utils import Logger
from pagecontrollers import SidebarController, PageController

with open("./config.json", "r") as f:
    config = json.load(f)
auth = "./auth.json"

st.set_page_config(
    page_title="TechMap IT",
    layout='wide',
    page_icon=":shark:",
    initial_sidebar_state="collapsed",
    menu_items=None
)

navbar = st_navbar(['home','job_informations','company_informations'])
st.write(navbar)

flag = -1
logger = Logger()
authenticator = Authenticate(
    secret_credentials_path = auth,
    cookie_name='oauth_connectivity',
    cookie_key=f'{logger.get_time()}',
    redirect_uri = 'http://localhost:8501',
)
pg_controller = PageController(authenticator=authenticator, logger=logger)
sb_controller = SidebarController(authenticator=authenticator, logger=logger, pg_controller=pg_controller)

if "current_page" not in st.session_state:
    st.session_state["current_page"] = "home"
try:
    flag = 0
    sb_controller.display_sidebar()
    flag = 1
    # if current page is home, render home page
    pg_controller.display_home_page()
    flag = 2
    # if current page is main, render main page
    pg_controller.display_main_page()
    flag = 3
    # if current page is job_informations, render that page
    pg_controller.display_job_informations()
except Exception as e:
    logger.log(f"Exception occured on main at flag #{flag}: {e}", 1)
