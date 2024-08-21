import os, json
import streamlit as st
from streamlit_google_auth import Authenticate
from utils import Logger
import views
from time import strftime, gmtime

def on_click_home():
    st.session_state['current_view'] = "home"

def on_click_login():
    st.session_state['current_view'] = "login"
    
def on_click_user_info():
    st.session_state['current_view'] = "user_information"

def on_click_job_info():
    st.session_state['current_view'] = "job_informations"


def main():
    # init
    flag = -1
    parent_path = os.path.dirname(os.path.abspath(__file__))
    logger = Logger(options={"name":__name__})
    st.logo(f"{parent_path}/images/horizontal.png", icon_image=f"{parent_path}/images/logo.png")
    try:
        ### page configuration ###
        flag = 0
        st.set_page_config(
            page_title="TechMap IT",
            layout='centered', # 'centered' or 'wide'
            page_icon=":shark:",
            initial_sidebar_state="auto"
        )
        if st.session_state.get('current_view', False):
            if st.session_state.get('connected', False):
                st.session_state['current_view'] = "job_informations"
        else:
            st.session_state['current_view'] = "home"
        logger.log(f"flag #{flag} | page config loaded")
        
        # authenticator init
        flag = 1
        authenticator = Authenticate(
            secret_credentials_path = f"{parent_path}/auth.json",
            cookie_name='oauth_connectivity',
            cookie_key=f'{strftime("%Y%m%d%H%M%S", gmtime())}',
            redirect_uri = 'http://localhost:8501',
        )
        authenticator.check_authentification()
        logger.log(f"flag #{flag} | authenticator loaded")
        
        ### sidebar configuration
        st.Page(on_click_home,title="Home2",icon=":material/home:")
        
        flag = 2
        with st.sidebar:
            if not st.session_state.get('connected', False):
                st.button("Home", on_click=on_click_home)
                st.button("Login", on_click=on_click_login)
            else:
                st.button("Job Informations", on_click=on_click_job_info)
                st.button("User Information", on_click=on_click_user_info)
        logger.log(f"flag #{flag} | sidebar loaded")
        
        ### display page according to current_view
        flag = 3
        if st.session_state.get('current_view') == "home":
            logger.log(f"flag #{flag} | displaying home page", name=__name__)
            views.display_home_page(logger)
            
        if st.session_state.get('current_view') == "login":
            logger.log(f"flag #{flag} | displaying login page", name=__name__)
            views.display_login_page(logger, authenticator)
            
        if st.session_state.get('current_view') == "job_informations":
            logger.log(f"flag #{flag} | displaying job_informations page", name=__name__)
            views.display_job_informations(logger)
            
        if st.session_state.get('current_view') == "user_information":
            logger.log(f"flag #{flag} | displaying user_information page", name=__name__)
            views.display_user_information(logger, authenticator)
            
        logger.log(f"flag #{flag} | display functions loaded")
        
    except Exception as e:
        logger.log(f"Exception occurred at flag #{flag}: {e}", flag=1)
        
if __name__=="__main__":
    main()