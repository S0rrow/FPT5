import os, json
import streamlit as st
from streamlit_google_auth import Authenticate
from utils import Logger
import views
from time import strftime, gmtime

### Button Callback Functions

# Home button
def on_click_home(logger:Logger):
    logger.log(f"home button clicked; session state current view changed to home", name=__name__)
    st.session_state['current_view'] = "home"

# Login Button
def on_click_login(logger:Logger):
    logger.log(f"login button clicked; session state current view changed to login", name=__name__)
    st.session_state['current_view'] = "login"
    
# User Information Button
def on_click_user_info(logger:Logger):
    logger.log(f"user_info button clicked; session state current view changed to user_information", name=__name__)
    st.session_state['current_view'] = "user_information"

# Job Informations Button
def on_click_job_info(logger:Logger):
    logger.log(f"job_info button clicked; session state current view changed to job_informations", name=__name__)
    st.session_state['current_view'] = "job_informations"

# Filter Log Button
def on_click_filter_log(logger:Logger):
    logger.log(f"filter_log button clicked; session state current view changed to filter_log", name=__name__)
    st.session_state['current_view'] = "filter_log"

def main():
    ### initial configurations
    flag = -1
    parent_path = os.path.dirname(os.path.abspath(__file__))
    logger = Logger(options={"name":__name__})
    st.logo(f"{parent_path}/images/horizontal.png", icon_image=f"{parent_path}/images/logo.png")
    
    try:
        ### page configuration ###
        flag = 0
        st.set_page_config(
            page_title="Tech Map IT | Prototype",
            layout='centered', # 'centered' or 'wide'
            page_icon=f"{parent_path}/images/logo.png",
            initial_sidebar_state="auto"
        )
        logger.log(f"flag #{flag} | page config loaded", name=__name__)
        
        
        ### current_view init
        flag = 1
        # if no session_state.current_view exists
        if not st.session_state.get('current_view', False):
            st.session_state['current_view'] = "home"
        logger.log(f"flag #{flag} | current_view initial config loaded; current_view:{st.session_state.get('current_view')}", name=__name__)
        
        
        ### authenticator init
        flag = 2
        authenticator = Authenticate(
            secret_credentials_path = f"{parent_path}/auth.json",
            cookie_name='oauth_connectivity',
            cookie_key=f'{strftime("%Y%m%d%H%M%S", gmtime())}',
            redirect_uri = 'http://localhost:8501',
        )
        authenticator.check_authentification()
        logger.log(f"flag #{flag} | authenticator loaded", name=__name__)
        
        
        ### sidebar configuration
        flag = 3
        with st.sidebar:
            ### if not logged in
            if not st.session_state.get('connected', False):
                st.write("Home")
                st.button("Home :material/home:", on_click=on_click_home, kwargs={"logger":logger}, use_container_width=True)
                st.write("Dashboard")
                st.button("Job Informations :material/search:", on_click=on_click_job_info, kwargs={"logger":logger}, use_container_width=True)
                st.write("Account")
                st.button("Login :material/login:", on_click=on_click_login, kwargs={"logger":logger}, use_container_width=True)
                st.write("History")
                st.button("History :material/history:", on_click=on_click_filter_log, kwargs={"logger":logger}, use_container_width=True)
            ## if logged in
            else:
                st.write("Home")
                st.button("Home :material/home:", on_click=on_click_home, kwargs={"logger":logger}, use_container_width=True)
                st.write("Dashboard")
                st.button("Job Informations :material/search:", on_click=on_click_job_info, kwargs={"logger":logger}, use_container_width=True)
                st.write("Account")
                st.button("User Information :material/person:", on_click=on_click_user_info, kwargs={"logger":logger}, use_container_width=True)
                st.write("History")
                st.button("History :material/history:", on_click=on_click_filter_log, kwargs={"logger":logger}, use_container_width=True)
        logger.log(f"flag #{flag} | sidebar loaded", name=__name__)
        
        
        ### display page according to current_view
        flag = 4
        if st.session_state.get('current_view') == "home":
            logger.log(f"flag #{flag} | displaying home page", name=__name__)
            views.display_home_page(logger)
            
        elif st.session_state.get('current_view') == "login":
            logger.log(f"flag #{flag} | displaying login page", name=__name__)
            views.display_login_page(logger, authenticator)
            
        elif st.session_state.get('current_view') == "job_informations":
            logger.log(f"flag #{flag} | displaying job_informations page", name=__name__)
            views.display_job_informations(logger, url="http://127.0.0.1:8000")
            
        elif st.session_state.get('current_view') == "user_information":
            logger.log(f"flag #{flag} | displaying user_information page", name=__name__)
            views.display_user_information(logger, authenticator)
        elif st.session_state.get('current_view') == "filter_log":
            logger.log(f"flag #{flag} | displaying filter_log page", name=__name__)
            views.display_filter_log(logger)
        else:
            logger.log(f"flag #{flag} | session state not correctly set; plz check logic flow", name=__name__, flag=1)
            views.display_error_page(logger)
            
        logger.log(f"flag #{flag} | display functions loaded", name=__name__)
    
    except Exception as e:
        logger.log(f"Exception occurred at flag #{flag}: {e}", flag=1, name=__name__)
        
if __name__=="__main__":
    main()