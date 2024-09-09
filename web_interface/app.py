import hashlib
import os, json, base64
import streamlit as st
from streamlit_google_auth import Authenticate
from views.utils import Logger
import views
from time import strftime, gmtime
from datetime import datetime

logger = Logger(options={"name":__name__})

### Button Callback Functions

# Home button
def on_click_home(logger:Logger):
    method_name = __name__ + ".on_click_home"
    logger.log(f"home button clicked; session state current view changed to home", name=__name__)
    logger.log(f"action:click, element:home_button",flag=4,name=method_name)
    st.session_state['current_view'] = "home"

# Login Button
def on_click_login(logger:Logger):
    method_name = __name__ + ".on_click_login"
    logger.log(f"login button clicked; session state current view changed to login", name=__name__)
    logger.log(f"action:click, element:login_button",flag=4,name=method_name)
    st.session_state['current_view'] = "login"
    
# User Information Button
def on_click_user_info(logger:Logger):
    method_name = __name__ + ".on_click_user_info"
    logger.log(f"user_info button clicked; session state current view changed to user_information", name=__name__)
    logger.log(f"action:click, element:user_information_button",flag=4,name=method_name)
    st.session_state['current_view'] = "user_information"

# Job Informations Button
def on_click_job_info(logger:Logger):
    method_name = __name__ + ".on_click_job_info"
    logger.log(f"job_info button clicked; session state current view changed to job_informations", name=__name__)
    logger.log(f"action:click, element:job_informations_button",flag=4,name=method_name)
    st.session_state['current_view'] = "job_informations"

# Filter Log Button
def on_click_history(logger:Logger):
    method_name = __name__ + ".on_click_history"
    logger.log(f"filter_log button clicked; session state current view changed to filter_log", name=__name__)
    logger.log(f"action:click, element:history_button",flag=4,name=method_name)
    st.session_state['current_view'] = "filter_log"

def main():
    ### initial configurations
    flag = -1
    method_name = __name__ + ".main"
    parent_path = os.path.dirname(os.path.abspath(__file__))
    st.logo(f"{parent_path}/images/horizontal.png", icon_image=f"{parent_path}/images/logo.png")
    logger.log(f"action:load, element:logo",flag=4, name=method_name)
    
    try:
        ### page configuration ###
        flag = 0
        st.set_page_config(
            page_title="Tech Map IT | Prototype",
            layout='centered', # 'centered' or 'wide'
            page_icon=f"{parent_path}/images/logo.png",
            initial_sidebar_state="auto"
        )
        
        ### current_view init
        flag = 1
        # if no session_state.current_view exists
        if not st.session_state.get('current_view', False):
            st.session_state['current_view'] = "home"
        # 세션 상태에 'apply_last_filter'가 없으면 초기화
        if 'apply_last_filter' not in st.session_state:
            st.session_state['apply_last_filter'] = False
            logger.log(f"action:apply, element:session_state['apply_last_filter']={st.session_state['apply_last_filter']}",flag=4,name=method_name)
        # 세션 ID 생성
        if 'session_id' not in st.session_state:
            # encode session_id as string of current timestamp with base64
            st.session_state['session_id'] = base64.b64encode(str(datetime.now().timestamp()).encode()).decode()
            logger.log(f"action:apply, element:session_state['session_id']={st.session_state['session_id']}",flag=4,name=method_name)
        # user id init
        if 'user_id' not in st.session_state and st.session_state.get('user_info', False):
            st.session_state['user_id'] = hashlib.md5(st.session_state['user_info'].get('email').encode()).hexdigest()
            logger.log(f"action:apply, element:session_state['user_id']={st.session_state['user_id']}",flag=4,name=method_name)
        logger.log(f"action:apply, element:session_state['current_view']={st.session_state.get('current_view')}", flag=4, name=method_name)
        
        
        ### authenticator init
        flag = 2
        authenticator = Authenticate(
            secret_credentials_path = f"{parent_path}/auth.json",
            cookie_name='oauth_connectivity',
            cookie_key=f'{strftime("%Y%m%d%H%M%S", gmtime())}',
            redirect_uri = 'http://localhost:8501',
        )
        authenticator.check_authentification()
        logger.log(f"action:apply, element:session_state['connected']={st.session_state.get('connected')}",flag=4,name=method_name)
        
        ### sidebar configuration
        flag = 3
        with st.sidebar:
            ### if not logged in
            if not st.session_state.get('connected', False):
                st.write("Home")
                st.button("Home :material/home:", on_click=on_click_home, kwargs={"logger":logger}, use_container_width=True)
                logger.log(f"action:load, element:home_button", flag=4, name=method_name)
                st.write("Dashboard")
                st.button("Job Informations :material/search:", on_click=on_click_job_info, kwargs={"logger":logger}, use_container_width=True)
                logger.log(f"action:load, element:job_informations_button", flag=4, name=method_name)
                st.write("Account")
                st.button("Login :material/login:", on_click=on_click_login, kwargs={"logger":logger}, use_container_width=True)
                logger.log(f"action:load, element:login_button", flag=4, name=method_name)
                st.write("History")
                st.button("History :material/history:", on_click=on_click_history, kwargs={"logger":logger}, use_container_width=True)
                logger.log(f"action:load, element:history_button", flag=4, name=method_name)
            ## if logged in
            else:
                st.write("Home")
                st.button("Home :material/home:", on_click=on_click_home, kwargs={"logger":logger}, use_container_width=True)
                logger.log(f"action:load, element:home_button", flag=4, name=method_name)
                st.write("Dashboard")
                st.button("Job Informations :material/search:", on_click=on_click_job_info, kwargs={"logger":logger}, use_container_width=True)
                logger.log(f"action:load, element:job_informations_button", flag=4, name=method_name)
                st.write("Account")
                st.button("User Information :material/person:", on_click=on_click_user_info, kwargs={"logger":logger}, use_container_width=True)
                logger.log(f"action:load, element:user_information_button", flag=4, name=method_name)
                st.write("History")
                st.button("History :material/history:", on_click=on_click_history, kwargs={"logger":logger}, use_container_width=True)
                logger.log(f"action:load, element:history_button", flag=4, name=method_name)
        logger.log(f"action:load, element:sidebar", flag=4, name=method_name)
        
        
        ### display page according to current_view
        flag = 4
        if st.session_state.get('current_view') == "home":
            views.display_home_page(logger)
        elif st.session_state.get('current_view') == "login":
            views.display_login_page(logger, authenticator)
        elif st.session_state.get('current_view') == "job_informations":
            views.display_job_informations(logger, url="http://127.0.0.1:8000")
        elif st.session_state.get('current_view') == "user_information":
            views.display_user_information(logger, authenticator)
        elif st.session_state.get('current_view') == "filter_log":
            views.display_history(logger)
        else:
            views.display_error_page(logger)
            logger.log(f"action:load, element:{st.session_state.get('current_view')}_page",flag=4,name=method_name)
    
    except Exception as e:
        logger.log(f"Exception occurred at flag #{flag}: {e}", flag=1, name=__name__)
        
if __name__=="__main__":
    main()