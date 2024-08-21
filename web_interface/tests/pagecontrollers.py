import streamlit as st
import pandas as pd
import requests, json
from utils import Logger, get_dataframe
from streamlit_google_auth import Authenticate

class PageController():
    logger = None
    authenticator = None
    def __init__(self, authenticator:Authenticate, logger:Logger=Logger()):
        '''
        logger will generate log files inside logs directory under current directory by default.
        `auth` variable is to designate path for google oauth api credential json file.
        '''
        self.logger = logger
        self.authenticator = authenticator
        
    def display_home_page(self):
        '''
        show initial page before login
        '''
        if st.session_state.get(["current_page"], False) =="home":
            st.header("This is home page")
    
    def display_main_page(self):
        '''
        show initial page after login
        '''
        if st.session_state.get(["current_page"], False) =="main":
            st.header("This is main page")
        
    def display_job_informations(self):
        if st.session_state.get(["current_page"], False) == "job_informations":
            url = "http://127.0.0.1:8000/query"
            query = f"SELECT * from job_information"
            df = get_dataframe(url=url, query=query)
            
            st.header("Job Informations")
            st.subheader("query result from API")
            st.dataframe(df, use_container_width=True)

class SidebarController():
    logger = None
    authenticator = None
    pg_controller= None
    def __init__(self, authenticator:Authenticate, pg_controller:PageController,logger:Logger=Logger()):
        '''
        logger will generate log files inside logs directory under current directory by default.
        `auth` variable is to designate path for google oauth api credential json file.
        '''
        self.logger = logger
        self.authenticator = authenticator
        self.pg_controller = pg_controller
        
    def display_sidebar(self):
        flag = -1
        try:
            if st.sidebar.button("Home"):
                flag = 0
                st.session_state["current_page"] = "home"
                self.pg_controller.display_home_page()
            if st.sidebar.button("Main"):
                st.session_state["current_page"] = "main"
                self.pg_controller.display_main_page()
            # Display login button in the sidebar
            with st.sidebar:
                flag = 1
                self.authenticator.login(color="white")
                if st.session_state.get('connected', False):
                    flag = 2
                    st.image(st.session_state['user_info'].get('picture'))
                    st.write('Hello, ' + st.session_state['user_info'].get('name'))
                    st.write(st.session_state['user_info'].get('email'))
                    if st.button('Log out'):
                        flag = 3
                        self.authenticator.logout()
            if st.sidebar.button("job_informations"):
                flag = 4
                st.session_state["current_page"] = "job_informations"
                self.pg_controller.display_job_informations()
        except Exception as e:
            self.logger.log(f"Exception occurred while rendering sidebar at flag #{flag}: {e}", 1)