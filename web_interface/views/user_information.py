import streamlit as st
from streamlit_google_auth import Authenticate
from .utils import Logger

def display_user_information(logger:Logger, authenticator:Authenticate):
    method_name = __name__ + ".display_user_information"
    try:
        authenticator.check_authentification()
        if st.session_state.get('connected', False):
            logger.log(f"session state connected = True", name=__name__)
            st.image(st.session_state['user_info'].get('picture'))
            logger.log(f"action:load, element:user_info_profile_picture", flag=4, name=method_name)
            st.write('Hello, ' + st.session_state['user_info'].get('name') + '!')
            logger.log(f"action:load, element:user_info_name", flag=4, name=method_name)
            st.write(st.session_state['user_info'].get('email'))
            logger.log(f"action:load, element:user_info_email", flag=4, name=method_name)
            logout_btn = st.button('Log out')
            logger.log(f"action:load, element:logout_button", flag=4, name=method_name)
            if logout_btn:
                logger.log(f"action:click, element:logout_button", flag=4, name=method_name)
                authenticator.logout()
                st.session_state['user_id'] = None
                st.session_state['current_view'] = "home"
        else:
            st.write("Something went wrong while loading your information :(")
            logger.log(f"action:load, element:error_text_loading_user_info", flag=4, name=method_name)
    except Exception as e:
        logger.log(f"Exception occurred while rendering user information: {e}", flag=1, name=method_name)
        st.write("Something went wrong while loading your information :(")