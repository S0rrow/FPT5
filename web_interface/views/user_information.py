import streamlit as st
from streamlit_google_auth import Authenticate

def display_user_information(logger, authenticator:Authenticate):
    logger.log(f"rendering user informations page...", name=__name__)
    try:
        authenticator.check_authentification()
        if st.session_state.get('connected', False):
            logger.log(f"session state connected = True", name=__name__)
            st.image(st.session_state['user_info'].get('picture'))
            st.write('Hello, ' + st.session_state['user_info'].get('name') + '!')
            st.write(st.session_state['user_info'].get('email'))
            if st.button('Log out'):
                logger.log(f"logging out...", name=__name__)
                authenticator.logout()
                st.session_state['current_view'] = "home"
        else:
            logger.log(f"error occurred", name=__name__)
            st.write("Something went wrong while loading your information :(")
    except Exception as e:
        logger.log(f"Exception occurred while rendering user information: {e}", name=__name__)