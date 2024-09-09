import streamlit as st
from google_auth_oauthlib.flow import Flow
from streamlit_google_auth import Authenticate
from .utils import Logger

def display_login_page(logger:Logger, authenticator:Authenticate):
    method_name = __name__ + ".display_login_page"
    try:
        st.header("Login with Google Account")
        if st.session_state.get('connected', False):
            st.error(f"authentification failed due to error; please try again :(")
        else:
            if not st.session_state['connected']:
                flow = Flow.from_client_secrets_file(
                    authenticator.secret_credentials_path,
                    scopes=[
                        "openid",
                        "https://www.googleapis.com/auth/userinfo.profile",
                        "https://www.googleapis.com/auth/userinfo.email"
                        ],
                    redirect_uri=authenticator.redirect_uri,
                )
                authorization_url, state = flow.authorization_url(
                        access_type="offline",
                        include_granted_scopes="true",
                )
                login_btn = st.link_button(label="Login with Google :material/login:", url=authorization_url)
                logger.log(f"action:load, element:login_button", flag=4, name=method_name)
    except Exception as e:
        logger.log(f"Exception occurred on login page: {e}", flag=1, name=method_name)
        
    