import streamlit as st
from google_auth_oauthlib.flow import Flow
from streamlit_google_auth import Authenticate
from ..utils import Logger

def display_login_page(logger:Logger, authenticator:Authenticate):
    logger.log(f"rendering login button...",name=__name__)
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
                # justify_content = "left"
                # color = "white"
                # style = f"""
                #     background-color: {'#fff' if color == 'white' else '#4285f4'};
                #     color: {'#000' if color == 'white' else '#fff'};
                #     text-decoration: none;
                #     text-align: center;
                #     margin: 4px 2px; 
                #     cursor: pointer; 
                #     padding: 8px 12px; 
                #     border-radius: 4px; 
                #     display: flex; 
                #     align-items: center;
                # """
                # html_content = f"""
                #     <div style="display: flex; justify-content: {justify_content};">
                #         <a href="{authorization_url}" target="_self" style={style}>
                #             Log in with Google
                #         </a>
                #     </div>
                # """
                st.link_button(label="Login with Google :material/login:", url=authorization_url)
                # st.markdown(html_content, unsafe_allow_html=True)
                logger.log(f"state:{state}",name=__name__)
    except Exception as e:
        logger.log(f"Exception occurred on login page: {e}", flag=1, name=__name__)
        
    