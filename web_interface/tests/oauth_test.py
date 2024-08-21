import streamlit as st
from streamlit_google_auth import Authenticate

st.title('Streamlit Google Auth Example')

authenticator = Authenticate(
    secret_credentials_path='auth.json',
    cookie_name='connectivity',
    cookie_key='this_is_secret',
    redirect_uri='http://localhost:8501',
)

# Catch the login event
authenticator.check_authentification()

# Create the login button in the sidebar
#with st.sidebar:
authenticator.login(color="white")

if st.session_state.get('connected', False):
    st.image(st.session_state['user_info'].get('picture'))
    st.write('Hello, ' + st.session_state['user_info'].get('name'))
    st.write('Your email is ' + st.session_state['user_info'].get('email'))
    if st.button('Log out'):
        authenticator.logout()
