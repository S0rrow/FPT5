import streamlit as st

def display_user_information(logger, authenticator):
    if st.session_state.get('connected', False):
        st.image(st.session_state['user_info'].get('picture'))
        st.write('Hello, ' + st.session_state['user_info'].get('name') + '!')
        st.write(st.session_state['user_info'].get('email'))
        if st.button('Log out'):
            authenticator.logout()
    else:
        st.write("Something went wrong while loading your information :(")