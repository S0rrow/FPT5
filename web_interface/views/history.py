from numpy import empty
import streamlit as st
import pandas as pd
import json, requests
from datetime import datetime
from .utils import Logger
from .datastore import clear_search_history, load_config, get_search_history

def display_history(logger):
    method_name = __name__+".display_history"
    try:
        config = load_config()
        endpoint = f"{config['API_URL']}/history"
        if not st.session_state.get('session_id', None):
            st.write("No session id found")
            return
        
        search_history = get_search_history(endpoint, logger)
        
        if not search_history.empty:
            ## only show search term, timestamp with index
            history = []
            for index, row in search_history.iterrows():
                history_row = {}
                # history_row['index'] = index
                # row['timestamp'] is string, convert to datetime
                history_row['timestamp'] = datetime.strptime(row['timestamp'], "%Y-%m-%dT%H:%M:%S")
                search_term = json.loads(row['search_term'])
                search_term_str = ""
                ## append as string in form of key:value
                for key, value in search_term.items():
                    # if value is not empty and not null
                    if value:
                        if isinstance(value, list) and len(value) > 0:
                            search_term_str += f"{key}:{', '.join(value)}, "
                        else:
                            search_term_str += f"{key}:{value}, "
                # remove last comma
                search_term_str = search_term_str[:-2]
                history_row['search_term'] = search_term_str
                history.append(history_row)
            result = pd.DataFrame(history)
        else:
            result = pd.DataFrame()
        col1, col2, col3 = st.columns(3, vertical_alignment="bottom")
        with col1:
            st.header("Filter Log")
            logger.log(f"action:load, element:header", flag=4, name=method_name)
        with col2:
            clear_btn = st.button("Clear Filter Log")
            logger.log(f"action:load, element:clear_button", flag=4, name=method_name)
            if clear_btn:
                logger.log(f"action:click, element:clear_button", flag=4, name=method_name)
                if clear_search_history(endpoint, logger):
                    st.success("Filter log cleared successfully")
                else:
                    st.error("Failed to clear filter log")
        with col3:
            download_btn = st.button("Download Filter Log")
            logger.log(f"action:load, element:download_button", flag=4, name=method_name)
            if download_btn:
                logger.log(f"action:click, element:download_button", flag=4, name=method_name)
                st.download_button(
                    label="Download Filter Log",
                    data=result.to_csv(index=False),
                    file_name="filter_log.csv",
                    mime="text/csv"
                )
        if result is None or result.empty:
            st.write("No search history found")
            return
        else:
            st.dataframe(result, use_container_width=True)
            logger.log(f"action:load, element:search_history_dataframe", flag=4, name=method_name)
            
    except Exception as e:
        logger.log(f"Exception occurred while rendering history: {e}", name=method_name, flag=1)
        st.write("Something went wrong while loading your information :(")
