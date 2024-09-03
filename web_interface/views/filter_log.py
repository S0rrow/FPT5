from numpy import empty
import streamlit as st
import pandas as pd
import json, requests
from datetime import datetime

def load_config(config_path:str='config.json'):
    with open(config_path, 'r') as f:
        return json.load(f)

def clear_search_history(endpoint:str, session_id:str, logger)->bool:
    '''
        Clear search history from the endpoint
        - endpoint: API endpoint
        - session_id: session id to clear search history
        - logger: logger to log the exception
    '''
    payload = {"session_id": session_id}
    clear_response = requests.delete(endpoint, json=payload)
    return clear_response.status_code == 200


def get_search_history(endpoint:str, session_id:str, logger)->pd.DataFrame:
    '''
        Get search history from the endpoint
        - endpoint: API endpoint
        - session_id: session id to get search history
        - logger: logger to log the exception
    '''
    history_response = requests.get(endpoint, params={"session_id": session_id})
    # result is serialized dataframe
    # check if result is not empty and status code is 200
    if history_response.status_code == 200 and history_response.text:
        logger.log(f"Search history retrieved successfully from session_id: {session_id}", name=__name__)
        # deserialize dataframe
        return pd.DataFrame(json.loads(history_response.text))
    else:
        logger.log(f"Exception occurred while retrieving search history: {history_response}", flag=1, name=__name__)
        return None

def display_filter_log(logger):
    logger.log(f"rendering filter_log page...", name=__name__)
    try:
        col1, col2 = st.columns(2)
        with col1:
            st.header("Filter Log")
        with col2:
            clear_btn = st.button("Clear Filter Log")
            
        config = load_config()
        endpoint = f"{config['API_URL']}/history"
        session_id = st.session_state.get('session_id', None)
        
        if session_id is None:
            st.write("No session id found")
            return
        
        search_history = get_search_history(endpoint, session_id, logger)
        
        if search_history.empty:
            st.write("No search history found")
            return
        
        if clear_btn:
            if clear_search_history(endpoint, session_id, logger):
                st.success("Filter log cleared successfully")
                st.rerun()
            else:
                st.error("Failed to clear filter log")
        
        ### search history is dataframe with columns: session_id, search_term, timestamp
        if not search_history.empty:
            ## only show search term, timestamp with index
            history = []
            for index, row in search_history.iterrows():
                history_row = {}
                history_row['index'] = index
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
            ## show history as table with coulmn: index, history[index]
            # column 1: index, column 2: "Time", column 3: "Search History"
            st.dataframe(history, use_container_width=True)
        else:
            st.write("No filter logs found")

    except Exception as e:
        logger.log(f"Exception occurred while rendering filter_log: {e}", name=__name__, flag=1)
        st.write("Something went wrong while loading your information :(")
