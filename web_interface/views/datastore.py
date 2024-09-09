import streamlit as st
import pandas as pd
import json, requests, ast, base64
import matplotlib.pyplot as plt
from collections import Counter
from datetime import datetime
from .utils import Logger

def load_config(config_path:str='config.json'):
    with open(config_path, 'r') as f:
        return json.load(f)
    
### clear search history from endpoint
def clear_search_history(endpoint:str, logger)->bool:
    '''
        Clear search history from the endpoint
        - endpoint: API endpoint
        - logger: logger to log the exception
    '''
    method_name = __name__+".clear_search_history"
    is_logged_in = st.session_state.get('connected', False)
    session_id = st.session_state.get('session_id', None)
    user_id = st.session_state.get('user_id', "")
    if is_logged_in:
        if user_id is None or user_id == "":
            logger.log("No user id found", name=method_name, flag=1)
            return False
    if session_id is None:
        logger.log("No session id found", name=method_name, flag=1)
        return False
    payload = {
        "session_id": session_id,
        "user_id": user_id,
        "is_logged_in": is_logged_in
    }
    try:
        clear_response = requests.delete(endpoint, json=payload)
        return clear_response.status_code == 200
    except Exception as e:
        logger.log(f"Exception occurred while clearing search history: {e}", name=method_name, flag=1)
        return False

### retrieve search history from endpoint as dataframe
def get_search_history(endpoint:str, logger)->pd.DataFrame:
    '''
        Get search history from the endpoint
        - endpoint: API endpoint
        - session_id: session id to get search history
        - logger: logger to log the exception
    '''
    method_name = __name__ + ".get_search_history"
    session_id = st.session_state['session_id']
    user_id = st.session_state.get('user_id', "")
    is_logged_in = st.session_state.get('connected', False)
    # @app.get("/history")
    # async def get_search_history(session_id:str, user_id:str, is_logged_in:bool)->list:
    
    history_response = requests.get(endpoint, params={"session_id":session_id, "user_id":user_id, "is_logged_in":is_logged_in})
    # result is serialized dataframe
    # check if result is not empty and status code is 200
    if history_response.status_code == 200 and history_response.text:
        if is_logged_in and user_id is None:
            logger.log("No user id found", name=method_name, flag=1)
            return None
        elif not is_logged_in and session_id is None:
            logger.log("No session id found", name=method_name, flag=1)
            return None
            # deserialize dataframe
        return pd.DataFrame(json.loads(history_response.text))
    else:
        logger.log(f"Exception occurred while retrieving search history: {history_response}", flag=1, name=method_name)
        return None
    
### filter search history
def filter_search_history(search_history:pd.DataFrame, logger:Logger, connected:bool=False)->pd.DataFrame:
    '''
        Filter search history with connected status.
        If connected is False, return search history according to the session id.
        If connected is True, return according to user_id.
        - search_history: search history to filter
        - logger: logger to log the exception
        - connected: connected status
    '''
    method_name = __name__ + ".filter_search_history"
    if connected:
        ## if connected, return search history according to user_id
        return search_history[search_history['user_id'] == st.session_state['user_id']]
    else:
        ## if not connected, return search history according to session_id
        return search_history[search_history['session_id'] == st.session_state['session_id']]
        
### save search history to endpoint and return status as boolean
def save_search_history(endpoint:str, search_history:dict, logger)->requests.Response:
    '''
        Save search history to the endpoint
        - endpoint: API endpoint
        - search_history: search history to save
        - logger: logger to log the exception
    '''
    method_name = __name__ + ".save_search_history"
    try:
        payloads = {
            "session_id": st.session_state['session_id'],
            "search_history": search_history,
            "timestamp": datetime.now().isoformat(),
            "user_id": st.session_state.get('user_id', ""),
            "is_logged_in": st.session_state.get('connected', False),
        }
        save_history_response = requests.post(endpoint, json=payloads)
        return save_history_response
    except Exception as e:
        logger.log(f"Exception occurred while saving search history: {e}", flag=1, name=method_name)
        return False

### retrieve dataframe from endpoint
@st.cache_data
def get_job_informations(_logger, endpoint:str, database:str, query:str)->pd.DataFrame:
    '''
        Send query as post method to the endpoint, and return query results in pandas dataframe format.
        - endpoint: API endpoint
        - database: database name to use
        - query: sql query to execute in database
    '''
    method_name = __name__ + ".get_job_informations"
    try:
        payload = {"database":f"{database}", "query":f"{query}"}
        query_result = json.loads(requests.post(endpoint, data=json.dumps(payload)).text)
        df = pd.DataFrame(query_result)
        return df
    except Exception as e:
        _logger.log(f"Exception occurred while getting dataframe: {e}", flag=1, name=method_name)
        return None