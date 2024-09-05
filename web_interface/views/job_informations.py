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

### retrieve search history from endpoint as dataframe
def get_search_history(endpoint:str, logger)->pd.DataFrame:
    '''
        Get search history from the endpoint
        - endpoint: API endpoint
        - session_id: session id to get search history
        - logger: logger to log the exception
    '''
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
            logger.log("No user id found", name=__name__, flag=1)
            return None
        elif not is_logged_in and session_id is None:
            logger.log("No session id found", name=__name__, flag=1)
            return None
        else:
            if is_logged_in:
                logger.log(f"Search history retrieved successfully from user_id: {user_id}", name=__name__)
            else:
                logger.log(f"Search history retrieved successfully from session_id: {session_id}", name=__name__)
            # deserialize dataframe
        return pd.DataFrame(json.loads(history_response.text))
    else:
        logger.log(f"Exception occurred while retrieving search history: {history_response}", flag=1, name=__name__)
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
    try:
        logger.log(f"Search history to save: {search_history}", name=__name__)
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
        logger.log(f"Exception occurred while saving search history: {e}", flag=1, name=__name__)
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
    try:
        _logger.log(f"getting dataframe...", name=__name__)
        payload = {"database":f"{database}", "query":f"{query}"}
        query_result = json.loads(requests.post(endpoint, data=json.dumps(payload)).text)
        _logger.log(f"query result : {query_result}", name=__name__)
        df = pd.DataFrame(query_result)
        return df
    except Exception as e:
        _logger.log(f"Exception occurred while getting dataframe: {e}", flag=1, name=__name__)
        return None

### render charts
def plot_pie_chart(stack_counts):
    fig, ax = plt.subplots()
    ax.pie(stack_counts.values(), labels=stack_counts.keys(), autopct='%1.1f%%', startangle=90)
    ax.axis('equal')
    st.subheader("Tech Stacks as Pie Chart")
    st.pyplot(fig)
    
def plot_donut_chart(stack_counts):
    fig, ax = plt.subplots()
    ax.pie(stack_counts.values(), labels=stack_counts.keys(), autopct='%1.1f%%', startangle=90, wedgeprops=dict(width=0.4))
    ax.axis('equal')
    st.pyplot(fig)

def plot_histogram(stack_counts):
    fig, ax = plt.subplots()
    ax.hist(list(stack_counts.values()), bins=10)
    plt.xlabel("Stack Count")
    plt.ylabel("Frequency")
    st.pyplot(fig)
    
def plot_bar_chart(stack_counts):
    fig, ax = plt.subplots()
    ax.bar(stack_counts.keys(), stack_counts.values())
    plt.xticks(rotation=45, ha='right')
    st.subheader("Tech Stacks as Bar Chart")
    st.pyplot(fig)
    
def plot_horizontal_bar_chart(stack_counts):
    fig, ax = plt.subplots()
    ax.barh(list(stack_counts.keys()), list(stack_counts.values()))
    plt.xticks(rotation=45, ha='right')
    st.subheader("Tech Stacks as Horizontal Bar Chart")
    st.pyplot(fig)
    
def display_filters(df:pd.DataFrame, search_history:pd.DataFrame, logger)->pd.DataFrame:
    '''
    Generate filters for each column in the dataframe.
    - df: dataframe to filter
    - search_history: dataframe to store search history
    '''
    method_name = __name__ + ".display_filters"
    logger.log(f"search_history: {search_history.to_dict()}", name=method_name)
    # Get the latest search term for the current session
    if search_history is not None and not search_history.empty:
        if st.session_state.get('connected', False):
            logger.log(f"current user id: {st.session_state['user_id']}", name=method_name)
        else:
            logger.log(f"current session id: {st.session_state['session_id']}", name=method_name)
        # get latest search term
        if len(search_history) == 1:
            latest_search_term = json.loads(search_history.iloc[0]['search_term'])
        else:
            latest_search_term = json.loads(search_history.iloc[-1]['search_term'])
    else:
        latest_search_term = {}
    
    logger.log(f"latest search term: {latest_search_term}", name=method_name)
    
    if df.empty or df is None:
        logger.log(f"Dataframe is empty", flag=1, name=method_name)
        return None, latest_search_term
    else:
        filtered_df = df.copy()

    try:
        for column in df.columns:
            ### if column is 'stacks', show unique stacks in multiselect
            if column == 'stacks':
                all_stacks = []
                for stack in df['stacks']:
                    stack_list = ast.literal_eval(stack)
                    all_stacks.extend(stack_list)
                unique_stacks = list(set(all_stacks))
                if st.session_state.get('apply_last_filter', None):
                    default_filter = latest_search_term.get(column, [])
                    logger.log(f"default_filter: {default_filter}", name=__name__)
                else:
                    default_filter = []
                selected_stacks = st.multiselect(f"Select {column}", unique_stacks, default=default_filter)
                if selected_stacks:
                    filtered_df = filtered_df[filtered_df['stacks'].apply(lambda x: any(stack in ast.literal_eval(x) for stack in selected_stacks))]
                latest_search_term[column] = selected_stacks
            ### if column is not 'stacks', show unique values in multiselect
            elif df[column].dtype == 'object':
                unique_values = df[column].unique().tolist()
                if None in unique_values:
                    unique_values = ['None' if v is None else v for v in unique_values]
                unique_values = sorted(unique_values)
                if st.session_state['apply_last_filter']:
                    default_filter = latest_search_term.get(column, [])
                    logger.log(f"default_filter: {default_filter}", name=__name__)
                else:
                    default_filter = []
                selected_values = st.multiselect(f"Select {column}", unique_values, default=default_filter)
                if selected_values:
                    if 'None' in selected_values:
                        filtered_df = filtered_df[(filtered_df[column].isin([v for v in selected_values if v != 'None'])) | (filtered_df[column].isna())]
                    else:
                        filtered_df = filtered_df[filtered_df[column].isin(selected_values)]
                latest_search_term[column] = selected_values
                
            ### if column is 'salary', show salary range in slider
            elif df[column].dtype in ['int64', 'float64']:
                min_value = float(df[column].min())
                max_value = float(df[column].max())
                use_range = st.checkbox(f"Use {column} range")
                if use_range:
                    if st.session_state.get('apply_last_filter', None):
                        default_range = latest_search_term.get(column, (min_value, max_value))
                        logger.log(f"default_range: {default_range}", name=__name__)
                    else:
                        default_range = (min_value, max_value)
                    selected_range = st.slider(f"Select {column} range", min_value, max_value, default_range)
                    filtered_df = filtered_df[(filtered_df[column] >= selected_range[0]) & (filtered_df[column] <= selected_range[1])]
                    latest_search_term[column] = selected_range
                    
            ### if column is 'start_date' or 'end_date', show date range in date picker
            elif column in ['start_date', 'end_date']:
                if st.session_state['apply_last_filter']:
                    default_range = latest_search_term.get(column, (min_value, max_value))
                    logger.log(f"default_range: {default_range}", name=__name__)
                else:
                    default_range = (min_value, max_value)
                selected_range = st.date_input(f"Select {column} range", value=default_range)
                filtered_df = filtered_df[(filtered_df[column] >= selected_range[0]) & (filtered_df[column] <= selected_range[1])]
                latest_search_term[column] = selected_range
    
        logger.log(f"Filters applied successfully", name=method_name)
        return filtered_df, latest_search_term
    except Exception as e:
        logger.log(f"Exception occurred while displaying filters: {e}", flag=1, name=method_name)
        return None, latest_search_term


### page display

def display_job_informations(logger, url:str=None, database:str=None, query:str=None):
    '''
        display job informations retreived from given url
    '''
    ### seperator for debug
    seperator = -1
    try:
        config = load_config()
        if not url:
            url = config.get("API_URL")
        if not query:
            query = f"SELECT * from {config.get('TABLE')}"
        if not database:
            database = config.get("DATABASE")
        if 'job_info_filtered' not in st.session_state:
            st.session_state['job_info_filtered'] = False
        seperator = 0
        
        logger.log(f"url:{url}, query:{query}, database:{database}", name=__name__)
        st.title("Job Information - Tech Stack Visualizations")
        st.header("Job Informations")
        data_load_state = st.text('Loading data...')
        seperator = 1
        
        ### test endpoint로부터 데이터프레임 받아오기
        endpoint_test = f"{url}/test"
        df = get_job_informations(logger, endpoint_test, database, query)
        seperator = 2

        ### 검색 기록 받아오기
        endpoint_history = f"{url}/history"
        search_history = get_search_history(endpoint_history, logger)
        if search_history is None or search_history.empty:
            logger.log(f"No search history found", name=__name__)
            search_history = pd.DataFrame()
        seperator = 3

        ### 데이터가 없을 경우 예외 처리
        if df is None or df.empty:
            st.write("No data found")
            return
        else:
            st.session_state['apply_last_filter'] = True
        visualized_df = df.copy()
        data_load_state.text("Data loaded from st.cached_data")
        seperator = 4
        
        ### show raw dataframe
        if st.checkbox('Show raw data'):
            st.subheader("Raw data")
            st.dataframe(df, use_container_width=True)
        seperator = 5
        
        ### 필터 옵션 표시 여부
        show_filters = st.checkbox("필터 옵션 표시", value=False)
        seperator = 6
        
        if show_filters:
            st.session_state['job_info_filtered'] = True
            filtered_df, current_filter = display_filters(df, search_history, logger)
            filter_btn = st.button("필터 적용")
            reset_filter_btn = st.button("필터 초기화")
            seperator = 7
            col1, col2 = st.columns([2, 1])
            with col1:
                if filter_btn:
                    # 필터 로그 저장
                    ## class SearchHistory(BaseModel):
                    # session_id: str
                    # search_history: dict
                    # timestamp: datetime
                    # user_id: str
                    # is_logged_in: bool
                    save_history_response = save_search_history(endpoint_history, current_filter, logger)
                    if save_history_response.status_code == 200 and save_history_response.json().get("status") == "success":
                        st.success("필터가 저장되었습니다.")
                        st.session_state['apply_last_filter'] = True
                    else:
                        st.error("필터 저장에 실패했습니다.")
                        st.session_state['apply_last_filter'] = False
                    visualized_df = filtered_df.copy()
                    seperator = 8
            with col2:
                if reset_filter_btn:
                    st.session_state['apply_last_filter'] = False
                    visualized_df = df.copy()
                    st.success("필터가 초기화되었습니다.")
                    st.rerun()
            seperator = 9

            # 필터링된 데이터프레임 표시. 단, filter_btn이 눌리지 않았을 때는 이전의 df 유지
            st.subheader("필터링된 데이터")
            st.dataframe(visualized_df, use_container_width=True)
            seperator = 10
        else:
            st.session_state['job_info_filtered'] = False
            st.subheader("전체 데이터")
            st.dataframe(df, use_container_width=True)
            seperator = 11
        ### select type of chart to show
        chart_type = st.selectbox("Select chart type", ("Pie Chart", "Donut Chart", "Bar Chart", "Horizontal Bar Chart", "Histogram"))
        seperator = 12
        ### convert stacks to df to visualize counts
        all_stacks = []
        if st.session_state['job_info_filtered']:
            for stack in visualized_df['stacks']:
                stack_list = ast.literal_eval(stack)  # string to list
                all_stacks.extend(stack_list)  # combine into single list
        else:
            for stack in df['stacks']:
                stack_list = ast.literal_eval(stack)  # string to list
                all_stacks.extend(stack_list)  # combine into single list
        stack_counts = Counter(all_stacks)
        seperator = 13
        ### col1 = selected chart, col2 = df of tech stacks with ['stack name', 'count of stacks'] as columns
        col1, col2 = st.columns([2, 1])
        with col1:
            if chart_type == "Pie Chart":
                plot_pie_chart(stack_counts)
            elif chart_type == "Donut Chart":
                plot_donut_chart(stack_counts)
            elif chart_type == "Bar Chart":
                plot_bar_chart(stack_counts)
            elif chart_type == "Horizontal Bar Chart":
                plot_horizontal_bar_chart(stack_counts)
            elif chart_type == "Histogram":
                plot_histogram(stack_counts)
        with col2:
            st.subheader("Tech Stack List")
            stack_df = pd.DataFrame(stack_counts.items(), columns=['Stack', 'Count'])
            st.dataframe(stack_df)
        seperator = 14
    except Exception as e:
        logger.log(f"Exception occurred while rendering job informations at #{seperator}: {e}", flag=1, name=__name__)