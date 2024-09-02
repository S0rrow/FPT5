import streamlit as st
import pandas as pd
import json, requests, ast
import matplotlib.pyplot as plt
from collections import Counter

def load_config(config_path:str='config.json'):
    with open(config_path, 'r') as f:
        return json.load(f)

@st.cache_data
def _get_dataframe_(_logger, url:str, database:str, query:str)->pd.DataFrame:
    '''
        Send query as post method to the url, and return query results in pandas dataframe format.
        - url: API endpoint
        - database: database name to use
        - query: sql query to execute in database
    '''
    try:
        _logger.log(f"getting dataframe...", name=__name__)
        query_result = json.loads(requests.post(url, data=json.dumps({"database":f"{database}", "query":f"{query}"})).text)
        _logger.log(f"query result : {query_result}", name=__name__)
        df = pd.DataFrame(query_result)
        return df
    except Exception as e:
        _logger.log(f"Exception occurred while getting dataframe: {e}", flag=1, name=__name__)
        return None

### charts
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

def display_filters(df:pd.DataFrame, current_filter:dict, logger)->pd.DataFrame:
    '''
        Generate filters for each column in the dataframe.
        - df: dataframe to filter
        - current_filter: dictionary to store current filter values
    '''
    if df.empty:
        logger.log(f"Dataframe is empty", flag=1, name=__name__)
        return None, current_filter
    else:
        filtered_df = df.copy()
    try:
        for column in df.columns:
            if column == 'stacks':
                # 'stacks' 열에 대한 특별 처리
                all_stacks = []
                for stack in df['stacks']:
                    stack_list = ast.literal_eval(stack)
                    all_stacks.extend(stack_list)
                unique_stacks = list(set(all_stacks))
                ## current_filter의 column값이 비어있지 않은 경우 current_filter의 값을 default로 설정
                default_filter = current_filter.get(column, None)
                selected_stacks = st.multiselect(f"{column} 선택", unique_stacks, default=default_filter)
                if selected_stacks:
                    filtered_df = filtered_df[filtered_df['stacks'].apply(lambda x: any(stack in ast.literal_eval(x) for stack in selected_stacks))]
            elif df[column].dtype == 'object':
                unique_values = df[column].unique().tolist()
                if None in unique_values:
                    unique_values = ['None' if v is None else v for v in unique_values]
                unique_values = sorted(unique_values)
                ## current_filter의 column값이 비어있지 않은 경우 current_filter의 값을 default로 설정
                default_filter = current_filter.get(column, None)
                selected_values = st.multiselect(f"{column} 선택", unique_values, default=default_filter)
                if selected_values:
                    if 'None' in selected_values:
                        filtered_df = filtered_df[(filtered_df[column].isin([v for v in selected_values if v != 'None'])) | (filtered_df[column].isna())]
                    else:
                        filtered_df = filtered_df[filtered_df[column].isin(selected_values)]
            elif df[column].dtype in ['int64', 'float64']:
                min_value = float(df[column].min())
                max_value = float(df[column].max())
                use_range = st.checkbox(f"{column} 범위 사용")
                if use_range:
                    selected_range = st.slider(f"{column} 범위 선택", min_value, max_value, (min_value, max_value))
                    filtered_df = filtered_df[(filtered_df[column] >= selected_range[0]) & (filtered_df[column] <= selected_range[1])]
            current_filter[column] = selected_values
        return filtered_df, current_filter
    except Exception as e:
        logger.log(f"Exception occurred while displaying filters: {e}", flag=1, name=__name__)
        return None, current_filter


### page display

def display_job_informations(logger, url:str=None, database:str=None, query:str=None):
    '''
        display job informations retreived from given url
    '''
    try:
        config = load_config()
        if not url:
            url = config.get("API_URL")
        if not query:
            query = f"SELECT * from {config.get('TABLE')}"
        if not database:
            database = config.get("DATABASE")
        logger.log(f"url:{url}, query:{query}, database:{database}", name=__name__)
        st.title("Job Information - Tech Stack Visualizations")
        st.header("Job Informations")
        data_load_state = st.text('Loading data...')
        df = _get_dataframe_(logger, url, database, query)
        visualized_df = df.copy()
        data_load_state.text("Data loaded from st.cached_data")
        
        ### show raw dataframe
        if st.checkbox('Show raw data'):
            st.subheader("Raw data")
            st.dataframe(df, use_container_width=True)

        ### 필터 옵션 표시 여부
        show_filters = st.checkbox("필터 옵션 표시", value=False)
        st.write(st.session_state.get('filter_log', None))
        ### 필터링한 결과물을 log로 저장, session_state['filter_log']는 dict의 log
        if not st.session_state.get('filter_log', None):
            st.session_state['filter_log'] = []

        ### 필터링 옵션을 current_filter에 저장
        if st.session_state.get('filter_log', None):
            current_filter = st.session_state['filter_log'][-1]
        else:
            current_filter = {}
        
        if show_filters:
            filtered_df, current_filter = display_filters(df, current_filter, logger)
            filter_btn = st.button("필터 적용")
            reset_filter_btn = st.button("필터 초기화")
            ### column1 = filter_btn, column2 = reset_filter_btn
            col1, col2 = st.columns([2, 1])
            with col1:
                if filter_btn:
                    st.session_state['filter_log'].append(current_filter)
                    visualized_df = filtered_df.copy()
            with col2:
                if reset_filter_btn:
                    current_filter = {}
                    st.session_state['last_filter'] = {}
                    visualized_df = df.copy()

            # 필터링된 데이터프레임 표시. 단, filter_btn이 눌리지 않았을 때는 이전의 df 유지
            st.subheader("필터링된 데이터")
            st.dataframe(visualized_df, use_container_width=True)
        else:
            st.subheader("전체 데이터")
            st.dataframe(df, use_container_width=True)
        
        ### select type of chart to show
        chart_type = st.selectbox("Select chart type", ("Pie Chart", "Donut Chart", "Bar Chart", "Horizontal Bar Chart", "Histogram"))
        
        ### convert stacks to df to visualize counts
        all_stacks = []
        for stack in df['stacks']:
            stack_list = ast.literal_eval(stack)  # string to list
            all_stacks.extend(stack_list)  # combine into single list
        stack_counts = Counter(all_stacks)

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
    except Exception as e:
        logger.log(f"Exception occurred while rendering job informations: {e}", flag=1, name=__name__)