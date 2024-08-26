import streamlit as st
import pandas as pd
import json, requests, ast
import matplotlib.pyplot as plt
from collections import Counter
from ..utils import Logger

@st.cache_data
def _get_dataframe_(_logger:Logger, url:str, database:str, query:str)->pd.DataFrame:
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

### page display

def display_job_informations(logger:Logger, url:str=None, database:str=None, query:str=None):
    '''
        display job informations retreived from given url
    '''
    try:
        if not url:
            url = "http://192.168.0.61:8000/test"
        if not query:
            query = f"SELECT * from job_information"
        if not database:
            database = "streamlit"
        logger.log(f"url:{url}, query:{query}, database:{database}", name=__name__)
        st.title("Job Information - Tech Stack Visualizations")
        st.header("Job Informations")
        data_load_state = st.text('Loading data...')
        df = _get_dataframe_(logger, url, database, query)
        data_load_state.text("Data loaded from st.cached_data")
        
        ### show raw dataframe
        if st.checkbox('Show raw data'):
            st.subheader("Raw data")
            st.dataframe(df, use_container_width=True)

        chart_type = st.selectbox("Select chart type", ("Pie Chart", "Donut Chart", "Bar Chart", "Horizontal Bar Chart", "Histogram"))
        
        # stacks to df
        all_stacks = []
        for stack in df['stacks']:
            stack_list = ast.literal_eval(stack)  # string to list
            all_stacks.extend(stack_list)  # combine into single list
        stack_counts = Counter(all_stacks)

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