import os, json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from sqlalchemy import create_engine, text
from views.utils import Logger
from datetime import datetime

parent_path = os.path.dirname(os.path.abspath(__file__))
config_path = f"{parent_path}/config.json"
app = FastAPI()
logger = Logger()
class QueryCall(BaseModel):
    database: str
    query : str
    
class SessionCall(BaseModel):
    session_id: str
    user_id: str
    is_logged_in: bool

class SearchHistory(BaseModel):
    session_id: str
    search_history: dict
    timestamp: datetime
    user_id: str
    is_logged_in: bool

@app.delete("/history")
def clear_search_history(input:SessionCall):
    method_name = __name__ + ".clear_search_history"
    # connect to db, and clear search history of session_id
    logger.log(f"clearing search history of session_id: {input.session_id}", name=__name__)
    session_id = input.session_id
    user_id = input.user_id
    is_logged_in = input.is_logged_in
    try:
        if is_logged_in:
            query = f"DELETE FROM search_history WHERE user_id = '{user_id}'"
        else:
            query = f"DELETE FROM search_history WHERE session_id = '{session_id}'"
        if execute_query(database="streamlit", query=query):
            return {"status": "success", "message": "Search history cleared successfully"}
        else:
            return {"status": "error", "message": "Failed to clear search history"}
    except Exception as e:
        logger.log(f"Exception occurred while clearing search history: {e}", flag=1, name=method_name)
        return {"status": "error", "message": f"Exception occurred while clearing search history: {e}"}


@app.post("/history")
def save_search_history(input: SearchHistory):
    method_name = __name__ + ".save_search_history"
    try:
        # Convert the search_history dict to a JSON string
        search_history_json = json.dumps(input.search_history)
        
        ### DB ERD
        # search_history (session_id, search_term, timestamp, user_id, is_logged_in)
        query = """
        INSERT INTO search_history (session_id, search_term, timestamp, is_logged_in, user_id) 
        VALUES (:session_id, :search_term, :timestamp, :is_logged_in, :user_id)
        """
        params = {
            "session_id": input.session_id,
            "search_term": search_history_json,
            "timestamp": input.timestamp,
            "user_id": input.user_id,
            "is_logged_in": input.is_logged_in
        }
        
        if execute_query(database="streamlit", query=query, params=params):
            return {"status": "success", "message": "Search history saved successfully"}
        else:
            return {"status": "error", "message": "Failed to save search history"}
    except Exception as e:
        logger.log(f"Exception occurred while saving search history: {e}", flag=1, name=method_name)
        return {"status": "error", "message": f"Exception occurred while saving search history: {str(e)}"}

@app.get("/history")
async def get_search_history(session_id:str, user_id:str, is_logged_in:bool)->list:
    method_name = __name__ + ".get_search_history"
    try:
        # Check if session_id exists in db
        if is_logged_in:
            validate_query = f"SELECT COUNT(*) as count FROM search_history WHERE user_id = '{user_id}'"
        else:
            validate_query = f"SELECT COUNT(*) as count FROM search_history WHERE session_id = '{session_id}'"
        result = query_to_dataframe(database="streamlit", query=validate_query)
        
        if result.empty or result.iloc[0]['count'] == 0:
            logger.log(f"No records found for session_id: {session_id}", name=__name__)
            return []
        
        # Get search history
        if is_logged_in:
            get_query = f"SELECT * FROM search_history WHERE user_id = '{user_id}'"
        else:
            get_query = f"SELECT * FROM search_history WHERE session_id = '{session_id}'"
        df = query_to_dataframe(database="streamlit", query=get_query)
        
        if df.empty:
            if is_logged_in:
                logger.log(f"No search history found for user_id: {user_id}", name=__name__)
            else:
                logger.log(f"No search history found for session_id: {session_id}", name=__name__)
            return []
        else:
            serialized_df = df.astype(object).to_dict(orient='records')
            return serialized_df
    except Exception as e:
        logger.log(f"Exception occurred while retrieving search history: {e}", flag=1, name=__name__)
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

def get_test_dataframe()->pd.DataFrame:
    data = {
        'job_title': ['Backend Software Engineer', 'Frontend Developer', 'Data Scientist', 'Full Stack Developer', 'DevOps Engineer', 'Mobile App Developer', 'UI/UX Designer', 'System Administrator', 'Cloud Architect', 'Security Specialist', 'Machine Learning Engineer', 'QA Engineer'],
        'company_name': ['Quotabook', 'TechCorp', 'DataScience Inc.', 'Naver', 'Kakao', 'Line', 'Coupang', 'Baemin', 'Toss', 'Karrot', 'Wadiz', 'Zigbang'],
        'country': ['South Korea', 'USA', 'UK', 'South Korea', 'South Korea', 'Japan', 'South Korea', 'South Korea', 'South Korea', 'South Korea', 'South Korea', 'South Korea'],
        'salary': [None, '$120,000', '$95,000', '$110,000', '$130,000', '10,000,000 JPY', '$90,000', '$100,000', '$150,000', '$85,000', '$140,000', '$95,000'],
        'remote': [False, True, True, False, True, False, True, False, True, True, False, True],
        'job_category': ['Backend Engineer', 'Frontend Engineer', 'Data Science', 'Full Stack Development', 'DevOps', 'Mobile Development', 'Design', 'System Administration', 'Cloud Computing', 'Information Security', 'Artificial Intelligence', 'Quality Assurance'],
        'stacks': [
            "['Python', 'Django', 'Docker', 'AWS EKS', 'GitHub Actions', 'Node.js', 'TypeScript', 'ReactJS']",
            "['JavaScript', 'ReactJS', 'Redux', 'CSS', 'HTML', 'Node.js']",
            "['Python', 'Pandas', 'NumPy', 'TensorFlow', 'Keras', 'Docker']",
            "['JavaScript', 'Python', 'React', 'Django', 'PostgreSQL', 'Redis']",
            "['Kubernetes', 'Docker', 'Jenkins', 'Terraform', 'AWS', 'Prometheus']",
            "['Swift', 'Kotlin', 'React Native', 'Firebase', 'GraphQL']",
            "['Figma', 'Sketch', 'Adobe XD', 'InVision', 'Zeplin']",
            "['Linux', 'Bash', 'Ansible', 'Nagios', 'VMware']",
            "['AWS', 'Azure', 'GCP', 'Terraform', 'Kubernetes', 'Docker']",
            "['Wireshark', 'Metasploit', 'Nmap', 'Burp Suite', 'Python']",
            "['Python', 'TensorFlow', 'PyTorch', 'Scikit-learn', 'Keras']",
            "['Selenium', 'JUnit', 'TestNG', 'Postman', 'Jenkins']"
        ],
        'required_career': [True, False, True, True, True, False, True, True, True, True, True, False],
        'start_date': ['2023-07-01', '2023-07-02', '2023-07-03', '2023-07-04', '2023-07-05', '2023-07-06', '2023-07-07', '2023-07-08', '2023-07-09', '2023-07-10', '2023-07-11', '2023-07-12'],
        'end_date': ['2023-08-01', '2023-08-02', '2023-08-03', '2023-08-04', '2023-08-05', '2023-08-06', '2023-08-07', '2023-08-08', '2023-08-09', '2023-08-10', '2023-08-11', '2023-08-12'],
        'domain': ['Tech', 'Tech', 'Data Science', 'Tech', 'Tech', 'Mobile', 'Design', 'Infrastructure', 'Cloud', 'Security', 'AI', 'QA'],
        'URL': ['http://example.com/job1', 'http://example.com/job2', 'http://example.com/job3', 'http://example.com/job4', 'http://example.com/job5', 'http://example.com/job6', 'http://example.com/job7', 'http://example.com/job8', 'http://example.com/job9', 'http://example.com/job10', 'http://example.com/job11', 'http://example.com/job12']
    }
    return data

@app.post("/test")
def query_test(input:QueryCall):
    try:
        data = get_test_dataframe()
        df = pd.DataFrame(data)
        serialized_df = df.astype(object).to_dict(orient='records')
        return serialized_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Exception occurred while querying as test: {e}")

@app.post("/query")
def query(input:QueryCall):
    try:
        df = query_to_dataframe(input.database, input.query)
        serialized_df = df.astype(object).to_dict(orient='records')
        return serialized_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Exception occurred while querying from database: {e}")

def load_config(config_path:str='config.json')->dict:
    """return configuration informations from config.json"""
    with open(config_path, 'r') as f:
        return json.load(f)

def create_db_engine(database:str, config):
    """generate db engine through configuration file."""
    method_name = __name__ + ".create_db_engine"
    try:
        user = config.get("USER")
        password = config.get("PASSWORD")
        host = config.get("ENDPOINT")
        port = config.get("PORT")
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        return create_engine(connection_string)
    except Exception as e:
        logger.log(f"Exception occurred while creating db engine: {e}", flag=1, name=method_name)
        raise e
    
def execute_query(database:str, query:str, params:dict=None, config_path:str='config.json')->bool:
    """
    Execute SQL query and return True if successful, False otherwise.
    - database: database name to connect
    - query: SQL query to execute
    - params: parameters for the query (optional)
    - config_path: path to config.json
    """
    method_name = __name__ + ".execute_query"
    try:
        config = load_config(config_path)
        engine = create_db_engine(database, config)
        
        with engine.connect() as connection:
            try:
                if params:
                    connection.execute(text(query), params)
                else:
                    connection.execute(text(query))
                connection.commit()

            except Exception as e:
                logger.log(f"Exception occurred while executing query: {e}", flag=1, name=method_name)
                return False
        return True
    except Exception as e:
        logger.log(f"Exception occurred while executing query: {e}", flag=1, name=method_name)
        return Exception(e)
    

def query_to_dataframe(database:str, query:str, config_path:str='config.json')->pd.DataFrame:
    """
        execute sql query and return results in dataframe.
        - database: database name to connect
        - query: sql query to execute
        - config_path: path to config.json
    """
    method_name = __name__ + ".query_to_dataframe"
    try:
        config = load_config(config_path)
        engine = create_db_engine(database, config)
        with engine.connect() as connection:
            try:
                df = pd.read_sql(query, connection)
            except Exception as e:
                logger.log(f"Exception occurred while connecting: {e}", flag=1, name=method_name)
        return df
    except Exception as e:
        logger.log(f"Exception occurred while querying: {e}", flag=1, name=method_name)
        raise e