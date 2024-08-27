import os, json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
from sqlalchemy import create_engine
from utils import Logger
from boto3.session import Session

parent_path = os.path.dirname(os.path.abspath(__file__))
config_path = f"{parent_path}/config.json"
app = FastAPI()
logger = Logger()
class QueryCall(BaseModel):
    database: str
    query : str

@app.post("/test")
def query(input:QueryCall):
    logger.log(f"query called on db [{input.database}] for test: {input.query}", name=__name__)
    try:
        data = {
            'job_title': ['Backend Software Engineer', 'Frontend Developer', 'Data Scientist'],
            'company_name': ['quotabook', 'techcorp', 'datascience inc.'],
            'country': ['South Korea', 'USA', 'UK'],
            'salary': [None, '$120k', '$95k'],
            'remote': [False, True, True],
            'job_category': ['Backend Engineer', 'Frontend Engineer', 'Data Science'],
            'stacks': [
                "['Python', 'Django', 'Docker', 'AWS EKS', 'GitHub Actions', 'Node.js', 'TypeScript', 'ReactJS']",
                "['JavaScript', 'ReactJS', 'Redux', 'CSS', 'HTML', 'Node.js']",
                "['Python', 'Pandas', 'NumPy', 'TensorFlow', 'Keras', 'Docker']"
            ],
            'required_career': [True, False, True],
            'domain': ['Tech', 'Tech', 'Data Science']
        }
        df = pd.DataFrame(data)
        serialized_df = df.astype(object).to_dict(orient='records')
        return serialized_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Exception occurred while querying as test: {e}")

@app.post("/query")
def query(input:QueryCall):
    logger.log(f"query called on db [{input.database}]: {input.query}", name=__name__)
    try:
        df = query_to_dataframe(input.database, input.query)
        serialized_df = df.astype(object).to_dict(orient='records')
        return serialized_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Exception occurred while querying from database: {e}")

def load_config(config_path:str='config.json'):
    """return configuration informations from config.json"""
    with open(config_path, 'r') as f:
        return json.load(f)

def create_db_engine(database:str, config):
    """generate db engine through configuration file."""
    try:
        user = config.get("USER")
        password = config.get("PASSWORD")
        host = config.get("ENDPOINT")
        port = config.get("PORT")
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        return create_engine(connection_string)
    except Exception as e:
        logger.log(f"Exception occurred while creating db engine: {e}", flag=1, name=__name__)
        raise e

def query_to_dataframe(database:str, query:str, config_path:str='config.json')->pd.DataFrame:
    """execute sql query and return results in dataframe."""
    try:
        config = load_config(config_path)
        engine = create_db_engine(database, config)
        
        with engine.connect() as connection:
            try:
                logger.log(f"attempting connection through sqlalchemy...", name=__name__)
                df = pd.read_sql(query, connection)
            except Exception as e:
                logger.log(f"Exception occurred while connecting: {e}", flag=1, name=__name__)
        return df
    except Exception as e:
        logger.log(f"Exception occurred while querying: {e}", flag=1, name=__name__)
        raise e