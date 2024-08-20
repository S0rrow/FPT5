import json
from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
from sqlalchemy import create_engine

app = FastAPI()

class QueryCall(BaseModel):
    query : str

@app.post("/query")
def query(input:QueryCall):
    return query_to_dataframe(input.query)

def load_config(config_path:str='config.json'):
    """return configuration informations from config.json"""
    with open(config_path, 'r') as f:
        return json.load(f)

def create_db_engine(config):
    """generate db engine through configuration file."""
    user = config.get("USER")
    password = config.get("PASSWORD")
    host = config.get("ENDPOINT")
    port = config.get("PORT")
    database = config.get("DBNAME")

    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    return create_engine(connection_string)

def query_to_dataframe(query:str, config_path:str='config.json')->pd.DataFrame:
    """execute sql query and return results in dataframe."""
    config = load_config(config_path)
    engine = create_db_engine(config)
    
    with engine.connect() as connection:
        df = pd.read_sql(query, connection)
    return df