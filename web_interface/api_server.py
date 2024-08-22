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

@app.post("/query")
def query(input:QueryCall):
    logger.log(f"query called on db {input.database}: {input.query}", name=__name__)
    try:
        df = query_to_dataframe(input.database, input.query)
        serialized_df = df.astype(object).to_dict(orient='records')
        return serialized_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Exception occurred while querying from databse: {e}")

@app.post("/rds")
def query(input:QueryCall):
    logger.log(f"rds query called on db {input.database}: {input.query}", name=__name__)
    try:
        df = query_to_dataframe(input.database, input.query, is_rds=True)
        serialized_df = df.astype(object).to_dict(orient='records')
        return serialized_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Exception occurred while querying from databse: {e}")

def load_config(config_path:str='config.json'):
    """return configuration informations from config.json"""
    with open(config_path, 'r') as f:
        return json.load(f)

def create_db_engine(database:str, config, is_rds:bool=False):
    """generate db engine through configuration file."""
    user = config.get("USER")
    try:
        if is_rds: password = get_rds_secret(config)
        else: password = config.get("PASSWORD")
        if is_rds: host = config.get("ENDPOINT")
        else: host = config.get("HOST")
        port = config.get("PORT")
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        return create_engine(connection_string)
    except Exception as e:
        logger.log(f"Exception occurred while creating db engine: {e}", flag=1, name=__name__)

def query_to_dataframe(database:str, query:str, config_path:str='config.json', is_rds:bool=False)->pd.DataFrame:
    """execute sql query and return results in dataframe."""
    config = load_config(config_path)
    engine = create_db_engine(database, config, is_rds)
    
    with engine.connect() as connection:
        try:
            logger.log(f"attempting connection through sqlalchemy...", name=__name__)
            df = pd.read_sql(query, connection)
        except Exception as e:
            logger.log(f"Exception occurred while connecting: {e}", flag=1, name=__name__)
    return df

def get_rds_secret(config:dict)->str:
    secret_name = config.get("SECRET_NAME")
    region_name = config.get("REGION")
    
    session = Session(
        aws_access_key_id=config.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=config.get("AWS_SECRET_KEY"),
        region_name=region_name
    )
    
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        logger.log(f"retrieving secret from rds", name=__name__)
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except Exception as e:
        logger.log(f"Exception occurred while retrieving secret from rds: {e}", flag=1, name=__name__)
        raise e
    secret = get_secret_value_response['SecretString']
    logger.log(f"secret:{secret}",flag=3,name=__name__)
    return secret