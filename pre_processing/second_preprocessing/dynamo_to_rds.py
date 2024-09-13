import re, datetime, pytz, subprocess, os, json
from botocore.exceptions import ClientError
from time import gmtime, strftime
import boto3
import redis
import mysql.connector
import pandas as pd

import logging
# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)
# from logging_utils import logging_to_cloudwatch as ltc
import logging_to_cloudwatch as ltc
    
logger = ltc.log('/aws/preprocessing/second','from_dynamodb_to_rds')



# 접속정보 json파일 로드
with open("./.KEYS/DATA_SRC_INFO.json", "r") as f:
# with open("./DATA_SRC_INFO.json", "r") as f:
    access_info = json.load(f)
# key 정보 json파일 로드
with open("./.KEYS/API_KEYS.json", "r") as f:
# with open("./API_KEYS.json", "r") as f:
    key = json.load(f)
# rds 접속 정보 json파일 로드
with open("./.KEYS/RDS.json", "r") as f:
# with open("./DATA_SRC_INFO.json", "r") as f:
    rds_info = json.load(f)
    
    
    
def get_ids_from_redis():
    """
    Retrieve all ID values stored in Redis.

    This function connects to a Redis database using the connection information
    provided in the `access_info` dictionary. It fetches all keys from the 
    Redis hash named 'compare_id' and decodes them from bytes to UTF-8 strings.

    Returns:
        list: A list of ID values (as strings) retrieved from Redis.
    """
    
    redis_ip = access_info['redis_conn_info']['ip']
    redis_port = access_info['redis_conn_info']['port']
    try:
        redis_client = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
        # redis에 적재되어있는 id 값 가져오기
        return [x.decode('utf-8') for x in redis_client.hkeys('compare_id')]
    except redis.exceptions.RedisError as e:
        logger.error(f"Redis connection error: {e}")
        return []


def get_data_from_dynamodb(ids):
    """
    Retrieve all data from the DynamoDB table 'precessed-data-table' that matches the provided IDs.

    This function connects to a DynamoDB database using the AWS credentials
    provided in the `key` dictionary. It queries the 'precessed-data-table' table
    to fetch all items that match the IDs provided in the `ids` list.

    The function handles pagination to ensure that all items are retrieved,
    even if they span multiple pages.

    Args:
        ids (list): A list of IDs to query in the DynamoDB table.

    Returns:
        list: A list of data items (as dictionaries) retrieved from the DynamoDB table that match the provided IDs.

    Raises:
        ClientError: If there is an error while accessing DynamoDB.
        Exception: For any unexpected errors that may occur during execution.
    """
    
    dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=key['aws_access_key_id'],
            aws_secret_access_key=key['aws_secret_key'],
            region_name=key['region']
        )    
    table = dynamodb.Table('precessed-data-table')

    # 빈 리스트에 추가적으로 받아오기
    data = []
    for id in ids:
        try:
            response = table.query(KeyConditionExpression=Key('pid').eq(id))
            data.append(response['Items'])  # 데이터 값을 리스트에 추가
        except ClientError as e:
            logger.error(f"[get_data_from_dynamodb()]ClientError: {e.response['Error']['Message']}")
#             print(f"ClientError: {e.response['Error']['Message']}")
            continue  # 다음 ID로 이동
        except Exception as e:
            logger.error(f"[get_data_from_dynamodb()]Unexpected error: (id: {id}){str(e)}")
#             print(f"Unexpected error: (id: {id}){str(e)}")
            continue  # 다음 ID로 이동

    df = pd.DataFrame(data)
    return df


def get_pid_from_rds():
    """
    Retrieve all PID values from the RDS database.

    This function connects to the RDS database using the connection information
    provided in the `rds_info` dictionary. It executes a SQL query to fetch all
    PID values from the 'Job_Info' and returns them as a list.

    Returns:
        list: A list of PID values (as strings) retrieved from the RDS database.

    Raises:
        mysql.connector.Error: If there is an error while connecting to the RDS
        or executing the SQL query.
    """
    # RDS 접속 정보
    rds_host = rds_info['host']
    username = rds_info['username']
    password = rds_info['password']
    database = rds_info['database']

    # RDS에 연결
    connection = mysql.connector.connect(
        host=rds_host,
        user=username,
        password=password,
        database=database
    )

    try:
        cursor = connection.cursor()
        # pid를 추출하는 쿼리
        sql = "SELECT pid FROM Job_Info"
        cursor.execute(sql)
        pids = cursor.fetchall()  # 모든 pid를 가져옴
        return [pid[0] for pid in pids]  # pid 리스트 반환

    except mysql.connector.Error as e:
        logger.error(f"[get_pid_from_rds()]Database error: {str(e)}")  # 에러 로그 출력
        return []  # 빈 리스트 반환

    finally:
        cursor.close()  # 커서 종료
        connection.close()  # 연결 종료


def compare_id(redis_list, rds_list):
    """
    Generate a list of IDs that are present in the DynamoDB list but not in the Redis list.

    This function takes two lists: one containing IDs from Redis and another containing IDs from DynamoDB.
    It removes duplicates from the DynamoDB list and returns a list of IDs that are not found in the Redis list.

    Args:
        r_list (list): A list of IDs retrieved from Redis.
        d_list (list): A list of IDs retrieved from DynamoDB.

    Returns:
        list: A list of unique IDs from the DynamoDB list that are not present in the Redis list.
    """
    rds_id_df = pd.DataFrame({"id":rds_list})
    rds_id_df.drop_duplicates(ignore_index=True, inplace=True)
    mask = rds_id_df.isin(redis_list)
    return list(rds_id_df[~mask].dropna().reset_index(drop=True).id)


def load_to_rds(df: pd.DataFrame):
    
    # RDS 접속 정보
    rds_host = rds_info['host']
    username = rds_info['username']
    password = rds_info['password']
    database = rds_info['database']

    # RDS에 연결
    connection = mysql.connector.connect(
        host=rds_host,
        user=username,
        password=password,
        database=database
    )
    cursor = connection.cursor()
    
    sql_query = """
        INSERT INTO Job_Info (
            site_symbol, industry_types, job_title, job_categories, job_prefers, crawl_url, end_date, pid,
            post_status, required_career, crawl_domain, company_name, resume_required, start_date, get_date, dev_stacks, cid, job_requirements
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            site_symbol = VALUES(site_symbol),
            industry_types = VALUES(industry_types),
            job_title = VALUES(job_title),
            job_categories = VALUES(job_categories),
            job_prefers = VALUES(job_prefers),
            crawl_url = VALUES(crawl_url),
            end_date = VALUES(end_date),
            post_status = VALUES(post_status),
            required_career = VALUES(required_career),
            crawl_domain = VALUES(crawl_domain),
            company_name = VALUES(company_name),
            resume_required = VALUES(resume_required),
            start_date = VALUES(start_date),
            get_date = VALUES(get_date),
            dev_stacks = VALUES(dev_stacks),
            cid = VALUES(cid),
            job_requirements = VALUES(job_requirements)
    """

    for i, values in df.iterrows():
        data = (
            values['site_symbol'],
            f"[{', '.join(f"'{item}'" for item in values['indurstry_type'])}]" if isinstance(values['indurstry_type'], list) else values['indurstry_type'],
            values['job_title'],
            f"[{', '.join(f"'{item}'" for item in values['job_category'])}]" if isinstance(values['job_category'], list) else values['job_category'],
            f"[{', '.join(f"'{item}'" for item in values['job_prefer'])}]"  if isinstance(values['job_prefer'], list) else values['job_prefer'],
            values['crawl_url'],
            values['end_date'],
            values['pid'],
            values['post_status'],
            values['required_career'],
            values['crawl_domain'],
            values['company_name'],
            values['resume_required'],
            values['start_date'],
            values['get_date'],
            f"[{', '.join(f"'{item}'" for item in values['dev_stack'])}]" if isinstance(values['dev_stack'], list) else values['dev_stack'],  
            values['cid'],
            f"[{', '.join(f"'{item}'" for item in values['job_requirements'])}]" if isinstance(values['job_requirements'], list) else values['job_requirements'] 
        )
        try:
            cursor.execute(sql_query, data)
            connection.commit()
            logger.info(f"Data successfully load to rds")
            
        except mysql.connector.Error as error:
            logger.error(f"Error executing SQL query for PID {values['pid']}: {error}")  # 에러 로깅
        except Exception as e:
            logger.error(f"Unexpected error for PID {values['pid']}: {str(e)}")  # 예기치 않은 에러 로깅
    cursor.close()  # 커서 종료
    connection.close()  # 연결 종료



def main():
    
    # cache에 저장된 id 값 불러오기
    logger.info(f"start getting ids from redis")
    id_list_from_redis = get_ids_from_redis()
    
    # 추가된 id값 list 생성
    # rds id 값 가져와서 비교
    logger.info(f"start getting ids from rds")
    id_list_from_rds = get_pid_from_rds()
    list_to_update = compare_id(id_list_from_redis, id_list_from_rds)
    
    # 추가된 데이터 불러오기
    df_to_update = get_data_from_dynamodb(list_to_update)
    
    # rds mysql에 적재
    load_to_rds(df_to_update)
    

if __name__ == "__main__":
    main()




