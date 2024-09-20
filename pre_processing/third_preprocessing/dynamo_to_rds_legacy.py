import re, datetime, pytz, subprocess, os, json
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
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
with open("./.KEYS/SECOND_PREPROCESSING_KEY.json", "r") as f:
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
        return [x.decode('utf-8') for x in redis_client.hkeys('testing_compare_id')]
    except redis.exceptions.RedisError as e:
        logger.error(f"Redis connection error: {e}")
        return []


def get_data_from_dynamodb(ids):
    """
    Retrieve data from DynamoDB for a list of IDs.

    This function connects to a DynamoDB table and queries it for each ID in the provided list.
    It collects the results into a single DataFrame, handling any errors that may occur during
    the querying process.

    Args:
        ids (list): A list of IDs to query from the DynamoDB table.

    Returns:
        pd.DataFrame: A DataFrame containing the combined results from the DynamoDB queries.
    """
    dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=key['aws_access_key_id'],
            aws_secret_access_key=key['aws_secret_key'],
            region_name=key['region']
        )    
    table = dynamodb.Table('precessed-data-table')

    # 빈 dataframe에 추가적으로 받아오기
    combined_df = pd.DataFrame()
    for index, id in enumerate(ids):  # 변경: 인덱스 추가
        if (index % 300) == 0:
            logger.info(f"Processing ID {id} ({index + 1}/{len(ids)})")  # 진행상황 로깅
        try:
            response = table.query(KeyConditionExpression=Key('pid').eq(int(id)))
            df = pd.DataFrame(response['Items'])
            combined_df = pd.concat([combined_df, df])
        except ClientError as e:
            logger.error(f"ClientError: {e.response['Error']['Message']}")
            # print(f"ClientError: {e.response['Error']['Message']}")
            continue
        except Exception as e:
            logger.error(f"Unexpected error: (id: {id}){str(e)}")
            # print(f"Unexpected error: (id: {id}){str(e)}")
            continue
    combined_df.reset_index(drop=True, inplace=True)
    return combined_df


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
    database = rds_info['legacy_database']

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


def find_final_id_list(list_to_update):
    dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=key['aws_access_key_id'],
            aws_secret_access_key=key['aws_secret_key'],
            region_name=key['region']
        )    
    table = dynamodb.Table('precessed-data-table')
    tmp = table.scan(Select='ALL_ATTRIBUTES')
    precesed_data= list(map(lambda x: str(x['pid']), tmp['Items']))
    final_id = list(set(precesed_data).intersection(list_to_update))
    return final_id


from decimal import Decimal
def get_processed_data(final_id):
    final_id = list(map(Decimal, final_id))
    dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=key['aws_access_key_id'],
            aws_secret_access_key=key['aws_secret_key'],
            region_name=key['region']
        )    
    table = dynamodb.Table('precessed-data-table')
    data = []
    for id in final_id:
        try:
            response = table.query(KeyConditionExpression=Key('pid').eq(id))
            data.append(response['Items'][0])  # 데이터 값을 리스트에 추가
        except ClientError as e:
            logger.error(f"[get_processed_data()]ClientError: {e.response['Error']['Message']}")
            # print(f"[get_processed_data()]ClientError: {e.response['Error']['Message']}")
            continue  # 다음 ID로 이동
        except Exception as e:
            logger.error(f"[get_processed_data()]Unexpected error: (id: {id}){str(e)}")
            # print(f"[get_processed_data()]Unexpected error: (id: {id}){str(e)}")
            continue  # 다음 ID로 이동
    return data


def load_to_rds(df: pd.DataFrame):
    count = 0
    # RDS 접속 정보
    rds_host = rds_info['host']
    username = rds_info['username']
    password = rds_info['password']
    database = rds_info['legacy_database']

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
        try:
            data = (
                values['site_symbol'], 
                "[{}]".format(', '.join("'{}'".format(item) for item in values['indurstry_type'])) if isinstance(values['indurstry_type'], list) else values['indurstry_type'],
                values['job_title'],
                "[{}]".format(', '.join("'{}'".format(item) for item in values['job_category'])) if isinstance(values['job_category'], list) else values['job_category'],
                "[{}]".format(', '.join("'{}'".format(item) for item in values['job_prefer'])) if isinstance(values['job_prefer'], list) else values['job_prefer'],
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
                "[{}]".format(', '.join("'{}'".format(item) for item in values['dev_stack'])) if isinstance(values['dev_stack'], list) else values['dev_stack'],  
                values['cid'],
                "[{}]".format(', '.join("'{}'".format(item) for item in values['job_requirements'])) if isinstance(values['job_requirements'], list) else values['job_requirements'] 
            )
            cursor.execute(sql_query, data)
            connection.commit()
            logger.info(f"Data successfully load to rds")
            count += 1
            
        except mysql.connector.Error as error:
            logger.error(f"Error executing SQL query for PID {values['pid']}: {error}")  # 에러 로깅
            # print(f"Error executing SQL query for PID {values['pid']}: {error}")
        except Exception as e:
            logger.error(f"Unexpected error for PID {values['pid']}: {str(e)}")  # 예기치 않은 에러 로깅
            # print(f"Unexpected error for PID {values['pid']}: {str(e)}")
    cursor.close()  # 커서 종료
    connection.close()  # 연결 종료
    print(f"{count} data were stored")
    
    
def main():
    # redis에서 id값 list 가져오기
    id_list_from_redis = get_ids_from_redis()
    # rds에서 id 값 list 가져오기
    id_list_from_rds = get_pid_from_rds()
    # redis에서 update할 id list
    list_to_update = set(id_list_from_redis) - set(id_list_from_rds)
    print(f"length of list_to_update in redis: {len(list_to_update)}")

    # 최종 update 할 id list
    final_id = find_final_id_list(list_to_update)
    print(f"length of final_id in dynamodb: {len(final_id)}")

    # 추가할 데이터 불러오기
    df_to_update = pd.DataFrame(get_processed_data(final_id))

    # rds mysql에 적재
    load_to_rds(df_to_update)
    print("all done")
    
    

if __name__ == "__main__":
    main()

