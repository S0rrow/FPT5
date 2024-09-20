import re, pytz, subprocess, os, json
from botocore.exceptions import ClientError
from time import gmtime, strftime
import boto3
import redis
import mysql.connector
import pandas as pd
from farmhash import FarmHash32 as fhash
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime


import logging
# parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# sys.path.append(parent_dir)
# from logging_utils import logging_to_cloudwatch as ltc
from logging_utils import logging_to_cloudwatch as ltc
    
logger = ltc.log('/aws/preprocessing/third','from_dynamodb_to_rds')



# 접속정보 json파일 로드
with open("./.KEYS/DATA_SRC_INFO.json", "r") as f:
# with open("./DATA_SRC_INFO.json", "r") as f:
    access_info = json.load(f)
# key 정보 json파일 로드
with open("./.KEYS/DATA_PROVIDING_KEY.json", "r") as f:
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
            continue  # 다음 ID로 이동
        except Exception as e:
            logger.error(f"[get_data_from_dynamodb()]Unexpected error: (id: {id}){str(e)}")
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
        sql = "SELECT pid FROM job_information"
        cursor.execute(sql)
        pids = cursor.fetchall()  # 모든 pid를 가져옴
        return [pid[0] for pid in pids]  # pid 리스트 반환

    except mysql.connector.Error as e:
        logger.error(f"[get_pid_from_rds()]Database error: {str(e)}")  # 에러 로그 출력
        return []  # 빈 리스트 반환

    finally:
        cursor.close()  # 커서 종료
        connection.close()  # 연결 종료
def find_final_id_list():
    logger.info(f"start getting ids from redis")
    #user function
    id_list_from_redis = get_ids_from_redis()

    # 추가된 id값 list 생성
    # rds id 값 가져와서 비교
    logger.info(f"start getting ids from rds")
    #user function
    id_list_from_rds = get_pid_from_rds()
    list_to_update = set(id_list_from_redis) - set(id_list_from_rds)
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
            print(f"[get_data_from_dynamodb()]ClientError: {e.response['Error']['Message']}")
            continue  # 다음 ID로 이동
        except Exception as e:
            print(f"[get_data_from_dynamodb()]Unexpected error: (id: {id}){str(e)}")
            continue  # 다음 ID로 이동
    return data

import numpy as np
def get_rds_pid_list():
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
    cursor.execute("SELECT pid FROM job_information")
    a1 = cursor.fetchall()
    rds_pid_list = list(map(lambda x : int(x[0]), list(np.array(a1))))
    cursor.close()
    connection.close()
    return rds_pid_list

def get_rds_did_list():
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
    cursor.execute("SELECT did FROM dev_stack")
    a2 = cursor.fetchall()
    rds_did_list = list(map(lambda x : str(x[0]), list(np.array(a2))))
    cursor.close()
    connection.close()
    return rds_did_list

def get_did_jobstack(pid):
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
    sql_query_tmp = f"""
        SELECT did FROM job_stack
        WHERE pid = {pid}
    """
    cursor.execute(sql_query_tmp)
    a3 = cursor.fetchall()
    rds_pid_did = list(map(lambda x : int(x[0]), list(np.array(a3))))
    
    cursor.close()
    connection.close()
    return rds_pid_did

def get_pid_jobstack():
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
    sql_query_tmp2 = """
    SELECT pid FROM job_stack
    """
    cursor.execute(sql_query_tmp2)
    a4 = cursor.fetchall()
    pid_list = list(map(lambda x : int(x[0]), list(np.array(a4))))
    cursor.close()
    connection.close()
    return pid_list

def get_pid_ic():
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
    sql_query_tmp2 = """
    SELECT pid FROM include_cartegory
    """
    cursor.execute(sql_query_tmp2)
    a4 = cursor.fetchall()
    pid_list = list(map(lambda x : int(x[0]), list(np.array(a4))))
    cursor.close()
    connection.close()
    return pid_list

def get_crid_ic(pid):
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
    sql_query_tmp = f"""
        SELECT crid FROM include_cartegory
        WHERE pid = {pid}
    """
    cursor.execute(sql_query_tmp)
    a3 = cursor.fetchall()
    rds_crid_pid = list(map(lambda x : int(x[0]), list(np.array(a3))))
    
    cursor.close()
    connection.close()
    return rds_crid_pid

def get_pid_ir():
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
    sql_query_tmp2 = """
    SELECT pid FROM industry_relation
    """
    cursor.execute(sql_query_tmp2)
    a4 = cursor.fetchall()
    pid_list = list(map(lambda x : int(x[0]), list(np.array(a4))))
    cursor.close()
    connection.close()
    return pid_list

def get_iid_ic(pid):
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
    sql_query_tmp = f"""
        SELECT iid FROM industry_relation
        WHERE pid = {pid}
    """
    cursor.execute(sql_query_tmp)
    a3 = cursor.fetchall()
    rds_iid_pid = list(map(lambda x : int(x[0]), list(np.array(a3))))
    
    cursor.close()
    connection.close()
    return rds_iid_pid

def insert_data(result):
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
    rds_pid_list= get_rds_pid_list()
    rds_did_list=rds_pid_list
    pid_list = rds_pid_list
    pid_ic = rds_pid_list
    pid_ir = rds_pid_list
    
    
    sql_query = """
        INSERT INTO job_information (
            pid, job_title, site_symbol, job_prefer, crawl_url, start_date, end_date, post_status,
            get_date, required_career, resume_required, crawl_domain, company_name, job_requirements
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            pid = VALUES(pid),
            job_title = VALUES(job_title),
            site_symbol = VALUES(site_symbol),
            job_prefer = VALUES(job_prefer),
            crawl_url = VALUES(crawl_url),
            start_date = VALUES(start_date),
            end_date = VALUES(end_date),
            post_status = VALUES(post_status),
            get_date = VALUES(get_date),
            required_career = VALUES(required_career),
            resume_required = VALUES(resume_required),
            crawl_domain = VALUES(crawl_domain),
            company_name = VALUES(company_name),
            job_requirements = VALUES(job_requirements)
    """
    sql_query2 = """
    INSERT INTO dev_stack (
        did, dev_stack
    ) 
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        dev_stack = VALUES(dev_stack)
    """
    
    sql_query3 = """
    INSERT INTO job_stack (
        pid, did
    ) 
    VALUES (%s, %s)
    """
    sql_query4 = """
    INSERT INTO category (
        crid, job_category
    ) 
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        job_category = VALUES(job_category)
    """
    sql_query5 = """
    INSERT INTO include_cartegory (
        crid, pid
    ) 
    VALUES (%s, %s)
    """
    sql_query6 = """
    INSERT INTO industry (
        iid, industry_type
    ) 
    VALUES (%s, %s)
    ON DUPLICATE KEY UPDATE
        industry_type = VALUES(industry_type)
    """
    sql_query7 = """
    INSERT INTO industry_relation (
        pid, iid
    ) 
    VALUES (%s, %s)
    """
    for i, values in result.iterrows():
        data = (
            values['pid'],
            values['job_title'],
            values['site_symbol'],
            f"[{', '.join(map(repr, values['job_prefer']))}]"  if isinstance(values['job_prefer'], list) else values['job_prefer'],
            values['crawl_url'],
            values['start_date'],
            values['end_date'],
            values['post_status'],
            values['get_date'],
            values['required_career'],
            values['resume_required'],
            values['crawl_domain'],
            values['company_name'],
            f"[{', '.join(map(repr, values['job_requirements']))}]" if isinstance(values['job_requirements'], list) else values['job_requirements']
        )
        try:
            if values['pid'] not in rds_pid_list:
                cursor.execute(sql_query, data)
                connection.commit()
                print(f"Data successfully load to rds job_information PID : {values['pid']}")
            else:
                print(f"overload data to rds")
        except mysql.connector.Error as error:
            print(f"Error executing SQL query for PID {values['pid']}: {error}")  # 에러 로깅
        except Exception as e:
            print(f"Unexpected error for PID {values['pid']}: {str(e)}")  # 예기치 않은 에러 로깅
            
        for stack in values['dev_stack']:
            data2 = (str(fhash(stack)), stack)
            data3 = (values['pid'],int(fhash(stack)))
            try:
                if data2[0] not in rds_did_list:
                    cursor.execute(sql_query2, data2)
                    connection.commit()
                    logger.info(f"Data successfully load to rds")
                else:
                    logger.debug("Data overload")
            except mysql.connector.Error as error:
                logger.error(f"Error executing SQL query: for dev_stack {stack}: {error}")  # 에러 로깅
            except Exception as e:
                logger.error(f"Unexpected error for dev_stack {stack}: {str(e)}")  # 예기치 않은 에러 로깅
            try:
                if int(data3[0]) in pid_list and data3[1] in get_did_jobstack(values['pid']):
                    logger.debug("Data overload")
                else:
                    cursor.execute(sql_query3, data3)
                    connection.commit()
                    logger.info(f"Data successfully load to rds")
            except mysql.connector.Error as error:
                logger.error(f"Error dev_stack executing SQL query for PID {values['pid']}: {error}")  # 에러 로깅
            except Exception as e:
                logger.error(f"[dev_stack]Unexpected error for PID {values['pid']}: {str(e)}")  # 예기치 않은 에러 로깅
        for cart in values['job_category']:
            data4 = (str(fhash(cart)), cart)
            data5 = (int(fhash(cart)),values['pid'])
            try:
                cursor.execute(sql_query4, data4)
                connection.commit()
                logger.info(f"Data successfully load to rds")
            except mysql.connector.Error as error:
                logger.error(f"Error executing SQL query for PID {values['pid']}: {error}")  # 에러 로깅
            except Exception as e:
                logger.error(f"Unexpected error for PID {values['pid']}: {str(e)}")  # 예기치 않은 에러 로깅
            try:
                if data5[1] in pid_ic and data5[0] in get_crid_ic(values['pid']):
                    logger.debug("Data overload")
                else:
                    cursor.execute(sql_query5, data5)
                    connection.commit()
                    logger.info(f"[category] Data successfully load to rds")
            except mysql.connector.Error as error:
                logger.error(f"Error executing SQL query for PID {values['pid']}: {error}")  # 에러 로깅
            except Exception as e:
                logger.error(f"Unexpected error for PID {values['pid']}: {str(e)}")  # 예기치 않은 에러 로깅
        for e_ind in values['indurstry_type']:
            data6 = (str(fhash(e_ind)), e_ind)
            data7 = (values['pid'], int(fhash(e_ind)))
            try:
                cursor.execute(sql_query6, data6)
                connection.commit()
                logger.info(f"Data successfully load to rds")
            except mysql.connector.Error as error:
                logger.error(f"Error executing SQL query for PID {values['pid']}: {error}")  # 에러 로깅
            except Exception as e:
                logger.error(f"Unexpected error for PID {values['pid']}: {str(e)}")  # 예기치 않은 에러 로깅
            try:
                if data7[0] in pid_ir and data7[1] in get_iid_ic(values['pid']):
                    logger.debug("Data overloaded")
                else:
                    cursor.execute(sql_query7, data7)
                    connection.commit()
                    logger.info(f"Data successfully load to rds")
            except mysql.connector.Error as error:
                logger.error(f"Error executing SQL query for PID {values['pid']}: {error}")  # 에러 로깅
            except Exception as e:
                logger.error(f"Unexpected error for PID {values['pid']}: {str(e)}")  # 예기치 않은 에러 로깅
    cursor.close()  # 커서 종료
    connection.close()  # 연결 종료
    


def preprocessing_data(result):
    result['end_date'] = result['end_date'].apply(lambda x: datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S') if x != None and x != 'null' and x != 'None' and x != '' else None)
    result['get_date'] = result['get_date'].apply(lambda x: datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S') if x != None and x != 'null' and x != 'None' and x != '' else None)
    result['start_date'] = result['start_date'].apply(lambda x: datetime.fromtimestamp(int(x)).strftime('%Y-%m-%d %H:%M:%S') if x != None and x != 'null' and x !='None' and x != '' else None)
    result['post_status'] = result['post_status'].apply(lambda x: False if type(x) != bool else x)
    return result
    
def main():
    final_id = find_final_id_list()
    print(final_id)
    result = pd.DataFrame(get_processed_data(final_id))
    result = preprocessing_data(result)
    insert_data(result)
    
    
if __name__ == "__main__":
    main()