import requests
from bs4 import BeautifulSoup
from datetime import datetime
import subprocess, os, sys
import re
import time
import pandas as pd
import subprocess, os
from farmhash import FarmHash32 as fhash
import pytz
from utils import get_curr_kst_time,set_kst_timezone
import json, boto3, pytz
from jobkorea import jobkorea
from logging_to_cloudwatch import log


def get_time():
    kst_tz = pytz.timezone('Asia/Seoul') # kst timezone 설정
    return str(datetime.strftime(datetime.now().astimezone(kst_tz),"%Y-%m-%d_%H%M%S"))

    
def get_bucket_metadata(s3_client, pull_bucket_name,target_folder_prefix):
    # 특정 폴더 내 파일 목록 가져오기
    response = s3_client.list_objects_v2(Bucket=pull_bucket_name, Prefix=target_folder_prefix, Delimiter='/')
    curr_date = get_curr_kst_time()
    kst_tz = set_kst_timezone()

    if 'Contents' in response:
        return [obj for obj in response['Contents']]
    else:
        #print("No objects found in the folder.")
        return None

def upload_data(logger, records, key, push_table_name):
    # DynamoDB 클라이언트 생성
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )
    table = dynamodb.Table(push_table_name)
    # 25개씩 묶어서 배치로 처리
    with table.batch_writer() as batch:
        for item in records:
            batch.put_item(Item=item)
    
def main():
    logger = log('/aws/preprocessing/jobkorea-first','jobkorea_logs')
    # S3 client 생성에 필요한 보안 자격 증명 정보 get
    with open("./.KEYS/FIRST_PREPROCESSING_KEY.json", "r") as f:
        aws_key = json.load(f)

    # S3 버킷 정보 get
    with open("./.KEYS/DATA_SRC_INFO.json", "r") as f:
        storage_info = json.load(f)
        
    # S3 섹션 및 client 생성
    session = boto3.Session(
        aws_access_key_id=aws_key['aws_access_key_id'],
        aws_secret_access_key=aws_key['aws_secret_key'],
        region_name=aws_key['region']
    )
    s3 = session.client('s3')

    # S3 버킷 정보 init
    pull_bucket_name = storage_info['pull_bucket_name']
    data_archive_bucket_name = storage_info['crawl_data_bucket_name']
    push_table_name = storage_info['restore_table_name']
    id_list_bucket_name = storage_info['id_storage_bucket_name']
    target_folder_prefix = storage_info['target_folder_prefix']['jobkorea_path']
    # 특정 폴더 내 파일 목록 가져오기
    # TODO: 
    # - 마지막 실행일(년,월,일)을 datetime으로 저장한 파일을 읽어들여 curr_date에 적용하기; 당담: 유정연
    # response = s3.list_objects_v2(Bucket=push_bucket_name, Prefix='data/', Delimiter='/')
    kst_tz = pytz.timezone('Asia/Seoul') # kst timezone 설정

    metadata_list = get_bucket_metadata(s3,pull_bucket_name,target_folder_prefix)
    # meatadata_list[0] is directory path so ignore this item
    # copy files in crawl-data-lake to 
    for obj in metadata_list[1:]:
        try:
            copy_source = {"Bucket":pull_bucket_name,"Key":obj['Key']} 
            response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
            s3.copy(copy_source,data_archive_bucket_name,obj['Key'])
            s3.delete_object(Bucket=pull_bucket_name,Key=obj['Key'])      
            json_context = response['Body'].read().decode('utf-8')
            cleaned_text = re.sub(r'[\r\u2028\u2029]+', ' ', json_context) # 파싱을 위해 unuseal line terminators 제거
            json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()] # pandas format으로 맞추기
            instance = jobkorea(logger)
            result = instance.pre_processing_first(json_list)
            if len(result):
                upload_data(logger,result,aws_key,push_table_name)
            
        except Exception as e:
            s3.copy({"Bucket":data_archive_bucket_name,"Key":obj["Key"]},pull_bucket_name,obj["Key"])
            logger.error(f"{obj['Key']} upload error")
            #s3.delete_object(Bucket=dump_bucket_name,Key=obj["Key"])
        else:
            logger.info(f"{obj['Key']} upload complete")
    sys.exit(0)
        

if __name__ == "__main__":
    main()
