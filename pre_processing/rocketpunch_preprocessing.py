from farmhash import FarmHash32 as fh
import pandas as pd
import pytz
import boto3
import json
import os
import datetime
from json.decoder import JSONDecodeError
from botocore.exceptions import ClientError
import logging
import re

from src import utils


def import_bucket(path):
    os.listdir(path)
    with open(f'{path}/API_KEYS.json', 'r') as f:
        key = json.load(f)
    with open(f'{path}/DATA_SRC_INFO.json', 'r') as f:
        bucket_info = json.load(f)
    pull_bucket_name = bucket_info['pull_bucket_name']
    restore_table_name = bucket_info['restore_table_name']
    target_folder_prefix = bucket_info['target_folder_prefix']['rocketpunch_path']
    
    # S3 섹션 및 client 생성
    session = boto3.Session(
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )

    s3 = session.client('s3')
    
    # 특정 폴더 내 파일 목록 가져오기
    # TODO: 
    # - 마지막 실행일(년,월,일)을 datetime으로 저장한 파일을 읽어들여 curr_date에 적용하기; 당담: 유정연
    response = s3.list_objects_v2(Bucket=pull_bucket_name, Prefix=target_folder_prefix, Delimiter='/')
    curr_date = datetime.datetime.now(pytz.timezone('Asia/Seoul')).date()  # 로컬 시간대(UTC+9)로 현재 날짜 설정
    kst_tz = pytz.timezone('Asia/Seoul') # kst timezone 설정
    #curr_date = datetime.date(2024, 8, 21)

    # curr_date 보다 날짜가 늦은 data josn 파일 metadata 객체 분류
    if 'Contents' in response:
        target_file_list = [obj for obj in response['Contents'] if curr_date >= obj['LastModified'].astimezone(kst_tz).date()]
        #print(target_file_list)
    else:
        print("No objects found in the folder.")
        
    for obj in target_file_list:
        try:
            #print(obj)
            response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
            print(f'response: {response}\n')
            json_context = response['Body'].read().decode('utf-8')
            #print(f'json_context: {json_context}')
            cleaned_text = re.sub(r'[\r\u2028\u2029]+', ' ', json_context) # 파싱을 위해 unuseal line terminators 제거
            json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()] # pandas format으로 맞추기
            data = pd.DataFrame(json_list)
            return data
        except JSONDecodeError as e:
            logging.error(f"JSONDecodeError encountered: {e}")
            continue
        except ClientError as e:
            logging.error(f"ClientError encountered: {e}")
            continue
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            continue
        break
    
    #return data

def preprocessing(df):
    processing_df = pd.DataFrame()
    for i, data in df.iterrows() :
    # rocketpunch key > merged key
    # job_task > job_tasks
        if pd.notnull(data['job_task']) :
            processing_df.at[i, 'job_tasks'] = ' '.join(
                [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ', \
                    re.sub(r'\\/', '/', data['job_task'])).split() if item not in ['-', '+']]
            )
    # job_specialites > stacks
    processing_df['stacks'] = re.sub(r'\\/', '/', data['job_specialties'])
    # job_detail > job_requirements
    processing_df['job_requirements'] = ' '.join(
        [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ', \
            re.sub(r'\\/', '/', data['job_detail'])).split() if item not in ['-', '+']]
        )
    # job_industry > indurstry_type
    processing_df['indurstry_type'] = re.sub(r'\\/', '/', data['job_industry'])
    # date_start > start_date
    date_start = datetime.datetime.strptime(data['date_start'], "%Y-%m-%d")
    processing_df['start_date'] = date_start.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # date_end > end_date
    if data['date_end']:
        date_end = datetime.datetime.strptime(data['date_end'], "%Y-%m-%d")
        processing_df['end_date'] = date_end.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    
    #job_career > required_career
    processing_df['required_career'] = any(career in "신입" for career in data['job_career'])
    
    # symbol 추가
    processing_df['symbol'] = "RP"
    
    # id
    processing_df['id'] = fh("RP" + data['company_name'] + str(data['job_id']))
    
    return processing_df

def main():
    file_path = '/home/team3/repository/keys/'
    df = import_bucket(file_path)
    #processing_df = preprocessing(df)
    
if __name__=="__main__":
    main()
