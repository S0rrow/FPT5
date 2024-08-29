import farmhash
import pandas as pd
import pytz
import boto3
import json
import os
import datetime
from json.decoder import JSONDecodeError
from botocore.exceptions import ClientError
import re
from decimal import Decimal
import logging

import utils

'''
 local에서 엑세스 키를 가져옵니다.  
'''
def access_keys(path):
    with open(f'{path}/API_KEYS.json', 'r') as f:
        key = json.load(f)
    with open(f'{path}/DATA_SRC_INFO.json', 'r') as f:
        bucket_info = json.load(f)
        
    return key, bucket_info

'''
  S3 버킷에서 데이터를 가져옵니다.
  버킷의 모든 데이터를 가져오고, archive 쪽에 적재합니다.
  이후 모든 데이터를 지우고 가져온 자료만 데이터 정제 처리를 진행합니다.
'''
def import_bucket(key, bucket_info):
    pull_bucket_name = bucket_info['pull_bucket_name']
    dump_bucket_name = bucket_info['dump_bucket_name']
    # S3 섹션 및 client 생성
    session = boto3.Session(
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )

    s3 = session.client('s3')
    
    response = s3.list_objects_v2(Bucket=pull_bucket_name, Prefix='rocketpunch')
    print(f'response: {response}')
    if 'Contents' not in response:
        print("No objects found in the folder.")
        return None

    all_data = []

    for obj in response['Contents']:
        try:
            s3_response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
            json_context = s3_response['Body'].read().decode('utf-8')
            cleaned_text = re.sub(r'[\r\u2028\u2029]+', ' ', json_context) # 파싱을 위해 unuseal line terminators 제거
            json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()] # pandas format으로 맞추기
            data = pd.DataFrame(json_list)
            all_data.append(data)

            # 데이터 처리 후, 파일을 다른 버킷으로 복사 후 삭제
            copy_source = {"Bucket": pull_bucket_name, "Key": obj['Key']}
            s3.copy(copy_source, dump_bucket_name, obj['Key'])
            s3.delete_object(Bucket=pull_bucket_name, Key=obj['Key'])

        except JSONDecodeError as e:
            logging.error(f"JSONDecodeError encountered: {e}")
            continue
        except ClientError as e:
            logging.error(f"ClientError encountered: {e}")
            continue
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}")
            continue
    
    if all_data:
        return pd.concat(all_data, ignore_index=True)
    else:
        return None

'''
  1차 전처리를 진행합니다.
  제이슨을 한 줄씩 읽고 한 줄씩 DynamoDB로 전송해 데이터를 넣습니다.
'''
def preprocessing(df, key):
    print("preprocessing start")
    for i, data in df.iterrows() :
        print(data)
        processing_dict = {}
        # rocketpunch key > merged key
        # job_task > job_tasks
        if pd.notnull(data['job_task']) :
            processing_dict['job_tasks'] = ' '.join(
                [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ', \
                    re.sub(r'\\/', '/', data['job_task'])).split() if item not in ['-', '+']]
            )
        # job_specialites > stacks
        processing_dict['stacks'] = re.sub(r'\\/', '/', data['job_specialties'])
        # job_detail > job_requirements
        processing_dict['job_requirements'] = ' '.join(
            [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ', \
                re.sub(r'\\/', '/', data['job_detail'])).split() if item not in ['-', '+']]
            )
        # job_industry > indurstry_type
        processing_dict['indurstry_type'] = re.sub(r'\\/', '/', data['job_industry'])
        # date_start > start_date
        date_start = datetime.datetime.strptime(data['date_start'], "%Y.%m.%d")
        date_start_stp = int(date_start.timestamp())
        processing_dict['start_date'] = date_start_stp
        # date_end > end_date
        if not 'Null' in data['date_end']:
            date_end = datetime.datetime.strptime(data['date_end'], "%Y.%m.%d")
            date_end_stp = int(date_end.timestamp())
            processing_dict['end_date'] = date_end_stp
        
        # job_career > required_career
        processing_dict['required_career'] = any(career in "신입" for career in data['job_career'])
        
        # symbol 추가
        processing_dict['site_symbol'] = "RP"
        
        # crawl_url 추가
        processing_dict['crawl_url'] = data['job_url']
        
        # crawl_domain 추가
        processing_dict['crawl_domain'] = data['crawl_domain']
        
        # id
        id = farmhash.Fingerprint32("RP" + data['company_name'] + str(data['job_id']))
        processing_dict['id'] = id
        
        # get_date 추가
        dt = datetime.datetime.strptime(data['timestamp'], "%Y-%m-%d_%H:%M:%S")
        processing_dict['get_date'] = int(dt.timestamp())
    
        #### dynamoDB에 바로 저장
        export_dynamo(processing_dict, key)

'''
  DynamoDB로 전처리한 데이터를 적재합니다.
'''
def export_dynamo(processing_dict, key):
    # DynamoDB 클라이언트 생성
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )
    
    # 테이블 접근
    table = dynamodb.Table('merged-data-table')
    
    # 데이터 삽입
    # float 타입을 Decimal 타입으로 변환 (다이나모에서 지원하지 않는 타입 이슈)
    for k, v in processing_dict.items():
        if isinstance(v, float):
            processing_dict[k] = Decimal(str(v))
    
    # NaN을 None으로 변경 (NaN은 다이나모에서 지원하지 않는 타입입니다.)
    processing_dict = {k: (v if pd.notnull(v) else None) for k, v in processing_dict.items()}
    
    # DynamoDB에 데이터 삽입
    table.put_item(Item=processing_dict) 
    
def main():
    file_path = '/home/team3/repository/keys/'  # local file path
    key, bucket_info = access_keys(file_path)
    df = import_bucket(key, bucket_info)
 
    # df에 값이 있으면 파싱 처리
    if df is not None and not df.empty: 
        preprocessing(df, key)
    else:
        print('No task for preprocessing.')
    
if __name__ == "__main__":
    main()
