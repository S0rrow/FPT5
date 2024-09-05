import farmhash
import pandas as pd
import pytz
import boto3
import json
import os, sys
import datetime
from json.decoder import JSONDecodeError
from botocore.exceptions import ClientError
import re
from decimal import Decimal
import logging

import utils

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler("app.log"), logging.StreamHandler()]
)

def access_keys(path):
    try:
        with open(f'{path}/FIRST_PREPROCESSING_KEY.json', 'r') as f:
            key = json.load(f)
        with open(f'{path}/DATA_SRC_INFO.json', 'r') as f:
            storage_info = json.load(f)
        logging.info(f"Access keys and bucket info loaded successfully from {path}")
        return key, storage_info
    except FileNotFoundError as e:
        logging.error(f"File not found: {e}")
        raise
    except JSONDecodeError as e:
        logging.error(f"JSON decode error: {e}")
        raise
    except Exception as e:
        logging.error(f"An error occurred while accessing keys: {e}")
        raise

def import_bucket(key, storage_info):
    pull_bucket_name = storage_info['pull_bucket_name']
    data_archive_bucket_name = storage_info['crawl_data_bucket_name']

    try:
        session = boto3.Session(
            aws_access_key_id=key['aws_access_key_id'],
            aws_secret_access_key=key['aws_secret_key'],
            region_name=key['region']
        )
        s3 = session.client('s3')

        response = s3.list_objects_v2(Bucket=pull_bucket_name, Prefix='rocketpunch')
        if 'Contents' not in response:
            logging.warning(f"No objects found in the bucket: {pull_bucket_name}")
            return None

        all_data = []
        for obj in response['Contents']:
            try:
                s3_response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
                json_context = s3_response['Body'].read().decode('utf-8')
                cleaned_text = re.sub(r'[\r\u2028\u2029]+', ' ', json_context)
                json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()]
                data = pd.DataFrame(json_list)
                all_data.append(data)

                # Copy and delete the processed file
                copy_source = {"Bucket": pull_bucket_name, "Key": obj['Key']}
                s3.copy(copy_source, data_archive_bucket_name, obj['Key'])
                s3.delete_object(Bucket=pull_bucket_name, Key=obj['Key'])
                logging.info(f"Processed and moved file {obj['Key']} from {pull_bucket_name} to {data_archive_bucket_name}")

            except JSONDecodeError as e:
                logging.error(f"JSONDecodeError encountered while processing {obj['Key']}: {e}")
            except ClientError as e:
                logging.error(f"ClientError encountered while accessing S3: {e}")
            except Exception as e:
                logging.error(f"An unexpected error occurred while processing {obj['Key']}: {e}")

        if all_data:
            logging.info(f"Data successfully imported from bucket {pull_bucket_name}")
            return pd.concat(all_data, ignore_index=True)
        else:
            logging.info(f"No data to import from bucket {pull_bucket_name}")
            return None

    except Exception as e:
        logging.error(f"An error occurred while importing bucket data: {e}")
        raise

# TODO: record 단위로 putitem 하는 것 대신 모든 전처리 작업 완료 후 전체 record를 df로 반환할 것
def preprocessing(df, key):
    logging.info("Preprocessing started")
    precessed_list = [] # 수정한 부분
    for i, data in df.iterrows():
        try:
            processing_dict = {}
            # Data processing logic
            if pd.notnull(data['job_task']):
                processing_dict['job_tasks'] = ' '.join(
                    [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ',
                        re.sub(r'\\/', '/', data['job_task'])).split() if item not in ['-', '+']]
                )
            processing_dict['stacks'] = re.sub(r'\\/', '/', data['job_specialties'])
            processing_dict['job_requirements'] = ' '.join(
                [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ',
                    re.sub(r'\\/', '/', data['job_detail'])).split() if item not in ['-', '+']]
            )
            processing_dict['indurstry_type'] = re.sub(r'\\/', '/', data['job_industry'])

            date_start = datetime.datetime.strptime(data['date_start'], "%Y.%m.%d")
            processing_dict['start_date'] = int(date_start.timestamp())

            if 'Null' not in data['date_end']:
                date_end = datetime.datetime.strptime(data['date_end'], "%Y.%m.%d")
                processing_dict['end_date'] = int(date_end.timestamp())
            else:
                processing_dict['end_date'] = 'null'

            processing_dict['required_career'] = any(career in "신입" for career in data['job_career'])
            processing_dict['site_symbol'] = "RP"
            processing_dict['crawl_url'] = data['job_url']
            processing_dict['crawl_domain'] = data['crawl_domain']

            id = farmhash.Fingerprint32("RP" + data['company_name'] + str(data['job_id']))
            processing_dict['id'] = id

            dt = datetime.datetime.strptime(data['timestamp'], "%Y-%m-%d_%H:%M:%S")
            processing_dict['get_date'] = int(dt.timestamp())

            # Export to DynamoDB
            #export_dynamo(processing_dict, key)
            precessed_list.append(processing_dict) # 수정한 부분
            logging.info(f"Successfully processed data row {i}")

        except Exception as e:
            logging.error(f"An error occurred during preprocessing row {i}: {e}")
    
    return precessed_list

def export_dynamo(processing_dict, key):
    try:
        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=key['aws_access_key_id'],
            aws_secret_access_key=key['aws_secret_key'],
            region_name=key['region']
        )

        table = dynamodb.Table('merged-data-table')

        for k, v in processing_dict.items():
            if isinstance(v, float):
                processing_dict[k] = Decimal(str(v))

        processing_dict = {k: (v if pd.notnull(v) else None) for k, v in processing_dict.items()}

        table.put_item(Item=processing_dict)
        logging.info(f"Data successfully exported to DynamoDB: {processing_dict['id']}")

    except ClientError as e:
        logging.error(f"ClientError encountered while exporting to DynamoDB: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while exporting to DynamoDB: {e}")
        raise

# TODO: 추가된 batch 단위 upload 함수 이 함수를 사용하거나 참고하여 반영할 것
def upload_data(records):
    # DynamoDB 클라이언트 생성
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=aws_key['aws_access_key_id'],
        aws_secret_access_key=aws_key['aws_secret_key'],
        region_name=aws_key['region']
    )
    table = dynamodb.Table(push_table_name)
    # 25개씩 묶어서 배치로 처리
    with table.batch_writer() as batch:
        for item in records:
            batch.put_item(Item=item)

def main():
    file_path = '/mnt/data/airflow/.KEYS/'  # local file path
    try:
        key, storage_info = access_keys(file_path)
        df = import_bucket(key, storage_info)

        if df is not None and not df.empty:
            precessed_list = preprocessing(df, key)
            preprocessed_df = pd.Dataframe(precessed_list)
            unique_df = preprocessed_df.drop_duplicates(subset='id', keep='first')
            # TODO: remove_duplicate_id 함수를 사용할려면 s3 색션과 id_list_buket_name을 가져와야됨 s3 init 관련을 전역으로 빼거나 추가 조치를 취할 것
            upload_record_ids = utils.remove_duplicate_id(s3, id_list_bucket_name, unique_df)
            filtered_df = unique_df[unique_df['id'].isin(upload_record_ids)]
            upload_data(filtered_df.to_dict(orient='records'))
        else:
            logging.info('No task for preprocessing.')
        sys.exit(0)
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
