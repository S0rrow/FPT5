import re, json, datetime, boto3, redis, sys, farmhash
import pandas as pd
from json.decoder import JSONDecodeError
from botocore.exceptions import ClientError
import logging_to_cloudwatch as ltc

import utils 

## key를 전역 변수로 설정
path = '/mnt/data/airflow/.KEYS/'  # local file path
with open(f'{path}/FIRST_PREPROCESSING_KEY.json', 'r') as f:
    key = json.load(f)
with open(f'{path}/DATA_SRC_INFO.json', 'r') as f:
    storage_info = json.load(f)

# 로깅 설정 복원
logger = ltc.log('/aws/preprocessing/rocketpunch-first','rocketpunch_logs')

# S3 세션 전역 변수로 선언
session = utils.return_aws_session(key['aws_access_key_id'], key['aws_secret_key'], key['region'])
s3 = session.client('s3')

# redis 연결 작업
redis_ip = storage_info['redis_conn_info']['ip']
redis_port = storage_info['redis_conn_info']['port']
redis_sassion = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)

# sqs url get
target_id_queue_url = storage_info['target_id_sqs_queque_arn']

'''
S3의 특정 버킷에서 json 형식의 데이터를 가져와
lake > archive로 크롤링한 데이터를 이식합니다.
'''
def import_bucket():
    pull_bucket_name = storage_info['pull_bucket_name']
    data_archive_bucket_name = storage_info['crawl_data_bucket_name']

    try:
        response = s3.list_objects_v2(Bucket=pull_bucket_name, Prefix='rocketpunch')
        if 'Contents' not in response:
            logger.warning(f"No objects found in the bucket: {pull_bucket_name}")
            return None
        
        logger.info(f"Object found in the bucket: {pull_bucket_name}")
        all_data = []
        for obj in response['Contents']:
            try:
                # s3에서 데이터를 가져와 all_data 리스트에 데이터를 담습니다.
                s3_response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
                json_context = s3_response['Body'].read().decode('utf-8')
                cleaned_text = re.sub(r'[\r\u2028\u2029]+', ' ', json_context)
                json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()]
                data = pd.DataFrame(json_list)
                all_data.append(data)

                # 파일 이동 및 삭제
                copy_source = {"Bucket": pull_bucket_name, "Key": obj['Key']}
                logger.info("Start to Copy data from lake to archive")
                s3.copy(copy_source, data_archive_bucket_name, obj['Key'])
                logger.info("Start to Delete data from lake")
                s3.delete_object(Bucket=pull_bucket_name, Key=obj['Key'])
                logger.info(f"Processed and moved file {obj['Key']}")

            except JSONDecodeError as e:
                logger.error(f"JSONDecodeError while processing {obj['Key']}: {e}")
            except ClientError as e:
                logger.error(f"ClientError while accessing S3: {e}")
            except Exception as e:
                logger.error(f"An unexpected error occurred while processing {obj['Key']}: {e}")

        # all_data가 있는 경우 pandas concat으로 합친 값을 return 합니다.
        # 없는 경우, none 타입으로 return 합니다.
        if all_data:
            logger.info(f"Data successfully imported from bucket {pull_bucket_name}")
            return pd.concat(all_data, ignore_index=True)
        else:
            logger.info(f"No data to import from bucket {pull_bucket_name}")
            return None

    except Exception as e:
        logger.error(f"An error occurred while importing bucket data: {e}")
        raise

''''
 시간 변환 함수입니다.
 preprocessing 함수에서 호출합니다.
'''
def convert_to_timestamp(date_str):
    date_pattern = re.compile(r'(\d{4})[./-](\d{2})[./-](\d{2})')
    #date_pattern = re.compile(r'\d{4}.\d{2}.\d{2}')
    match = date_pattern.match(date_str)
    if match:
        yy, mm, dd = match.groups()
        date = f"{yy}.{mm}.{dd}"
        date_obj = datetime.datetime.strptime(date, "%Y.%m.%d")
        return int(date_obj.timestamp())
    return None

'''
 전처리를 진행합니다. 
 규칙은 pre-processing_policy.md의 규칙을 따릅니다.
'''
def preprocessing(df):
    logger.info("Preprocessing started")
    processed_list = []
    
    for i, data in df.iterrows():
        try:
            processing_dict = {}
            if pd.notnull(data['job_task']):
                processing_dict['job_tasks'] = ' '.join(
                    [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ', re.sub(r'\\/', '/', data['job_task'])).split() if item not in ['-', '+']]
                )
            processing_dict['stacks'] = re.sub(r'\\/', '/', data['job_specialties'])
            processing_dict['job_requirements'] = ' '.join(
                [item for item in re.sub(r'[^.,/\-+()\s\w]', ' ', re.sub(r'\\/', '/', data['job_detail'])).split() if item not in ['-', '+']]
            )
            processing_dict['indurstry_type'] = re.sub(r'\\/', '/', data['job_industry'])
            
            #date parsing
            # Process date_start (always convert to timestamp, even if format is wrong or missing)
            if data['date_start']:
                processing_dict['start_date'] = convert_to_timestamp(data['date_start']) or int(datetime.datetime.now().timestamp())
            else:
                processing_dict['start_date'] = int(datetime.datetime.now().timestamp())
            
            # Process date_end (only convert if valid, otherwise set to 'null')
            if data['date_end']:
                end_timestamp = convert_to_timestamp(data['date_end'])
                processing_dict['end_date'] = end_timestamp if end_timestamp is not None else 'null'
            else:
                processing_dict['end_date'] = 'null'    

            processing_dict['required_career'] = "신입" in data['job_career']
            processing_dict['site_symbol'] = "RP"
            processing_dict['crawl_url'] = data['job_url']
            processing_dict['crawl_domain'] = data['crawl_domain']
            processing_dict['job_title'] = data['job_title']
            processing_dict['company_name'] = data['company_name']
            processing_dict['company_id'] = data['company_id']
            processing_dict['job_id'] = data['job_id']

            id = farmhash.Fingerprint32("RP" + data['company_name'] + str(data['job_id']))
            processing_dict['id'] = id

            dt = datetime.datetime.strptime(data['timestamp'], "%Y-%m-%d_%H:%M:%S")
            processing_dict['get_date'] = int(dt.timestamp())

            processed_list.append(processing_dict) 
            logger.info(f"Successfully processed data row {i}")

        except Exception as e:
            logger.error(f"An error occurred during preprocessing row {i}: {e}")
    
    return pd.DataFrame(processed_list)


'''
 데이터프레임의 데이터를 DynamoDB에 업로드합니다.
'''
def upload_data(records):
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )
    table = dynamodb.Table(storage_info['restore_table_name'])
    with table.batch_writer() as batch:
        for item in records:
            batch.put_item(Item=item)

def main():
    try:
        df = import_bucket()
        # df가 있는 경우만 전처리 진행
        if df is not None and not df.empty:
            preprocessed_df = preprocessing(df)
            unique_df = preprocessed_df.drop_duplicates(subset='id', keep='first')
            upload_ids_records = utils.check_id_in_redis(logger, redis_sassion, unique_df.to_dict(orient='records'))
            filtered_df = unique_df[unique_df['id'].isin([record['id'] for record in upload_ids_records])]
            upload_data(filtered_df.to_dict(orient='records'))
            utils.upload_id_into_redis(logger, redis_sassion, upload_ids_records)
            session = utils.return_aws_session(key['aws_access_key_id'], key['aws_secret_key'], key['region'])
            utils.send_msg_to_sqs(logger, session, target_id_queue_url, "RP", upload_ids_records)
            #print(json.dumps(upload_ids_records)) # Airflow DAG Xcom으로 값 전달하기 위해 stdout 출력 
        # df가 없는 경우 전처리 진행 하지 않음
        else:
            logger.info('No task for preprocessing.')
        sys.exit(0)
    except Exception as e:
        logger.error(f"An error occurred in the main function: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

