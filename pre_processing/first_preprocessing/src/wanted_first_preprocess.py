import json, boto3, sys, redis
from farmhash import FarmHash32 as fhash
from botocore.exceptions import ClientError
import pandas as pd

import logging_to_cloudwatch as ltc
import utils


# S3 client 생성에 필요한 보안 자격 증명 정보 get
with open("./.KEYS/FIRST_PREPROCESSING_KEY.json", "r") as f:
    aws_key = json.load(f)

# S3 버킷 정보 get
with open("./.KEYS/DATA_SRC_INFO.json", "r") as f:
    storage_info = json.load(f)
    
# S3 섹션 및 client 생성
# session = boto3.Session(
#     aws_access_key_id=aws_key['aws_access_key_id'],
#     aws_secret_access_key=aws_key['aws_secret_key'],
#     region_name=aws_key['region']
# )

# S3 버킷 정보 init
#s3 = session.client('s3')
pull_bucket_name = storage_info['pull_bucket_name']
push_table_name = storage_info['restore_table_name']
data_archive_bucket_name = storage_info['crawl_data_bucket_name']
target_id_queue_url = storage_info['target_id_sqs_queque_arn']
#id_list_bucket_name = storage_info['id_storage_bucket_name'] # s3 id 저장용 버킷
target_folder_prefix = storage_info['target_folder_prefix']['wanted_path']
redis_ip = storage_info['redis_conn_info']['ip']
redis_port = storage_info['redis_conn_info']['port']

redis_sassion = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
logger = ltc.log('/aws/preprocessing/wanted-first','wanted_logs')


def data_pre_process(df):
    logger.info("Start data_pre_process.")
    try:
        df['id'] = "WAN" + df['company_name'] + df['job_id'].astype(str)
        df['id'] = df['id'].apply(lambda x: fhash(x))
        df['position'] = df['position'].apply(lambda x: utils.remove_multiful_space(utils.replace_special_to_space(x)))
        df['tasks'] = df['tasks'].apply(lambda x: utils.remove_multiful_space(utils.replace_special_to_space(utils.change_slash_format(x.replace("\n", " ")), pattern=r'[^a-zA-Z0-9가-힣\s.,-]')))
        df['requirements'] = df['requirements'].apply(lambda x: utils.remove_multiful_space(utils.replace_special_to_space(x.replace("\n", " "), pattern=r'[^a-zA-Z0-9가-힣\s.,-/]')))
        df['prefer'] = df['prefer'].apply(lambda x: utils.remove_multiful_space(utils.replace_special_to_space(x.replace("\n", " "), pattern=r'[^a-zA-Z0-9가-힣\s.,-/]')))
        df['due_date'] = df['due_date'].apply(lambda x: utils.change_str_to_timestamp(x))
        df['site_symbol'] = "WAN"
        df['crawl_url'] = "https://www.wanted.co.kr/wd/" + df['job_id'].astype(str)
    except KeyError as e:
        logger.error(f"KeyError: {e} - Available columns: {df.columns}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during data_pre_process: {e}")
    return df

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
    logger.info("Start wanted first pre-processing code")
    new_columns = ['job_title', 'job_tasks', 'job_requirements', 'job_prefer', 'end_date', 'job_id', 'company_id', 'company_name', 'crawl_domain', 'get_date', 'id', 'site_symbol', 'crawl_url']
    session = utils.return_aws_session(aws_key['aws_access_key_id'], aws_key['aws_secret_key'], aws_key['region'])
    s3 = session.client('s3')
    metadata_list = utils.get_bucket_metadata(s3, pull_bucket_name,target_folder_prefix)
    if metadata_list:
        for obj in metadata_list[1:]:
            try:
                # crawl-data-lake 내 대상 file에 대한 data load 이후, 해당 파일을 archive에 옳김. 
                copy_source = {"Bucket":pull_bucket_name,"Key":obj['Key']}
                response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
                s3.copy(copy_source, data_archive_bucket_name, obj['Key'])
                s3.delete_object(Bucket=pull_bucket_name,Key=obj['Key'])
                json_context = response['Body'].read().decode('utf-8')
                cleaned_text = utils.remove_unusual_line_terminators(json_context)
                json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()] # pandas format으로 맞추기
                preprocessed_df = data_pre_process(pd.DataFrame(json_list))
                preprocessed_df.columns = new_columns
                unique_df = preprocessed_df.drop_duplicates(subset='id', keep='first')
                #upload_record_ids = utils.remove_duplicate_id(s3, id_list_bucket_name, unique_df)
                upload_ids_records = utils.check_id_in_redis(logger, redis_sassion, unique_df.to_dict(orient='records'))
                filtered_df = unique_df[unique_df['id'].isin([record['id'] for record in upload_ids_records])]
                upload_data(filtered_df.to_dict(orient='records'))
                utils.upload_id_into_redis(logger, redis_sassion, upload_ids_records)
                session = utils.return_aws_session(aws_key['aws_access_key_id'], aws_key['aws_secret_key'], aws_key['region'])
                utils.send_msg_to_sqs(logger, session, target_id_queue_url, "WAN", upload_ids_records)
                #update_respone = utils.update_ids_to_s3(s3, id_list_bucket_name, "obj_ids.json", upload_record_ids)
            except json.JSONDecodeError as e:
                logger.error(f"JSONDecodeError encountered: {e}")
                continue
            except ClientError as e:
                logger.error(f"ClientError encountered: {e}")
                continue
            except Exception as e:
                logger.error(f"An unexpected error occurred: {e}")
                s3.copy({"Bucket":data_archive_bucket_name, "Key":obj["Key"]}, pull_bucket_name,obj['Key'])
                continue
        sys.exit(0) # return True
    else:
        sys.exit(1) # return False

if __name__ == '__main__':
    main()