import json, boto3, logging, sys
from farmhash import FarmHash32 as fhash
from botocore.exceptions import ClientError
import pandas as pd
import utils


# S3 client 생성에 필요한 보안 자격 증명 정보 get
with open("./.KEYS/API_KEYS.json", "r") as f:
    key = json.load(f)

# S3 버킷 정보 get
with open("./.KEYS/DATA_SRC_INFO.json", "r") as f:
    storage_info = json.load(f)
    
# S3 섹션 및 client 생성
session = boto3.Session(
    aws_access_key_id=key['aws_access_key_id'],
    aws_secret_access_key=key['aws_secret_key'],
    region_name=key['region']
)

# S3 버킷 정보 init
s3 = session.client('s3')
pull_bucket_name = storage_info['pull_bucket_name']
push_table_name = storage_info['restore_table_name']
data_archive_bucket_name = storage_info['crawl_data_bucket_name']
id_list_bucket_name = storage_info['id_storage_bucket_name']
target_folder_prefix = storage_info['target_folder_prefix']['wanted_path']


def data_pre_process(df):
    # 1. id key 생성
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
        logging.error(f"KeyError: {e} - Available columns: {df.columns}")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}")
    return df

def upload_data(records):
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

def put_id_to_s3(s3_client, buket_name, prefix, records):
    pass

def get_id_from_s3(s3_client, buket_name, prefix):
    metadata_list = utils.get_bucket_metadata(s3_client, buket_name,prefix)
    if len(metadata_list) > 1:
        try:
            _obj = metadata_list[1]
            response = s3.get_object(Bucket=buket_name, Key=_obj['Key'])
            json_context = response['Body'].read().decode('utf-8')
            join_dict = json.loads(json_context)
            return join_dict.get('ids')
        except json.JSONDecodeError as e:
            logging.error(f"JSONDecodeError encountered: {e}")
            return False
        except ClientError as e:
            logging.error(f"ClientError encountered: {e}")
            return False
        except Exception as e:
            logging.error(f"Unknow Error. encountered: {e}")
            return False
        

def remove_duplicate_id(buket_name, records):
    pass

def main():
    new_columns = ['job_title', 'job_tasks', 'job_requirements', 'job_prefer', 'end_date', 'job_id', 'company_id', 'company_name', 'crawl_domain', 'get_date', 'id', 'site_symbol', 'crawl_url']
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
                upload_data(preprocessed_df.to_dict(orient='records'))
            except json.JSONDecodeError as e:
                logging.error(f"JSONDecodeError encountered: {e}")
                continue
            except ClientError as e:
                logging.error(f"ClientError encountered: {e}")
                continue
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                s3.copy({"Bucket":data_archive_bucket_name, "Key":obj["Key"]}, pull_bucket_name,obj['Key'])
                continue
        sys.exit(0) # return True
    else:
        sys.exit(1) # return False

if __name__ == '__main__':
    main()