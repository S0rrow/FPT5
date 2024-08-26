import json, boto3
from farmhash import FarmHash32 as fhash
import pandas as pd
import src.utils as utils


# S3 client 생성에 필요한 보안 자격 증명 정보 get
with open("./.KEYS/API_KEYS.json", "r") as f:
    key = json.load(f)

# S3 버킷 정보 get
with open("./.KEYS/DATA_SRC_INFO.json", "r") as f:
    bucket_info = json.load(f)
    
# S3 섹션 및 client 생성
session = boto3.Session(
    aws_access_key_id=key['aws_access_key_id'],
    aws_secret_access_key=key['aws_secret_key'],
    region_name=key['region']
)

# S3 버킷 정보 init
s3 = session.client('s3')
pull_bucket_name = bucket_info['pull_bucket_name']
push_table_name = bucket_info['restore_table_name']
target_folder_prefix = bucket_info['target_folder_prefix']['wanted_path']


def get_bucket_metadata():
    # 특정 폴더 내 파일 목록 가져오기
    response = s3.list_objects_v2(Bucket=pull_bucket_name, Prefix=target_folder_prefix, Delimiter='/')
    curr_date = utils.set_curr_kst_time()
    kst_tz = utils.set_kst_timezone()

    # curr_date 보다 날짜가 늦은 data josn 파일 metadata 객체 분류
    if 'Contents' in response:
        return [obj for obj in response['Contents'] if curr_date <= obj['LastModified'].astimezone(kst_tz).date()]
    else:
        #print("No objects found in the folder.")
        return None

def data_pre_process(df):
    # 1. id key 생성
    df['id'] = fhash("WAN" + df['company_name'] + df['job_id'].astype(str))
    df['position'] = df['position'].apply(lambda x: utils.remove_multiful_space(utils.replace_special_to_space(x)))
    df['tasks'] = df['tasks'].apply(lambda x: utils.remove_multiful_space(utils.replace_special_to_space(utils.change_slash_format(x.replace("\n", " ")), pattern=r'[^a-zA-Z0-9가-힣\s.,-]')))
    df['requirements'] = df['requirements'].apply(lambda x: utils.remove_multiful_space(utils.replace_special_to_space(x.replace("\n", " "), pattern=r'[^a-zA-Z0-9가-힣\s.,-/]')))
    df['prefer'] = df['prefer'].apply(lambda x: utils.remove_multiful_space(utils.replace_special_to_space(x.replace("\n", " "), pattern=r'[^a-zA-Z0-9가-힣\s.,-/]')))
    df['due_date'] = df['due_date'].apply(lambda x: utils.change_str_to_timestamp(x))
    df['site_symbol'] = "WAN"
    df['crawl_url'] = "https://www.wanted.co.kr/wd/" + df['job_id'].astype(str)
    
    return df.to_dict(orient='records')

def upload_data(records):
    # DynamoDB 클라이언트 생성
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )
    table = dynamodb.Table(push_table_name)
    for item in records:
        table.put_item(Item=item)
    
    dynamodb.close()
    
def main():
    metadata_list = get_bucket_metadata()
    if metadata_list:
        for obj in metadata_list:
            try:
                response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
                json_context = response['Body'].read().decode('utf-8')
                cleaned_text = utils.remove_unusual_line_terminators(json_context)
                json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()] # pandas format으로 맞추기
                upload_data(data_pre_process(pd.DataFrame(json_list)))
                
            except JSONDecodeError as e:
                logging.error(f"JSONDecodeError encountered: {e}")
                continue
            except ClientError as e:
                logging.error(f"ClientError encountered: {e}")
                continue
            except Exception as e:
                logging.error(f"An unexpected error occurred: {e}")
                continue
            
            #break
    else:
        return False

if __name__ == '__main__':
    main()