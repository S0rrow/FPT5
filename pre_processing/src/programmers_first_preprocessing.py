# 전체 코드
from json import JSONDecodeError
from botocore.exceptions import ClientError
from farmhash import FarmHash32 as fhash

# from utils import get_curr_kst_time, set_kst_timezone

import json, boto3, datetime, pytz
import pandas as pd
import logging
import requests
import re
import os



def get_bucket_metadata(s3,pull_bucket_name,target_folder_prefix):
    # 특정 폴더 내 파일 목록 가져오기
    response = s3.list_objects_v2(Bucket=pull_bucket_name, Prefix=target_folder_prefix, Delimiter='/')
    # curr_date = datetime.datetime.now(pytz.timezone('Asia/Seoul')).date()  # 로컬 시간대(UTC+9)로 현재 날짜 설정
    # kst_tz = pytz.timezone('Asia/Seoul') # kst timezone 설정

    if 'Contents' in response:
        # return [obj for obj in response['Contents'] if curr_date <= obj['LastModified'].astimezone(kst_tz).date()]
        return response['Contents']
    
    print("No objects found in the folder.")
    return None



def replace_strings(text):
    # 입력 타입 체크 및 처리
    if text is None or text == "":
        return None
    
    # 문자열이 아닌 경우 문자열로 변환
    text = str(text)
    
    # \r\n을 ;로 치환
    text = re.sub(r'\r\n', ' ', text)
    # \n, \r을 공백으로 치환
    text = re.sub(r'[\n\r]', ' ', text)
    # HTML 태그와 그 안의 내용을 공백으로 치환
    text = re.sub(r'<[^>]*>', ' ', text)
    # \/를 /로 치환
    text = re.sub(r'\\/', '/', text)
    # 온점 ., 반점 ,, /, -, +, ;, 공백을 제외한 나머지 특수기호를 공백으로 치환
    text = re.sub(r'[^.,/\-\+;()% \w]', ' ', text)
    # 언더바 _를 공백으로 치환
    # text = re.sub(r'_', ' ', text)
    # 공백 처리
    text = ' '.join(text.split())


    # 처리 후 빈 문자열이면 None 반환
    return None if text == "" else text


# jobCategorytags.json 파일 경로
file_path = "./jobCategorytags.json"

# 파일이 존재하는지 확인
if os.path.exists(file_path):
    # 파일이 존재하면 파일에서 데이터 읽기
    with open(file_path, "r") as f:
        job_category_json = json.load(f)
else:
    # 파일이 존재하지 않으면 API에서 데이터 가져오기
    url = "https://career.programmers.co.kr/api/job_positions/job_categories"
    r = requests.get(url)
    job_category_json = r.json()
    
    # 가져온 데이터를 파일로 저장
    with open(file_path, "w", encoding='utf-8', ensure_ascii=False) as f:
        json.dump(job_category_json, f)

# JSON 데이터를 DataFrame으로 변환
job_category_table = pd.DataFrame(job_category_json)

def tagid_to_tagname(tags, job_table):
    """job_table 데이터프레임을 사용하여 tags 숫자와 매칭 리스트를 반환

    Args:
        tags (int): df['jobCategoryIds']
        job_table (pandas.DataFrame): job_category_table

    Returns:
        _type_: list
    """
    return ', '.join(job_table[job_table['id'].isin(tags)]['name'].tolist())


def preprocess_dataframe(tmpdf):
    
    df = tmpdf.copy()
    
    # description 전처리
    df['description'] = df['description'].apply(lambda x: replace_strings(x))
    
    # requirement 전처리
    df['requirement'] = df['requirement'].apply(lambda x: replace_strings(x))

    # preferredExperience 전처리
    df['preferredExperience'] = df['preferredExperience'].apply(lambda x: replace_strings(x))
    
    # jobCategoryIds 전처리
    df['jobCategoryIds'] = df['jobCategoryIds'].apply(lambda x: tagid_to_tagname(x, job_category_table))
    
    # 날짤 형식 전처리
    df['updatedAt'] = pd.to_datetime(df['updatedAt']).dt.strftime('%Y-%m-%d')
    df['endAt'] = df['endAt'].apply(lambda x: pd.to_datetime(x).strftime('%Y-%m-%d') if pd.notnull(x) else None)
    # df['endAt'] = df['endAt'].apply(lambda x: pd.to_datetime(x).date() if pd.notnull(x) else x)

    # boolean 형식 전처리
    df['careerRange'] = df['careerRange'].apply(lambda x: False if pd.isnull(x) else True) 
    df['resumeRequired'] = df['resumeRequired'].apply(lambda x: True if x else False)
    df['isAppliable'] = df['isAppliable'].apply(lambda x: True if x else False)

    # site_symbol 추가
    df['site_symbol'] = 'PRO'
    
    # crawl_domain 추가
    df['crawl_domain'] = 'https://career.programmers.co.kr/'
    
    # get_date 필드 추가 및 숫자로 변환
    df['get_date'] = int(pd.to_datetime('today').strftime('%Y%m%d'))
    
    # id 추가
    df['id'] = df.apply(lambda row: fhash(f"PRO{row['companyname']}{row['jobcode']}"), axis=1)
    
    # 필요없는 컬럼 삭제
    df.drop(['career','jobType', 'address', 'period', 'minCareerRequired', 'minCareer', 'additionalInformation'], axis=1, inplace=True)
    
    
    
    # 컬럼명 변경
    df.rename(columns={'title':'job_title', 'jobcode':'job_id', 'companyId': 'company_id', 
                       'companyname': 'company_name', 'description':'job_tasks', 
                       'technicalTags':'stacks', 'requirement':'job_requirements', 
                       'preferredExperience':'job_prefer','jobCategoryIds':'job_category', 
                       'updatedAt':'start_date', 'endAt':'end_date', 'careerRange':'required_career', 
                       'resumeRequired':'resume_required', 'isAppliable':'post_status',
                       'page_url':'crawl_url'}, inplace=True)
    
    return df


def upload_data(records,key,push_table_name):
    """DynamoDB 적제 코드

    Args:
        records (dictionary): dict 타입으로 dataframe을 변환하여 받는다
    """
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



def main():
    flag = 0

    # S3 client 생성에 필요한 보안 자격 증명 정보 get
    with open("./.KEYS/API_KEYS.json", "r") as f:
    # with open("./API_KEYS.json", "r") as f:
        key = json.load(f)

    # S3 버킷 정보 get
    with open("./.KEYS/DATA_SRC_INFO.json", "r") as f:
    # with open("./DATA_SRC_INFO.json", "r") as f:
        bucket_info = json.load(f)
        
    # S3 섹션 및 client 생성
    session = boto3.Session(
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )
    s3 = session.client('s3')

    # S3 버킷 정보 init
    pull_bucket_name = bucket_info['pull_bucket_name']
    dump_bucket_name = bucket_info['dump_bucket_name']
    push_table_name = bucket_info['restore_table_name']
    target_folder_prefix = bucket_info['target_folder_prefix']['programmers_path']

    kst_tz = pytz.timezone('Asia/Seoul') # kst timezone 설정

    data_list = get_bucket_metadata(s3,pull_bucket_name,target_folder_prefix)
    # meatadata_list[0] is directory path so ignore this item
    # copy files in crawl-data-lake to 
    all_df = pd.DataFrame()
    error_data_list = []
    for obj in data_list[1:]:
        try:
            response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
            s3.copy({"Bucket":pull_bucket_name,"Key":obj['Key']}, dump_bucket_name, obj['Key']) # 덤프 버킷으로 파일 복사
            s3.delete_object(Bucket=pull_bucket_name,Key=obj['Key']) # 원본 버킷에서 파일 삭제
            
            json_context = response['Body'].read().decode('utf-8')
            cleaned_text = re.sub(r'[\r\u2028\u2029]+', ' ', json_context) # 파싱을 위해 unuseal line terminators 제거
            json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()] # pandas format으로 맞추기
            
            master_df = preprocess_dataframe(pd.DataFrame(json_list))
            if len(master_df): # 처리 완료시 dynamoDB에 적제
                upload_data(master_df.to_dict(orient='records'),key,push_table_name)
            
            
        except Exception as e:
            logging.error(f"'{obj['Key']}' went wrong: {e}")
            error_data_list.append({obj['Key']})
            s3.copy({"Bucket":dump_bucket_name, "Key":obj["Key"]}, pull_bucket_name,obj["Key"]) # 문제 데이터 다시 s3에 적제
            flag = 1
            continue
    
        
    print("Data successfully uploaded to DynamoDB")
    if flag:
        print("except: ")
        for a in error_data_list:
            print(a)
        

if __name__ == '__main__':
    main()

