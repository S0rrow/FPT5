import json, boto3, logging, time
from botocore.exceptions import ClientError
import google.generativeai as genai
from farmhash import FarmHash32 as fhash
import asyncio
import time
import botocore.exceptions
from boto3.dynamodb.conditions import Key, Attr
from datetime import datetime
from logging_utils.logging_to_cloudwatch import log
import sys


id_list = list(map(int,sys.argv[1].split(',')))
logger = log('/aws/preprocessing/second','logs')
with open("./.KEYS/SECOND_PREPROCESSING_KEY.json", "r") as f:
    key = json.load(f)
# S3 버킷 정보 get
with open("./.KEYS/DATA_SRC_INFO.json", "r") as f:
    bucket_info = json.load(f)
with open("./.KEYS/GEMINI_API_KEY.json", "r") as f:
    gemini_api_key = json.load(f)
with open("./.DATA/PROMPT_INFO.json") as f:
    prompt_metadata = json.load(f)
    
async def send_data_async(logger,chat_session,source_data,_response):
    prompt_data = prompt_metadata.get("data", {})
    task = []
    try:
        for idx, _obj in enumerate(source_data):
            _symbol = _obj.get('site_symbol', "").upper()
            if _symbol in prompt_data.keys():
                _data_source_keys = return_object_source_keys(prompt_data, _symbol)
                _prompt = return_object_prompt(prompt_data, _symbol).format(
                    data_source_keys=_data_source_keys, 
                    input_data=str(_obj)
                )
            task.append(asyncio.create_task(chat_session.send_message_async(_prompt)))
        logger.debug(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        await asyncio.sleep(60)
        for idx in range(len(source_data)):
            _response[idx] = await task[idx]
        logger.debug(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        try:
            for idx, _obj in enumerate(source_data):
                json_data = _response[idx].text.replace("```json\n", "").replace("\n```", "")
                dict_data = json.loads(json_data)
                data_item = return_concat_data_record(obj=_obj, data_dict=dict_data)
                upload_data(data_item)
        except Exception as e:
            logger.error(str(e))
    except Exception as e:
        logger.error(str(e))
        



# 지수 백오프를 포함한 스캔 작업
def scan_with_backoff(table, scan_kwargs):
    retry_attempts = 0
    max_retries = 10
    backoff_factor = 0.5
    source_data = []

    while True:
        try:
            response = table.scan(**scan_kwargs)
            source_data.extend(response.get('Items', []))
            last_evaluated_key = response.get('LastEvaluatedKey', None)
            if last_evaluated_key:
                scan_kwargs['ExclusiveStartKey'] = last_evaluated_key
            else:
                break
        except ClientError as error:
            if error.response['Error']['Code'] == 'ProvisionedThroughputExceededException':
                if retry_attempts < max_retries:
                    retry_attempts += 1
                    time.sleep(backoff_factor * (2 ** retry_attempts))  # 지수 백오프
                else:
                    raise
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            break

    return source_data

def return_object_prompt(data, symbol_key):
    return data.get(symbol_key, {}).get("prompt")

def return_object_source_keys(data, symbol_key):
    return data.get(symbol_key, {}).get("source_key")

def return_concat_data_record(obj, data_dict):
    data_record = {
        "pid": obj.get("id"),
        "get_date": obj.get("get_date"),
        "site_symbol": obj.get("site_symbol"),
        "job_title": obj.get("job_title", None),
        "dev_stack": data_dict.get("dev_stack", []),
        "job_requirements": data_dict.get("job_requirements", []),
        "job_prefer": data_dict.get("job_prefer", []),
        "job_category": data_dict.get("job_category", []),
        "indurstry_type": data_dict.get("indurstry_type", []),
        "required_career": obj.get("required_career", None),
        "resume_required": obj.get("resume_required", None),
        "post_status": obj.get("post_status", None),
        "company_name": obj.get("company_name", None),
        "cid": fhash(obj.get("site_symbol")+obj.get("company_name")),
        "start_date": obj.get("start_date", None),
        "end_date": obj.get("end_date", None),
        "crawl_domain": obj.get("crawl_domain", None),
        "crawl_url": obj.get("crawl_url", None)
    }
    return data_record

def upload_data(_item):
    # DynamoDB 클라이언트 생성
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )
    table = dynamodb.Table("precessed-data-table")
    table.put_item(Item=_item)




def main_debug():
    logger = log('/aws/preprocessing/second','logs')
    with open("../.KEYS/SECOND_PREPROCESSING_KEY.json", "r") as f:
        key = json.load(f)
    # S3 버킷 정보 get
    with open("../.KEYS/DATA_SRC_INFO.json", "r") as f:
        bucket_info = json.load(f)

    with open("../.KEYS/GEMINI_API_KEY.json", "r") as f:
        gemini_api_key = json.load(f)

    with open("../.DATA/PROMPT_INFO.json") as f:
        prompt_metadata = json.load(f)

    dynamo_table_name = bucket_info['restore_table_name']

    dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=key['aws_access_key_id'],
            aws_secret_access_key=key['aws_secret_key'],
            region_name=key['region']
        )
    table = dynamodb.Table(dynamo_table_name)
    source_data = scan_with_backoff(table, scan_kwargs)
    # Pagination을 고려하여 모든 데이터를 가져오기
    while True:
        response = table.scan(**scan_kwargs)
        source_data.extend(response.get('Items', []))
        scan_kwargs['ExclusiveStartKey'] = response.get('LastEvaluatedKey', None)
        if not scan_kwargs['ExclusiveStartKey']:
            break

    genai.configure(api_key=gemini_api_key['GEMINI_API'])

    # Create the model
    generation_config = {
        "temperature": 0.7,
        "top_p": 0.95,
        "top_k": 64,
        "max_output_tokens": 8192,
        "response_mime_type": "text/plain",
    }

    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        generation_config=generation_config,
        # safety_settings = Adjust safety settings
        # # See https://ai.google.dev/gemini-api/docs/safety-settings
    )

    chat_session = model.start_chat(
        history=[

        ]
    )

    count = 10
    for i in range(len(source_data) // count):
        _response = [None for _ in range(count)]
        # 비동기 코드 실행
        asyncio.run(send_data_async(source_data[i:i+count],_response))
    asyncio.run(send_data_async(source_data[len(source_data)*count:],_response))
    
async def main():
    dynamo_table_name = bucket_info['restore_table_name']
    dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=key['aws_access_key_id'],
            aws_secret_access_key=key['aws_secret_key'],
            region_name=key['region']
        )
    table = dynamodb.Table(dynamo_table_name)
    source_data = []    
    for i in id_list:
        source_data.append(table.query(Select='ALL_ATTRIBUTES',KeyConditionExpression=Key("id").eq(i))["Items"][0])
    genai.configure(api_key=gemini_api_key['GEMINI_API'])
    
    # Create the model
    generation_config = {
        "temperature": 0.7,
        "top_p": 0.95,
        "top_k": 64,
        "max_output_tokens": 8192,
        "response_mime_type": "text/plain",
    }

    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        generation_config=generation_config,
        # safety_settings = Adjust safety settings
        # # See https://ai.google.dev/gemini-api/docs/safety-settings
    )

    chat_session = model.start_chat(
        history=[

        ]
    )

    count = 10
    try:
        _response = [None for _ in range(count)]
        for i in range(len(source_data) // count):
            # 비동기 코드 실행
            await send_data_async(logger,chat_session,source_data[(i*count):(i+1)*count],_response)
        if len(source_data)%count != 0:
            await send_data_async(logger,chat_session,source_data[-(len(source_data)%count):],_response)
    except Exception as e:
        logger.error(f'{_response} : {e}')
    
if __name__ == '__main__':
    asyncio.run(main())
