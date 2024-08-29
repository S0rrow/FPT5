from datetime import datetime, timezone
import pandas as pd
import awswrangler as wr
import requests, json, boto3

def current_time_in_milliseconds():
    now = datetime.now(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    milliseconds_since_epoch = int((now - epoch).total_seconds() * 1000)
    return milliseconds_since_epoch

def get_notices_list(url, curr_time, limit, offset):
    r = requests.get(url.format(time=curr_time, limit=limit, offset=offset))
    r_result = json.loads(r.text)
    if len(r_result['data']) == 0:
        return False
    return [data['id'] for data in r_result['data']]

def get_detail(url, curr_t, position_id):
    try:
        r = requests.get(url.format(id=position_id, time=curr_t))
        detail = json.loads(r.text)['job']
        p = {
            "job_id": detail["id"],
            "due_date": detail["due_time"],
            "position": detail["detail"]["position"],
            "tasks": detail["detail"]["main_tasks"],
            "requirements": detail["detail"]["requirements"],
            "prefer": detail["detail"]["preferred_points"],
            "company_id": detail["company"]["id"]
        }
        return p
    except Exception as e:
        return None  # Ensure the function returns None in case of an error

def get_positions_info(url_positions, url_detail, limit, offset_max):
    current_time = current_time_in_milliseconds()
    positions_data = []
    for i in range(0, offset_max, limit):
        id_list = get_notices_list(url=url_positions, curr_time=current_time, limit=limit, offset=i)
        if id_list == False:
            break
        details = [get_detail(url=url_detail, curr_t=current_time, position_id=id) for id in id_list]
        positions_data += [d for d in details if d is not None]
    return positions_data

def load_offset(s3_client, bucket_name, file_key):
    try:
        # S3에서 파일 읽기
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        
        # 파일 내용 읽기
        file_content = response['Body'].read().decode('utf-8')
        
        # JSON 데이터 파싱
        data = json.loads(file_content)
        return (data['positions_url'], data['detail_url'], data['limit'], data['offset_max'])
    except Exception as e:
        print("Error retrieving or processing file:", e)
        raise


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'crawl-data-lake'
    file_key = 'wanted/wanted_offset.json'
    
    try:
        result = load_offset(s3_client=s3, bucket_name=bucket_name, file_key=file_key)
        if len(result) != 4:
            raise ValueError(f"Unexpected number of values returned from load_offset")

        wanted_positions_url, wanted_detail_url, limit, offset_max = result
    
        data = get_positions_info(url_positions=wanted_positions_url, url_detail=wanted_detail_url, limit=limit, offset_max=offset_max)
    
        try:
            curr_date = datetime.now()
            export_date = curr_date.strftime("%Y-%m-%d_%H%M%S")
            new_order = ['position', 'tasks', 'requirements', 'prefer', 'due_date', 'job_id', 'company_id', 'crawl_domain', 'get_date']
    
            df = pd.DataFrame(data)
            df['crawl_domain'] = "www.wanted.co.kr"
            df['get_date'] = int(curr_date.timestamp())
            df = df[new_order]
            wr.s3.to_json(df=df, path=f"s3://{bucket_name}/wanted/data/{export_date}.json", orient='records', lines=True, force_ascii=False, date_format='iso')
        except Exception as e:
            return {"statusCode": 500, "body": f"Error processing data: {str(e)}"}
    
        return {"statusCode": 200, "body": "Data processed successfully"}

    except Exception as e:
        return {"statusCode": 500, "body": f"Error loading offset: {str(e)} "}