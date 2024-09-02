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
            "company_id": detail["company"]["id"],
            "company_name": detail["company"]["name"]
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

def send_sqs_message(sqs_url, message):
    sqs = boto3.client('sqs')
    try:
        message_body = json.dumps(message)
        response = sqs.send_message(
            QueueUrl=sqs_url,
            MessageBody=message_body
        )
        return response['MessageId']
    except Exception as e:
        raise e

def lambda_handler(event, context):
    bucket_name = 'crawl-data-lake'
    
    try:
        payload = event.get('data', {})
        wanted_positions_url = payload.get('positions_url')
        wanted_detail_url = payload.get('detail_url')
        limit = payload.get('limit')
        offset_max = payload.get('offset_max')
        sqs_url = payload.get('sqs_url')
    
        data = get_positions_info(url_positions=wanted_positions_url, url_detail=wanted_detail_url, limit=limit, offset_max=offset_max)
    
        try:
            curr_date = datetime.now()
            export_date = curr_date.strftime("%Y-%m-%d_%H%M%S")
            new_order = ['position', 'tasks', 'requirements', 'prefer', 'due_date', 'job_id', 'company_id', 'company_name', 'crawl_domain', 'get_date']
            message = {
                "status": "SUCCESS",
                "site_symbol": "WAN",
                "filePath": f"/wanted/data/{export_date}.json",
                "completionDate": curr_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "message": "Data crawl completed successfully.",
                "errorDetails": None
            }
            
            df = pd.DataFrame(data)
            df['crawl_domain'] = "www.wanted.co.kr"
            df['get_date'] = int(curr_date.timestamp())
            df = df[new_order]
            wr.s3.to_json(df=df, path=f"s3://{bucket_name}/wanted/data/{export_date}.json", orient='records', lines=True, force_ascii=False, date_format='iso')
            send_respone = send_sqs_message(sqs_url, message)
        except Exception as e:
            message['status'] = "FAILURE"
            message['filePath'] = None
            message['message'] = "Data crawl failed due to an unspecified error."
            message['errorDetails'] = {
                "errorCode": "UNKNOWN_ERROR",
                "errorMessage": "The crawl process failed unexpectedly."
            }
            send_respone = send_sqs_message(sqs_url, message)
            return {"statusCode": 500, "body": f"Error processing data: {str(e)}"}

        return {"statusCode": 200, "body": f"Data processed successfully. SQSMessageId: {str(send_respone)}"}

    except Exception as e:
        return {"statusCode": 500, "body": f"Error loading offset: {str(e)} "}