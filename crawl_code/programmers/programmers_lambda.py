from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup as BS
import awswrangler as wr
import requests, json, boto3


def company_code(payload):
    company_id_url = payload.get('company_id_url')
    page_url = company_id_url + "?page={index}"

    url = "https://career.programmers.co.kr/companies"
    r = requests.get(company_id_url, timeout=5)
    bs = BS(r.text)
    # 마지막 페이지 숫자 추출
    pages = int(bs.find_all('li', class_="page-item")[-2].text) + 1

    # 모든 페이지 기업코드 추출
    hrefs = []
    for i in range(1, pages):
        url = page_url.format(index=i)
        r = requests.get(url, timeout=5)
        bs = BS(r.text)
        
        jobs_container = bs.find('section', class_='jobs__container')
        col_items = jobs_container.find_all('li', class_='col-item')
        hrefs += [item.find('a', class_='jobs__card').get('href') for item in col_items if item.find('a', class_='jobs__card')]
        r.close()
        
    company_num = [a.split('/')[-1] for a in hrefs]
    return company_num

def job_id(payload):
    company_info_url = payload.get('company_info_url')
    company_number = company_code(payload=payload)
    jobid=[]
    for num in company_number:
        company_url = company_info_url.format(num=num)
        r = requests.get(company_url, timeout=5)
        jobdata = json.loads(r.text)
        r.close()
        jobpositions = jobdata['company']['jobPositions']
        for job in jobpositions:
            jobid.append(job['id'])
    return jobid

def makedf(payload):
    job_detail_url = payload.get('job_detail_url')
    jobid = job_id(payload=payload)

    jobcode=[]
    address=[]
    career=[]
    careerRange=[]
    companyId=[]
    jobType=[]
    status=[]
    title=[]
    updatedAt=[]
    jobCategoryIds=[]
    period=[]
    minCareerRequired=[]
    minCareer=[]
    resumeRequired=[]
    endAt=[]
    additionalInformation=[]
    description=[]
    preferredExperience=[]
    requirement=[]
    isAppliable=[]
    technicalTags=[]
    companyname=[]

    for i in jobid:
        job_url = job_detail_url.format(index=i)
        jobr = requests.get(job_url, timeout=5)
        jobbs = json.loads(jobr.text)
        jobr.close()
        try:
            jobcode.append(jobbs['jobPosition']['id'])
            address.append(jobbs['jobPosition']['address'])
            career.append(jobbs['jobPosition']['career'])
            careerRange.append(jobbs['jobPosition']['careerRange'])
            jobType.append(jobbs['jobPosition']['jobType'])
            status.append(jobbs['jobPosition']['status'])
            title.append(jobbs['jobPosition']['title'])
            updatedAt.append(jobbs['jobPosition']['updatedAt'])
            jobCategoryIds.append(jobbs['jobPosition']['jobCategoryIds'])
            period.append(jobbs['jobPosition']['period'])
            minCareerRequired.append(jobbs['jobPosition']['minCareerRequired'])
            minCareer.append(jobbs['jobPosition']['minCareer'])
            resumeRequired.append(jobbs['jobPosition']['resumeRequired'])
            endAt.append(jobbs['jobPosition']['endAt'])
            additionalInformation.append(jobbs['jobPosition']['additionalInformation'])
            description.append(jobbs['jobPosition']['description'])
            preferredExperience.append(jobbs['jobPosition']['preferredExperience'])
            requirement.append(jobbs['jobPosition']['requirement'])
            isAppliable.append(jobbs['jobPosition']['isAppliable'])
            technicalTags.append(', '.join([a['name'] for a in jobbs['jobPosition']['technicalTags']]))
            companyId.append(jobbs['jobPosition']['companyId'])
            companyname.append(jobbs['jobPosition']['company']['name'])
        except: continue

    df = pd.DataFrame({'jobcode':jobcode, 
                        'career':career, 
                        'careerRange':careerRange, 
                        'jobType':jobType, 
                        'status':status, 
                        'title':title, 
                        'updatedAt':updatedAt, 
                        'jobCategoryIds':jobCategoryIds, 
                        'period':period, 
                        'minCareerRequired':minCareerRequired, 
                        'minCareer':minCareer, 
                        'resumeRequired':resumeRequired, 
                        'endAt':endAt, 
                        'additionalInformation':additionalInformation, 
                        'description':description, 
                        'preferredExperience':preferredExperience, 
                        'requirement':requirement, 
                        'isAppliable':isAppliable, 
                        'technicalTags':technicalTags, 
                        'companyId':companyId, 
                        'companyname':companyname, 
                        'address':address, })
    return df

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
    curr_time = datetime.now()
    crawl_time = curr_time.strftime("%Y-%m-%d_%H%M")
    payload_data = event.get('data', {})
    s3_path = payload_data.get('s3_path')
    sqs_url = payload_data.get('sqs_url')
    
    message = {
        "status": "SUCCESS",
        "site_symbol": "PRO",
        "filePath": f"/programmers/data/{crawl_time}.json",
        "completionDate": curr_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        "message": "Data crawl completed successfully.",
        "errorDetails": None
    }
    
    try:
        df = makedf(payload = payload_data)
        wr.s3.to_json(df=df, path=s3_path.format(crawl_time=crawl_time), orient='records', lines=True, force_ascii=False, date_format='iso')
        send_respone = send_sqs_message(sqs_url, message)
        return {"statusCode": 200, "body": f"Data processed successfully. SQSMessageId: {str(send_respone)}"}
    except Exception as e:
        message['status'] = "FAILURE"
        message['filePath'] = None
        message['message'] = "Data crawl failed due to an unspecified error."
        message['errorDetails'] = {
            "errorCode": "UNKNOWN_ERROR",
            "errorMessage": "The crawl process failed unexpectedly."
        }
        send_respone = send_sqs_message(sqs_url, message)
        return {"statusCode": 500, "body": f"Error loading offset: {str(e)} "}
