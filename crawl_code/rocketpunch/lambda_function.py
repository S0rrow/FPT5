from bs4 import BeautifulSoup as BS
import time, datetime
import pandas as pd
import awswrangler as wr
import json, re, requests, boto3

# 세션 객체를 전역에서 정의하여 재사용
session = requests.Session()

'''
가장 먼저 실행하는 함수. 
채용 목록에서 api를 호출
'''
def rocketpunch_crawler(url, headers):
    res = session.get(url.format(1), headers=headers)
    res = json.loads(res.text)
    soup = BS(res['data']['template'], 'html.parser')

    page_size = soup.find('div', {'class': 'tablet computer large screen widescreen only'})\
                .find_all('a', {'class': 'item'})[-1].text.strip()

    data = parse_page(soup)
    
    for i in range(2, int(page_size) + 1):
        res = session.get(url.format(i), headers=headers)
        res = json.loads(res.text)
        soup = BS(res['data']['template'], 'html.parser')
        data.extend(parse_page(soup))
        #parse_job_page(data, headers)
        time.sleep(1) # for sake of politeness
    
    return data 

'''
2번째 호출 함수
API 호출
company_id, company_name, job_id, description, job_title, job_career
파싱
'''
def parse_page(soup):
    data_list = []
    current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    
    companies = soup.find_all('div', {'class': 'company item'})
    
    for company in companies:
        company_id = company['data-company_id']
        content = company.find('div', {'class': 'content'})
        company_name = content.find('a', {'class': 'company-name nowrap header name'}).text.strip()
        description = content.find('div', {'class': 'description'}).text.strip()
        
        job_details = content.find_all('div', {'class': 'job-detail'})
        
        for job_detail in job_details:
            job_id = job_detail.find('a', {'class': 'nowrap job-title'})['href'].split('/')[2]
            job_title = job_detail.find('a', {'class': 'nowrap job-title'}).text.strip()
            job_career = job_detail.find('div', {'class': 'job-stat-info'}).text.strip().split(' / ')
            
            job_data = {
                'company_id': company_id,
                'company_name': company_name,
                'description': description,
                'job_id': job_id,
                'job_title': job_title,
                'job_career': job_career,
                'timestamp': current_timestamp,
                'crawl_domain': 'www.rocketpunch.com'
            }
            
            data_list.append(job_data)
    return data_list

'''
3번째 호출 함수.
job_id를 통해서 채용 공고 상세란에서 html 내용 파싱
date_end, date_start, job_task, job_specialties, job_detail, job_industry
파싱
'''
def parse_job_page(data, headers):
    job_url = 'https://www.rocketpunch.com/jobs/{}'
    pattern = re.compile('[ㄱ-힣]+')
    current_year = datetime.datetime.now().year

    for job in data:
        res = session.get(job_url.format(job['job_id']), headers=headers)
        soup = BS(res.text, 'html.parser')
        job['job_url'] = job_url.format(job['job_id'])
        
        # 채용 시작일/만료일 : date_start, date_end
        job_date = soup.find('div', class_='job-dates')
        date_span = job_date.find_all('span') if job_date else []
        only_date_span = [re.sub(pattern, '', span.text) for span in date_span]
        
        valid_date = []
        for mmdd in only_date_span:
            mmdd = mmdd.strip()
            if mmdd == "" :
                valid_date.append('Null')
            else:
                date_obj = datetime.datetime.strptime(f'{current_year}/{mmdd.strip()}', '%Y/%m/%d')
                formatted_date = date_obj.strftime('%Y.%m.%d')
                valid_date.append(formatted_date)
                
        job['date_end'] = valid_date[0]
        job['date_start'] = valid_date[-1]
        
        # 주요 업무(업무 내용) : job_task
        job_task_div = soup.find('div', class_='duty break')
        task_span_hidden = job_task_div.find('span', class_='hide full-text') if job_task_div else None
        task_span_short = job_task_div.find('span', class_='short-text') if job_task_div and not task_span_hidden else None
        task_span = task_span_short if task_span_short else task_span_hidden
        if task_span == None:
            task_span = job_task_div.get_text(strip=True)
            job['job_task'] = task_span
        else:
            job['job_task'] = task_span.get_text(strip=True) if task_span else ""
        
        # 업무 기술/활동분야 : job_specialties
        specialties_raw = soup.find('div', class_='job-specialties')
        specialties = [a.text for a in specialties_raw.find_all('a')] if specialties_raw else []
        job['job_specialties'] = ', '.join(specialties)
        
        # 채용 상세 : job_detail
        detail_div = soup.find('div', class_='content break')
        detail_span_hidden = detail_div.find('span', class_='hide full-text') if detail_div else None
        detail_span_short = detail_div.find('span', class_='short-text') if detail_div and not detail_span_hidden else None
        detail_span = detail_span_short if detail_span_short else detail_span_hidden
        if detail_span == None:
            detail_span = detail_div.get_text(strip=True)
            job['job_detail'] = detail_span
        else:
            job['job_detail'] = detail_span.get_text(strip=True) if task_span else ""
            
        # 산업 분야 : job_industry
        industry_div = soup.find('div', class_='job-company-areas')
        industry_text = [a.text for a in industry_div.find_all('a')] if industry_div else []
        job['job_industry'] = ', '.join(industry_text)
        
    return data

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
    payload = event.get('data', {})
    s3_path = payload.get('s3_path')
    sqs_url = payload.get('sqs_url')
    crawl_time = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }
    url = 'https://www.rocketpunch.com/api/jobs/template?page={}&job=1'
    
    message = {
        "status": "SUCCESS",
        "site_symbol": "RP",
        "filePath": f"/rocketpunch/data/{crawl_time}.json",
        "completionDate": crawl_time,
        "message": "Data crawl completed successfully.",
        "errorDetails": None
    }
    
    try:
        data_dic = rocketpunch_crawler(url, headers)
        detailed_data = parse_job_page(data_dic, headers)
        
        ##금일자 새로 올라온 채용공고만 따로 저장
        mm = datetime.datetime.today().year
        dd = datetime.datetime.today().month
    
        new_hired = []
        for data in detailed_data :
            year, month, day = data['date_start'].split('.')
            if month == mm and day == dd:
                new_hired.append(data)
        
        # 전체 데이터 저장
        #df = pd.DataFrame(detailed_data)
        
        #금일자 데이터 저장
        df = pd.DataFrame(new_hired)
        
        wr.s3.to_json(df=df, path=f"s3://crawl-data-lake/rocketpunch/data/{crawl_time}.json", orient='records', lines=True, force_ascii=False, date_format='iso')
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
   
session.close()

