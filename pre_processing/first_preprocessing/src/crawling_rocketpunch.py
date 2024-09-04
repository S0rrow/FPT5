import requests
from bs4 import BeautifulSoup as BS
import json
import time
import datetime
import re

#import utils
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from src import utils

# 세션 객체를 전역에서 정의하여 재사용
session = requests.Session()

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
        time.sleep(2) # for sake of politeness
    
    return data 

# API 호출
# company_id, company_name, job_id, description, job_title, job_career
def parse_page(soup):
    data_list = []
    try:
        for company in soup.find_all('div', {'class': 'company item'}):
            company_data = {}
            company_data['company_id'] = company['data-company_id']
            for content in company.find_all('div', {'class': 'content'}):
                company_data['company_name'] = content.find('a', {'class': 'company-name nowrap header name'}).text.strip()
                company_data['description'] = content.find('div', {'class': 'description'}).text.strip()
                
                for job_detail in content.find_all('div', {'class': 'job-detail'}):
                    job_data = company_data.copy()
                    job_data['job_id'] = job_detail.find('a', {'class': 'nowrap job-title'})['href'].split('/')[2]
                    job_data['job_title'] = job_detail.find('a', {'class': 'nowrap job-title'}).text.strip()
                    job_data['job_career'] = job_detail.find('div', {'class': 'job-stat-info'}).text.strip().split(' / ')
                    job_data['timestamp'] = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
                    job_data['crawl_domain'] = 'www.rocketpunch.com'
                    data_list.append(job_data)
        # export log
        utils.log("parsed_page module succeeded", flag=4) # info
    except Exception as e:
        utils.log(f"parsed_page module failed : {e}", flag=1) # error
                
    return data_list

# company_id를 통해서 html 내용 파싱
def parse_job_page(data, headers):
    job_url = 'https://www.rocketpunch.com/jobs/{}'
    pattern = re.compile('[ㄱ-ㅎ가-힣]+')

    try:
        for job in data:
            res = session.get(job_url.format(job['job_id']), headers=headers)
            soup = BS(res.text, 'html.parser')

            # 주요 업무(업무 내용) : job_task
            job_task_div = soup.find('div', class_='duty break')
            task_span_hidden = job_task_div.find('span', class_='hide full-text') if job_task_div else None
            task_span_short = job_task_div.find('span', class_='short-text') if job_task_div and not task_span_hidden else None
            task_span = task_span_hidden.text if task_span_hidden else (task_span_short.text if task_span_short else "")
            job['job_task'] = task_span.strip() if task_span else ""

            # 업무 기술/활동분야 : job_specialties
            specialties_raw = soup.find('div', class_='job-specialties')
            specialties = [a.text for a in specialties_raw.find_all('a')] if specialties_raw else []
            job['job_specialties'] = ', '.join(specialties)

            # 채용 상세 : job_detail
            detail_div = soup.find('div', class_='content break')
            detail_span_hidden = detail_div.find('span', class_='hide full-text') if detail_div else None
            detail_span_short = detail_div.find('span', class_='short-text') if detail_div and not detail_span_hidden else None
            detail_span = detail_span_hidden.text if detail_span_hidden else (detail_span_short.text if detail_span_short else "")
            job['job_detail'] = detail_span.strip() if detail_span else ""

            # 산업 분야 : job_industry
            industry_div = soup.find('div', class_='job-company-areas')
            industry_text = [a.text for a in industry_div.find_all('a')] if industry_div else []
            job['job_industry'] = ', '.join(industry_text)

            # 채용 시작일/만료일 : date_start, date_end
            job_date = soup.find('div', class_='job-dates')
            date_span = job_date.find_all('span') if job_date else []

            # 수시채용, 상시채용 예외처리
            if any(pattern.search(span.text) for span in date_span):
                job['date_start'] = datetime.datetime.now().strftime('%Y-%m-%d')
                job['date_end'] = None
            else:
                if len(date_span) > 1:
                    job['date_start'] = datetime.datetime.strptime(date_span[0].text.strip(), '%Y.%m.%d').date()
                    job['date_end'] = datetime.datetime.strptime(date_span[1].text.strip(), '%Y.%m.%d').date()
                elif len(date_span) == 1:
                    job['date_start'] = datetime.datetime.strptime(date_span[0].text.strip(), '%Y.%m.%d').date()
                    
        print(job)
        # export log
        utils.log("parse_page module succeeded",flag=4) # info
    except Exception as e:
        utils.log(f"parse_page module failed : {e}",flag=1) # error

    return data