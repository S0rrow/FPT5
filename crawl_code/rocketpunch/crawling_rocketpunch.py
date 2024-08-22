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
        time.sleep(2) # for sake of politeness
    
    return data 

# API 호출
# company_id, company_name, job_id, description, job_title, job_career
def parse_page(soup):
    data_list = []
    current_timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
    
    try:
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
        # export log
        utils.log("parsed_page module succeeded", flag=4) # info
    except Exception as e:
        utils.log(f"parsed_page module failed : {e}", flag=1) # error
                
    return data_list

# company_id를 통해서 html 내용 파싱
def parse_job_page(data, headers):
    job_url = 'https://www.rocketpunch.com/jobs/{}'
    pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
    
    try:
        for job in data:
            res = session.get(job_url.format(job['job_id']), headers=headers)
            soup = BS(res.text, 'html.parser')
            job['job_url'] = job_url.format(job['job_id'])
            
            # 채용 시작일/만료일 : date_start, date_end
            deadline_div = soup.find('div', class_='title', text='마감일').find_next('div', class_='content').get_text(strip=True)
            if not pattern.match(deadline_div):
                job['date_end'] = None
            else:
                job['date_end'] = deadline_div
            
            start_div = soup.find('div', class_='title', text='등록일')
            if not start_div:
                job['date_start'] = soup.find('div', class_='title', text='수정일').find_next_sibling('div').get_text(strip=True)
            else:
                job['date_start'] = start_div.find_next_sibling('div').get_text(strip=True)
            
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
        # export log
        utils.log("parse_job_page module succeeded",flag=4) # info
    except Exception as e:
        utils.log(f"parse_job_page module failed : {e}",flag=1) # error
        
    return data

session.close()