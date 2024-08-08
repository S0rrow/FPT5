import requests
from bs4 import BeautifulSoup as BS
import datetime

#import utils
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from src import utils

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