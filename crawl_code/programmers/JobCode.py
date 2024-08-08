# 회사 페이지에서 채용 공고 코드 추출
import json
import requests
from bs4 import BeautifulSoup as BS
from tqdm import tqdm
from time import sleep
import CompanyCode


def job_id():
    company_number = CompanyCode.company_code()
    jobid=[]
    for num in tqdm(company_number):
        company_url = f"https://career.programmers.co.kr/api/companies/{num}"
        r = requests.get(company_url)
        jobdata = json.loads(r.text)
        jobpositions = jobdata['company']['jobPositions']
        for job in jobpositions:
            jobid.append(job['id'])
        sleep(0.5)

    print("get job_id done")
    
    return jobid