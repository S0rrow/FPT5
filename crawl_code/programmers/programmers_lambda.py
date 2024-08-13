from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup as BS
import awswrangler as wr
import requests, json, boto3
import requests, json, time


def company_code():
    # 기업 크롤링
    url = "https://career.programmers.co.kr/companies"
    r = requests.get(url, timeout=5)
    bs = BS(r.text)
    # 마지막 페이지 추출
    pages = int(bs.find_all('li', class_="page-item")[-2].text)

    # 모든 페이지 기업코드 추출
    hrefs = []
    for i in range(pages):
        url = f"https://career.programmers.co.kr/companies?page={i+1}"
        r = requests.get(url, timeout=5)
        bs = BS(r.text)
        
        jobs_container = bs.find('section', class_='jobs__container')
        col_items = jobs_container.find_all('li', class_='col-item')
        hrefs += [item.find('a', class_='jobs__card').get('href') for item in col_items if item.find('a', class_='jobs__card')]
        r.close()
        
    company_num = [a.split('/')[-1] for a in hrefs]
    return company_num

def get_tagtable():
    '''
    api를 통해 jobCategoryIds 컬럼과 메칭할 jobCategorytag 테이블 생성
    pd.DataFrame() 반환
    '''
    url = "https://career.programmers.co.kr/api/job_positions/job_categories"
    r = requests.get(url, timeout=5)

    tagid=[]
    tagname=[]
    for tag in r.json():
        tagid.append(tag['id'])
        tagname.append(tag['name'])
    r.close()
    return pd.DataFrame({'tagid':tagid, 'tagname':tagname})

def job_id():
    company_number = company_code()
    jobid=[]
    for num in company_number:
        company_url = f"https://career.programmers.co.kr/api/companies/{num}"
        r = requests.get(company_url, timeout=5)
        jobdata = json.loads(r.text)
        r.close()
        jobpositions = jobdata['company']['jobPositions']
        for job in jobpositions:
            jobid.append(job['id'])
        #time.sleep(0.1)
    return jobid

def makedf():
    jobid = job_id()

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
        job_url = f"https://career.programmers.co.kr/api/job_positions/{i}"
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
        #time.sleep(0.1)
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

def lambda_handler(event, context):
    crawl_time = datetime.now().strftime("%Y-%m-%d_%H%M") 
    directory = './programmers_data/'
    #file_path = f"{directory}programmers{crawl_time}.json" #s3://crawl-data-lake/programmers/data/
    
    try:
        df = makedf()
        wr.s3.to_json(df=df, path=f"s3://crawl-data-lake/programmers/data/{crawl_time}.json", orient='records', lines=True, force_ascii=False, date_format='iso')
        return {"statusCode": 200, "body": "Data processed successfully"}
    except Exception as e:
       return {"statusCode": 500, "body": f"Error loading offset: {str(e)} "}
