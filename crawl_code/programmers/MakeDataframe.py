import json
import pandas as pd
import requests
from bs4 import BeautifulSoup as BS
from tqdm import tqdm
from time import sleep
import JobCode


def makedf():

    jobid = JobCode.job_id()

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

    for i in tqdm(jobid):
        job_url = f"https://career.programmers.co.kr/api/job_positions/{i}"
        jobr = requests.get(job_url)
        jobbs = json.loads(jobr.text)
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
        sleep(0.5)

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
    
    print("make dataframe done")

    return df