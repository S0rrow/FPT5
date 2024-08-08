import requests
import json
import pandas as pd

def get_tagtable():
    '''
    api를 통해 jobCategoryIds 컬럼과 메칭할 jobCategorytag 테이블 생성
    pd.DataFrame() 반환
    '''
    url = "https://career.programmers.co.kr/api/job_positions/job_categories"
    r = requests.get(url)

    tagid=[]
    tagname=[]
    for tag in r.json():
        tagid.append(tag['id'])
        tagname.append(tag['name'])

    return pd.DataFrame({'tagid':tagid, 'tagname':tagname})
