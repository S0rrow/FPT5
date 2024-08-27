import requests
from bs4 import BeautifulSoup
from datetime import datetime
import subprocess, os
import re
import time
import pandas as pd
import subprocess, os
from farmhash import FarmHash32 as fhash
import pytz
from utils import set_curr_kst_time,set_kst_timezone

import json, boto3, pytz



def get_time():
    kst_tz = pytz.timezone('Asia/Seoul') # kst timezone 설정
    return str(datetime.strftime(datetime.now().astimezone(kst_tz),"%Y-%m-%d_%H%M%S"))

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    try:
        bucket_name = 'crawl-data-lake'
        instance = jobkorea()
        instance.get_job()
        export_date = get_time()
        df = instance.to_dataframe()
        df['crawl_domain'] = "www.jobkorea.co.kr"
        df['get_date'] = export_date
        wr.s3.to_json(df=df, path=f"s3://{bucket_name}/wanted/data/{export_date}.json", orient='records', lines=True, force_ascii=False, date_format='iso')
    except Exception as e:
        return {"statusCode": 500, "body": f"Error: {str(e)} "}
    else:
        return {"statusCode": 200, "body": "Data processed successfully"}



class jobkorea:
    header = {"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"}
    post_header= {"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36","X-Requested-With":"XMLHttpRequest"}
    all_dict = {}
    query_log = []
    error_jobid_list = []
    def get_job(self, query='duty=1000229%2C1000230%2C1000231%2C1000232%2C1000233%2C1000234%2C1000235%2C1000236%2C1000237%2C1000238%2C1000239%2C1000240%2C1000241%2C1000242%2C1000243%2C1000244%2C1000245%2C1000246&sort=6',flag='daily'):
        if flag == 'all':
            base_url = f'https://m.jobkorea.co.kr/Recruit/JobList/arealist?{query}&page=1'
            response = self.get_url(base_url)
            if response:
                soup = BeautifulSoup(response.text)
                self.query_log.append(query)
            else:
                #log(f"Don't get jobs with this query: {query}",1)
                return
            all_num = int(soup.find("div",attrs={'id':"devNormalListContainer"})['data-agicnt'])
            response.close()
            del soup
        if flag == 'daily':
            all_num = 100
        #log(f"query({query} include {all_num} jobs)",4)
        pagenum = (all_num //40) + 1
        for p in range(1,pagenum+1):
            target_url = f'https://m.jobkorea.co.kr/Recruit/JobList/arealist?page={p}&sort=6'
            
            response = self.get_url(target_url)
            if response:
                soup = BeautifulSoup(response.text)
                _list = soup.find("div",attrs={"class":"list list-recruit list-recruit-badge"}).find_all('li')
                for e in _list:
                    job_id = re.compile("[0-9]+").findall(e.find('a')['href'])[0]
                    _dict = {}
                    _dict["company"] = e.find('div',attrs={"class":"company"}).text 
                    _dict["title"] = e.find('div',attrs={"class":"title"}).text
                    self.all_dict[job_id] = _dict
                    # self.get_giread(job_id)
                response.close()
                del soup
            #else:
                #log(f"{target_url} error",4)
            
        for job_id in self.all_dict:
            self.all_dict[job_id]['job_id'] = job_id
            self.post_swipgegiread(job_id)  

                

    def post_swipgegiread(self,_number):
        target_url = f'https://m.jobkorea.co.kr/Recruit/SwipeGIReadInfo/{_number}'
        response = self.post_url(target_url)
        if not response:
            return
        soup = BeautifulSoup(response.text)
        tmp = soup.find('div',attrs={"id":"rowReceipt"})
        for e in tmp.find_all('div',attrs={"class":"receiptTermDate"}):
            self.all_dict[_number][e.find('div',attrs={"class":"badge"}).text] = e.find('div',attrs={"class":"date"}).text
        for e in tmp.find_all('div',attrs={'class':'field'}):
            self.all_dict[_number][e.find('div',attrs={'class':'label'}).text.strip()] = e.find('div',attrs={'class':'value'}).text.strip()
        tmp = soup.find('div',attrs={"id":"rowGuidelines"})
        for e in tmp.find_all('div',attrs={'class':'field'}):
            self.all_dict[_number][e.find('div',attrs={'class':'label'}).text.strip()] = e.find('div',attrs={'class':'value'}).text.strip()
        tmp = soup.find('div',attrs={"id":"rowCompany"})
        for e in tmp.find_all('div',attrs={'class':'field'}):
            self.all_dict[_number][e.find('div',attrs={'class':'label'}).text.strip()] = e.find('div',attrs={'class':'value'}).text.strip()
        response.close()
        del soup

            
    def get_giread(self,_number):
        response = self.get_url(f"https://www.jobkorea.co.kr/Recruit/GI_Read/{_number}")
        soup = BeautifulSoup(response.text)
        if not response and not soup.find('meta',attrs={"name":"description"}):
            return
        self.all_dict[_number]['description']=soup.find('meta',attrs={"name":"description"})
        self.all_dict[_number]['keywards'] = soup.find('meta',attrs={"name":"keywords"})
        response.close()
        del soup

    def get_url(self,url):
        time.sleep(1)
        try:
            r = requests.get(url,headers=self.header, timeout=3)
        except Exception as e:
            #log(f"request get {url} error {e}",1)
            return None 
        else:
            #log(f"request get : {url} status_conde: {r.status_code}",4)
            return r
    def post_url(self,url):
        try:
            r = requests.post(url,headers=self.post_header,timeout=3)
        except Exception as e:
            #log(f"request post {url} error {e}",1)
            return None 
        else:
            #log(f"request post :{url} status_conde: {r.status_code}",4)
            return r
    
    def to_dataframe(self):
        _list = []
        for i in self.all_dict:
            _list.append(self.all_dict[i])
        return pd.DataFrame(_list)

    def pre_processing_first(self,json_object):
        df = pd.DataFrame(json_object)
        result = pd.DataFrame()
        # jobkrea title
        result['job_title'] = df['title'].apply(lambda x: ' '.join(re.sub(r'[^.,/\-+()\s\w]',' ',x.replace('\\/','/')).split()))
        # jobkorea id
        result['job_id'] =  df['job_id']
        #company_name
        result['company_name'] = df['company']
        # 모집분야
        result['job_tasks'] = df['모집분야'].apply(lambda x: ' '.join(re.sub(r'[^.,/\-+()\s\w]',' ',x.replace('\\/','/')).split()) if x != None else x)
        # 스킬
        result['stacks'] = df['스킬'].apply(lambda x: x.replace('\\/','/') if x !=None else x) 
        #산업
        result['job_category'] = df['산업'].apply(lambda x: x.replace('\\/','/') if x !=None else x)
        # 주요사업
        result['indurstry_type'] = df['주요사업'].apply(lambda x: x.replace('\\/','/') if x !=None else x) 
        #시작
        result['start_date'] = df['시작'].apply(lambda x: str(int(datetime.strptime('-'.join(list(map(lambda y: re.findall(r'[0-9]+', y)[0],x.split('.')))),'%Y-%m-%d').timestamp())) if x != None else x)

        #마감
        result['end_date'] = df['마감'].apply(lambda x: str(int(datetime.strptime('-'.join(list(map(lambda y: re.findall(r'[0-9]+', y)[0],x.split('.')))),'%Y-%m-%d').timestamp())) if x != None else x)
        #경력

        result['required_career'] = df['경력'].apply(lambda x: (False if x.find('신입')else True) if x !=None else x)
        result['resume_required'] = df['이력서'].apply(lambda x: False if x==None else True)
        result['get_date'] = df['get_date'].apply(lambda x: int(datetime.strptime(x,"%Y-%m-%d_%H%M%S").timestamp()))
        result['crawl_url']=df['target_url'].str.replace('\\/','/')
        # jobkorea symbol create
        result['site_symbol'] = "JK"
        result['id'] = result.apply(lambda x: fhash(f'{x[13]}{x[2]}{x[1]}'),axis=1)
        del df
        return result
    
def get_bucket_metadata(s3_client, pull_bucket_name,target_folder_prefix):
    # 특정 폴더 내 파일 목록 가져오기
    response = s3_client.list_objects_v2(Bucket=pull_bucket_name, Prefix=target_folder_prefix, Delimiter='/')
    curr_date = set_curr_kst_time()
    kst_tz = set_kst_timezone()

    # curr_date 보다 날짜가 늦은 data josn 파일 metadata 객체 분류
    if 'Contents' in response:
        return [obj for obj in response['Contents']]
    else:
        #print("No objects found in the folder.")
        return None

def upload_data(records, key, push_table_name):
    # DynamoDB 클라이언트 생성
    records = records.to_dict(orient="records")
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )
    table = dynamodb.Table(push_table_name)
    for item in records:
        table.put_item(Item=item)
    
    
def main():

    # S3 client 생성에 필요한 보안 자격 증명 정보 get
    with open("./API_KEYS.json", "r") as f:
        key = json.load(f)

    # S3 버킷 정보 get
    with open("./DATA_SRC_INFO.json", "r") as f:
        bucket_info = json.load(f)
    # S3 섹션 및 client 생성
    session = boto3.Session(
        aws_access_key_id=key['aws_access_key_id'],
        aws_secret_access_key=key['aws_secret_key'],
        region_name=key['region']
    )

    s3 = session.client('s3')

    # S3 버킷 정보 init
    pull_bucket_name = bucket_info['pull_bucket_name']
    push_table_name = bucket_info['restore_table_name']
    target_folder_prefix = bucket_info['target_folder_prefix']['jobkorea_path']
    # 특정 폴더 내 파일 목록 가져오기
    # TODO: 
    # - 마지막 실행일(년,월,일)을 datetime으로 저장한 파일을 읽어들여 curr_date에 적용하기; 당담: 유정연
    # response = s3.list_objects_v2(Bucket=push_bucket_name, Prefix='data/', Delimiter='/')
    kst_tz = pytz.timezone('Asia/Seoul') # kst timezone 설정

    metadata_list = get_bucket_metadata(s3,pull_bucket_name,target_folder_prefix)
    # meatadata_list[0] is directory path so ignore this item
    for i in metadata_list[1:]:
        _key= i["Key"]
        copy_source = {"Bucket":"crawl-data-lake","Key":_key}
        s3.copy(copy_source,"crawl-data-archive","jobkorea/data/")
        #s3.delete_object(Bucket='string',Key='string')
    all_data = pd.DataFrame()
    for obj in metadata_list[1:]:
        try:
            response = s3.get_object(Bucket=pull_bucket_name, Key=obj['Key'])
            json_context = response['Body'].read().decode('utf-8')
            cleaned_text = re.sub(r'[\r\u2028\u2029]+', ' ', json_context) # 파싱을 위해 unuseal line terminators 제거
            json_list = [json.loads(line) for line in cleaned_text.strip().splitlines()] # pandas format으로 맞추기
            instance = jobkorea()
            result = instance.pre_processing_first(json_list)
            all_data = pd.concat([all_data,result])
        except Exception as e:
            pass
        if len(all_data):
            upload_data(all_data,key,push_table_name)

if __name__ == "__main__":
    main()