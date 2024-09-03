import requests
from bs4 import BeautifulSoup
from datetime import datetime
import subprocess, os
import re
import time
import pandas as pd
import subprocess, os
from time import gmtime, strftime
import pytz


def log(msg, flag=None, path="./logs"):
    if flag==None:
        flag = 0
    head = ["DEBUG", "ERROR", "WARN", "STATUS", "INFO"]
    now = strftime("%H:%M:%S", gmtime())
    
    if not os.path.isdir(path):
        os.mkdir(path)
    
    if not os.path.isfile(f"{path}/{head[flag]}.log"):
        assert subprocess.call(f"echo \"[{now}][{head[flag]}] > {msg}\" > {path}/{head[flag]}.log", shell=True)==0, print(f"[ERROR] > shell command failed to execute")
    else: assert subprocess.call(f"echo \"[{now}][{head[flag]}] > {msg}\" >> {path}/{head[flag]}.log", shell=True)==0, print(f"[ERROR] > shell command failed to execute")

def get_time():
    kst_tz = pytz.timezone('Asia/Seoul')
    return datetime.strftime(datetime.now().astimezone(kst_tz),"%Y-%m-%d_%H%M%S")



class jobkorea:
    header = {"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"}
    post_header= {"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36","X-Requested-With":"XMLHttpRequest"}
    logger = None
    all_dict = {}
    query_log = []
    error_jobid_list = []
    
    def __init__(self,_logger):
        self.logger = _logger
    
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
        self.logger.info(f"[jobkorea] get job list {flag}")
        if flag == 'daily':
            all_num = 100
        #log(f"query({query} include {all_num} jobs)",4)
        pagenum = (all_num //40) + 1
        for p in range(1,pagenum+1):
            target_url = f'https://m.jobkorea.co.kr/Recruit/JobList/arealist?page={p}&sort=6'
            try:
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
            except Exception as e:
                self.logger.error(f"[jobkorea] get job list{target_url}:{e}")

        base_url = 'https://www.jobkorea.co.kr/Recruit/GI_Read/'
        for job_id in self.all_dict:
            try:
                self.all_dict[job_id]['job_id'] = job_id
                self.all_dict[job_id]['target_url'] = base_url + job_id
                self.post_swipgegiread(job_id)  
            except Exception as e:
                self.logger.error(f"[jobkorea] {base_url + job_id} error ")

                

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
    
def main():
    try:
        bucket_name = 'crawl-data-lake'
        instance = jobkorea()
        instance.get_job(flag='all')
        export_date = get_time()
        df = instance.to_dataframe()
        df['crawl_domain'] = "www.jobkorea.co.kr"
        df['get_date'] = export_date
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
        s3.put_object(Body=df.to_json(orient='records',lines=True,force_ascii=False,date_format='iso'),Bucket=bucket_name,Key=f'jobkorea/data/{export_date}.json')
        
    except Exception as e:
        return {"statusCode": 500, "body": f"Error: {str(e)} "}
    else:
        return {"statusCode": 200, "body": "Data processed successfully"}

if __name__ == "__main__":
    main()
        