import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
import logging 
import re
from datetime import datetime
import subprocess, os
from time import gmtime, strftime
import pandas as pd
import json
import requests

class DGLogger:
    '''
    flag 0:DEBUG, 1:ERROR, 2:WARN, 3:STATUS, 4:INFO
    '''
    path = None

    def __init__(self, path="./logs"):
        self.path = path

    def log(self, msg, flag=None):
        if flag==None:
            flag = 4
        head = ["DEBUG", "ERROR", "WARN", "STATUS", "INFO"]
        now = strftime("%Y-%m-%d %H:%M:%S", gmtime())

        if not os.path.isdir(self.path):
            os.mkdir(self.path)

        if not os.path.isfile(f"{self.path}/{head[flag]}.log"):
            assert subprocess.call(f"echo \"[{now}][{head[flag]}] > {msg}\" > {self.path}/{head[flag]}.log", shell=True)==0, print(f"[ERROR] > shell command failed to execute")
        else: assert subprocess.call(f"echo \"[{now}][{head[flag]}] > {msg}\" >> {self.path}/{head[flag]}.log", shell=True)==0, print(f"[ERROR] > shell command failed to execute")

class LinkedIn:
    logger = DGLogger()
    driver = None
    err_job_list = []
    def DGWebDriverWait(self, _CSS, msg="wait time out error"):
        try:
            WebDriverWait(self.driver,3).until(EC.presence_of_element_located((By.CSS_SELECTOR, _CSS)))
        except:
            self.logger.log(msg,1)
            return False
        return True
    
    def init_selenium(self):
        options = webdriver.ChromeOptions()
        service = webdriver.ChromeService(executable_path='/usr/bin/chromedriver')
        options.add_argument(argument="--headless")
        options.add_argument(argument="--window-size=1920,1080")
        self.driver = webdriver.Chrome(options=options)


    def login_linkedin(self, _user, _pw):
        '''
        if you want this function you must create driver
        '''
        if not self.driver:
            self.logger.log("not set driver",1)
            return False
        self.driver.get("https://www.linkedin.com/checkpoint/lg/sign-in-another-account?trk=guest_homepage-basic_nav-header-signin")
        user = self.driver.find_element(By.CSS_SELECTOR, '#username')
        pw = self.driver.find_element(By.CSS_SELECTOR, '#password')
        user.clear(); pw.clear()
        user.send_keys(_user)
        pw.send_keys(_pw)
        self.driver.find_element(By.CSS_SELECTOR,'#organic-div > form > div.login__form_action_container > button').click()
        self.logger.log("linkedin login",4)
        if "checkpoint" in self.driver.current_url:
            self.logger.log("can't login because puzzle",1)
            return False
        return True

    def logout_linkedin(self):
        self.driver.get("https://www.linkedin.com/m/logout")
        self.logger.log("linkedin logout",4)

    def linkedin_get_jobid(self,_query="f_F=it"):
        with open("metadata.json ") as f:
            keyword = json.load(f)
        
        self.logger.log(f"{_query} get job_id list start",4)
        jobid_list = []
        self.driver.get(f"https://www.linkedin.com/jobs/search/?{_query}&start=0")
        numoflist = re.compile("[0-9]+").findall(self.driver.find_elements(By.CSS_SELECTOR, "div.jobs-search-results-list__subtitle")[0].text.replace(',',''))[0]
        self.logger.log(f"{_query} list number: {int(numoflist)}",4)
        numpage = (int(numoflist) // 25) + 1
        _items = self.driver.find_elements(By.CSS_SELECTOR,"ul.scaffold-layout__list-container > li")
        if len(_items) == 0:
                self.logger.log("Cannot get list of job with this query",1)
        for i in _items:
            jobid_list.append(i.get_attribute('data-occludable-job-id'))
        for p in range(1,numpage):
            self.logger.log(f"{_query} get job_id list {p} page start",4)
            t_url =f"https://www.linkedin.com/jobs/search/?f_I=25&start={p*25}"

            #print list from start to end (25items)
            self.driver.get(t_url)
            _items = self.driver.find_elements(By.CSS_SELECTOR,"ul.scaffold-layout__list-container > li")
            if len(_items) == 0:
                self.logger.log("Cannot get list of job with this query",1)
                break
            for i in _items:
                jobid_list.append(i.get_attribute('data-occludable-job-id'))
            self.logger.log(f"{_query} get job_id list {p} page end",4)
        self.logger.log(f"{_query} Job ID end",4)
        return jobid_list
    
    def linkedin_get_data(self,job_id,contents,sign_in=True):
        '''
        contents 
        '''
        self.logger.log(f"(sign in data) job id: {job_id} get data start",4)
        target_link= f"https://www.linkedin.com/jobs/view/{job_id}/"
        self.driver.get(target_link)
        if sign_in:
            contents["job_id"] = job_id    
            # Title
            _list = self.driver.find_elements(By.CSS_SELECTOR,"h1")
            # error 
            if len(_list) != 1:
                self.logger.log(f"not exist job_id {job_id} data",1)
                self.err_job_list.append(job_id)
                return False
            contents["title"] = _list[0].text
            self.logger.log(f"job_id {job_id} get title",4)
            #overview
            if not self.DGWebDriverWait(".mt2.mb2", f"job_id {job_id} overview open error"):
                self.err_job_list.append(job_id)
                return False
            overview = self.driver.find_element(By.CSS_SELECTOR,".mt2.mb2").text.split('\n')
            contents['wr'] = overview[0]
            contents['etc']= ";".join(overview[1:])
            self.logger.log(f"job_id {job_id} overview",4)
            #tech stack
            #modal up
            if not self.DGWebDriverWait("button.mv5.t-16", f"job_id {job_id} tech modal open error"):
                self.err_job_list.append(job_id)
                return False
            self.driver.find_element(By.CSS_SELECTOR, "button.mv5.t-16").click()
            self.logger.log(f"job_id {job_id} tech modal open",4)
            if not self.DGWebDriverWait(".job-details-skill-match-status-list__unmatched-skill", f"job_id {job_id} tech modal close error"):
                self.err_job_list.append(job_id)
                return False
            self.logger.log(f"job_id {job_id} tech modal open complete",4)
            _list = self.driver.find_elements(By.CSS_SELECTOR,".job-details-skill-match-status-list__unmatched-skill")
            tech_stacks = []
            for i in _list:
                _str = i.text 
                tech_stacks.append(_str[:_str.find("\n")])
            #modal down
            self.driver.find_elements(By.CSS_SELECTOR,".artdeco-button.artdeco-button--circle.artdeco-button--muted.artdeco-button--2.artdeco-button--tertiary.ember-view.artdeco-modal__dismiss")[0].click()
            tech_stacks = ";".join(tech_stacks)
            contents['tech_stacks'] = tech_stacks
            self.logger.log(f"job_id {job_id} get tech data",4)
            if not self.DGWebDriverWait(".job-details-jobs-unified-top-card__primary-description-container", f"job_id {job_id} tech modal close error"):
                self.err_job_list.append(job_id)
                return False
            self.logger.log(f"job_id {job_id} tech modal close",4)
            _tmp = self.driver.find_element(By.CSS_SELECTOR,".job-details-jobs-unified-top-card__primary-description-container").text.split("·")
            if len(_tmp) != 3:
                contents["geo"] = ''
                contents["apt"] = None 
                self.logger.log("not geo,apt", 1)
            else:
                contents["geo"],_,apt = _tmp
                contents["apt"]= int(re.compile("[0-9]+").findall(apt)[0])
                self.logger.log("set geo,apt", 4)
            self.driver.find_element(By.CSS_SELECTOR,".jobs-description__footer-button.t-14.t-black--light.t-bold.artdeco-card__action.artdeco-button.artdeco-button--icon-right.artdeco-button--3.artdeco-button--fluid.artdeco-button--tertiary.ember-view").click()
            if not self.DGWebDriverWait(".jobs-box--fadein.jobs-box--full-width.jobs-box--with-cta-large.jobs-description.artdeco-card.jobs-poster--is-expanded-redesign.mt4", f"job_id {job_id} contents error"):
                self.err_job_list.append(job_id)
                return False
            contents['content'] = self.driver.find_element(By.CSS_SELECTOR,"#job-details > div.mt4").text
            self.driver.find_element(By.CSS_SELECTOR,".jobs-description__footer-button.t-14.t-black--light.t-bold.artdeco-card__action.artdeco-button.artdeco-button--icon-right.artdeco-button--3.artdeco-button--fluid.artdeco-button--tertiary.ember-view").click()
            if not self.DGWebDriverWait(".jobs-box--fadein.jobs-box--full-width.jobs-box--with-cta-large.jobs-description.jobs-description--is-truncated.jobs-description--is-truncated-poster.artdeco-card.mt4", f"job_id {job_id} show less error"):
                self.err_job_list.append(job_id)
                return False
            contents['get_date'] = datetime.today().strftime("%Y-%m-%d_%H:%M:%s")
            #company
            contents['company']= self.driver.find_element(By.CSS_SELECTOR,".ember-view.link-without-visited-state.inline-block.t-black").text
            self.logger.log(f"job id {job_id}get company data", 4)
        else:
            self.driver.get(target_link)
            _list = self.driver.find_elements(By.CSS_SELECTOR,"li.description__job-criteria-item")
            contents["jf"] = _list[2].text[_list[2].text.find("\n")+1:]
            contents["industry"] = _list[3].text[_list[3].text.find("\n")+1:]
            data = {"user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36"}
            soup = BeautifulSoup(requests.get("https://www.linkedin.com/jobs/view/3990451742/",data=data).text)
            _dict = json.loads(soup.find('script',attrs={"type":"application/ld+json"}).text)
            expr=[]
            if "educationRequirements" in  _dict:
                expr.append(_dict["educationRequirements"]['credentialCategory'])
            if "experienceRequirements" in _dict:
                expr.append(str(_dict["experienceRequirements"]['monthsOfExperience']) + " month")
            contents["reqr"] = ";".join(expr)
            contents['dateposted']= _dict['datePosted']
        return True
    
    def printjson(_list,_path):
        pd.DataFrame(_list).to_json(_path)

