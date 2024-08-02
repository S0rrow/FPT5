import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json


#'https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?keywords=&location=United States&geoId=103644278&f_TPR=r86400&start=50'
def getLinkedinJoblist(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.text)
    _list = []
    for i in soup.findAll('li'):
        # link"
        _list.append(i.find('div',attrs={'class':'base-card relative w-full hover:no-underline focus:no-underline base-card--link base-search-card base-search-card--link job-search-card'}).a['href'])
        # time
        if i.find('time',attrs={'class':'job-search-card__listdate--new'}):
            _list.append(i.find('time',attrs={'class':'job-search-card__listdate--new'})['datetime'])
        else:
            _list.append(i.find('time',attrs={'class':'job-search-card__listdate'})['datetime'])

def init_selenium():
    options = webdriver.ChromeOptions()
    service = webdriver.ChromeService(executable_path='/usr/bin/chromedriver')
    options.add_argument(argument="--headless")
    options.add_argument(argument="--window-size=1920,1080")
    driver = webdriver.Chrome(options=options)
    return driver


def login_linkedin(driver, _user, _pw):
    '''
    if you want this function you must create driver
    '''
    driver.get("https://www.linkedin.com/checkpoint/lg/sign-in-another-account?trk=guest_homepage-basic_nav-header-signin")
    user = driver.find_element(By.CSS_SELECTOR, '#username')
    pw = driver.find_element(By.CSS_SELECTOR, '#password')
    user.clear(); pw.clear()
    user.send_keys(_user)
    pw.send_keys(_pw)
    driver.find_element(By.CSS_SELECTOR,'#organic-div > form > div.login__form_action_container > button').click()

def logout_linkedin(driver):
    driver.get("https://www.linkedin.com/m/logout")
    
def linkedin_get_data(driver,job_id,contents,sign_in=True):
    target_link= f"https://www.linkedin.com/jobs/view/{job_id}/"
    driver.get(target_link)
    if sigin_in:
        contents["job_id"] = job_id    
        # Title
        _list = driver.find_elements(By.CSS_SELECTOR,"h1")
        # error 
        if len(_list) != 1:
            return False
        contents["title"] = _list[0].text    
        #overview
        overview = driver.find_element(By.CSS_SELECTOR,".mt2.mb2").text.split('\n')
        contents['wr'] = overview[0]
        contents['etc']= ";".join(overview[1:])
        #tech stack
        #modal up
        driver.find_elements(By.CSS_SELECTOR, "button.mv5.t-16")[0].click()
        WebDriverWait(driver,3).until(EC.presence_of_element_located((By.CSS_SELECTOR,".job-details-skill-match-status-list__unmatched-skill")))
        _list = driver.find_elements(By.CSS_SELECTOR,".job-details-skill-match-status-list__unmatched-skill")
        tech_stacks = []
        for i in _list:
            _str = i.text 
            tech_stacks.append(_str[:_str.find("\n")])
        #modal down
        driver.find_elements(By.CSS_SELECTOR,".artdeco-button.artdeco-button--circle.artdeco-button--muted.artdeco-button--2.artdeco-button--tertiary.ember-view.artdeco-modal__dismiss")[0].click()
        tech_stacks = ";".join(tech_stacks)
        contents['tech_stacks'] = tech_stacks

        WebDriverWait(driver,3).until(EC.presence_of_element_located((By.CSS_SELECTOR,".job-details-jobs-unified-top-card__primary-description-container")))
        contents["geo"],_,apt = driver.find_element(By.CSS_SELECTOR,".job-details-jobs-unified-top-card__primary-description-container").text.split("·")
        contents["apt"]= int(re.compile("[0-9]+").findall(apt)[0])
        driver.find_element(By.CSS_SELECTOR,".jobs-description__footer-button.t-14.t-black--light.t-bold.artdeco-card__action.artdeco-button.artdeco-button--icon-right.artdeco-button--3.artdeco-button--fluid.artdeco-button--tertiary.ember-view").click()
        WebDriverWait(driver,3).until(EC.presence_of_element_located((By.CSS_SELECTOR,".jobs-box--fadein.jobs-box--full-width.jobs-box--with-cta-large.jobs-description.artdeco-card.jobs-poster--is-expanded-redesign.mt4")))
        contents['content'] = driver.find_element(By.CSS_SELECTOR,"#job-details > div.mt4").text
        driver.find_element(By.CSS_SELECTOR,".jobs-description__footer-button.t-14.t-black--light.t-bold.artdeco-card__action.artdeco-button.artdeco-button--icon-right.artdeco-button--3.artdeco-button--fluid.artdeco-button--tertiary.ember-view").click()
        WebDriverWait(driver,3).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR,".jobs-box--fadein.jobs-box--full-width.jobs-box--with-cta-large.jobs-description.jobs-description--is-truncated.jobs-description--is-truncated-poster.artdeco-card.mt4")))
        contents['get_date'] = datetime.today().strftime("%Y-%m-%d_%H:%M:%s")
        #company
        contents['company']= driver.find_element(By.CSS_SELECTOR,".ember-view.link-without-visited-state.inline-block.t-black").text
    else:
        driver.get(target_link)
        _list = driver.find_elements(By.CSS_SELECTOR,"li.description__job-criteria-item")
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
    return contents

