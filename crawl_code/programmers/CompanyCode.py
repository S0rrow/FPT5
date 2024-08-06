# 코드 조합
import requests
from bs4 import BeautifulSoup as BS
from tqdm import tqdm
from time import sleep


def company_code():
    # 기업 크롤링
    url = "https://career.programmers.co.kr/companies"
    r = requests.get(url)
    bs = BS(r.text)
    # 마지막 페이지 추출
    pages = int(bs.find_all('li', class_="page-item")[-2].text)

    # 모든 페이지 기업코드 추출
    hrefs = []
    for i in tqdm(range(pages)):
        url = f"https://career.programmers.co.kr/companies?page={i+1}"
        r = requests.get(url)
        bs = BS(r.text)

        jobs_container = bs.find('section', class_='jobs__container')
        col_items = jobs_container.find_all('li', class_='col-item')

        hrefs += [item.find('a', class_='jobs__card').get('href') for item in col_items if item.find('a', class_='jobs__card')]
        
        sleep(0.5)
        
    company_num = [a.split('/')[-1] for a in hrefs]
    
    print("get company_code done")
    
    return company_num