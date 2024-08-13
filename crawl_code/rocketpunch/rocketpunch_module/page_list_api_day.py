import requests
from bs4 import BeautifulSoup as BS
import json
import parse_page as pg
import time

#import utils
import sys, os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from src import utils

def rocketpunch_crawler(url, headers):
    try:
        session = requests.Session()
        res = session.get(url.format(1), headers=headers)
        res = json.loads(res.text)
        soup = BS(res['data']['template'], 'html.parser')

        page_size = soup.find('div', {'class': 'tablet computer large screen widescreen only'}).find_all('a', {'class': 'item'})[-1].text.strip()

        data_list = pg.parse_page(soup)

        for i in range(2, int(page_size) + 1):
            res = session.get(url.format(i), headers=headers)
            res = json.loads(res.text)
            soup = BS(res['data']['template'], 'html.parser')
            data_list.extend(pg.parse_page(soup))
            time.sleep(2) # for sake of politeness
        utils.log("page_list_api_day module succeeded",flag=4) # info
    except Exception as e:
        utils.log(f"page_list_api_day module failed : {e}",flag=1) # error
    return data_list