import requests
from bs4 import BeautifulSoup as BS
import json
import rocketpunch_parse_page as rpg
import time

def rocketpunch_crawler(url, headers):
    session = requests.Session()
    res = session.get(url.format(1), headers=headers)
    res = json.loads(res.text)
    soup = BS(res['data']['template'], 'html.parser')

    page_size = soup.find('div', {'class': 'tablet computer large screen widescreen only'}).find_all('a', {'class': 'item'})[-1].text.strip()

    data_list = rpg.parse_page(soup)

    for i in range(2, int(page_size) + 1):
        res = session.get(url.format(i), headers=headers)
        res = json.loads(res.text)
        soup = BS(res['data']['template'], 'html.parser')
        data_list.extend(rpg.parse_page(soup))
        time.sleep(2) # for sake of politeness

    return data_list