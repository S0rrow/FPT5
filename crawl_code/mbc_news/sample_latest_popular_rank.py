import requests
from bs4 import BeautifulSoup as BS
import json
import time

url="https://imnews.imbc.com/operate/common/content/popular/rank_imnews.js?"
response = requests.get(url)

#디코딩 에러 해결
response_text = response.content.decode('utf-8-sig')
# JSONP의 callback 부분 제거
data = response_text.lstrip('callback(').rstrip(');')
json_data = json.loads(data)

# 링크 추출
links = [item['Link'] for item in json_data['Data']]

news_title=list()
news_contents=list()
news_date=list()
news_organizer='mbc'

for url in links:
    news_r = requests.get(url).text
    news_bs = BS(news_r, 'html.parser')
    news_title.append(news_bs.find('meta', attrs={'name': 'title'}).get('content'))
    news_date.append(news_bs.find('meta', attrs={'name': 'nextweb:createDate'}).get('content'))
    news_contents.append(news_bs.find("div", class_="news_txt").text.replace("\r", "\n").strip())

print(news_title)

