import requests
from bs4 import BeautifulSoup as BS
import mbc_category
import pandas as pd

# 카테고리에서 가져온 url을 이용해 필요한 데이터를 추출합니다.
def https_crawling(links):
    news_title=list() # 뉴스 제목
    news_contents=list() # 뉴스 본문
    news_date=list() # 뉴스 일자
    news_organizer='mbc' # 뉴스 플랫폼

    for url in links:
        news_r = requests.get(url).text
        news_bs = BS(news_r, 'html.parser')
        news_title.append(news_bs.find('meta', attrs={'name': 'title'}).get('content'))
        news_date.append(news_bs.find('meta', attrs={'name': 'nextweb:createDate'}).get('content'))
        news_contents.append(news_bs.find("div", class_="news_txt").text.replace("\r", "\n").strip())
    #print(news_title)
    #print("숫자: ", news_title.__len__())
    
    return news_title, news_contents, news_date, news_organizer
        
# def main():
#     url = "https://imnews.imbc.com/news/2024/{}/"
#     # mbc 뉴스에서 정치, 사회, 국제, 경제, 스포츠, 연예 뉴스의 최신 기사를 크롤링합니다.
#     for sector in ['politics','society','world','econo', 'sports', 'enter']:
#         links = mbc_category.social_crawling(url.format(sector))
#         https_crawling(links)
    
# if __name__ == "__main__":
#     main()

