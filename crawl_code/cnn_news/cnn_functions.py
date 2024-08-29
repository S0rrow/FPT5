import os
import requests
from bs4 import BeautifulSoup as BS
import pandas as pd
from datetime import datetime
import re

def get_required_id():
    # 첫 번째 웹 페이지 URL을 지정합니다.
    url1 = 'https://edition.cnn.com/search?q=&from=0&size=10&page=1&sort=newest&types=all&section='
    
    response1 = requests.get(url1)
    soup = BS(response1.text, 'html.parser')
    
    # <script type="text/javascript"> 태그 찾기
    scripts = soup.find_all('script', {'type': 'text/javascript'})
    
    # uuidv4 값을 찾기
    uuidv4_value = None
    for script in scripts:
        script_str = str(script.string)
        pattern = r'uuidv4\(\)'
        match = re.search(pattern, script_str)
        if match:
            uuidv4_value = match.group()

    return uuidv4_value


def make_df():

    # id 토큰 받아오기
    uuidv4_value = get_required_id()
    
    # 두 번째 웹 페이지 URL을 지정
    size = 100
    from_ =  0
    page = 1
    sort = "newest"
    url2 = f'https://search.prod.di.api.cnn.io/content?q=&size={size}&from={from_}&page={page}&sort={sort}&request_id={uuidv4_value}'
    
    # requests 세션을 생성
    session = requests.Session()
    
    # 세션을 유지하며 두 번째 웹 페이지를 가져옴
    response2 = session.get(url2)
    articlelink = response2.json()['result']
    
    # 기사 링크 리스트 생성
    newslink = []
    titlelist = []
    for x in articlelink:
        pathlink = x['path']
        if pathlink.split('/')[-1] == 'index.html' and pathlink.split('/')[-3] != 'live-news':
            newslink.append(pathlink)
            titlelist.append(x['headline'])
        
    # category
    category = []
    for a in newslink:
        category.append(a.split('/')[-3])
        
    
    # 각 링크로 크롤링
    newbs= []
    for link in newslink:
        newurl = link
        r = requests.get(link)
        newbs.append(BS(r.text))
        
        
    # 데이터프레임 준비작업
    articlelist=[]
    regDate=[]
    for x in newbs:    
        # contents
        contentlist = x.find('div', class_="article__content").find_all('p', class_="paragraph inline-placeholder vossi-paragraph-primary-core-light")
        for i, a in enumerate(contentlist):
            contentlist[i] = a.text.strip()
        articlelist.append(' '.join(contentlist))
    
        # updated
        regDate.append(' '.join(x.find('div', class_='timestamp vossi-timestamp-primary-core-light').text.strip().split()[1:]))
    
    cnn_news = {"articleTitle": titlelist, "articleContents": articlelist, "regDate": regDate, "category":category, }
    
    
    # 데이터프레임 생성
    cnn_newsdf = pd.DataFrame(cnn_news)
    cnn_newsdf = cnn_newsdf.drop_duplicates(subset=['articleTitle', 'articleContents']).reset_index(drop=True)
    cnn_newsdf['institution'] = "CNN"
    now = datetime.now().strftime("%Y-%m-%d_%H%M")
    cnn_newsdf['getDate'] = now
    
    return cnn_newsdf


# 하나의 데이터프레임으로 생성
def make_total_df(f_path):
    # 파일 경로를 지정합니다.
    # filepath = "/home/hadoop/repository/news_crawling/"
    filepath = f_path
    
    # 파일이 존재하는지 확인합니다.
    if os.path.exists(filepath):
        files = glob(filepath+'cnn*.csv')
        # 데이터프레임으로 변환 후 리스트생성, 하나의 데이터프레임으로 concat
        dfs = [pd.read_csv(file, sep='|', ) for file in files]
        df = pd.concat(dfs, ignore_index=True)
        df = df.drop_duplicates(subset=['articleTitle', 'articleContents']).sort_values(by=['regDate'], ascending=False).reset_index(drop=True)
        return df
    else:
        print('파일이 존재하지 않습니다.')
        return 0
    

