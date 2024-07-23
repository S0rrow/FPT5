import requests
from bs4 import BeautifulSoup as BS

# mbc 사회면에서 https 링크를 가져옵니다.
# 현재 최신 일자의 링크만 가져오고 있습니다. (다른 일자도 가져오고 싶은데 어떻게 가져와야 할 지 모름)
# 숫자는 15개 정도만 가져오고 있습니다. 왜 그런지 확인이 필요함
def social_crawling(url):
    social_response = requests.get(url).text
    social_bs = BS(social_response, 'html.parser')
    social_a_tags=social_bs.find_all('a')
    links = [a['href'] for a in social_a_tags if 'href' in a.attrs]
    https_links = [link for link in links if link.startswith('https:')]
    return https_links 

# 테스트 진행
#if __name__=="__main__":
#    url="https://imnews.imbc.com/news/2024/society/"
#    social_crawling(url)