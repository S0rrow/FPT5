import news_crawling
import mbc_category
import pandas as pd

def merge_news():
    # mbc 크롤링 url
    url = "https://imnews.imbc.com/news/2024/{}/"
    
    # 빈 dataframe 생성
    df = pd.DataFrame(columns=["Title", "Contents", "Date", "Organizer"])
    
    # mbc 뉴스에서 정치, 사회, 국제, 경제, 스포츠, 연예 뉴스의 최신 기사를 크롤링합니다.
    for sector in ['politics','society','world','econo', 'sports', 'enter']:
        links = mbc_category.social_crawling(url.format(sector))
        news_title, news_contents, news_date, news_organizer = news_crawling.https_crawling(links)
        
        # 새로운 데이터를 딕셔너리로 생성
        new_data = {
            "Title": news_title,
            "Contents": news_contents,
            "Date": news_date,
            "Organizer": news_organizer
        }
        
        # 데이터프레임으로 변환
        new_df = pd.DataFrame(new_data)
        
        # 기존 데이터프레임에 새로운 데이터 추가
        df = pd.concat([df, new_df], ignore_index=True)
    #print(df)
    return df
    
# 테스트 진행    
# def main(): 
#     # 기사를 전부 모으고 하나의 데이터 프레임으로 변환
#     merge_news()
    
# if __name__ == "__main__":
#     main()
