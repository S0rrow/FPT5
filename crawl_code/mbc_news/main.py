import merge_news
import input_news_data 

def main():
    # hadoop 연결
    client = input_news_data.connect_hadoop()
    
    # 하나로 합쳐진 news dataframe 불러오기
    df = merge_news.merge_news()
    
    # 뉴스 기사 중복값 체크 
    input_news_data.duplication_check(client, df)
    
if __name__ == "__main__":
    main()