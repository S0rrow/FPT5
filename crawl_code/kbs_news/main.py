import os
import argparse
from datetime import datetime
import pandas as pd

from kbs_news import kbs_functions
from src.utils import log


def main():
    parser = argparse.ArgumentParser(description="KBS 뉴스 크롤링")

    # 명령줄 인수 정의
    parser.add_argument('arg1', type=str, help="뉴스 카테고리 인수 ")
    parser.add_argument('arg2', type=str, help="시작일 인수")
    parser.add_argument('arg3', type=str, help="종료일 인수")
    # 명령줄 인수 파싱
    args = parser.parse_args()
    
    log(f"KBS main: Request get from KBS news count. Parameters: {args.arg1}, {args.arg2}, {args.arg3}", 4)
    count_result = kbs_functions.get_kbsNews_count(cate_code=args.arg1, startDate=args.arg2, endDate=args.arg3)
    
    if count_result:
        log(f"KBS main: Request get from KBS news.", 4)
        current_time = datetime.now()
        papers = kbs_functions.get_kbsNews(news_count=count_result, 
                                  cate_code=args.arg1, 
                                  startDate=args.arg2, 
                                  endDate=args.arg3
                                  )
        
        if papers is not False:
            target="./data"
            if not os.path.isdir(target):
                log("KBS main: The data directory doesn't exist. Create ./data directory", 2)
                os.mkdir(target)

            crawl_time = current_time.strftime("%Y-%m-%d_%H%M")
            df = pd.DataFrame(papers)
            df['institution'] = 'KBS'
            df['getDate'] = current_time.strftime("%Y-%m-%d %H:%M:%s")
            new_order = ['institution', 'articleTitle', 'articleContents', 'category', 'regDate', 'getDate']
            df.to_csv(f"./data/kbs_{crawl_time}.csv", 
                        index=False, sep='|', 
                        header=True, 
                        columns=new_order, 
                        encoding='utf-8'
                    )
            log(f"KBS main: Sucess create new file './data/kbs_{crawl_time}.csv'", 4)
        else:
            log(f"KBS main: Crawl passed. Teh number of papers is 0. Parameters: {args.arg1}, {args.arg2}, {args.arg3}", 4)
    else:
        log(f"KBS main: KBS request Failed. The status code is not 200. Parameters: {args.arg1}, {args.arg2}, {args.arg3}", 1)


if __name__ == "__main__":
    main()