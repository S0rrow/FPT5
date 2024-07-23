import os
from datetime import datetime
import pandas as pd

from kbs_news import kbs_functions
from kbs_news.utils import log


def crawl(cate_code, startDate, endDate):
    log(f"KBS main: Request get from KBS news count. Parameters: {cate_code}, {startDate}, {endDate}", 4)
    count_result = kbs_functions.get_kbsNews_count(cate_code=cate_code, startDate=startDate, endDate=endDate)
    
    if count_result:
        log(f"KBS main: Request get from KBS news.", 4)
        current_time = datetime.now()
        papers = kbs_functions.get_kbsNews(news_count=count_result, 
                                  cate_code=cate_code, 
                                  startDate=startDate, 
                                  endDate=endDate
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
                        index=False, sep=';', 
                        header=True, 
                        columns=new_order, 
                        encoding='utf-8'
                    )
            log(f"KBS main: Sucess create new file './data/kbs_{crawl_time}.csv'", 4)
        else:
            log(f"KBS main: Crawling passed. Teh number of papers is 0. Parameters: {cate_code}, {startDate}, {endDate}", 4)
    else:
        log(f"KBS main: KBS request Failed. The status code is not 200. Parameters: {cate_code}, {startDate}, {endDate}", 1)