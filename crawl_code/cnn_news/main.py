import os
import sys
from datetime import datetime
import cnn_functions
import input_to_hadoop
import logging
import pathlib



def main():

    BASEDIR = pathlib.Path(__file__).parent.resolve()
    now = datetime.now().strftime("%H:%M:%S")

    log_directory = f"{BASEDIR}/logs"
    # 경로가 없을시 생성
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logging.basicConfig(filename=os.path.join(log_directory, 'cnn_issue.log'), level=logging.INFO)

    try:    
        logging.info(f"[{now}] > Work start")
        
        newsdf = cnn_functions.make_df()
        crawl_time = datetime.now().strftime("%Y-%m-%d_%H%M")
        new_order = ['institution', 'articleTitle', 'articleContents', 'category', 'regDate', 'getDate']

        # 폴더 이름
        folder_name = 'data'

        # 폴더가 없을 경우 생성
        if not os.path.exists("/home/hadoop/"+folder_name):
            os.makedirs("/home/hadoop/"+folder_name)

        newsdf.to_csv(f"/home/hadoop/data/cnn_{crawl_time}.csv", 
                            index=False, sep='|', 
                            header=True, 
                            columns=new_order, 
                            encoding='utf-8'
                        )
        
        
        input_to_hadoop.put_data(newsdf)                    
        logging.info(f"[{now}] > Work finished")

    except Exception as e:
        logging.error(f"[{now}] > {e}", exc_info=True)
        print("check ./logs/cnn_issue.log")
                    
if __name__ == "__main__":
    main()