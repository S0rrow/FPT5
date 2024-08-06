# 폴더로 생성후 저장
import os
from datetime import datetime
import MakeDataframe
# import input_to_hadoop
import logging
import pathlib


def main():

    BASEDIR = pathlib.Path(__file__).parent.resolve()
    now = datetime.now().strftime("%H:%M:%S")

    log_directory = f"{BASEDIR}/logs"
    # 경로가 없을시 생성
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    logging.basicConfig(filename=os.path.join(log_directory, 'issues.log'), level=logging.INFO)

    try:    
        logging.info(f"[{now}] > Work start")

        df = MakeDataframe.makedf()
        crawl_time = datetime.now().strftime("%Y-%m-%d_%H%M")
#         new_order = ['institution', 'articleTitle', 'articleContents', 'category', 'regDate', 'getDate']

        # 폴더 이름
        directory = './programmers_data/'

        # 폴더가 없을 경우 생성
        if not os.path.exists(directory):
            os.makedirs(directory)
    
        # json 형식으로 저장
        file_path = f"{directory}programmers{crawl_time}.json" 
        df.to_json(file_path)
        print(f"DataFrame has been saved to {file_path}")
        
        
#         input_to_hadoop.put_data(newsdf)  
        now = datetime.now().strftime("%H:%M:%S")
        logging.info(f"[{now}] > Work finished")

    except Exception as e:
        now = datetime.now().strftime("%H:%M:%S")
        logging.error(f"[{now}] > {e}", exc_info=True)
        print("check ./logs/issues.log")
                    
if __name__ == "__main__":
    main()
