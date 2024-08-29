import json, argparse
import pyarrow as pa

from hdfs_sender.hdfs_functions import *
from src.utils import log

hdfs_info_path = "./API_KEYS/HDFS_INFO.json"
with open(hdfs_info_path, 'r') as header_f:
    hdfs_info = json.load(header_f)


def main():
    parser = argparse.ArgumentParser(description="수집한 csv 파일 hdfs에 전송")

    # 명령줄 인수 정의
    parser.add_argument('start_prefix', type=str, help="대상 파일의 시작 문자열. 수집한 기관의 csv을 대상으로함")
    args = parser.parse_args()
    
    try:
        hdfs_connection = connect_hdfs(hdfs_info)
        upload_csv_files_to_hdfs(start_prefix=args.start_prefix, 
                                local_dir=hdfs_info["local_file_dir"], 
                                hdfs_dir=hdfs_info["hdfs_file_dir"],
                                hdfs=hdfs_connection)
        remove_files(start_prefix=args.start_prefix,
                     end_prefix=".csv",
                     local_dir=hdfs_info["local_file_dir"])
        
        log(f"Success transfort csv files to hdfs.", 4)
        return True
        
    except Exception as e:
        print(f"Failed sending csv to hdfs. Check logs : {e}")
        log(f"Failed sending csv to hdfs. Check logs : {e}", 1)
        return False

    
if __name__ == "__main__":
    main()