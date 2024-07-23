# news_crawling에서 가져온 내용을 datalake에 저장합니다.

import merge_news
import pyhdfs # hdfs 연결
import datetime
from hdfs import InsecureClient
import io
import pandas as pd
import subprocess

# hadoop 연결
def connect_hadoop():
    namenode_host = '192.168.0.160'
    namenode_port = 50070
    return pyhdfs.HdfsClient(hosts=f'{namenode_host}:{namenode_port}')

# hadoop에 데이터 밀어넣기
def input_hadoop(client, df, hdfs_path):
    # DataFrame을 CSV로 변환하고 메모리에 저장
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep='|', index=False)
    csv_data = csv_buffer.getvalue()
    
    # CSV 데이터를 바이트로 변환
    csv_bytes = bytes(csv_data, encoding='utf-8')
    client.create(hdfs_path, csv_bytes, overwrite=True) # hdfs에 저장
    
    #print(f"DataFrame saved to {hdfs_data}")

# 중복 체크 / 파일이 없을 시 hadoop에 밀어넣기
def duplication_check(client, df):
    hdfs_data = datetime.datetime.now().strftime("mbc_%Y-%m-%d_%H%M")
    hdfs_path = f'/P3T5/{hdfs_data}.csv'
    # 기존 값이 있을 시 중복 체크
    if client.exists(hdfs_path):
        #print("yes")
        command = ["hdfs", "dfs", "-rm", "-r", hdfs_path]
        subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        input_hadoop(client, df, hdfs_path)
        # hdfs에서 기존 파일 읽어오기
        # with client.open(hdfs_path) as r:
        #     df_hdfs = pd.read_csv(r)
        # # hdfs와 현재 df의 중복값 확인
        # merged_df = pd.merge(df_hdfs, df, how='outer', indicator=True)
        # if merged_df.count() != 0:
        #     new_rows = merged_df[merged_df['_merge'] == 'left_only'].drop('_merge', axis=1)
        #     # 중복되지 않은 값만 추가
        #     df_combined = pd.concat([df, new_rows], ignore_index=True)
        #     input_hadoop(client, df_combined, hdfs_path)
        #else :
        #    print("No added news")
    # 기존 값이 없을 시 데이터 밀어넣음
    else:
        input_hadoop(client, df, hdfs_path)

# def main():
#     # hadoop 연결
#     client = connect_hadoop()
    
#     # 하나로 합쳐진 news dataframe 불러오기
#     df = merge_news.merge_news()
    
#     # 뉴스 기사 중복값 체크 
#     duplication_check(client, df)
    
# if __name__ == "__main__":
#     main()
