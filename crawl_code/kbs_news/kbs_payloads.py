import json

header_path = "./API_KEYS/HEADER_INFO.json"
kbs_url_path = "./API_KEYS/KBS_URL.json"

with open(header_path, 'r') as header_f:
    header_key = json.load(header_f)

with open(kbs_url_path, 'r') as kbs_f:
    kbs_url = json.load(kbs_f)
    
kbs_news_count_url = kbs_url['kbs_news_count']
kbs_news_get_url = kbs_url['kbs_news_get']

payload_kbsCount = {
    'datetimeBegin': '20240517000000',
    'datetimeEnd': '20240518000000',
    'contentsCode': 'ALL',
    'localCode': '00'
    }
payload = {
    'currentPageNo': '1',
    'rowsPerPage': '12',
    'exceptPhotoYn': 'Y',
    'datetimeBegin': '20240517000000',
    'datetimeEnd': '20240518000000',
    'contentsCode': 'ALL',
    'localCode': '00'
    }
# 정치 경제 사회 국제 과학
cate_list = {
    "political": '0003', 
    "economic": '0004', 
    "social": '0005', 
    "international": '0006', 
    "science": '0007', 
    "all": 'ALL'
    }