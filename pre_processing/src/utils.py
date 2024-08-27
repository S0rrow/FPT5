import re, datetime, pytz

import subprocess, os
from time import gmtime, strftime

def log(msg, flag=None, path="./logs"):
    if flag==None:
        flag = 0
    head = ["DEBUG", "ERROR", "WARN", "STATUS", "INFO"]
    now = strftime("%H:%M:%S", gmtime())
    
    if not os.path.isdir(path):
        os.mkdir(path)
    
    if not os.path.isfile(f"{path}/{head[flag]}.log"):
        assert subprocess.call(f"echo \"[{now}][{head[flag]}] > {msg}\" > {path}/{head[flag]}.log", shell=True)==0, print(f"[ERROR] > shell command failed to execute")
    else: assert subprocess.call(f"echo \"[{now}][{head[flag]}] > {msg}\" >> {path}/{head[flag]}.log", shell=True)==0, print(f"[ERROR] > shell command failed to execute")

# 로컬 시간대(UTC+9)로 현재 날짜 설정
def set_curr_kst_time():
    return datetime.datetime.now(pytz.timezone('Asia/Seoul')).date()

# kst timezone 설정
def set_kst_timezone():
    return pytz.timezone('Asia/Seoul')

# 파싱을 위해 unuseal line terminators 제거
def remove_unusual_line_terminators(text):
    return re.sub(r'[\r\u2028\u2029]+', ' ', text)

# 정규 표현식을 사용하여 한글, 영어 알파벳, 숫자, 공백을 제외한 모든 문자를 공백으로 치환
def replace_special_to_space(text, pattern=r'[^a-zA-Z0-9가-힣\s]'):
    return re.sub(pattern, ' ', text)

def remove_multiful_space(text):
    return (' '.join(text.split())).strip()

def change_slash_format(text):
    return text.replace(" /", ",").replace("/", ",")

def change_str_to_timestamp(text):
    if text:
        return str(int(datetime.datetime.strptime(text, "%Y-%m-%d").timestamp()))
    else:
        return None