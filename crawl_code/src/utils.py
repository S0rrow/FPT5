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

def get_time():
    return strftime("%Y-%m-%d_%H%M%S", gmtime())
