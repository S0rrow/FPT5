import os
from time import gmtime, strftime

def log(msg, flag=None, path="./logs"):
    if flag is None:
        flag = 4
    head = ["DEBUG", "ERROR", "WARN", "STATUS", "INFO"]
    now = strftime("%Y-%m-%d %H:%M:%S", gmtime())

    if not os.path.isdir(path):
        os.mkdir(path)

    log_file = f"{path}/{head[flag]}.log"
    log_message = f"[{now}][{head[flag]}] > {msg}\n"
    
    with open(log_file, "a") as f:
        f.write(log_message)
        
def get_time():
    return strftime("%Y-%m-%d_%H%M%S", gmtime())
