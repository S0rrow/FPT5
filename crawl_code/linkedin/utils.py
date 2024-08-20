import os
from time import gmtime, strftime

class Logger:
    '''
    flag 0:DEBUG, 1:ERROR, 2:WARN, 3:STATUS, 4:INFO
    '''
    path = None

    def __init__(self, path="./logs"):
        self.path = path

    def log(self, msg, flag:int=None):
        if flag is None:
            flag = 4
        head = ["DEBUG", "ERROR", "WARN", "STATUS", "INFO"]
        now = strftime("%Y-%m-%d %H:%M:%S", gmtime())

        if not os.path.isdir(self.path):
            os.mkdir(self.path)

        log_file = f"{self.path}/{head[flag]}.log"
        log_message = f"[{now}][{head[flag]}] > {msg}\n"
        
        with open(log_file, "a") as f:
            f.write(log_message)
    