import json, os, requests
from time import gmtime, strftime
import streamlit as st
import pandas as pd

class Logger():
    '''
        Logger for generating log messages given in string format to files under given path.
        - path: location directory of log files to be generated at
        - options: logger options getting inputs in dictionary format
            - name(optional): name of source logger is running at. if not set, will call __name__ variable of utils.py
    '''
    path = None
    options = None

    def __init__(self, options:dict=None, path="./logs"):
        self.path = path
        self.options = options

    def log(self, msg:str, flag:int=None, name:str=__name__):
        '''
            Save given log messages according to level of depth as files.
            - flag: logs being printed will be saved according to level of depth given in flag
                - 0: debug
                - 1: error
                - 2: warn
                - 3: status
                - 4: info
            - name(optional): name of source logger is running at. if not set, will call __name__ variable of utils.py
        '''
        options = self.options
        if not name and options.get('name', False):
            name = options.get('name')
        if not flag:
            flag = 0
        head = ["DEBUG", "ERROR", "WARN", "STATUS", "INFO"]
        now = strftime("%Y-%m-%d %H:%M:%S", gmtime())

        if not os.path.isdir(self.path):
            os.mkdir(self.path)

        msg.replace("\n", " ")
        msg.replace("  ", " ")
        log_file = f"{self.path}/{head[flag]}.log"
        if not name:
            log_message = f"[{now}][{head[flag]}]({__name__}) > {msg}\n"
        else:
            log_message = f"[{now}][{head[flag]}]({name}) > {msg}\n"
        
        with open(log_file, "a") as f:
            f.write(log_message)
