import crawling_rocketpunch as cr
import save_json as sj
import pandas as pd
import datetime as dt

def main():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }
    url = 'https://www.rocketpunch.com/api/jobs/template?page={}&job=1'
    
    data_dic = cr.rocketpunch_crawler(url, headers)
    detailed_data = cr.parse_job_page(data_dic, headers)
    
    mm = dt.datetime.today().year
    dd = dt.datetime.today().month
    
    new_hired = []
    for data in detailed_data :
        year, month, day = data['date_start'].split('.')
        if month == mm and day == dd:
            new_hired.append(data)
    
    #sj.save_dataframe(detailed_data)
    sj.save_dataframe(new_hired)
    
if __name__=="__main__":
    main()