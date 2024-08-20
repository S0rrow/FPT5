import crawling_rocketpunch as cr
import save_json as sj

def main():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }
    url = 'https://www.rocketpunch.com/api/jobs/template?page={}'
    
    data_dic = cr.rocketpunch_crawler(url, headers)
    detailed_data = cr.parse_job_page(data_dic, headers)
    # 전체 코드 적재
    sj.save_dataframe(detailed_data)
    
    # 추가된 코드 적재
    
    
if __name__=="__main__":
    main()