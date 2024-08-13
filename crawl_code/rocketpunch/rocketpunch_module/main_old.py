import parse_page as pg
import page_list_api_day as plad
import parse_job_page as pjp
import save_json as sj

def main():
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'
    }
    url = 'https://www.rocketpunch.com/api/jobs/template?page={}'
    
    data_dic = plad.rocketpunch_crawler(url, headers)
    detailed_data = pjp.parse_job_page(data_dic, headers)
    sj.save_dataframe(detailed_data)
    
if __name__=="__main__":
    main()