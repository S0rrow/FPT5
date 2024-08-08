from datetime import datetime, timezone
from tqdm import tqdm
import pandas as pd
import requests, json, os, time

def current_time_in_milliseconds():
    now = datetime.now(timezone.utc)
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    milliseconds_since_epoch = int((now - epoch).total_seconds() * 1000)
    return milliseconds_since_epoch

def get_notices_list(url, curr_time, limit, offset):
    r = requests.get(url.format(time=curr_time, limit=limit, offset=offset))
    r_result = json.loads(r.text)
    if len(r_result['data']) == 0:
        return False
    return [data['id'] for data in r_result['data']]

def get_detail(url, curr_t, position_id):
    try:
        r = requests.get(url.format(id=position_id, time=curr_t))
        detail = json.loads(r.text)['job']
        p = {
            "job_id": detail["id"],
            "due_date": detail["due_time"],
            "position": detail["detail"]["position"],
            "tasks": detail["detail"]["main_tasks"],
            "requirements": detail["detail"]["requirements"],
            "prefer": detail["detail"]["preferred_points"],
            "company_id": detail["company"]["id"]
        }
        return p
    except Exception as e:
        # Changed: Improved error logging to capture more details and handle the undefined `detail` variable issue
        with open("./error.txt", 'a', encoding='utf-8') as file:
            file.write(f"status code: {r.status_code}\n")
            file.write(f"error: {str(e)}\n")
            file.write(f"response: {r.text}\n")
            file.write("\n")
        return None  # Ensure the function returns None in case of an error

def get_positions_info(url_positions, url_detail, limit, offset_max):
    current_time = current_time_in_milliseconds()
    positions_data = []
    for i in tqdm(range(0, offset_max, limit)):
        id_list = get_notices_list(url=url_positions, curr_time=current_time, limit=limit, offset=i)
        if id_list == False:
            break
        details = [get_detail(url=url_detail, curr_t=current_time, position_id=id) for id in tqdm(id_list, desc="Fetching details")]
        # Changed: Filter out None values to ensure only valid data is added
        positions_data += [d for d in details if d is not None]
    return positions_data

def export_json(p_data_list, d_path="./wanted_data"):
    curr_date = datetime.now()
    timestamp = int(curr_date.timestamp())
    # Changed: Include seconds in the export_date to ensure unique file names
    export_date = curr_date.strftime("%Y-%m-%d_%H%M%S")
    df = pd.DataFrame(p_data_list)
    if not os.path.isdir(d_path):
        os.mkdir(d_path)
    
    df['crawl_domain'] = "www.wanted.co.kr"
    df['get_date'] = timestamp
    new_order = ['position', 'tasks', 'requirements', 'prefer', 'due_date', 'job_id', 'company_id', 'crawl_domain', 'get_date']
    df = df[new_order]
    df.to_json(f"{d_path}/wanted_positions_{export_date}.json", 
               orient='records', 
               lines=True, 
               force_ascii=False,
               date_format='iso')

def main():
    wanted_positions_url = "https://www.wanted.co.kr/api/chaos/navigation/v1/results?{time}=&job_group_id=518&country=kr&job_sort=job.latest_order&years=-1&locations=all&limit={limit}&offset={offset}"
    wanted_detail_url = "https://www.wanted.co.kr/api/chaos/jobs/v2/{id}/details?{time}="
    limit = 100
    offset_max = 5000
    target = "./wanted_data"
    
    data = get_positions_info(url_positions=wanted_positions_url, 
                       url_detail=wanted_detail_url, 
                       limit=limit, 
                       offset_max=offset_max)
    
    if data:  # Changed: Ensure data is not empty before exporting
        export_json(p_data_list=data, d_path=target)

if __name__ == "__main__":
    main()
