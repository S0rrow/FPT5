from jobspy import scrape_jobs
import awswrangler as wr
import pandas as pd
import boto3
import time, os
from time import gmtime, strftime
job_titles = [
    "Software Engineer",
    "Data Scientist",
    "Systems Administrator",
    "Database Administrator",
    "Network Engineer",
    "Cybersecurity Analyst",
    "IT Project Manager",
    "DevOps Engineer",
    "UX UI Designer",
    "Cloud Engineer",
    "Technical Support Specialist",
    "Business Intelligence Analyst"
]

def scrape_it_jobs(site_name, search_term, location, results_wanted, hours_old):
    try:
        jobs = scrape_jobs(
            site_name=[site_name],
            search_term=search_term,
            location=location,
            results_wanted=results_wanted,
            hours_old=hours_old,  # (only Linkedin/Indeed is hour specific, others round up to days old)
            country_indeed=location,  # only needed for indeed / glassdoor
            # linkedin_fetch_description=True  # get full description and direct job url for linkedin (slower)
            # proxies=["208.195.175.46:65095", "208.195.175.45:65095", "localhost"],
        )
    except Exception as e:
        pass
    return jobs
    
        
def get_time():
    return strftime("%Y-%m-%d_%H%M%S", gmtime())

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    try:
        site_name = "linkedin"
        location = "South Korea"
        results_wanted = 100
        hours_old = 72
        prev = pd.DataFrame()
        for job_title in job_titles:
            jobs = scrape_it_jobs(
                site_name=site_name,
                search_term=job_title,
                location=location,
                results_wanted=results_wanted,
                hours_old=hours_old
            )
            prev = pd.concat([prev,jobs])
            time.sleep(5)
        bucket_name = 'crawl-data-lake'
        export_date = get_time()
        wr.s3.to_json(df=prev, path=f"s3://{bucket_name}/linkedin/data/{export_date}.json", orient='records', lines=True, force_ascii=False, date_format='iso')
    except Exception as e:
        return {"statusCode": 500, "body": f"Error: {str(e)} "}
    else:
        return {"statusCode": 200, "body": "Data processed successfully"}