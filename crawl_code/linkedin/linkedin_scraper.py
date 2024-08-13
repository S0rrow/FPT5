from jobspy import scrape_jobs
from utils import Logger
import time, os
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

def scrape_it_jobs(logger, site_name, search_term, location, results_wanted, hours_old):
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
        logger.log(f"Found {len(jobs)} jobs", 4)
    except Exception as e:
        logger.log(f"Error during scrape jobs: {e}", 1)
    try:
        # Dynamically create the CSV filename
        curdate = time.strftime("%Y%m%d")
        resultpath = "./downloaded_files"
        if not os.path.isdir(resultpath):
            os.mkdir(resultpath)
        filename = f"{resultpath}/{site_name}_{search_term.replace(' ', '_')}_{location.replace(' ', '_')}_{curdate}.json"
        jobs.to_json(filename, orient='records', force_ascii=False)
        logger.log(f"CSV saved as {filename}", 4)
    except Exception as e:
        logger.log(f"Error during jsonizing results: {e}", 1)
        
def main():
    logger = Logger()
    site_name = "linkedin"
    location = "South Korea"
    results_wanted = 100
    hours_old = 72
    for job_title in job_titles:
        logger.log(f"scraping for {job_title}...", 4)
        scrape_it_jobs(
            logger=logger,
            site_name=site_name,
            search_term=job_title,
            location=location,
            results_wanted=results_wanted,
            hours_old=hours_old
        )
        time.sleep(5)
        
if __name__=="__main__":
    main()