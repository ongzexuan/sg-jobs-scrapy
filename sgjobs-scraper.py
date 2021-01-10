import json
import logging
import re
import pendulum
import scrapy

from scrapy.crawler import CrawlerProcess
from scrapy.utils import log
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
sg_timezone = pendulum.timezone("Singapore")
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 20, tzinfo=sg_timezone),
    'email': ['ong.zexuan@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily'
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

OUTPUT_DIR="/home/airflow/scrape"

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

class JobsbankPipeline():

    async def process_item(self, item, spider):
        # downsample skills which takes up too much space
        item["skills"] = [{"id": i["id"], "skill": i["skill"]} for i in item["skills"]]
        return item


class JobsbankSpider(scrapy.Spider):
    name = 'jobsbank'
    allowed_domains = ['mycareersfuture.sg', 'mycareersfuture.gov.sg']
    base_url = 'https://api.mycareersfuture.sg/v2/jobs?limit={}&page={}'
    default_max_limit = -1
    results_per_page = 100
    parsed_pages = 0
    start_urls = [base_url.format(results_per_page, 1)]
    crawl_page_limit = -1


    def parse(self, response):
        data = json.loads(response.body)
        results = data.get('results', [])

        # Yield every result on the page
        for entry in results:
            yield entry

        self.parsed_pages += 1

        links = data.get('_links', None)
        if not links:
            return

        if not (links['self']['href'] == links['last']['href']):
            next_page_nums = re.findall(r"page=([0-9]+)&", links['next']['href'])
            if self.crawl_page_limit < 0 or (next_page_nums and int(next_page_nums[0]) <= self.crawl_page_limit):                
                yield scrapy.Request(links['next']['href'], callback=self.parse)

def jobsbank_callable(output_dir, date):

    process = CrawlerProcess(settings={
        "FEEDS": {
            "file://{}/jobsbank-{}.json".format(output_dir, date): {
                "format": "json",
                "encoding": "utf8"                
            }
        },
        "ITEM_PIPELINES": {
            JobsbankPipeline : 100  
        },
        "USER_AGENT": "Mozilla/5.0 (Windows NT 6.1; Win64; x64)",
        "ROBOTSTXT_OBEY" : "False",
        "COOKIES_ENABLED" : "False",
        "LOG_LEVEL": "INFO"
    })
    log.dictConfig({
        "version": 1,
        "disable_existing_loggers": True,
        "loggers": {
            "scrapy": {
                "level": "INFO"
            }
        }
    })

    process.crawl(JobsbankSpider)
    process.start()


dag = DAG(
    'sgjobs-scraper',
    default_args=default_args,
    description='DAG to scrape data from SG job sites',
    schedule_interval=timedelta(days=1),
)

today = "{{ ds }}"
task_scape_jobsbank = PythonOperator(
    task_id='scrape-jobsbank',
    depends_on_past=False,
    python_callable=jobsbank_callable,
    op_args=[OUTPUT_DIR, today],
    dag=dag
)
