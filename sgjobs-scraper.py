
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['ong.zexuan@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
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

OUTPUT_DIR="~/scrape"

dag = DAG(
    'sgjobs-scraper',
    default_args=default_args,
    description='DAG to scrape data from SG job sites',
    schedule_interval=timedelta(days=1),
)

task_scape_jobsbank = BashOperator(
    task_id='scrape-jobsbank',
    depends_on_past=False,
    bash_command='scrapy crawl jobsbank -o {}/{{ ds }}.csv'.format(OUTPUT_DIR),
    retries=2,
    dag=dag,
)
