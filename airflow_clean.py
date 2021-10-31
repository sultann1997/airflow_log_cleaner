from builtins import range
from datetime import timedelta
import datetime as dt
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
    'email': ['my_email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'owner': 'airflow_user',
    'start_date': dt.datetime(2021, 7, 26),
}

dag = DAG(
    dag_id='airflow_log_cleaner',
    default_args=args,
    schedule_interval='10 0 * * *',
    dagrun_timeout=timedelta(minutes=60),
    tags=['Airflow Cleaner'],
    catchup=False
)

# [START howto_operator_bash]
run_this = BashOperator(
    task_id='clean_airflow_logs',
    bash_command="""find airflow/logs/* -mtime +14 -exec rm {} \; ;
    find /hdd/data01/airflow/logs/* -type d -empty -exec rmdir {} \;; 2> >(grep -v 'No such file or directory' >&2)""",
    dag=dag,
)
# [END howto_operator_bash]

run_this

if __name__ == "__main__":
    dag.cli()
