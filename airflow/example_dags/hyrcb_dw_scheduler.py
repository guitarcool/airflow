import logging
from datetime import timedelta
import time
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime
import pendulum

DAG_NAME = 'hyrcb_dw_scheduler'
local_tz = pendulum.timezone("Asia/Shanghai")
_log = logging.getLogger("airflow.task")

dag_args = {
    'owner': 'hyrcb',
    'depends_on_past': False,
    'start_date': datetime(2019, 9, 6, tzinfo=local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15),
    'provide_context': True,
   }

main_dag = DAG(
    dag_id=DAG_NAME,
    default_args=dag_args,
    schedule_interval='00 05 * * *',
)


def func(etl_task, **kwargs):
    etl_date = kwargs['ds']
    _log.info(etl_date)
    ti = kwargs['ti']
    return etl_task.execute(etl_date, ti)

etl_tasks = main_dag.etl_tasks()

task_dict = {}  # type: Dict[str, ETLTask]

for etl_task in etl_tasks:
    task = PythonOperator(
        task_id=etl_task.task_id.strip(),
        etl_task_type=etl_task.task_type,
        python_callable=func,
        provide_context=True,
        trigger_rule=etl_task.get_trigger_rule(),
        dag=main_dag)

    task_dict[etl_task.task_id] = task

for etl_task in etl_tasks:
    dependencies = etl_task.dependencies
    for dependency in dependencies:
        if dependency in task_dict:
            task_dict[dependency] >> task_dict[etl_task.task_id]
