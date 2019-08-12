import time
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pendulum

DAG_ID = 'rerun__b'
BASE_DAG_ID = 'None'
START_DATE = '2019-08-12'
END_DATE = '2019-08-12'
RERUN_TASK_IDS = 'None'

local_tz = pendulum.timezone("Asia/Shanghai")


dag_args = {
    'owner': 'hyrcb',
    'depends_on_past': False,
    'start_date': datetime.strptime(START_DATE, '%Y-%m-%d').astimezone(local_tz),
    'end_date': datetime.strptime(END_DATE, '%Y-%m-%d').astimezone(local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'provide_context': True,
}

main_dag = DAG(
    dag_id=DAG_ID,
    default_args=dag_args,
    max_active_runs=1,
    schedule_interval='00 00 * * *',
)


def load(**kwargs):
    print('just test')


etl_tasks = dag.get_etl_tasks(dag_id=BASE_DAG_ID)
rerun_etl_tasks = [etl_task for etl_task in etl_tasks if etl_task.task_id in eval(RERUN_TASK_IDS)]

task_dict = {}  # type: Dict[str, ETLTask]

for t in rerun_etl_tasks:
    task = PythonOperator(
        task_id=t.task_id.strip(),
        python_callable=load,
        provide_context=True,
        dag=main_dag)
    task_dict[t.task_id] = task

for t in rerun_etl_tasks:
    for dependency in t.dependencies:
        if dependency in eval(RERUN_TASK_IDS):
            task_dict[dependency] >> task_dict[t.task_id]
