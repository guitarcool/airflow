from airflow.models import DAG, dag
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pendulum

DAG_ID = 'rerun_dag_1'
BASE_DAG_ID = 'hy_demo'
local_tz = pendulum.timezone("Asia/Shanghai")
START_DATE = '2019-06-20'
END_DATE = '2019-06-26'
RERUN_TASK_IDS = "['core_dds','udm1']"


dag_args = {
    'owner': 'hyrcb',
    'depends_on_past': False,
    'start_date': datetime.strptime(START_DATE, '%Y-%m-%d').astimezone(local_tz),
    'end_date': datetime.strptime(END_DATE, '%Y-%m-%d').astimezone(local_tz),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

main_dag = DAG(
    dag_id=DAG_ID,
    default_args=dag_args,
    max_active_runs=1,
    schedule_interval='00 00 * * *',
    rerun_type=1,
    base_dag_id=BASE_DAG_ID
)


etl_tasks = dag.get_etl_tasks(dag_id=BASE_DAG_ID)
rerun_etl_tasks = [etl_task for etl_task in etl_tasks if etl_task.task_id in eval(RERUN_TASK_IDS)]

task_dict = {}  # type: Dict[str, ETLTask]

for t in rerun_etl_tasks:
    task = PythonOperator(
        task_id=t.task_id.strip(),
        etl_task_type=t.task_type,
        python_callable=t.execute,
        op_kwargs={'etl_date': '{{ds}}'},
        dag=main_dag)
    task_dict[t.task_id] = task

for t in rerun_etl_tasks:
    for dependency in t.dependencies:
        if dependency in eval(RERUN_TASK_IDS):
            task_dict[dependency] >> task_dict[t.task_id]
