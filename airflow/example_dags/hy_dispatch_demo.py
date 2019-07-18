from datetime import timedelta
import time
import airflow
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from datetime import datetime
import pendulum

# DAG_NAME = 'hy_demo'
# local_tz = pendulum.timezone("Asia/Shanghai")
#
# dag_args = {
#     'owner': time.strftime('%Y.%m.%d', time.localtime(time.time())),
#     'depends_on_past': False,
#     'start_date': datetime(2019, 5, 29, tzinfo=local_tz),
#     'email': ['airflow@example.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 0,
#     'retry_delay': timedelta(minutes=15),
#     'provide_context': True,
# }
#
# main_dag = DAG(
#     dag_id=DAG_NAME,
#     default_args=dag_args,
#     schedule_interval=None,
# )
#
#
# def print_msg(etl_task, **kwargs):
#     print('task_id:' + etl_task.task_id)
#     print('task_id:' + etl_task.src_path)
#
#
# static_tasks = ['btop', 'card', 'ccpt', 'ccrd', 'core', 'credit']
# dynamic_tasks = main_dag.etl_tasks()
#
# for name in static_tasks:
#     task = PythonOperator(
#         task_id=name.strip(),
#         python_callable=print_msg,
#         provide_context=True,
#         dag=main_dag)
#
#     task
#
# for name in dynamic_tasks:
#     task = PythonOperator(
#         task_id=name.strip(),
#         python_callable=print_msg,
#         provide_context=True,
#         dag=main_dag)
#
#     task
