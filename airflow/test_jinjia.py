import os

import pytz
from jinja2 import Environment, PackageLoader
from datetime import datetime
import pendulum

from airflow import settings

# env = Environment(loader=PackageLoader('airflow.dags'))
# template = env.get_template('rerun_dag.j2')
# content = template.render(dag_name='rerun_dag_1', start_date='2019-06-21', end_date='2019-06-25')
# rerun_dag_file = 'rerun_dag_1.py'
# rerun_dag_path = os.path.join(settings.DAGS_FOLDER, rerun_dag_file)
# print(rerun_dag_path)
# with open(rerun_dag_path, 'w') as fp:
#     fp.write(content)
local_tz = pendulum.timezone("Asia/Shanghai")
# local_tz = pendulum.timezone("American")
execution_date = datetime.strptime('2019-06-21', '%Y-%m-%d').astimezone(pytz.utc)
print(execution_date)


